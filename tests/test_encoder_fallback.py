from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "scripts"))
import super_compositor as sc  # noqa: E402


def test_composite_group_auto_encoder_fallback(monkeypatch, tmp_path: Path) -> None:
    group_dir = tmp_path / "group_01"
    group_dir.mkdir(parents=True, exist_ok=True)
    out_path = group_dir / "composite.mp4"

    template = {
        "canvas": {"w": 1920, "h": 1080},
        "layers": [{"key": "frontCenter", "w": 640, "h": 540, "x": 0, "y": 0}],
    }

    monkeypatch.setattr(sc, "render_composite_preview", lambda *args, **kwargs: True)
    monkeypatch.setattr(sc, "_build_ffmpeg_inputs", lambda *args, **kwargs: (["-f", "lavfi", "-i", "color=size=1920x1080:color=black:rate=30"], []))
    monkeypatch.setattr(sc, "_ffprobe_shortest_duration", lambda _files: 1.0)
    monkeypatch.setattr(sc, "build_filter_complex", lambda _t: ("dummy_fc", []))

    calls = []

    def fake_build(*, inputs, filter_complex, final_label, out_path, fps, encoder, crf, preset):
        calls.append(("build", encoder))
        return [encoder]

    monkeypatch.setattr(sc, "_build_encode_cmd", fake_build)

    def fake_run(cmd, est_seconds):
        if cmd == ["videotoolbox"]:
            return 1, "videotoolbox failed"
        out_path.write_text("ok", encoding="utf-8")
        return 0, ""

    monkeypatch.setattr(sc, "_run_ffmpeg_with_progress", fake_run)

    ok = sc.composite_group(
        group_dir,
        template,
        out_path,
        fps=30,
        crf=18,
        preset="veryfast",
        preview_time=0.0,
        assume_yes=True,
        encoder="auto",
    )

    assert ok is True
    assert calls == [("build", "videotoolbox"), ("build", "libx264")]


def test_composite_group_disables_videotoolbox_after_hard_failure(monkeypatch, tmp_path: Path) -> None:
    group_dir = tmp_path / "group_01"
    group_dir.mkdir(parents=True, exist_ok=True)
    out_path = group_dir / "composite.mp4"

    template = {
        "canvas": {"w": 1920, "h": 1080},
        "layers": [{"key": "frontCenter", "w": 640, "h": 540, "x": 0, "y": 0}],
    }

    monkeypatch.setattr(sc, "_build_ffmpeg_inputs", lambda *args, **kwargs: (["-f", "lavfi", "-i", "color=size=1920x1080:color=black:rate=30"], []))
    monkeypatch.setattr(sc, "_ffprobe_shortest_duration", lambda _files: 1.0)
    monkeypatch.setattr(sc, "build_filter_complex", lambda _t: ("dummy_fc", []))

    calls = []

    def fake_build(*, inputs, filter_complex, final_label, out_path, fps, encoder, crf, preset):
        calls.append(encoder)
        return [encoder]

    monkeypatch.setattr(sc, "_build_encode_cmd", fake_build)

    def fake_run(cmd, _est_seconds):
        if cmd == ["videotoolbox"]:
            return 1, "Error: cannot create compression session: -12908"
        out_path.write_text("ok", encoding="utf-8")
        return 0, ""

    monkeypatch.setattr(sc, "_run_ffmpeg_with_progress", fake_run)

    sc._AUTO_DISABLE_VIDEOTOOLBOX = False
    ok1 = sc.composite_group(group_dir, template, out_path, assume_yes=True, encoder="auto")
    ok2 = sc.composite_group(group_dir, template, out_path, assume_yes=True, encoder="auto")

    assert ok1 is True
    assert ok2 is True
    assert calls == ["videotoolbox", "libx264", "libx264"]
