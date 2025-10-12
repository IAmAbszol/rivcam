
from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import ffmpeg
from ffmpeg_progress_yield import FfmpegProgress


def _resolve_ffmpeg_bin() -> str:
    env = os.environ.get("FFMPEG_BIN")
    if env:
        return env
    found = shutil.which("ffmpeg")
    if not found:
        raise RuntimeError("ffmpeg not found in PATH and FFMPEG_BIN is not set.")
    return found


def run_simple(argv: Sequence[str], check: bool = False) -> int:
    cmd = list(argv)
    cmd[0] = _resolve_ffmpeg_bin()
    cp = subprocess.run(cmd)
    if check and cp.returncode != 0:
        raise subprocess.CalledProcessError(cp.returncode, cmd)
    return cp.returncode


def run_with_progress(argv: Sequence[str], on_progress: Optional[Callable[[float], None]] = None) -> int:
    cmd = list(argv)
    cmd[0] = _resolve_ffmpeg_bin()
    with FfmpegProgress(cmd) as prog:
        for pct in prog.run_command_with_progress():
            if on_progress:
                on_progress(pct)
    return 0


def compile_graph(node: ffmpeg.nodes.Node, global_args: Optional[Sequence[str]] = None) -> List[str]:
    argv = node.compile()
    if global_args:
        argv = [argv[0], *global_args, *argv[1:]]
    return argv


def probe(path: Path) -> Dict:
    return ffmpeg.probe(str(path))


def probe_video_stream(path: Path) -> Dict:
    pr = probe(path)
    for s in pr.get("streams", []):
        if s.get("codec_type") == "video":
            return s
    return {}


def probe_duration_seconds(path: Path) -> float:
    pr = probe(path)
    for s in pr.get("streams", []):
        if s.get("codec_type") == "video" and "duration" in s:
            return float(s["duration"])
    fmt = pr.get("format", {})
    if "duration" in fmt:
        return float(fmt["duration"])
    return 0.0


def probe_fps(path: Path) -> Optional[float]:
    s = probe_video_stream(path)
    fr = s.get("r_frame_rate") or s.get("avg_frame_rate")
    if not fr or fr == "0/0":
        return None
    try:
        num, den = fr.split("/")
        num = float(num); den = float(den)
        if den == 0:
            return None
        return num / den
    except Exception:
        return None


def frame_duration_sec(path: Path, fallback_fps: float = 30.0) -> float:
    fps = probe_fps(path) or fallback_fps
    return 1.0 / max(1.0, fps)


def probe_params_signature(path: Path) -> Tuple:
    s = probe_video_stream(path)
    return (
        s.get("codec_name"),
        s.get("pix_fmt"),
        s.get("width"),
        s.get("height"),
        s.get("r_frame_rate"),
        s.get("avg_frame_rate"),
        s.get("time_base"),
        s.get("color_space"),
        s.get("color_transfer"),
        s.get("color_primaries"),
        s.get("profile"),
        s.get("level"),
    )


def streams_compatible(paths: Iterable[Path]) -> bool:
    it = iter(paths)
    try:
        first = next(it)
    except StopIteration:
        return True
    sig0 = probe_params_signature(first)
    for p in it:
        if probe_params_signature(p) != sig0:
            return False
    return True


@dataclass(frozen=True)
class Segment:
    path: Path
    start_sec: float
    dur_sec: float


@dataclass(frozen=True)
class ConcatEntry:
    path: Path
    inpoint: Optional[float] = None
    outpoint: Optional[float] = None


def _format_float(f: float) -> str:
    s = f"{f:.6f}"
    return s.rstrip("0").rstrip(".") if "." in s else s


def _escape_concat_path(p: str) -> str:
    return p.replace("'", "'\\''")


def concat_copy_demuxer(entries: Sequence[ConcatEntry], out_file: Path, on_progress: Optional[Callable[[float], None]] = None) -> int:
    if not entries:
        return 0
    if not streams_compatible(e.path for e in entries):
        return 2

    tmp = None
    try:
        with tempfile.NamedTemporaryFile("w", suffix=".concat.txt", delete=False) as fh:
            tmp = Path(fh.name)
            for e in entries:
                escaped = _escape_concat_path(str(e.path))
                fh.write(f"file '{escaped}'\n")
                if e.inpoint is not None:
                    fh.write(f"inpoint {_format_float(e.inpoint)}\n")
                if e.outpoint is not None:
                    fh.write(f"outpoint {_format_float(e.outpoint)}\n")

        argv = [
            _resolve_ffmpeg_bin(),
            "-hide_banner",
            "-y",
            "-safe", "0",
            "-f", "concat",
            "-i", str(tmp),
            "-map", "0:v",
            "-c", "copy",
            str(out_file),
        ]
        with FfmpegProgress(argv) as prog:
            for pct in prog.run_command_with_progress():
                if on_progress:
                    on_progress(pct)
        return 0
    except Exception:
        return 1
    finally:
        if tmp and tmp.exists():
            try:
                tmp.unlink()
            except Exception:
                pass


def trim_and_concat_encode(
    segments: Sequence[Segment],
    out_file: Path,
    *,
    vcodec: str = "libx264",
    crf: int = 18,
    preset: str = "fast",
    fps: Optional[float] = None,
    faststart: bool = True,
    progress_cb: Optional[Callable[[float], None]] = None,
) -> int:
    if not segments:
        return 0
    ins = [ffmpeg.input(str(s.path)) for s in segments]
    trimmed = []
    for inp, seg in zip(ins, segments):
        v = inp.video.filter("trim", start=seg.start_sec, duration=seg.dur_sec).filter("setpts", "PTS-STARTPTS")
        trimmed.append(v)
    node = ffmpeg.concat(*trimmed, v=1, a=0, n=len(trimmed))
    out_kwargs: Dict[str, object] = dict(vcodec=vcodec, preset=preset, crf=crf)
    if fps:
        out_kwargs["r"] = fps
    if faststart:
        out_kwargs["movflags"] = "+faststart"
    out = ffmpeg.output(node, str(out_file), **out_kwargs)
    argv = compile_graph(out, global_args=["-hide_banner", "-y"])
    return run_with_progress(argv, on_progress=progress_cb)
