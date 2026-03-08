#!/usr/bin/env python3
"""Template-driven compositor with per-group preview and progress.

Usage:
  python super_compositor.py [options] ROOT

ROOT:
  Root directory where stitched groups live under renders/<name>/group_XX.

Key features:
- Template-loaded canvas/layers; no hardcoded camera names.
- Liberal file resolution per template key inside each group dir.
- Optional preview (PNG) per group with Y/N confirmation (or -y).
- ffmpeg progress with tqdm if available (fallback to simple percent logs).
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from common_utils import (
    LOGGER,
    ensure_dir,
    setup_logger,
    run_cmd,
    run_ffprobe_json,
    ffprobe_duration_seconds,
    load_template,
    find_camera_files_for_template,
    build_filter_complex,
    render_composite_preview,
    prompt_yes_no,
)

# Optional tqdm, used if available
try:
    from tqdm import tqdm  # type: ignore
except Exception:
    tqdm = None

DEFAULT_TEMPLATE_NAME = "default_template.json"
_AUTO_DISABLE_VIDEOTOOLBOX = False


def _safe_unlink(path: Path) -> None:
    try:
        path.unlink()
    except Exception:
        pass


def _natural_key(s: str) -> List[Any]:
    parts = re.split(r"(\d+)", s)
    out: List[Any] = []
    for p in parts:
        if p.isdigit():
            out.append(int(p))
        else:
            out.append(p.lower())
    return out


def _write_default_template(path: Path) -> None:
    """Write a sane 1920x1080 template that matches stitched filenames.

    The keys below must match the stitched file basenames inside each group dir:
      frontCenter.mp4, rearCenter.mp4, sideLeft.mp4, sideRight_t.mp4, gearGuard.mp4
    """
    tpl = {
        "canvas": {"w": 1920, "h": 1080},
        "layers": [
            {"key": "rearCenter", "w": 1920, "h": 619, "x": 0, "y": 540},
            {"key": "sideLeft", "w": 640, "h": 1080, "x": 0, "y": 0, "transpose": 2, "mirror": True, "stretch_w": 2700, "pan_x": 1056, "auto_crop_y": 0},
            {"key": "sideRight", "w": 640, "h": 1080, "x": 1280, "y": 0, "transpose": 1, "stretch_w": 2392, "pan_x": 405, "auto_crop_y": 0},
            {"key": "frontCenter", "w": 640, "h": 540, "x": 640, "y": 0},
        ],
    }
    with path.open("w", encoding="utf-8") as f:
        json.dump(tpl, f, indent=2)


def _discover_group_dirs(root: Path) -> List[Path]:
    """Find group directories under common layouts:
       - ROOT is already renders/<name> (contains group_*)
       - ROOT is a single group dir (group_XX)
       - ROOT is repo/project root (contains renders/<name>/group_*)
    """
    if not root.exists():
        return []

    # Case 1: ROOT is already a group dir
    if root.is_dir() and root.name.startswith("group_"):
        return [root]

    # Case 2: ROOT directly contains group_* subdirs (i.e., it's renders/<name>)
    direct_groups = sorted([p for p in root.iterdir() if p.is_dir() and p.name.startswith("group_")])
    if direct_groups:
        return direct_groups

    # Case 3: ROOT has a renders/ subtree (repo root or similar)
    search_roots: List[Path] = []
    renders = root / "renders"
    if renders.is_dir():
        search_roots.append(renders)
    else:
        # Last resort: any nested 'renders' dir under ROOT
        search_roots.extend([p for p in root.rglob("renders") if p.is_dir()])

    group_dirs: List[Path] = []
    for base in search_roots:
        for p in sorted(base.rglob("group_*")):
            if p.is_dir():
                group_dirs.append(p)

    return group_dirs


def _ffprobe_shortest_duration(files: List[Path]) -> Optional[float]:
    """Return the minimum duration among the provided files, if known."""
    mins: List[float] = []
    for p in files:
        info = run_ffprobe_json(p)
        dur = ffprobe_duration_seconds(info)
        if dur and dur > 0:
            mins.append(dur)
    if not mins:
        return None
    return min(mins)


def _build_ffmpeg_inputs(group_dir: Path, template: Dict[str, Any], preview_seek: float = 0.0) -> Tuple[List[str], List[Path]]:
    """Build ffmpeg input args (color + cameras/fillers) and return existing file order."""
    cw = int(template["canvas"]["w"])
    ch = int(template["canvas"]["h"])

    # CRITICAL FIX: color background at 30 fps so it cannot drive the graph at 25 fps
    inputs: List[str] = ["-f", "lavfi", "-i", f"color=size={cw}x{ch}:color=black:rate=30"]

    file_order: List[Path] = []
    files = find_camera_files_for_template(group_dir, template)
    layers = template["layers"]
    for layer in layers:
        key = str(layer["key"])
        fp = files.get(key)
        if fp and fp.exists():
            # Per-input queue to avoid sync starvation on macOS
            inputs += ["-thread_queue_size", "2048", "-ss", f"{preview_seek:.3f}", "-i", str(fp)]
            file_order.append(fp)
            continue
        lw = int(layer.get("w", cw))
        lh = int(layer.get("h", ch))
        LOGGER.warning("Missing file for key '%s' in %s; using black filler.", key, group_dir.name)
        inputs += ["-f", "lavfi", "-i", f"color=size={lw}x{lh}:color=black:rate=30"]
    return inputs, file_order


def _run_ffmpeg_with_progress(cmd: List[str], est_seconds: Optional[float]) -> Tuple[int, str]:
    """Run ffmpeg and show progress via tqdm (if available) or log updates.

    Fix: avoid stdout deadlock, parse '-progress pipe:2', break on 'progress=end',
    and always close tqdm so it doesn't hang at 100%.
    """
    # Ensure single -progress, and add -nostats/-hide_banner
    full = cmd[:]
    cleaned: List[str] = []
    i = 0
    while i < len(full):
        if full[i] == "-progress" and i + 1 < len(full):
            i += 2  # drop existing -progress arg pair
            continue
        cleaned.append(full[i])
        i += 1
    full = cleaned
    if "-nostats" not in full:
        full += ["-nostats"]
    if "-hide_banner" not in full:
        full += ["-hide_banner"]
    full += ["-progress", "pipe:2"]

    LOGGER.debug("FFMPEG CMD: %s", " ".join(shlex.quote(c) for c in full))

    # Do NOT pipe stdout — it may block if not consumed; progress comes on stderr.
    proc = subprocess.Popen(
        full,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    bar = None
    last_pct = -1
    start = time.time()
    err_tail: List[str] = []

    try:
        if tqdm is not None and est_seconds and est_seconds > 0:
            total = int(math.ceil(est_seconds))
            bar = tqdm(total=total, unit="s", leave=False)

        assert proc.stderr is not None
        for raw in proc.stderr:
            line = raw.strip()
            if not line or "=" not in line:
                if line:
                    err_tail.append(line)
                    if len(err_tail) > 200:
                        err_tail.pop(0)
                continue
            key, val = line.split("=", 1)

            if key == "out_time_ms":
                try:
                    secs = float(val) / 1_000_000.0
                except Exception:
                    continue
                if bar is not None:
                    current = min(int(secs), bar.total or int(secs))
                    delta = current - (bar.n or 0)
                    if delta > 0:
                        bar.update(delta)
                elif est_seconds and est_seconds > 0:
                    pct = max(0, min(100, int(secs / est_seconds * 100)))
                    if pct != last_pct and pct % 5 == 0:
                        last_pct = pct
                        LOGGER.info("  progress ~%d%% (elapsed %ss)", pct, int(time.time() - start))

            elif key == "progress":
                if val == "end":
                    # Ensure bar shows 100%
                    if bar is not None and bar.total and (bar.n or 0) < bar.total:
                        bar.update(bar.total - (bar.n or 0))
                    break
                if val == "error":
                    break
            else:
                err_tail.append(line)
                if len(err_tail) > 200:
                    err_tail.pop(0)
    finally:
        if bar is not None:
            bar.close()

    # Drain and finalize
    try:
        proc.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            proc.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            pass

    rc = proc.wait() or 0
    return rc, "\n".join(err_tail[-40:])


def _build_encode_cmd(
    *,
    inputs: Sequence[str],
    filter_complex: str,
    final_label: str,
    out_path: Path,
    fps: int,
    encoder: str,
    crf: int,
    preset: str,
) -> List[str]:
    cmd: List[str] = [
        "ffmpeg", "-y",
        *inputs,
        "-filter_complex", filter_complex,
        "-map", final_label,
        "-pix_fmt", "yuv420p",
        "-r", str(fps),
    ]
    if encoder == "videotoolbox":
        cmd += ["-c:v", "h264_videotoolbox", "-allow_sw", "1", "-b:v", "12M"]
    elif encoder == "libx264":
        cmd += ["-c:v", "libx264", "-crf", str(crf), "-preset", preset]
    else:
        raise ValueError(f"Unsupported encoder: {encoder}")

    cmd += ["-movflags", "+faststart", "-shortest", str(out_path)]
    return cmd


def _concat_group_composites(group_dirs: Sequence[Path], final_out: Path) -> bool:
    composites = [g / "composite.mp4" for g in group_dirs if (g / "composite.mp4").exists()]
    if not composites:
        LOGGER.warning("No per-group composite outputs found; skipping final concat.")
        return False
    composites = sorted(composites, key=lambda p: _natural_key(str(p)))
    valid: List[Path] = []
    for p in composites:
        info = run_ffprobe_json(p)
        dur = ffprobe_duration_seconds(info)
        if dur and dur > 0:
            valid.append(p)
            continue
        LOGGER.warning("Skipping invalid composite (ffprobe failed): %s", p)
    if not valid:
        LOGGER.warning("No valid composite.mp4 files available for final concat.")
        return False

    with tempfile.NamedTemporaryFile("w", suffix=".concat.txt", delete=False) as fh:
        list_path = Path(fh.name)
        for p in valid:
            escaped = str(p.resolve()).replace("'", r"'\''")
            fh.write(f"file '{escaped}'\n")

    try:
        final_out.parent.mkdir(parents=True, exist_ok=True)
        cmd_copy = [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
            "-f", "concat", "-safe", "0",
            "-i", str(list_path),
            "-c", "copy",
            str(final_out),
        ]
        rc = subprocess.call(cmd_copy)
        if rc == 0 and final_out.exists() and final_out.stat().st_size > 0:
            LOGGER.info("Final composite written (stream copy): %s", final_out)
            return True

        LOGGER.warning("Final concat copy failed (rc=%s); retrying with re-encode.", rc)
        cmd_reencode = [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
            "-f", "concat", "-safe", "0",
            "-i", str(list_path),
            "-c:v", "libx264", "-crf", "18", "-preset", "veryfast",
            "-an",
            str(final_out),
        ]
        rc2 = subprocess.call(cmd_reencode)
        if rc2 == 0 and final_out.exists() and final_out.stat().st_size > 0:
            LOGGER.info("Final composite written (re-encode): %s", final_out)
            return True
        LOGGER.error("Final concat failed (copy rc=%s, re-encode rc=%s)", rc, rc2)
        _safe_unlink(final_out)
        return False
    finally:
        try:
            list_path.unlink()
        except Exception:
            pass


def composite_group(
    group_dir: Path,
    template: Dict[str, Any],
    out_path: Path,
    fps: int = 30,
    crf: int = 18,
    preset: str = "veryfast",
    preview_time: float = 0.0,
    assume_yes: bool = False,
    encoder: str = "auto",
) -> bool:
    """Composite one group dir into a single MP4 using the template.

    Args:
        group_dir: Directory containing stitched camera files (e.g., frontCenter.mp4).
        template: Loaded template dict (canvas + layers).
        out_path: Destination MP4 path.
        fps: Output fps (kept at 30 to match camera inputs).
        crf: x264 CRF when software encode is used.
        preset: x264 preset when software encode is used.
        preview_time: Seconds to seek for the preview frame.
        assume_yes: If True, skip prompt.
        encoder: "auto", "videotoolbox", or "libx264".

    Returns:
        bool: True if composed, False if skipped or failed.
    """
    global _AUTO_DISABLE_VIDEOTOOLBOX

    # Preview is only needed for interactive acceptance; skip it in -y mode for speed.
    preview_png = group_dir / "preview.png"
    if not assume_yes:
        if not render_composite_preview(group_dir, template, preview_png, timestamp_seconds=preview_time):
            LOGGER.warning("Failed to render preview for %s; skipping.", str(group_dir))
            return False
        LOGGER.info("Preview written: %s", str(preview_png))
        if not prompt_yes_no(f"Accept composite for {group_dir.name}", default_no=True, assume_yes=False):
            LOGGER.info("Skipped by user: %s", str(group_dir))
            return False

    # Inputs for full render (no -frames:v 1)
    inputs, file_order = _build_ffmpeg_inputs(group_dir, template, preview_seek=0.0)

    # Estimate duration (shortest input wins to avoid black tails).
    est = _ffprobe_shortest_duration(file_order)

    # Build filter complex from template and finalize ffmpeg command
    filter_complex, _ = build_filter_complex(template)
    final_label = f"[ov{len(template['layers'])}]"

    if encoder not in {"auto", "videotoolbox", "libx264"}:
        raise ValueError(f"Unsupported encoder: {encoder}")

    encoders: List[str]
    if encoder == "auto":
        encoders = ["libx264"] if _AUTO_DISABLE_VIDEOTOOLBOX else ["videotoolbox", "libx264"]
    else:
        encoders = [encoder]

    for enc in encoders:
        cmd = _build_encode_cmd(
            inputs=inputs,
            filter_complex=filter_complex,
            final_label=final_label,
            out_path=out_path,
            fps=fps,
            encoder=enc,
            crf=crf,
            preset=preset,
        )
        rc, err_tail = _run_ffmpeg_with_progress(cmd, est)
        if rc == 0:
            if enc != encoders[0]:
                LOGGER.info("Composite succeeded with encoder fallback: %s", enc)
            return True
        LOGGER.warning("Composite failed for %s with encoder=%s (rc=%s)", group_dir.name, enc, rc)
        if err_tail:
            LOGGER.warning("ffmpeg stderr tail (%s):\n%s", enc, err_tail)
        if enc == "videotoolbox":
            tail = (err_tail or "").lower()
            if "cannot create compression session" in tail or "error initializing output stream" in tail:
                _AUTO_DISABLE_VIDEOTOOLBOX = True
                LOGGER.warning("Disabling videotoolbox for remaining groups in this run.")
        _safe_unlink(out_path)

    LOGGER.error("Composite failed for %s after trying encoder strategy '%s'.", str(group_dir), encoder)
    _safe_unlink(out_path)
    return False


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Compose stitched groups into a template-defined 1920x1080 video with preview."
    )
    ap.add_argument("root", type=str, help="Root folder containing renders/<name>/group_XX")
    ap.add_argument("--template", type=str, default=DEFAULT_TEMPLATE_NAME, help="Template JSON path")
    ap.add_argument("--log-level", type=str, default="INFO", help="Logging level (debug/info/...)")
    ap.add_argument("--fps", type=int, default=30, help="Output fps")
    ap.add_argument("--crf", type=int, default=18, help="x264 CRF")
    ap.add_argument("--preset", type=str, default="veryfast", help="x264 preset")
    ap.add_argument(
        "--encoder",
        type=str,
        default="auto",
        choices=["auto", "videotoolbox", "libx264"],
        help="Encoder strategy: auto fallback, hardware-only, or software-only.",
    )
    ap.add_argument("-y", "--yes", action="store_true", help="Assume yes on prompts")
    ap.add_argument("--preview-time", type=float, default=0.0, help="Seek (seconds) for preview frame")
    ap.add_argument("-j", "--jobs", type=int, default=1, help="Number of groups to render in parallel")
    ap.add_argument("--final-name", type=str, default="final_composite.mp4", help="Output filename for concatenated final video.")
    ap.add_argument("--no-final", action="store_true", help="Skip concatenating per-group composites into one final output.")
    args = ap.parse_args()

    setup_logger(args.log_level)

    root = Path(args.root).expanduser().resolve()
    if not root.exists():
        LOGGER.error("Root not found: %s", root)
        sys.exit(1)

    # Load or materialize template
    tpl_path = Path(args.template)
    if not tpl_path.exists():
        _write_default_template(tpl_path)
        LOGGER.info("Wrote default template to %s", tpl_path)
    template = load_template(tpl_path)

    # Discover group dirs
    groups = _discover_group_dirs(root)
    if not groups:
        LOGGER.warning("No group folders found under %s", root)
        sys.exit(0)

    LOGGER.info("Compositing %d group(s)…", len(groups))

    # Worker that preserves existing composite_group behavior
    def _worker(gdir: Path) -> Tuple[Path, bool, str]:
        out_path = gdir / "composite.mp4"
        try:
            ok = composite_group(
                gdir,
                template,
                out_path,
                fps=args.fps,
                crf=args.crf,
                preset=args.preset,
                preview_time=args.preview_time,
                assume_yes=args.yes,
                encoder=args.encoder,
            )
            return (gdir, bool(ok), "")
        except FileNotFoundError as e:
            return (gdir, False, f"Skipping {gdir.name}: {e}")
        except Exception as e:
            return (gdir, False, f"Error on {gdir.name}: {e}")

    ok = 0
    if args.jobs <= 1:
        for gdir in groups:
            g, success, msg = _worker(gdir)
            if success:
                ok += 1
            elif msg:
                if "Skipping" in msg:
                    LOGGER.warning(msg)
                else:
                    LOGGER.error(msg)
    else:
        # Parallelize across groups with a thread pool (safe for subprocess ffmpeg)
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as ex:
            for g, success, msg in ex.map(_worker, groups):
                if success:
                    ok += 1
                elif msg:
                    if "Skipping" in msg:
                        LOGGER.warning(msg)
                    else:
                        LOGGER.error(msg)

    LOGGER.info("Composited %d/%d group(s).", ok, len(groups))
    if not args.no_final:
        # Keep ordering deterministic for the final assembly.
        ordered_groups = sorted(groups, key=lambda p: _natural_key(p.name))
        if root.is_dir() and root.name.startswith("group_"):
            final_out = root.parent / args.final_name
        else:
            final_out = root / args.final_name
        _concat_group_composites(ordered_groups, final_out)


if __name__ == "__main__":
    main()
