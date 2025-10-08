#!/usr/bin/env python3
"""
Video stitch processor — trims true overlaps (filename-start timeline) and losslessly stitches per camera.

This script:
  1) Loads time-based groups from `video_group_processor.get_groups(...)`.
  2) Prints the same summary table the grouper uses (via common_utils.print_groups_table).
  3) Prompts to continue (or auto-continues with -y/--yes).
  4) For each group, stitches each camera by trimming inter-clip overlaps using:
       - START from filename timestamp (fallback to ffprobe creation_time only if filename ts missing)
       - DURATION from ffprobe
     Then concatenates with lossless stream copy.

Outputs:
  renders/<ROOT_DIR_NAME>/group_XX/<camera>.mp4
  (Temporary segments: renders/<ROOT_DIR_NAME>/group_XX/_<camera>_segs/)

Usage:
  python video_stitch_processor.py <root_dir> [-y] [--keep-tmp] [--tolerance 0.0] [--out renders] [--log-level INFO]
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Any

import video_group_processor as vgp
from common_utils import (
    Clip,
    Group,
    LOGGER,
    ensure_dir,
    ffprobe_creation_time,
    ffprobe_duration_seconds,
    parse_filename_timestamp,
    print_groups_table,
    run_ffprobe_json,
    setup_logger,
)

# ------------------------- constants / tolerances --------------------------- #

EPS: float = 1e-3       # seconds; epsilon for float-safe comparisons
MIN_KEEP: float = 0.010 # seconds; ensure we don't zero-out tiny tails

# ------------------------- ffprobe timestamp helper ------------------------- #

def _best_creation_datetime(ffj: Dict[str, Any]) -> Optional[dt.datetime]:
    """
    Prefer sub-second accurate creation timestamps if present:
      1) format.tags.com.apple.quicktime.creationdate
      2) format.tags.creation_time
      3) streams[*].tags.creation_time
      4) fallback to common_utils.ffprobe_creation_time
    """
    def _parse_iso_z(s: str) -> Optional[dt.datetime]:
        if not isinstance(s, str) or not s:
            return None
        s2 = s
        if s2.endswith("Z"):
            s2 = s2[:-1] + "+00:00"
        if len(s2) >= 5 and (s2[-5] in ["+", "-"]) and s2[-3] != ":":
            s2 = s2[:-2] + ":" + s2[-2:]
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
                    "%Y-%m-%d %H:%M:%S.%f%z", "%Y-%m-%d %H:%M:%S%z"):
            try:
                return dt.datetime.strptime(s2, fmt)
            except Exception:
                pass
        return None

    fmt = ffj.get("format") or {}
    tags = fmt.get("tags") or {}
    for k in ("com.apple.quicktime.creationdate", "creation_time"):
        dv = _parse_iso_z(tags.get(k))
        if dv:
            return dv

    for st in ffj.get("streams") or []:
        dv = _parse_iso_z((st.get("tags") or {}).get("creation_time"))
        if dv:
            return dv

    # fallback to your existing helper
    ct = ffprobe_creation_time(ffj)
    if ct and ct.tzinfo is None:
        ct = ct.replace(tzinfo=dt.timezone.utc)
    return ct

# ------------------------- subprocess / ffmpeg wrappers --------------------- #

def _run(cmd: Sequence[str]) -> int:
    LOGGER.debug("RUN: %s", " ".join(shlex.quote(x) for x in cmd))
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.rstrip("\n")
            if line:
                LOGGER.debug("[ffmpeg] %s", line)
        return proc.wait()
    except FileNotFoundError:
        LOGGER.error("Command not found: %s", cmd[0])
        return 127

def _ffmpeg_trim_copy(src: Path, ss: float, dur: Optional[float], out_path: Path) -> int:
    """
    Precise copy-trim with timestamp reset so concat won't truncate tails.

    Key points:
    - Use -ss AFTER -i (more accurate cuts for MP4) while still -c copy.
    - DO NOT use -copyts/+genpts (can cause end truncation when concatenating).
    - Reset timestamps per segment.
    """
    cmd = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
        "-i", str(src),
        "-ss", f"{ss:.6f}",
        "-avoid_negative_ts", "make_zero",
        "-reset_timestamps", "1",
        "-map", "0", "-c", "copy",
    ]
    if dur is not None and dur > 0:
        cmd += ["-t", f"{dur:.6f}"]
    cmd.append(str(out_path))
    return _run(cmd)

def _ffmpeg_concat_copy(list_file: Path, out_path: Path) -> int:
    cmd = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
        "-f", "concat", "-safe", "0",
        "-i", str(list_file),
        "-c", "copy",
        str(out_path),
    ]
    return _run(cmd)

# ------------------------- overlap math (filename-start) -------------------- #

@dataclass(frozen=True)
class _Probe:
    path: Path
    start_s: float   # absolute UTC epoch seconds (from FILENAME; fallback ffprobe)
    dur_s: float     # real media duration (from ffprobe)
    end_s: float     # start_s + dur_s

def _probe_clip_real(c: Clip) -> Optional[_Probe]:
    """
    Build a real-timing probe for overlap trimming:
      - START from filename timestamp (source-of-truth).
      - If filename parse fails, fallback to best ffprobe creation time.
      - DURATION from ffprobe real duration.

    We purposely do NOT use the "normalized" common_utils duration here.
    """
    # Prefer filename timestamp
    start_dt = parse_filename_timestamp(c.path)

    info = run_ffprobe_json(c.path)
    if not info:
        LOGGER.warning("ffprobe failed for %s", c.path)
        return None

    if start_dt is None:
        start_dt = _best_creation_datetime(info)

    dur = ffprobe_duration_seconds(info)

    if not start_dt or not dur or dur <= 0:
        LOGGER.warning("Missing/invalid meta for %s", c.path)
        return None

    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=dt.timezone.utc)

    start_s = start_dt.timestamp()
    dur_s = float(dur)
    return _Probe(path=c.path, start_s=start_s, dur_s=dur_s, end_s=start_s + dur_s)

def _build_trim_spans(clips: List[Clip], tolerance_s: float) -> List[Tuple[Path, float, float]]:
    """
    Produce trim spans for a single camera, removing overlaps on the filename-start timeline.

    For each clip i (sorted by real start):
      overlap = prev_end - start_i
      if overlap > tolerance_s (+ EPS): trim head by (overlap - tolerance_s)
      keep = max(dur - ss, MIN_KEEP)
    Gaps are preserved.

    Returns list of (path, ss, dur).
    """
    if not clips:
        return []

    probes: List[_Probe] = []
    for c in clips:
        p = _probe_clip_real(c)
        if p:
            probes.append(p)
    if not probes:
        return []

    probes.sort(key=lambda p: p.start_s)

    spans: List[Tuple[Path, float, float]] = []
    prev_end = probes[0].start_s  # baseline for first comparison

    for p in probes:
        ss = 0.0
        raw_overlap = prev_end - p.start_s
        if raw_overlap > tolerance_s + EPS:
            ss = raw_overlap - tolerance_s

        # float-safe skip logic
        if ss > p.dur_s - EPS:
            # Nothing meaningful left; skip this file
            prev_end = max(prev_end, p.end_s)
            continue

        keep = max(p.dur_s - ss, MIN_KEEP)
        ss = round(ss, 6)
        keep = round(keep, 6)

        spans.append((p.path, ss, keep))
        prev_end = max(prev_end, p.start_s + ss + keep)

    return spans

# ------------------------- camera/group plumbing ---------------------------- #

def _bucket_by_camera(clips: List[Clip]) -> Dict[str, List[Clip]]:
    """
    Build per-camera buckets from a group.clips list by inspecting filenames.
    This is robust even if Group doesn't carry a by_camera dict.
    """
    key_re = re.compile(r"_video_(rearCenter|sideLeft|sideRight|frontCenter|gearGuard)", re.IGNORECASE)
    buckets: Dict[str, List[Clip]] = {}
    for c in clips:
        m = key_re.search(c.path.stem)
        cam = m.group(1) if m else "camera"
        buckets.setdefault(cam, []).append(c)
    for v in buckets.values():
        v.sort(key=lambda x: (x.start_utc, x.path.name))
    return buckets

def _write_concat_file(paths: List[Path], dest: Path) -> None:
    with dest.open("w", encoding="utf-8") as f:
        for p in paths:
            f.write(f"file '{p.as_posix()}'\n")

def _stitch_camera(out_dir: Path, cam: str, clips: List[Clip], keep_tmp: bool, tolerance_s: float) -> bool:
    """Stitch a single camera inside a group dir."""
    if not clips:
        LOGGER.info("No clips for %s; skipping.", cam)
        return True

    ensure_dir(out_dir)
    tmp_dir = out_dir / f"_{cam}_segs"
    ensure_dir(tmp_dir)

    spans = _build_trim_spans(clips, tolerance_s=tolerance_s)
    if not spans:
        LOGGER.warning("No spans produced for %s", cam)
        return False

    seg_paths: List[Path] = []
    try:
        for i, (src, ss, dur) in enumerate(spans, 1):
            seg = tmp_dir / f"{i:03d}.mp4"
            rc = _ffmpeg_trim_copy(src, ss, dur, seg)
            if rc != 0 or not seg.exists() or seg.stat().st_size == 0:
                LOGGER.error("ffmpeg trim failed: %s (ss=%.3f, dur=%.3f)", src, ss, dur)
                return False
            seg_paths.append(seg)

        if not seg_paths:
            LOGGER.warning("No segments for %s", cam)
            return False

        concat_txt = tmp_dir / "concat.txt"
        _write_concat_file(seg_paths, concat_txt)
        out_path = out_dir / f"{cam}.mp4"
        rc = _ffmpeg_concat_copy(concat_txt, out_path)
        if rc != 0 or not out_path.exists() or out_path.stat().st_size == 0:
            LOGGER.error("ffmpeg concat failed: %s", out_path)
            return False

        # Optionally clean tmp
        if not keep_tmp:
            for p in seg_paths:
                try:
                    p.unlink()
                except Exception:
                    pass
            try:
                concat_txt.unlink()
            except Exception:
                pass
            try:
                tmp_dir.rmdir()
            except Exception:
                pass

        LOGGER.info("Stitched %s → %s", cam, out_path)
        return True
    finally:
        # leave tmp if requested; nothing else to do here
        pass

# ------------------------- public API ------------------------- #

def stitch_group(out_base: Path, group: Group, keep_tmp: bool = False, tolerance_s: float = 0.0) -> bool:
    """Stitch all cameras for a group.

    Args:
      out_base: Base output directory (e.g., renders/<root_name>).
      group:    Group object.
      keep_tmp: Keep per-camera temporary segments directory.
      tolerance_s: Overlap tolerance in seconds. Overlap ≤ tolerance is kept (no trim).
    """
    # Stable group naming, fallback if index missing
    group_idx = getattr(group, "index", None)
    group_name = f"group_{int(group_idx):02d}" if isinstance(group_idx, int) else f"group_{int(getattr(group, 'group', 0)):02d}"
    out_dir = out_base / group_name
    ensure_dir(out_dir)

    ok_all = True
    # Robust per-camera bucketing (works whether or not Group carries a dict)
    by_cam = _bucket_by_camera(list(group.clips))
    for cam, clips in by_cam.items():
        ok = _stitch_camera(out_dir, cam, clips, keep_tmp=keep_tmp, tolerance_s=tolerance_s)
        ok_all = ok_all and ok
    return ok_all

# ------------------------- CLI ------------------------- #

def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Trim overlaps (filename-start) and stitch per camera.")
    ap.add_argument("root", type=str, help="Root directory passed to video_group_processor.")
    ap.add_argument("-y", "--yes", action="store_true", help="Auto-continue without prompt.")
    ap.add_argument("--keep-tmp", action="store_true", help="Keep intermediate trimmed segments.")
    ap.add_argument("--tolerance", type=float, default=0.0, help="Seconds of tolerated overlap (default 0).")
    ap.add_argument("--out", type=str, default="renders", help="Output base folder (default: renders).")
    ap.add_argument("--log-level", type=str, default="INFO", help="Logging level.")
    return ap.parse_args()

def main() -> None:
    args = _parse_args()
    setup_logger(args.log_level)

    root = Path(args.root).expanduser().resolve()
    groups = vgp.get_groups(root)  # grouping stays exactly as-is
    print_groups_table(root, groups)

    if not args.yes:
        try:
            ans = input("Proceed to stitch? [y/N]: ").strip().lower()
        except EOFError:
            ans = "n"
        if ans not in {"y", "yes"}:
            LOGGER.info("Aborted by user.")
            sys.exit(0)

    out_base = Path(args.out).expanduser().resolve() / root.name
    ensure_dir(out_base)

    ok = 0
    for g in groups:
        if stitch_group(out_base, g, keep_tmp=args.keep_tmp, tolerance_s=args.tolerance):
            ok += 1

    LOGGER.info("Stitched %d/%d group(s).", ok, len(groups))

if __name__ == "__main__":
    main()