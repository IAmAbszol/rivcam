#!/usr/bin/env python3
"""Video group viewer — filename-start + ffprobe-duration (stable API for stitcher/compositor)."""

from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
from typing import Dict, List, Optional

from common_utils import (
    Clip,
    Group,
    LOGGER,
    format_range_abs,
    format_range_norm,
    print_groups_table,
    setup_logger,
    build_clip_filename_start_ffprobe_len,  # <<< use filename start + ffprobe duration
    normalize_camera_id,
)

SUPPORTED_EXT = {".mp4", ".mov", ".m4v"}


def _camera_from_name(path: Path) -> str:
    name = path.stem
    if "_video_" in name:
        return normalize_camera_id(name.split("_video_", 1)[1])
    return normalize_camera_id("camera")


def _filename_to_utc(path: Path) -> Optional[dt.datetime]:
    stem = path.stem
    try:
        y = int("20" + stem[6:8])
        mo = int(stem[0:2])
        d = int(stem[3:5])
        hh = int(stem[9:11])
        mm = int(stem[11:13])
        ss = int(stem[13:15])
        return dt.datetime(y, mo, d, hh, mm, ss, tzinfo=dt.timezone.utc)
    except Exception:
        return None


def _build_clip(path: Path) -> Optional[Clip]:
    """Build a Clip using filename start + ffprobe duration (no normalization)."""
    cam = _camera_from_name(path)
    return build_clip_filename_start_ffprobe_len(path, camera=cam)


def get_groups(root: str | Path, tolerance: int = 60) -> List[Group]:
    root = Path(root)
    files = [p for p in root.rglob("*") if p.suffix.lower() in SUPPORTED_EXT and not p.name.startswith("._")]
    files.sort()

    by_dir: Dict[Path, List[Clip]] = {}
    for p in files:
        c = _build_clip(p)
        if c:
            by_dir.setdefault(p.parent, []).append(c)

    groups: List[Group] = []
    gid = 1
    for folder, clips in sorted(by_dir.items()):
        clips.sort(key=lambda c: c.start_utc)
        current: List[Clip] = []
        win_start: Optional[dt.datetime] = None
        win_end: Optional[dt.datetime] = None

        for c in clips:
            if not current:
                current = [c]
                win_start, win_end = c.start_utc, c.end_utc
                continue
            delta = (c.start_utc - win_end).total_seconds()
            if delta <= tolerance:
                current.append(c)
                if c.end_utc > win_end:
                    win_end = c.end_utc
            else:
                groups.append(Group(group=gid, folder=folder, start_utc=win_start, end_utc=win_end, clips=tuple(current)))
                gid += 1
                current = [c]
                win_start, win_end = c.start_utc, c.end_utc

        if current:
            groups.append(Group(group=gid, folder=folder, start_utc=win_start, end_utc=win_end, clips=tuple(current)))
            gid += 1

    return groups


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Group dashcam clips per folder using filename start + ffprobe duration; gap tolerance controls group splits."
    )
    ap.add_argument("root", type=str, help="Root directory to scan recursively for videos.")
    ap.add_argument("--tolerance", type=int, default=60, help="Gap tolerance (seconds) to keep clips in the same group.")
    ap.add_argument("--log-level", type=str, default="INFO", help="Logging level (debug/info/warning/error).")
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    setup_logger(args.log_level)

    root = Path(args.root).expanduser().resolve()
    groups = get_groups(root, tolerance=args.tolerance)
    print_groups_table(root, groups)


if __name__ == "__main__":
    main()
