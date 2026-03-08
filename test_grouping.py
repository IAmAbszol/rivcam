#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from rivcam.builders import build_clip, build_groups
from rivcam.parsers import Version
from rivcam.utils.logging import setup_logger
from rivcam.utils.paths import list_videos


def _fmt_duration(seconds: float) -> str:
    total = int(round(seconds))
    mm, ss = divmod(total, 60)
    hh, mm = divmod(mm, 60)
    return f"{hh:02d}:{mm:02d}:{ss:02d}"

def main() -> None:
    ap = argparse.ArgumentParser(description="Group Rivian dashcam clips (V1).")
    ap.add_argument("root", type=Path, help="Root directory to scan for videos (recursively).")
    ap.add_argument("--gap", type=float, default=60.0, help="Gap tolerance in seconds (default: 60).")
    ap.add_argument("--log-level", type=str, default="INFO", help="Logging level (INFO, DEBUG, WARNING, ...).")
    ap.add_argument("--version", type=str, default="V1", choices=["V1"], help="Parser version (default: V1).")
    args = ap.parse_args()

    setup_logger(args.log_level)

    root = args.root.resolve()
    vids = list_videos(root)

    clips = []
    for p in vids:
        c = build_clip(p, version=Version[args.version])
        if c is not None:
            clips.append(c)

    groups = build_groups(clips, version=Version[args.version], gap_tolerance_s=args.gap)

    print(f"Found {len(groups)} group(s) under {root}\n")
    for g in groups:
        cams = ", ".join(sorted(g.cameras())) or "(none)"
        approx_len = _fmt_duration(g.approximate_length())
        print(f"=== {g.name} ===")
        print(f"  folder: {getattr(g, 'folder', 'n/a')}")
        print(f"  approx length: {approx_len}")
        print(f"  cameras: {cams}")
        print(f"  clips: {len(g.clips)}\n")


if __name__ == "__main__":
    main()
