# test_stitching.py
#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from rivcam.builders import build_clip, build_groups
from rivcam.parsers import Version
from rivcam.stitch import stitch_groups
from rivcam.utils.logging import setup_logger
from rivcam.utils.paths import list_videos


def main() -> None:
    ap = argparse.ArgumentParser(description="Stitch Rivian dashcam groups (V1, exact & overlap-safe).")
    ap.add_argument("root", type=Path, help="Root directory to scan for videos (recursively).")
    ap.add_argument("--renders", type=Path, default=Path("renders"), help="Output base directory (default: ./renders).")
    ap.add_argument("--gap", type=float, default=60.0, help="Gap tolerance in seconds for grouping (default: 60).")
    ap.add_argument("--log-level", type=str, default="INFO", help="Logging level (INFO, DEBUG, WARNING, ...).")
    ap.add_argument("--version", type=str, default="V1", choices=["V1"], help="Parser version (default: V1).")
    ap.add_argument("--exact", action="store_true", help="Force exact overlap-safe trimming (default).")
    ap.add_argument("--no-exact", dest="exact", action="store_false", help="Disable exact trimming (try fast-path when possible).")
    ap.add_argument("--dev", action="store_true", help="Use OpenCV-based dev stitcher instead of ffmpeg.")
    ap.set_defaults(exact=True)
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

    # Outputs under: <renders>/<root-name>/<group.name>/<camera>.mp4
    out_base = args.renders / root.name
    out_base.mkdir(parents=True, exist_ok=True)

    if args.dev:
        print(f"[DEV] Stitching {len(groups)} group(s) to {out_base} using OpenCV ...")
        stitch_groups(out_base, groups, exact=args.exact, dev=True)
    else:
        print(f"Stitching {len(groups)} group(s) to {out_base} (exact={args.exact}) ...")
        stitch_groups(out_base, groups, exact=args.exact)

    print("Done.")


if __name__ == "__main__":
    main()