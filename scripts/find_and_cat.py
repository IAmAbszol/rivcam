#!/usr/bin/env python3
"""
find_and_cat.py — Find files by basename regex and concatenate them with ffmpeg.

Usage:
  python find_and_cat.py "<regex>" <directory> [--out output.mp4] [--reencode] [--dry-run]

Examples:
  python find_and_cat.py '^composite\\.mp4$' renders/OffRoading --out comp_stitch/final.mp4
  python find_and_cat.py '^[0-9]{3}\\.mp4$' comp_stitch --out final.mp4
"""

from __future__ import annotations
import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

def natural_key(s: str):
    # Simple natural sort key: splits digits so 2 < 10, 010 < 100
    import itertools
    parts = re.split(r'(\d+)', s)
    return [int(p) if p.isdigit() else p.lower() for p in parts]

def escape_concat_path(p: Path) -> str:
    # ffmpeg concat requires single-quoted path; escape embedded single quotes
    s = str(p.resolve())
    return s.replace("'", r"'\''")

def build_match_list(regex: re.Pattern, root: Path) -> List[Path]:
    hits: List[Path] = []
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            if regex.search(name):
                hits.append(Path(dirpath) / name)
    hits.sort(key=lambda p: natural_key(str(p)))
    return hits

def write_concat_list(paths: List[Path], list_path: Path) -> None:
    with list_path.open('w', encoding='utf-8') as f:
        for p in paths:
            f.write(f"file '{escape_concat_path(p)}'\n")

def run_ffmpeg(list_path: Path, out_path: Path, reencode: bool) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not reencode:
        cmd = [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
            "-f", "concat", "-safe", "0",
            "-i", str(list_path),
            "-c", "copy",
            str(out_path),
        ]
    else:
        cmd = [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
            "-f", "concat", "-safe", "0",
            "-i", str(list_path),
            "-c:v", "libx264", "-crf", "18", "-preset", "veryfast",
            "-c:a", "aac", "-b:a", "192k",
            str(out_path),
        ]
    print("Running:", " ".join(cmd))
    return subprocess.call(cmd)

def main():
    ap = argparse.ArgumentParser(description="Find by basename regex and concatenate with ffmpeg.")
    ap.add_argument("regex", type=str, help="Python regular expression to match basenames (e.g. '^composite\\.mp4$').")
    ap.add_argument("directory", type=str, help="Directory to search recursively.")
    ap.add_argument("--out", type=str, default="final_concat.mp4", help="Output MP4 path.")
    ap.add_argument("--reencode", action="store_true", help="Re-encode instead of stream copy (robust but slower).")
    ap.add_argument("--dry-run", action="store_true", help="List matches and exit.")
    args = ap.parse_args()

    root = Path(args.directory).expanduser().resolve()
    if not root.is_dir():
        print(f"Directory not found: {root}", file=sys.stderr)
        sys.exit(1)

    try:
        pattern = re.compile(args.regex)
    except re.error as e:
        print(f"Invalid regex: {e}", file=sys.stderr)
        sys.exit(1)

    matches = build_match_list(pattern, root)
    if not matches:
        print(f"No files matched regex '{args.regex}' under '{root}'.", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(matches)} file(s):")
    for p in matches:
        print("  ", p)

    if args.dry_run:
        sys.exit(0)

    out_path = Path(args.out).expanduser().resolve()
    list_path = out_path.with_suffix(".concat.txt")
    write_concat_list(matches, list_path)
    print("Wrote concat list:", list_path)

    rc = run_ffmpeg(list_path, out_path, args.reencode)
    if rc == 0:
        print("Done:", out_path)
    else:
        print("ffmpeg returned non-zero status:", rc, file=sys.stderr)
    sys.exit(rc)

if __name__ == "__main__":
    main()
