#!/usr/bin/env python3
# route_processor.py — Python 3.11 compatible
#
# Outputs per run (under CWD):
#   ./gps_slices/<ROOT_NAME>/group_##/route_slice.gpx
#   ./gps_slices/<ROOT_NAME>/group_##/route_slice.csv
#   ./gps_slices/<ROOT_NAME>/group_##/summary.txt
#
# Notes:
# - <ROOT_NAME> is the basename of --root (e.g., "OffRoading").
# - We try importing utilities from video_group_processor/group_viewer; if missing,
#   robust fallbacks are used so this script always runs.

import argparse
import datetime as dt
import os
import re
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

# ============= Optional imports from your project =============================
_FMT_TS = None
_FMT_RANGE = None
_BUILD_GROUPS = None
_COLLECT = None

def _try_import_helpers() -> None:
    """Try to resolve helpers from your project. Leave fallbacks in place if not found."""
    global _FMT_TS, _FMT_RANGE, _BUILD_GROUPS, _COLLECT
    # Try video_group_processor first
    for modname in ("video_group_processor", "group_viewer", "common_utils"):
        try:
            mod = __import__(modname, fromlist=["*"])
        except Exception:
            continue
        # names may or may not exist; only bind if present
        _FMT_TS = getattr(mod, "format_ts", _FMT_TS)
        _FMT_RANGE = getattr(mod, "format_normalized_range", _FMT_RANGE)
        _BUILD_GROUPS = getattr(mod, "build_overlap_groups", _BUILD_GROUPS)
        _COLLECT = getattr(mod, "collect_clips_by_directory", _COLLECT)

_try_import_helpers()

# ============= Fallback implementations (used if your project doesn't export) =

@dataclass(order=True)
class _Clip:
    start: dt.datetime   # naive local
    end: dt.datetime     # naive local
    path: str

# Match names like 10_04_25_142740_video_frontCenter.mp4
#   MM_DD_YY_HHMMSS_.+
_TS_RE = re.compile(r'(?P<mo>\d{2})_(?P<da>\d{2})_(?P<yy>\d{2})_(?P<hh>\d{2})(?P<mi>\d{2})(?P<ss>\d{2})_')

def _fallback_parse_filename_ts(p: Path, tz: dt.tzinfo, assumed_duration_s: int) -> _Clip:
    m = _TS_RE.search(p.name)
    if not m:
        raise ValueError(f"Cannot parse timestamp from filename: {p.name}")
    mo = int(m.group("mo"))
    da = int(m.group("da"))
    yy = int(m.group("yy"))
    hh = int(m.group("hh"))
    mi = int(m.group("mi"))
    ss = int(m.group("ss"))
    year = 2000 + yy  # '25' -> 2025
    # interpret filename time in provided tz, then make it naive (local-wall clock)
    aware = dt.datetime(year, mo, da, hh, mi, ss, tzinfo=tz)
    start = aware.replace(tzinfo=None)
    end = start + dt.timedelta(seconds=int(assumed_duration_s))
    return _Clip(start=start, end=end, path=str(p))

def _fallback_collect_clips_by_directory(
    root: str,
    assumed_duration_s: int,
    exts: Sequence[str],
    video_tz: dt.tzinfo,
) -> Dict[str, List[_Clip]]:
    root_p = Path(root)
    out: Dict[str, List[_Clip]] = {}
    extset = {e.lower().lstrip(".") for e in exts}
    for p in root_p.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix.lower().lstrip(".") not in extset:
            continue
        try:
            clip = _fallback_parse_filename_ts(p, video_tz, assumed_duration_s)
        except Exception:
            continue
        out.setdefault(str(p.parent.resolve()), []).append(clip)
    for k in out:
        out[k].sort(key=lambda c: (c.start, c.path))
    return out

def _fallback_build_overlap_groups(clips: Sequence[_Clip], tolerance_s: int) -> List[List[_Clip]]:
    if not clips:
        return []
    groups: List[List[_Clip]] = []
    cur: List[_Clip] = [clips[0]]
    tol = max(tolerance_s, 0)
    for c in clips[1:]:
        gap = (c.start - cur[-1].end).total_seconds()
        if gap <= tol:
            cur.append(c)
        else:
            groups.append(cur)
            cur = [c]
    groups.append(cur)
    return groups

def _fallback_format_ts(t: dt.datetime) -> str:
    return t.strftime("%Y-%m-%d %H:%M:%S")

def _fallback_format_normalized_range(start: dt.datetime, end: dt.datetime) -> str:
    total = int((end - start).total_seconds())
    hh, rem = divmod(total, 3600)
    mm, ss = divmod(rem, 60)
    if hh:
        return f"0:00 → {hh}:{mm:02d}:{ss:02d}"
    else:
        return f"0:00 → {mm}:{ss:02d}"

# Bind fallbacks where project helpers are missing
format_ts = _FMT_TS or _fallback_format_ts
format_normalized_range = _FMT_RANGE or _fallback_format_normalized_range
build_overlap_groups = _BUILD_GROUPS or _fallback_build_overlap_groups

# collect_clips_by_directory signature must match our use
def collect_clips_by_directory(
    root: str,
    assumed_duration_s: int,
    exts: Sequence[str],
    video_tz: Optional[dt.tzinfo] = None,
):
    tz = video_tz or dt.timezone.utc
    if _COLLECT:
        # Try the project’s function; support both (root, duration, exts) and (root, duration, exts, tz)
        try:
            return _COLLECT(root=root, assumed_duration_s=assumed_duration_s, exts=tuple(exts), video_tz=tz)  # type: ignore
        except TypeError:
            return _COLLECT(root=root, assumed_duration_s=assumed_duration_s, exts=tuple(exts))  # type: ignore
    return _fallback_collect_clips_by_directory(root, assumed_duration_s, exts, tz)

# ========================== GPX handling (gpxpy) ==============================
try:
    import gpxpy
    import gpxpy.gpx
except ImportError:
    print("Error: gpxpy not installed. Install with: python3 -m pip install gpxpy", file=sys.stderr)
    sys.exit(2)

try:
    from zoneinfo import ZoneInfo  # Python 3.11+
except Exception:
    ZoneInfo = None

def _to_utc(t: Optional[dt.datetime]) -> Optional[dt.datetime]:
    if t is None:
        return None
    if t.tzinfo is None:
        return t.replace(tzinfo=dt.timezone.utc)
    return t.astimezone(dt.timezone.utc)

def parse_gpx_utc(gpx_path: Path) -> Tuple[gpxpy.gpx.GPX, List[List[gpxpy.gpx.GPXTrackPoint]]]:
    with gpx_path.open("rb") as f:
        g = gpxpy.parse(f)
    tracks_points: List[List[gpxpy.gpx.GPXTrackPoint]] = []
    for trk in g.tracks:
        flat: List[gpxpy.gpx.GPXTrackPoint] = []
        for seg in trk.segments:
            for p in seg.points:
                p.time = _to_utc(p.time)
                flat.append(p)
        tracks_points.append(flat)
    return g, tracks_points

def slice_gpx_points_by_window(
    tracks_points: List[List[gpxpy.gpx.GPXTrackPoint]],
    window_start_utc: dt.datetime,
    window_end_utc: dt.datetime,
) -> Tuple[int, List[gpxpy.gpx.GPXTrackPoint]]:
    best_idx = -1
    best_pts: List[gpxpy.gpx.GPXTrackPoint] = []
    best_count = 0
    for ti, pts in enumerate(tracks_points):
        sel = [p for p in pts if (p.time is not None and window_start_utc <= p.time <= window_end_utc)]
        if len(sel) > best_count:
            best_idx = ti
            best_pts = sel
            best_count = len(sel)
    return best_idx, best_pts

def write_gpx_slice(out_path: Path, points: List[gpxpy.gpx.GPXTrackPoint]) -> None:
    g = gpxpy.gpx.GPX()
    trk = gpxpy.gpx.GPXTrack()
    g.tracks.append(trk)
    seg = gpxpy.gpx.GPXTrackSegment()
    trk.segments.append(seg)
    for p in points:
        seg.points.append(
            gpxpy.gpx.GPXTrackPoint(
                latitude=p.latitude,
                longitude=p.longitude,
                elevation=p.elevation,
                time=(p.time.astimezone(dt.timezone.utc) if p.time else None),
            )
        )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        f.write(g.to_xml())

def write_points_csv(out_path: Path, points: List[gpxpy.gpx.GPXTrackPoint]) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        f.write("time_utc,lat,lon,elevation_m\n")
        for p in points:
            iso = p.time.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if p.time else ""
            ele = "" if p.elevation is None else f"{float(p.elevation):.2f}"
            f.write(f"{iso},{p.latitude:.8f},{p.longitude:.8f},{ele}\n")

def write_group_summary(
    out_dir: Path,
    group_idx: int,
    g_start_local: dt.datetime,
    g_end_local: dt.datetime,
    clip_count: int,
    track_idx: int,
    pts_count: int,
    window_start_utc: dt.datetime,
    window_end_utc: dt.datetime,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "summary.txt").open("w", encoding="utf-8") as f:
        f.write(f"Group: {group_idx}\n")
        f.write(f"Clips: {clip_count}\n")
        f.write(f"Local Range: {format_ts(g_start_local)} → {format_ts(g_end_local)}\n")
        f.write(f"Normalized Range: {format_normalized_range(g_start_local, g_end_local)}\n")
        f.write(f"UTC Window: {window_start_utc.strftime('%Y-%m-%d %H:%M:%S UTC')} → {window_end_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
        f.write(f"Selected Track Index: {track_idx}\n")
        f.write(f"Extracted GPX Points: {pts_count}\n")

# ============================== Printing =====================================

def _print_group_table_header() -> None:
    print(
        f"{'Group':<6}  {'Abs Range (start → end, local)':<43}  {'Norm Range':<16}  "
        f"{'Clips':<5}  {'TrackIdx':<8}  {'GPX pts':<7}  {'Out'}"
    )
    print("-" * 120)

def _print_group_table_row(
    group_idx: int,
    g_start_local: dt.datetime,
    g_end_local: dt.datetime,
    clip_count: int,
    track_idx: int,
    pts_count: int,
    out_rel: str,
) -> None:
    abs_range = f"{format_ts(g_start_local)} → {format_ts(g_end_local)}"
    norm = format_normalized_range(g_start_local, g_end_local)
    print(f"{group_idx:<6d}  {abs_range:<43}  {norm:<16}  {clip_count:<5d}  {track_idx:<8d}  {pts_count:<7d}  {out_rel}")

def _print_group_header(idx: int, g_start_local: dt.datetime, g_end_local: dt.datetime) -> None:
    print(f"--- Group {idx}  [{format_ts(g_start_local)} → {format_ts(g_end_local)} | {format_normalized_range(g_start_local, g_end_local)}] ---")

def _print_group_files(clips: List[_Clip]) -> None:
    for c in sorted(clips, key=lambda x: (x.start, x.path)):
        print(f"    {format_ts(c.start)} → {format_ts(c.end)}  |  {c.path}")

# ============================== Core =========================================

def process_directory(
    dirpath: str,
    clips: List[_Clip],
    tolerance_s: int,
    gpx_tracks_points: List[List[gpxpy.gpx.GPXTrackPoint]],
    video_tz: dt.tzinfo,
    gps_shift_seconds: int,
    out_root: Path,
    root_dir: Path,
) -> None:
    rel_dir_for_print = os.path.relpath(dirpath, start=str(root_dir))
    print(f"\n=== Directory: {rel_dir_for_print} ===")

    if not clips:
        print("(no matching clips)")
        return

    groups = build_overlap_groups(clips, tolerance_s)

    _print_group_table_header()

    for gi, group in enumerate(groups, 1):
        g_start_local = min(c.start for c in group)
        g_end_local = max(c.end for c in group)

        # local-naive -> aware -> UTC
        g_start_aware = g_start_local.replace(tzinfo=video_tz)
        g_end_aware = g_end_local.replace(tzinfo=video_tz)
        g_start_utc = g_start_aware.astimezone(dt.timezone.utc)
        g_end_utc = g_end_aware.astimezone(dt.timezone.utc)

        # global shift to the comparison window (helps align GPX vs. video wall time)
        if gps_shift_seconds:
            delta = dt.timedelta(seconds=int(gps_shift_seconds))
            g_start_utc += delta
            g_end_utc += delta

        trk_idx, pts = slice_gpx_points_by_window(gpx_tracks_points, g_start_utc, g_end_utc)

        # Outputs under: CWD/gps_slices/<ROOT_NAME>/<relative-under-root>/group_##/
        safe_rel = Path(os.path.relpath(dirpath, start=str(root_dir)))
        if any(part == ".." for part in safe_rel.parts):
            safe_rel = Path(".")
        out_dir = (out_root / safe_rel / f"group_{gi:02d}")
        out_gpx = out_dir / "route_slice.gpx"
        out_csv = out_dir / "route_slice.csv"

        write_gpx_slice(out_gpx, pts)
        write_points_csv(out_csv, pts)
        write_group_summary(
            out_dir=out_dir,
            group_idx=gi,
            g_start_local=g_start_local,
            g_end_local=g_end_local,
            clip_count=len(group),
            track_idx=trk_idx,
            pts_count=len(pts),
            window_start_utc=g_start_utc,
            window_end_utc=g_end_utc,
        )

        out_rel = os.path.relpath(str(out_gpx), start=os.getcwd())
        _print_group_table_row(
            gi,
            g_start_local,
            g_end_local,
            len(group),
            trk_idx,
            len(pts),
            out_rel,
        )

    print()
    for gi, group in enumerate(groups, 1):
        g_start_local = min(c.start for c in group)
        g_end_local = max(c.end for c in group)
        _print_group_header(gi, g_start_local, g_end_local)
        _print_group_files(group)
        print()

# ============================== CLI ==========================================

def main(argv: Optional[Iterable[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Slice a GPX route per video group (per folder), writing route_slice.gpx/CSV/summary for each group."
        )
    )
    ap.add_argument("--gpx", required=True, help="Path to source GPX file (e.g., Gaia export).")
    ap.add_argument(
        "--root",
        default=".",
        help="Root directory to recurse (each subfolder grouped independently). Default: current directory.",
    )
    ap.add_argument("--duration", type=int, default=120, help="Assumed duration per clip in seconds. Default: 120.")
    ap.add_argument("--tolerance", type=int, default=0, help="Allowed gap (seconds) to merge clips. Default: 0.")
    ap.add_argument(
        "--ext",
        nargs="+",
        default=["mp4", "mov", "mkv"],
        help="Video extensions to include (no dot). Default: mp4 mov mkv.",
    )
    ap.add_argument(
        "--video-tz",
        default="America/New_York",
        help="IANA timezone for interpreting video filename timestamps. Default: America/New_York.",
    )
    ap.add_argument(
        "--gps-shift-seconds",
        type=int,
        default=0,
        help="Global shift (seconds) applied to the *comparison window* before slicing GPX. Default: 0.",
    )
    ap.add_argument(
        "--out-base",
        default="gps_slices",
        help="Base directory under CWD for outputs. The script creates a child named after ROOT's basename. Default: gps_slices",
    )

    args = ap.parse_args(list(argv) if argv is not None else None)

    # Timezone resolution
    if ZoneInfo is None:
        print("Warning: zoneinfo unavailable; falling back to UTC for video timestamps.", file=sys.stderr)
        video_tz = dt.timezone.utc
    else:
        try:
            video_tz = ZoneInfo(args.video_tz)
        except Exception:
            print(f"Warning: invalid --video-tz '{args.video_tz}', using UTC.", file=sys.stderr)
            video_tz = dt.timezone.utc

    gpx_path = Path(args.gpx).expanduser().resolve()
    if not gpx_path.exists():
        print(f"Error: GPX not found: {gpx_path}", file=sys.stderr)
        return 2

    try:
        _, gpx_tracks_points = parse_gpx_utc(gpx_path)
    except Exception as e:
        print(f"Error: failed to parse GPX with gpxpy: {e}", file=sys.stderr)
        return 2

    root = Path(args.root).expanduser().resolve()
    if not root.exists() or not root.is_dir():
        print(f"Error: not a directory: {root}", file=sys.stderr)
        return 2

    # Collect clips per directory (project helper or fallback)
    by_dir = collect_clips_by_directory(
        root=str(root),
        assumed_duration_s=args.duration,
        exts=tuple(e.lower() for e in args.ext),
        video_tz=video_tz,
    )

    # Derive output base as: CWD/<out-base>/<ROOT_NAME>
    cwd = Path.cwd()
    root_name = root.name if root.name not in ("", ".", "..") else "root"
    out_root = (cwd / args.out_base / root_name).resolve()

    # Clean previous output dir for this root name, then recreate
    if out_root.exists():
        shutil.rmtree(out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    # Process each directory independently
    for dirpath in sorted(by_dir.keys()):
        process_directory(
            dirpath=dirpath,
            clips=by_dir[dirpath],
            tolerance_s=args.tolerance,
            gpx_tracks_points=gpx_tracks_points,
            video_tz=video_tz,
            gps_shift_seconds=args.gps_shift_seconds,
            out_root=out_root,
            root_dir=root,
        )

    return 0

if __name__ == "__main__":
    sys.exit(main())
