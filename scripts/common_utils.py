#!/usr/bin/env python3
"""Common utilities shared by group viewer, stitcher, and compositor."""

from __future__ import annotations

import datetime as dt
import json
import logging
import re
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

LOGGER: logging.Logger = logging.getLogger("rivian")

FILENAME_TS_RE = re.compile(
    r"(?P<mm>\d{2})_(?P<dd>\d{2})_(?P<yy>\d{2})_(?P<hh>\d{2})(?P<mi>\d{2})(?P<ss>\d{2})"
)
_CAMERA_ALIASES = {
    "frontcenter": "frontCenter",
    "rearcenter": "rearCenter",
    "sideleft": "sideLeft",
    "sideright": "sideRight",
    "gearguard": "gearGuard",
}
_CAMERA_SUFFIX_RE = re.compile(r"(?:_t)+$", re.IGNORECASE)

try:
    from tqdm import tqdm  # type: ignore
except Exception:  # pragma: no cover
    tqdm = None


def setup_logger(level: str | int = "INFO") -> logging.Logger:
    """Configure and return the global logger.

    Args:
        level: Logging level name or int (e.g., "DEBUG", "info", 20).

    Returns:
        logging.Logger: Configured logger.
    """
    global LOGGER
    if isinstance(level, str):
        level = level.upper()
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")
    LOGGER = logging.getLogger("rivian")
    LOGGER.setLevel(level)
    return LOGGER


def ensure_dir(p: Path) -> Path:
    """Ensure the directory exists and return it.

    Args:
        p: Directory path.

    Returns:
        Path: The same directory path.
    """
    p.mkdir(parents=True, exist_ok=True)
    return p


def run_cmd(cmd: Sequence[str], capture: bool = False, check: bool = True) -> subprocess.CompletedProcess:
    """Run a subprocess command.

    Args:
        cmd: Command sequence.
        capture: If True, capture stdout/stderr.
        check: If True, raise on non-zero exit.

    Returns:
        CompletedProcess: Result of the command.
    """
    LOGGER.debug("CMD: %s", " ".join(shlex.quote(c) for c in cmd))
    if capture:
        cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    else:
        cp = subprocess.run(cmd, check=False)
    if check and cp.returncode != 0:
        if capture:
            LOGGER.error("Command failed (%s): %s", cp.returncode, cp.stderr.decode("utf-8", "ignore"))
        raise subprocess.CalledProcessError(cp.returncode, cmd)
    return cp


def run_ffprobe_json(path: Path) -> Optional[Dict[str, Any]]:
    """Return ffprobe JSON info for a media file.

    Args:
        path: Path to media file.

    Returns:
        Optional[Dict[str, Any]]: Parsed JSON or None if ffprobe fails.
    """
    cmd = [
        "ffprobe", "-v", "error",
        "-show_format", "-show_streams",
        "-print_format", "json",
        str(path),
    ]
    try:
        cp = run_cmd(cmd, capture=True, check=True)
    except Exception:
        return None
    try:
        return json.loads(cp.stdout.decode("utf-8", "ignore"))
    except Exception:
        return None


def ffprobe_duration_seconds(info: Optional[Dict[str, Any]]) -> Optional[float]:
    """Extract duration in seconds from ffprobe JSON.

    Args:
        info: ffprobe JSON.

    Returns:
        Optional[float]: Duration or None.
    """
    if not info:
        return None
    try:
        d = float(info.get("format", {}).get("duration", "nan"))
        if d > 0:
            return d
    except Exception:
        pass
    try:
        for s in info.get("streams", []):
            if s.get("codec_type") == "video":
                d = float(s.get("duration", "nan"))
                if d > 0:
                    return d
    except Exception:
        pass
    return None


def ffprobe_creation_time(info: Optional[Dict[str, Any]]) -> Optional[dt.datetime]:
    """Extract creation_time as UTC from ffprobe JSON.

    Args:
        info: ffprobe JSON.

    Returns:
        Optional[datetime]: Creation time in UTC if present.
    """
    if not info:
        return None
    tags = info.get("format", {}).get("tags", {}) or {}
    ct = tags.get("creation_time")
    if not ct:
        return None
    try:
        return dt.datetime.fromisoformat(ct.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
    except Exception:
        return None


def parse_filename_timestamp(path: Path) -> Optional[dt.datetime]:
    """
    Extract UTC datetime from filenames like: MM_DD_YY_HHMMSS.* (e.g., 10_04_25_151214.mp4)

    Returns:
        datetime in UTC if parsed, else None.
    """
    m = FILENAME_TS_RE.search(path.stem)
    if not m:
        return None

    try:
        mm = int(m.group("mm"))
        dd = int(m.group("dd"))
        yy = int(m.group("yy"))
        hh = int(m.group("hh"))
        mi = int(m.group("mi"))
        ss = int(m.group("ss"))
        year = 2000 + yy  # assume 20YY
        dt_utc = dt.datetime(year, mm, dd, hh, mi, ss, tzinfo=dt.timezone.utc)
        return dt_utc
    except Exception:
        return None


def normalize_camera_id(raw: str) -> str:
    """Normalize camera token to canonical names used by the pipeline."""
    token = _CAMERA_SUFFIX_RE.sub("", str(raw or ""))
    key = token.replace("-", "").replace("_", "").lower()
    return _CAMERA_ALIASES.get(key, token)


def choose_start_utc(path: Path, ffprobe_info: Optional[Dict[str, Any]]) -> Tuple[Optional[dt.datetime], str, str]:
    """
    Choose start time with filename as source-of-truth, ffprobe as fallback.

    Returns:
        (start_utc, source, note)
        - source: "filename" or "ffprobe.creation_time" or "none"
        - note: diagnostic string about any disagreement
    """
    note_parts: List[str] = []
    fn_dt = parse_filename_timestamp(path)
    fp_dt = ffprobe_creation_time(ffprobe_info) if ffprobe_info else None

    if fn_dt:
        if fp_dt:
            delta = abs((fp_dt - fn_dt).total_seconds())
            if delta > 15:
                note_parts.append(f"start_delta={delta:.0f}s (ffprobe later)")
        return fn_dt, "filename", "; ".join(note_parts)
    if fp_dt:
        note_parts.append("filename-missing")
        return fp_dt, "ffprobe.creation_time", "; ".join(note_parts)
    return None, "none", "no-start"


def choose_duration_seconds(
    ffprobe_info: Optional[Dict[str, Any]],
    expected_segment_seconds: int = 60,
    snap_window: Tuple[int, int] = (55, 125),
) -> Tuple[Optional[float], str, str]:
    """
    Prefer a normalized expected segment length; use ffprobe only as a hint.

    If ffprobe duration lies within [snap_window], snap to expected_segment_seconds.
    Otherwise, still return expected_segment_seconds for these datasets to keep groups aligned.

    Returns:
        (duration_seconds, source, note)
        - source: "expected", "ffprobe-snapped", or "none"
        - note: diagnostic with raw ffprobe duration if available
    """
    raw = ffprobe_duration_seconds(ffprobe_info) if ffprobe_info else None
    if raw is not None:
        if snap_window[0] <= raw <= snap_window[1]:
            return float(expected_segment_seconds), "ffprobe-snapped", f"raw={raw:.3f}"
        return float(expected_segment_seconds), "expected", f"raw={raw:.3f}; out_of_window"
    return float(expected_segment_seconds), "expected", "ffprobe-missing"


@dataclass(frozen=True)
class Clip:
    """Video clip with absolute UTC time span."""
    path: Path
    start_utc: dt.datetime
    end_utc: dt.datetime
    camera: str
    source: str
    note: str = ""


def build_clip_filename_first(
    path: Path,
    camera: str,
    expected_segment_seconds: int = 60,
) -> Optional[Clip]:
    """
    Build a Clip preferring filename timestamp for start and normalized duration.

    Returns:
        Clip or None if start time could not be determined.
    """
    info = run_ffprobe_json(path)
    start_utc, start_src, start_note = choose_start_utc(path, info)
    if not start_utc:
        LOGGER.warning("No start time for: %s", path)
        return None

    dur_s, dur_src, dur_note = choose_duration_seconds(info, expected_segment_seconds)
    end_utc = start_utc + dt.timedelta(seconds=dur_s if dur_s else expected_segment_seconds)

    note = f"start={start_src}; {start_note}; dur={dur_src}; {dur_note}"
    return Clip(path=path, start_utc=start_utc, end_utc=end_utc, camera=camera, source="filename-first", note=note)


def build_clip_filename_start_ffprobe_len(
    path: Path,
    camera: str,
) -> Optional[Clip]:
    """
    Build a Clip using filename timestamp as START and real ffprobe duration as END (no normalization).

    - Start: parse from filename MM_DD_YY_HHMMSS (preferred), fallback to ffprobe.creation_time if filename missing.
    - Duration: exact from ffprobe (container.format.duration or video stream duration).
    """
    info = run_ffprobe_json(path)
    # Start (filename-first, fallback to ffprobe)
    start_utc, start_src, start_note = choose_start_utc(path, info)
    if not start_utc:
        LOGGER.warning("No start time for: %s", path)
        return None

    # Duration (real)
    dur_raw = ffprobe_duration_seconds(info)
    if not dur_raw or dur_raw <= 0:
        LOGGER.warning("No valid duration for: %s", path)
        return None

    end_utc = start_utc + dt.timedelta(seconds=float(dur_raw))
    note = f"start={start_src}; {start_note}; dur=ffprobe; raw={dur_raw:.3f}"
    return Clip(
        path=path,
        start_utc=start_utc,
        end_utc=end_utc,
        camera=camera,
        source="filename+ffprobe",
        note=note,
    )


@dataclass(frozen=True)
class Group:
    """Group of clips all overlapping within a window."""
    group: int
    folder: Path
    start_utc: dt.datetime
    end_utc: dt.datetime
    clips: tuple[Clip, ...]


def format_hms(seconds: float) -> str:
    """Format seconds as h:mm:ss or m:ss.

    Args:
        seconds: Duration in seconds.

    Returns:
        str: Formatted time string.
    """
    if seconds < 0:
        seconds = 0
    total = int(round(seconds))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h:d}:{m:02d}:{s:02d}"
    return f"{m:d}:{s:02d}"


def format_range_abs(start: dt.datetime, end: dt.datetime, tzout: Optional[dt.tzinfo]) -> str:
    """Format absolute time window in local time.

    Args:
        start: Start UTC.
        end: End UTC.
        tzout: Output tz or None for local.

    Returns:
        str: Printable absolute window.
    """
    if tzout is None:
        tzout = dt.datetime.now().astimezone().tzinfo
    s = start.astimezone(tzout).strftime("%Y-%m-%d %H:%M:%S")
    e = end.astimezone(tzout).strftime("%Y-%m-%d %H:%M:%S")
    return f"{s} → {e}"


def format_range_norm(start: dt.datetime, end: dt.datetime) -> str:
    """Format a normalized range from 0.

    Args:
        start: Start UTC.
        end: End UTC.

    Returns:
        str: Printable normalized window.
    """
    return f"0:00 → {format_hms((end - start).total_seconds())}"


def prompt_yes_no(msg: str, default_no: bool = True, assume_yes: bool = False) -> bool:
    """Prompt user for yes/no with default.

    Args:
        msg: Message prefix.
        default_no: If True, default is No.
        assume_yes: If True, auto-yes.

    Returns:
        bool: True if yes selected.
    """
    if assume_yes:
        return True
    default = "n" if default_no else "y"
    prompt = f"{msg}? [y/N] " if default_no else f"{msg}? [Y/n] "
    try:
        ans = input(prompt).strip().lower()
    except EOFError:
        ans = ""
    if not ans:
        ans = default
    return ans.startswith("y")


def print_groups_table(root: Path, groups: Iterable[Group], tzout: Optional[dt.tzinfo] = None) -> None:
    """Print groups summary and details using the global logger.

    Args:
        root: Root directory.
        groups: Iterable of groups.
        tzout: Output timezone.
    """
    LOGGER.info("")
    LOGGER.info("=== Directory: %s ===", str(root))
    LOGGER.info("Group   %-42s  %-17s  %5s", "Local Range", "Norm Range", "Clips")
    LOGGER.info("-" * 76)
    for g in groups:
        LOGGER.info(
            "%-7d %-42s  %-17s  %5d",
            g.group,
            format_range_abs(g.start_utc, g.end_utc, tzout),
            format_range_norm(g.start_utc, g.end_utc),
            len(g.clips),
        )
    LOGGER.info("")
    for g in groups:
        LOGGER.info("--- Group %d  [%s | %s] ---",
                    g.group,
                    format_range_abs(g.start_utc, g.end_utc, tzout),
                    format_range_norm(g.start_utc, g.end_utc))
        for c in sorted(g.clips, key=lambda c: (c.start_utc, c.camera, c.path.name)):
            LOGGER.info(
                "    %s  |  %s  |  src=%s  |  %s",
                format_range_abs(c.start_utc, c.end_utc, tzout),
                str(c.path),
                c.source,
                c.note or "",
            )
        LOGGER.info("")


def load_template(path: Optional[Path]) -> Dict[str, Any]:
    """Load a compositor template JSON, or return {} if missing/invalid.

    Args:
        path: File path or None.

    Returns:
        dict: Template dictionary or {}.
    """
    if not path:
        return {}
    try:
        with Path(path).open("r", encoding="utf-8") as f:
            tpl = json.load(f)
        if not isinstance(tpl, dict) or "canvas" not in tpl or "layers" not in tpl:
            LOGGER.warning("Template missing 'canvas' or 'layers'; ignoring.")
            return {}
        return tpl
    except Exception as e:
        LOGGER.warning("Failed to load template %s: %s", str(path), e)
        return {}


def find_camera_files_for_template(group_dir: Path, template: Dict[str, Any]) -> Dict[str, Path]:
    """Resolve template keys to files in a group directory.

    Normalizes names by stripping a trailing "_t" on basenames during matching only
    (no on-disk rename). Matching tries (against normalized stems):
      1) exact:   stem == key
      2) suffix:  stem endswith(key)
      3) loose:   key in stem
      4) fallback legacy globs (no normalization)
    """
    def _norm(stem: str) -> str:
        return re.sub(r"_t$", "", stem)

    # Pre-scan candidates
    candidates: List[Path] = []
    for pat in ("*.mp4", "*.mov", "*.mkv"):
        candidates.extend(group_dir.glob(pat))

    # Deterministic map: normalized stem -> first file seen
    norm_index: Dict[str, Path] = {}
    for p in sorted(candidates, key=lambda q: q.name):
        norm_index.setdefault(_norm(p.stem), p)

    resolved: Dict[str, Path] = {}
    layer_keys: List[str] = [
        str(l.get("key", "")).strip()
        for l in template.get("layers", [])
        if str(l.get("key", "")).strip()
    ]

    for key in layer_keys:
        if key in resolved:
            continue

        # 1) exact normalized
        hit = norm_index.get(key)
        if hit and hit.exists():
            resolved[key] = hit
            continue

        # 2) suffix normalized
        suffix_hits = [p for p in candidates if _norm(p.stem).endswith(key)]
        if suffix_hits:
            resolved[key] = sorted(suffix_hits, key=lambda p: p.name)[0]
            continue

        # 3) loose normalized
        loose_hits = [p for p in candidates if key in _norm(p.stem)]
        if loose_hits:
            resolved[key] = sorted(loose_hits, key=lambda p: p.name)[0]
            continue

        # 4) fallback legacy (mp4 only)
        hits: List[Path] = list(group_dir.glob(f"{key}.mp4"))
        if not hits:
            hits = list(group_dir.glob(f"*{key}.mp4"))
        if not hits:
            hits = list(group_dir.glob(f"*{key}*.mp4"))
        if hits:
            resolved[key] = sorted(hits, key=lambda p: p.name)[0]

    return resolved


def build_filter_complex(template: Dict[str, Any]) -> Tuple[str, List[str]]:
    """Build the filter_complex graph and ordered input labels based on template.

    The template must define:
      - canvas: {"w": int, "h": int}
      - layers: list of objects each containing:
          - key: camera key
          - w, h: scale target
          - x, y: overlay position
          - transpose: optional (0..3), ffmpeg transpose filter code

    Returns:
        Tuple[str, List[str]]: (filter_complex, input_video_labels)
    """
    layers = template["layers"]

    chains: List[str] = []
    input_labels: List[str] = []

    # Map input #0 (lavfi color) to [base]; keep yuv420p like your CLI
    # NOTE: caller provides the color source as the first input.
    chains.append("[0:v]format=yuv420p[base]")
    last = "base"

    # Subsequent real inputs start at index 1, in the same order as layers.
    for idx, layer in enumerate(layers, start=1):
        key = str(layer["key"])
        w = int(layer["w"])
        h = int(layer["h"])
        x = int(layer["x"])
        y = int(layer["y"])
        t = layer.get("transpose", None)
        mirror = bool(layer.get("mirror", layer.get("hflip", False)))
        stretch_w = layer.get("stretch_w")
        pan_x = layer.get("pan_x")
        auto_crop_y = layer.get("auto_crop_y")

        in_lbl = f"{idx}:v"
        cur = f"v{idx-1}"
        ops: List[str] = []
        if t is not None:
            ops.append(f"transpose={int(t)}")
        if mirror:
            ops.append("hflip")

        needs_pan_crop = stretch_w is not None or pan_x is not None or auto_crop_y is not None
        scaled_w = w
        if stretch_w is not None:
            scaled_w = max(w, int(stretch_w))
            ops.append(f"scale={scaled_w}:{h}")
        else:
            ops.append(f"scale={w}:{h}")

        if needs_pan_crop:
            crop_x = int(pan_x) if pan_x is not None else max(0, (scaled_w - w) // 2)
            crop_y = int(auto_crop_y) if auto_crop_y is not None else 0
            max_x = max(0, scaled_w - w)
            max_y = 0
            crop_x = max(0, min(crop_x, max_x))
            crop_y = max(0, min(crop_y, max_y))
            ops.append(f"crop={w}:{h}:{crop_x}:{crop_y}")

        chains.append(f"[{in_lbl}]{','.join(ops)}[{cur}]")

        # CRITICAL FIX: add :shortest=1 to every overlay hop so timeline is driven by real footage
        chains.append(f"[{last}][{cur}]overlay=x={x}:y={y}:shortest=1[ov{idx}]")

        last = f"ov{idx}"
        input_labels.append(in_lbl)

    fc = ";".join(chains)
    return fc, input_labels


def render_composite_preview(
    group_dir: Path,
    template: Dict[str, Any],
    output_png: Path,
    timestamp_seconds: float = 0.0,
) -> bool:
    """Render a one-frame composite preview as PNG.

    Args:
        group_dir: Group directory containing camera mp4s.
        template: Template dictionary.
        output_png: Destination PNG path.
        timestamp_seconds: Seek time within inputs.

    Returns:
        bool: True if written, else False.
    """
    w = int(template["canvas"]["w"])
    h = int(template["canvas"]["h"])

    files = find_camera_files_for_template(group_dir, template)
    if not files:
        LOGGER.warning("No matching camera files in %s", str(group_dir))
        return False

    # Inputs: 0 is color src, 1..N are the matched keys in template order.
    # CRITICAL FIX: make the color source finite in rate so it never drives at 25 fps
    color = f"color=size={w}x{h}:color=black:rate=30"
    inputs = ["-f", "lavfi", "-i", color]
    for layer in template["layers"]:
        key = str(layer["key"])
        p = files.get(key)
        if p and p.exists():
            inputs.extend(["-ss", f"{timestamp_seconds:.3f}", "-i", str(p)])
            continue
        # Keep rendering even if a camera file is missing by inserting a black filler stream.
        lw = int(layer.get("w", w))
        lh = int(layer.get("h", h))
        LOGGER.warning("Missing file for key '%s' in %s; using black filler.", key, str(group_dir))
        inputs.extend(["-f", "lavfi", "-i", f"color=size={lw}x{lh}:color=black:rate=30"])

    fc, _ = build_filter_complex(template)
    cmd = [
        "ffmpeg", "-hide_banner", "-y",
        *inputs,
        "-filter_complex", fc,
        "-map", f"[ov{len(template['layers'])}]",
        "-frames:v", "1",
        "-f", "image2",
        str(output_png),
    ]
    try:
        run_cmd(cmd, capture=False, check=True)
        return True
    except Exception as e:
        LOGGER.error("Preview failed for %s: %s", str(group_dir), e)
        return False


def list_media_files(root: Path, patterns: Sequence[str] = ("*.mp4", "*.mov", "*.mkv")) -> List[Path]:
    """Recursively list media files under root.

    Args:
        root: Root directory to walk.
        patterns: Glob patterns.

    Returns:
        list[Path]: Matched files.
    """
    out: List[Path] = []
    for pat in patterns:
        out.extend(root.rglob(pat))
    return out
