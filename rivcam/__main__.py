from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, List, Optional, Sequence

import rivcam  # noqa: F401 - import side effects register parser specs
from rivcam.builders import build_clip, build_groups
from rivcam.compositor import compose_group, save_default_template
from rivcam.parsers import Version
from rivcam.stitch import DevCv2Options, stitch_groups
from rivcam.utils.logging import LOGGER, setup_logger
from rivcam.utils.paths import ensure_dir, list_videos


def _natural_key(s: str) -> List[Any]:
    parts = re.split(r"(\d+)", s)
    out: List[Any] = []
    for p in parts:
        out.append(int(p) if p.isdigit() else p.lower())
    return out


def _looks_like_group_dir(path: Path) -> bool:
    if not path.is_dir():
        return False
    name = path.name.lower()
    if name.startswith("group_") or name.startswith("rivian_"):
        return True
    return any(child.suffix.lower() == ".mp4" for child in path.iterdir())


def _discover_group_dirs(root: Path) -> List[Path]:
    direct = [p for p in root.iterdir() if _looks_like_group_dir(p)] if root.is_dir() else []
    if direct:
        return sorted(direct, key=lambda p: _natural_key(p.name))
    if _looks_like_group_dir(root):
        return [root]
    return sorted([p for p in root.rglob("*") if _looks_like_group_dir(p)], key=lambda p: _natural_key(str(p)))


def _escape_drawtext_text(value: str) -> str:
    # Escape characters that have special meaning to ffmpeg drawtext parser.
    return (
        value.replace("\\", r"\\")
        .replace(":", r"\:")
        .replace("'", r"\'")
        .replace(",", r"\,")
        .replace("[", r"\[")
        .replace("]", r"\]")
        .replace("%", r"\%")
    )


def _group_matches_token(group_name: str, token: str) -> bool:
    g = group_name.lower()
    t = token.strip().lower()
    if not t:
        return False
    if g == t:
        return True
    if t.startswith("group_"):
        return g == t
    if t.isdigit():
        m = re.match(r"group_(\d+)$", g)
        return bool(m) and int(m.group(1)) == int(t)
    return False


def _filter_groups(
    groups: Sequence[Path],
    *,
    include_tokens: Sequence[str],
    exclude_tokens: Sequence[str],
) -> List[Path]:
    include_tokens = [t for t in include_tokens if t.strip()]
    exclude_tokens = [t for t in exclude_tokens if t.strip()]

    selected = list(groups)
    if include_tokens:
        selected = [
            g for g in selected
            if any(_group_matches_token(g.name, t) for t in include_tokens)
        ]
    if exclude_tokens:
        selected = [
            g for g in selected
            if not any(_group_matches_token(g.name, t) for t in exclude_tokens)
        ]
    return selected


def _concat_final(
    group_dirs: Sequence[Path],
    out_file: Path,
    *,
    input_name: str = "composite.mp4",
    overlay_text: Optional[str] = None,
    overlay_x: str = "(w-text_w)/2",
    overlay_y: str = "h-text_h-40",
    overlay_fontsize: int = 54,
    overlay_fontcolor: str = "white",
) -> bool:
    composites = [g / input_name for g in group_dirs if (g / input_name).exists()]
    if not composites:
        LOGGER.warning("No per-group %s files found; skipping final concat.", input_name)
        return False
    valid: List[Path] = []
    for p in sorted(composites, key=lambda q: _natural_key(str(q))):
        cp = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=nw=1:nk=1",
                str(p),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            dur = float(cp.stdout.strip())
        except Exception:
            dur = 0.0
        if cp.returncode == 0 and dur > 0:
            valid.append(p)
            continue
        LOGGER.warning("Skipping invalid composite (ffprobe failed): %s", p)
    if not valid:
        LOGGER.warning("No valid %s files found; skipping final concat.", input_name)
        return False

    with tempfile.NamedTemporaryFile("w", suffix=".concat.txt", delete=False) as fh:
        list_path = Path(fh.name)
        for p in valid:
            escaped = str(p.resolve()).replace("'", r"'\\''")
            fh.write(f"file '{escaped}'\n")

    try:
        out_file.parent.mkdir(parents=True, exist_ok=True)
        if overlay_text:
            drawtext = (
                "drawtext="
                f"text='{_escape_drawtext_text(overlay_text)}':"
                f"x={overlay_x}:"
                f"y={overlay_y}:"
                f"fontsize={int(overlay_fontsize)}:"
                f"fontcolor={overlay_fontcolor}:"
                "box=1:boxcolor=black@0.45:boxborderw=10"
            )
            cmd_overlay = [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "warning",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                str(list_path),
                "-vf",
                drawtext,
                "-c:v",
                "libx264",
                "-crf",
                "18",
                "-preset",
                "veryfast",
                "-pix_fmt",
                "yuv420p",
                "-an",
                str(out_file),
            ]
            rc_overlay = subprocess.call(cmd_overlay)
            if rc_overlay == 0 and out_file.exists() and out_file.stat().st_size > 0:
                LOGGER.info("Final composite written with text overlay: %s", out_file)
                return True
            LOGGER.error("Final concat+overlay failed (rc=%s)", rc_overlay)
            try:
                out_file.unlink()
            except Exception:
                pass
            return False

        cmd_copy = [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
            "-f", "concat", "-safe", "0",
            "-i", str(list_path),
            "-c", "copy",
            str(out_file),
        ]
        rc = subprocess.call(cmd_copy)
        if rc == 0 and out_file.exists() and out_file.stat().st_size > 0:
            LOGGER.info("Final composite written (stream copy): %s", out_file)
            return True

        LOGGER.warning("Final concat copy failed (rc=%s); retrying with re-encode.", rc)
        cmd_re = [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "warning",
            "-f", "concat", "-safe", "0",
            "-i", str(list_path),
            "-c:v", "libx264", "-crf", "18", "-preset", "veryfast",
            "-an", str(out_file),
        ]
        rc2 = subprocess.call(cmd_re)
        if rc2 == 0 and out_file.exists() and out_file.stat().st_size > 0:
            LOGGER.info("Final composite written (re-encode): %s", out_file)
            return True

        LOGGER.error("Final concat failed (copy rc=%s, reencode rc=%s)", rc, rc2)
        try:
            out_file.unlink()
        except Exception:
            pass
        return False
    finally:
        try:
            list_path.unlink()
        except Exception:
            pass


def _concat_paths(files: Sequence[Path], out_file: Path) -> bool:
    valid: List[Path] = []
    for p in files:
        if not p.exists() or not p.is_file():
            LOGGER.warning("Skipping missing input file: %s", p)
            continue
        cp = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=nw=1:nk=1",
                str(p),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            dur = float(cp.stdout.strip())
        except Exception:
            dur = 0.0
        if cp.returncode == 0 and dur > 0:
            valid.append(p)
            continue
        LOGGER.warning("Skipping invalid input (ffprobe failed): %s", p)

    if not valid:
        LOGGER.warning("No valid input files found; skipping merge.")
        return False

    with tempfile.NamedTemporaryFile("w", suffix=".concat.txt", delete=False) as fh:
        list_path = Path(fh.name)
        for p in valid:
            escaped = str(p.resolve()).replace("'", r"'\\''")
            fh.write(f"file '{escaped}'\n")

    try:
        out_file.parent.mkdir(parents=True, exist_ok=True)
        cmd_copy = [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "warning",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(list_path),
            "-c",
            "copy",
            str(out_file),
        ]
        rc = subprocess.call(cmd_copy)
        if rc == 0 and out_file.exists() and out_file.stat().st_size > 0:
            LOGGER.info("Merged final written (stream copy): %s", out_file)
            return True

        LOGGER.warning("Merged copy failed (rc=%s); retrying with re-encode.", rc)
        cmd_re = [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "warning",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(list_path),
            "-c:v",
            "libx264",
            "-crf",
            "18",
            "-preset",
            "veryfast",
            "-an",
            str(out_file),
        ]
        rc2 = subprocess.call(cmd_re)
        if rc2 == 0 and out_file.exists() and out_file.stat().st_size > 0:
            LOGGER.info("Merged final written (re-encode): %s", out_file)
            return True

        LOGGER.error("Merged concat failed (copy rc=%s, reencode rc=%s)", rc, rc2)
        try:
            out_file.unlink()
        except Exception:
            pass
        return False
    finally:
        try:
            list_path.unlink()
        except Exception:
            pass


def _build_clips(root: Path, version: Version) -> List:
    clips = []
    for p in list_videos(root):
        c = build_clip(p, version=version)
        if c is not None:
            clips.append(c)
    return clips


def _run_stitch(args: argparse.Namespace) -> Path:
    root = args.root.expanduser().resolve()
    version = Version[args.version]
    clips = _build_clips(root, version)
    groups = build_groups(clips, version=version, gap_tolerance_s=args.gap)

    out_base = (args.renders.expanduser().resolve() / root.name)
    ensure_dir(out_base)

    dev_opts = DevCv2Options(
        tail_seconds=args.dev_tail_seconds,
        head_seconds=args.dev_head_seconds,
        resize_w=args.dev_resize_w,
        resize_h=args.dev_resize_h,
        sample_every=args.dev_sample_every,
        min_match_frames=args.dev_min_match_frames,
        score_threshold=args.dev_score_threshold,
        use_timestamp_hint=not args.dev_no_timestamp_hint,
        hint_slack_seconds=args.dev_hint_slack_seconds,
        max_hint_seconds=args.dev_max_hint_seconds,
    )

    LOGGER.info("Stitching %d group(s) to %s (dev=%s)", len(groups), out_base, args.dev)
    stitch_groups(
        out_base,
        groups,
        exact=args.exact,
        dev=args.dev,
        dev_opts=dev_opts,
        cleanup_on_failure=args.cleanup_on_failure,
    )
    return out_base


def _run_compose(args: argparse.Namespace) -> int:
    root = args.root.expanduser().resolve()
    template_path = args.template.expanduser().resolve()
    if not template_path.exists():
        save_default_template(template_path)
        LOGGER.info("Wrote default template: %s", template_path)

    groups = _discover_group_dirs(root)
    if not groups:
        LOGGER.warning("No group directories found under %s", root)
        return 1

    ok = 0
    group_output_name = args.group_output_name
    for g in groups:
        out_file = g / group_output_name
        try:
            compose_group(
                g,
                out_file,
                template_path,
                encoder=args.encoder,
                crf=args.crf,
                preset=args.preset,
            )
            ok += 1
            LOGGER.info("Composited %s", g.name)
        except Exception as exc:
            LOGGER.error("Compose failed for %s: %s", g.name, exc)
            if args.cleanup_on_failure:
                try:
                    out_file.unlink()
                except Exception:
                    pass

    LOGGER.info("Composited %d/%d group(s)", ok, len(groups))

    if not args.no_final:
        root_name = root.name.lower()
        is_single_group = root_name.startswith("group_") or root_name.startswith("rivian_")
        final_out = (root.parent / args.final_name) if is_single_group else (root / args.final_name)
        final_ok = _concat_final(groups, final_out, input_name=group_output_name)
        if not final_ok and args.cleanup_on_failure:
            try:
                final_out.unlink()
            except Exception:
                pass

    return 0 if ok > 0 else 1


def _run_final(args: argparse.Namespace) -> int:
    root = args.root.expanduser().resolve()
    groups = _discover_group_dirs(root)
    if not groups:
        LOGGER.warning("No group directories found under %s", root)
        return 1

    selected = _filter_groups(
        groups,
        include_tokens=args.include_group or [],
        exclude_tokens=args.exclude_group or [],
    )
    if not selected:
        LOGGER.warning("No groups left after include/exclude filters.")
        return 1

    LOGGER.info(
        "Final concat using %d group(s): %s",
        len(selected),
        ", ".join(g.name for g in selected),
    )

    root_name = root.name.lower()
    is_single_group = root_name.startswith("group_") or root_name.startswith("rivian_")
    final_out = (root.parent / args.final_name) if is_single_group else (root / args.final_name)

    ok = _concat_final(
        selected,
        final_out,
        input_name=args.input_name,
        overlay_text=args.overlay_text,
        overlay_x=args.overlay_x,
        overlay_y=args.overlay_y,
        overlay_fontsize=args.overlay_fontsize,
        overlay_fontcolor=args.overlay_fontcolor,
    )
    if not ok and args.cleanup_on_failure:
        try:
            final_out.unlink()
        except Exception:
            pass
        return 1
    return 0 if ok else 1


def _run_merge(args: argparse.Namespace) -> int:
    root = args.root.expanduser().resolve()

    selected: List[Path] = []
    for item in args.input or []:
        p = Path(item).expanduser()
        if not p.is_absolute():
            p = root / p
        selected.append(p.resolve())

    if args.pattern:
        matched = sorted(root.glob(args.pattern), key=lambda p: _natural_key(p.name))
        selected.extend([p.resolve() for p in matched if p.is_file()])

    # Stable de-dup preserving order.
    ordered_unique: List[Path] = []
    seen = set()
    for p in selected:
        key = str(p)
        if key in seen:
            continue
        seen.add(key)
        ordered_unique.append(p)

    if not ordered_unique:
        LOGGER.warning("No input files selected for merge. Provide --input and/or --pattern.")
        return 1

    out_path = Path(args.out).expanduser()
    if not out_path.is_absolute():
        out_path = root / out_path
    out_path = out_path.resolve()

    # Avoid self-referential concat entries when output file is also listed.
    inputs = [p for p in ordered_unique if p != out_path]
    if not inputs:
        LOGGER.warning("No usable inputs remain after excluding output path itself.")
        return 1

    LOGGER.info("Merging %d file(s) into %s", len(inputs), out_path)
    ok = _concat_paths(inputs, out_path)
    if not ok and args.cleanup_on_failure:
        try:
            out_path.unlink()
        except Exception:
            pass
        return 1
    return 0 if ok else 1


def _add_stitch_args(ap: argparse.ArgumentParser) -> None:
    ap.add_argument("root", type=Path, help="Root directory of source clips.")
    ap.add_argument("--renders", type=Path, default=Path("renders"), help="Output base directory (default: ./renders).")
    ap.add_argument("--gap", type=float, default=60.0, help="Gap tolerance in seconds for grouping.")
    ap.add_argument("--version", type=str, default="V1", choices=["V1"], help="Parser version.")
    ap.add_argument("--exact", action="store_true", help="Enable exact overlap-safe trimming (default).")
    ap.add_argument("--no-exact", dest="exact", action="store_false", help="Disable exact trimming.")
    ap.add_argument("--dev", action="store_true", help="Use OpenCV dev content-based stitch mode.")

    ap.add_argument("--dev-tail-seconds", type=float, default=4.0, help="Dev mode tail window in seconds.")
    ap.add_argument("--dev-head-seconds", type=float, default=4.0, help="Dev mode head window in seconds.")
    ap.add_argument("--dev-sample-every", type=int, default=3, help="Dev mode sampling stride in frames.")
    ap.add_argument("--dev-resize-w", type=int, default=160, help="Dev mode downsample width.")
    ap.add_argument("--dev-resize-h", type=int, default=90, help="Dev mode downsample height.")
    ap.add_argument("--dev-min-match-frames", type=int, default=8, help="Minimum sampled frames required for overlap match.")
    ap.add_argument("--dev-score-threshold", type=float, default=12.0, help="Maximum mean-diff score to accept overlap.")
    ap.add_argument("--dev-no-timestamp-hint", action="store_true", help="Disable timestamp hinting in dev overlap search.")
    ap.add_argument("--dev-hint-slack-seconds", type=float, default=1.5, help="Timestamp-hint slack in seconds for bounded search.")
    ap.add_argument("--dev-max-hint-seconds", type=float, default=20.0, help="Cap timestamp overlap hint in seconds.")
    ap.add_argument("--cleanup-on-failure", dest="cleanup_on_failure", action="store_true", help="Delete partial outputs when stitch fails.")
    ap.add_argument("--no-cleanup-on-failure", dest="cleanup_on_failure", action="store_false", help="Keep partial outputs on stitch failure.")
    ap.set_defaults(exact=True)
    ap.set_defaults(cleanup_on_failure=True)


def _add_compose_args(ap: argparse.ArgumentParser) -> None:
    ap.add_argument("root", type=Path, help="Root containing group_* directories (or a single group_* dir).")
    ap.add_argument("--template", type=Path, default=Path("default_template.json"), help="Template JSON path.")
    ap.add_argument("--encoder", choices=["auto", "videotoolbox", "libx264"], default="auto", help="Encoder strategy.")
    ap.add_argument("--crf", type=int, default=18, help="x264 CRF (software encoder).")
    ap.add_argument("--preset", type=str, default="veryfast", help="x264 preset (software encoder).")
    ap.add_argument("--group-output-name", type=str, default="composite.mp4", help="Per-group output filename.")
    ap.add_argument("--final-name", type=str, default="final_composite.mp4", help="Final concatenated output filename.")
    ap.add_argument("--no-final", action="store_true", help="Skip final concat step.")
    ap.add_argument("--cleanup-on-failure", dest="cleanup_on_failure", action="store_true", help="Delete partial outputs when compose fails.")
    ap.add_argument("--no-cleanup-on-failure", dest="cleanup_on_failure", action="store_false", help="Keep partial outputs on compose failure.")
    ap.set_defaults(cleanup_on_failure=True)


def _add_final_args(ap: argparse.ArgumentParser) -> None:
    ap.add_argument("root", type=Path, help="Root containing group_* directories (or a single group_* dir).")
    ap.add_argument("--input-name", type=str, default="composite.mp4", help="Per-group input filename to concatenate.")
    ap.add_argument("--final-name", type=str, default="final_composite.mp4", help="Final concatenated output filename.")
    ap.add_argument(
        "--include-group",
        action="append",
        default=[],
        help="Include only matching groups (repeatable; accepts names like group_02 or numbers like 2).",
    )
    ap.add_argument(
        "--exclude-group",
        action="append",
        default=[],
        help="Exclude matching groups (repeatable; accepts names like group_01 or numbers like 1).",
    )
    ap.add_argument("--overlay-text", type=str, default=None, help="Optional text to burn into the final output.")
    ap.add_argument("--overlay-x", type=str, default="(w-text_w)/2", help="drawtext x expression for final overlay.")
    ap.add_argument("--overlay-y", type=str, default="h-text_h-40", help="drawtext y expression for final overlay.")
    ap.add_argument("--overlay-fontsize", type=int, default=54, help="drawtext fontsize for final overlay.")
    ap.add_argument("--overlay-fontcolor", type=str, default="white", help="drawtext font color for final overlay.")
    ap.add_argument("--cleanup-on-failure", dest="cleanup_on_failure", action="store_true", help="Delete final output when concat fails.")
    ap.add_argument("--no-cleanup-on-failure", dest="cleanup_on_failure", action="store_false", help="Keep final output when concat fails.")
    ap.set_defaults(cleanup_on_failure=True)


def _add_merge_args(ap: argparse.ArgumentParser) -> None:
    ap.add_argument("root", type=Path, help="Base directory for relative input file paths.")
    ap.add_argument(
        "--input",
        action="append",
        default=[],
        help="Input video path to merge (repeatable; absolute or relative to root).",
    )
    ap.add_argument(
        "--pattern",
        type=str,
        default=None,
        help="Optional glob pattern under root to include more inputs (example: 'trail_*.mp4').",
    )
    ap.add_argument("--out", type=Path, default=Path("merged_final.mp4"), help="Merged output path (default: <root>/merged_final.mp4).")
    ap.add_argument("--cleanup-on-failure", dest="cleanup_on_failure", action="store_true", help="Delete merged output when concat fails.")
    ap.add_argument("--no-cleanup-on-failure", dest="cleanup_on_failure", action="store_false", help="Keep merged output when concat fails.")
    ap.set_defaults(cleanup_on_failure=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Rivian dashcam stitch/composite pipeline.")
    ap.add_argument("--log-level", type=str, default="INFO", help="Logging level.")

    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_stitch = sub.add_parser("stitch", help="Group and stitch clips into per-camera outputs.")
    _add_stitch_args(ap_stitch)

    ap_compose = sub.add_parser("compose", help="Compose stitched groups into per-group and final videos.")
    _add_compose_args(ap_compose)

    ap_final = sub.add_parser("final", help="Concatenate existing per-group outputs into one final video.")
    _add_final_args(ap_final)

    ap_merge = sub.add_parser("merge", help="Concatenate existing final videos into one merged output.")
    _add_merge_args(ap_merge)

    ap_all = sub.add_parser("all", help="Run stitch then compose in one command.")
    _add_stitch_args(ap_all)
    ap_all.add_argument("--template", type=Path, default=Path("default_template.json"), help="Template JSON path.")
    ap_all.add_argument("--encoder", choices=["auto", "videotoolbox", "libx264"], default="auto", help="Encoder strategy.")
    ap_all.add_argument("--crf", type=int, default=18, help="x264 CRF (software encoder).")
    ap_all.add_argument("--preset", type=str, default="veryfast", help="x264 preset (software encoder).")
    ap_all.add_argument("--group-output-name", type=str, default="composite.mp4", help="Per-group output filename.")
    ap_all.add_argument("--final-name", type=str, default="final_composite.mp4", help="Final concatenated output filename.")
    ap_all.add_argument("--no-final", action="store_true", help="Skip final concat step.")

    args = ap.parse_args()
    setup_logger(args.log_level)

    if args.cmd == "stitch":
        _run_stitch(args)
        return 0
    if args.cmd == "compose":
        return _run_compose(args)
    if args.cmd == "final":
        return _run_final(args)
    if args.cmd == "merge":
        return _run_merge(args)
    if args.cmd == "all":
        stitched_root = _run_stitch(args)
        compose_args = argparse.Namespace(**vars(args))
        compose_args.root = stitched_root
        return _run_compose(compose_args)

    ap.error(f"Unknown command: {args.cmd}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
