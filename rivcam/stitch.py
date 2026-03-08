# rivcam/stitch.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
from typing import Dict, List, Optional, Sequence, Tuple

from rivcam.common import ClipV1, Group, GroupV1
from rivcam.ffmpeg_runner import (
    ConcatEntry,
    Segment,
    concat_copy_demuxer,
    frame_duration_sec,
    probe_duration_seconds,
    trim_and_concat_encode,
)
from rivcam.manifest import (
    ManifestCamera,
    ManifestGroup,
    ManifestSegment,
    write_group_manifest,
)
from rivcam.parsers import Version
from rivcam.utils.logging import LOGGER
from rivcam.utils.paths import ensure_dir


@dataclass(frozen=True)
class _Seg:
    path: Path
    t_rel: float
    ss: float
    dur: float


_EPS = 1e-3


def _safe_unlink(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    except Exception as exc:
        LOGGER.warning("Failed removing partial output %s: %s", path, exc)


def _group_clips_by_camera(g: GroupV1) -> Dict[str, List[ClipV1]]:
    cams: Dict[str, List[ClipV1]] = {}
    for c in sorted(g.clips, key=lambda x: (x.camera() or "", x.get_date(), x.path.name)):
        cams.setdefault(c.camera() or "unknown", []).append(c)
    return cams


def _overlap(a0: float, a1: float, b0: float, b1: float) -> Tuple[float, float] | None:
    s = max(a0, b0)
    e = min(a1, b1)
    if e <= s:
        return None
    return s, e


def _build_raw_intersections(g: GroupV1, clips: List[ClipV1]) -> List[_Seg]:
    g0 = g.start_utc.timestamp()
    g1 = g.end_utc.timestamp()
    out: List[_Seg] = []
    for c in clips:
        c0 = c.get_date().timestamp()
        c1 = c0 + c.duration()
        ov = _overlap(c0, c1, g0, g1 - _EPS)
        if not ov:
            continue
        s, e = ov
        ss = max(0.0, s - c0)
        dur = max(0.0, e - s)
        if dur > _EPS:
            t_rel = s - g0
            out.append(_Seg(path=c.path, t_rel=t_rel, ss=ss, dur=dur))
    return out


def _snap_to_frame_grid(path: Path, ss: float, dur: float) -> Tuple[float, float, float]:
    fd = frame_duration_sec(path)
    if fd <= 0:
        return ss, dur, 0.0
    ss_frames = round(ss / fd)
    ss_snapped = max(0.0, ss_frames * fd)
    end = ss + dur
    end_frames = round(end / fd)
    end_snapped = max(ss_snapped, end_frames * fd)
    dur_snapped = max(0.0, end_snapped - ss_snapped)
    return ss_snapped, dur_snapped, fd


def _resolve_overlaps_and_snap(segs: List[_Seg]) -> List[Segment]:
    segs = sorted(segs, key=lambda s: (s.t_rel, s.path.name))
    resolved: List[Segment] = []
    current_end = 0.0
    for s in segs:
        if s.t_rel < current_end:
            delta = current_end - s.t_rel
            ss = s.ss + delta
            dur = max(0.0, s.dur - delta)
        else:
            ss = s.ss
            dur = s.dur
        ss, dur, _fd = _snap_to_frame_grid(s.path, ss, dur)
        if dur <= _EPS:
            continue
        current_end += dur
        resolved.append(Segment(path=s.path, start_sec=ss, dur_sec=dur))
    return resolved


def _as_concat_entries(segments: List[Segment]) -> List[ConcatEntry]:
    entries: List[ConcatEntry] = []
    for s in segments:
        inpoint = s.start_sec if s.start_sec > _EPS else None
        outpoint = (s.start_sec + s.dur_sec) if s.dur_sec > _EPS else None
        entries.append(ConcatEntry(path=s.path, inpoint=inpoint, outpoint=outpoint))
    return entries


def _stitch_camera_fast_or_fallback(out_path: Path, segments: List[Segment]) -> Tuple[str, List[ManifestSegment]]:
    entries = _as_concat_entries(segments)
    rc = concat_copy_demuxer(entries, out_file=out_path)
    if rc == 0:
        man_segments = [ManifestSegment(path=str(e.path), inpoint=e.inpoint, outpoint=e.outpoint) for e in entries]
        return "copy", man_segments
    LOGGER.debug("Fast path failed (rc=%s) for %s, falling back to encode.", rc, out_path.name)
    rc2 = trim_and_concat_encode(segments, out_file=out_path, fps=None)
    if rc2 != 0:
        raise RuntimeError(f"ffmpeg failed to stitch camera to {out_path} (encode fallback rc={rc2})")
    man_segments = [ManifestSegment(path=str(s.path), start_sec=s.start_sec, dur_sec=s.dur_sec) for s in segments]
    return "encode", man_segments


def stitch_group(
    out_base: Path,
    group: Group,
    *,
    exact: bool = True,
    cleanup_on_failure: bool = False,
) -> Path:
    if not isinstance(group, GroupV1):
        raise RuntimeError("Only GroupV1 is supported currently.")
    group.validate()

    gdir = ensure_dir(out_base / group.name)

    cams = _group_clips_by_camera(group)
    camera_manifests: List[ManifestCamera] = []

    for cam, clips in cams.items():
        raw = _build_raw_intersections(group, clips)
        if not raw:
            LOGGER.info("Skipping %s (no overlap with group '%s')", cam, group.name)
            continue
        segs = _resolve_overlaps_and_snap(raw)
        if not segs:
            LOGGER.info("Skipping %s after trims (no positive duration) in '%s'", cam, group.name)
            continue
        out_path = gdir / f"{cam}.mp4"
        LOGGER.info("Stitching %s (%d segs) → %s", cam, len(segs), out_path)
        try:
            method, man_segments = _stitch_camera_fast_or_fallback(out_path, segs)
        except Exception as exc:
            LOGGER.error("Failed stitching camera %s in %s: %s", cam, group.name, exc)
            if cleanup_on_failure:
                _safe_unlink(out_path)
            continue
        raw_total_s = sum(c.duration() for c in clips)
        kept_total_s = sum(s.dur_sec for s in segs)
        stitched_total_s = probe_duration_seconds(out_path) if out_path.exists() else 0.0
        trimmed_total_s = max(0.0, raw_total_s - kept_total_s)
        LOGGER.info(
            "Overlap accounting %s: raw_sum=%.3fs kept=%.3fs stitched=%.3fs trimmed=%.3fs",
            cam,
            raw_total_s,
            kept_total_s,
            stitched_total_s,
            trimmed_total_s,
        )
        camera_manifests.append(ManifestCamera(camera=cam, output=str(out_path), method=method, segments=man_segments))

    if camera_manifests:
        mg = ManifestGroup(
            version=str(Version.V1.name),
            name=group.name,
            folder=str(group.folder),
            start_utc=group.start_utc.isoformat(),
            end_utc=group.end_utc.isoformat(),
            approx_length_sec=group.approximate_length(),
            cameras=sorted(list(set(c.camera for c in camera_manifests))),
            outputs=camera_manifests,
        )
        write_group_manifest(gdir / "manifest.json", mg)
    elif cleanup_on_failure:
        try:
            shutil.rmtree(gdir, ignore_errors=True)
        except Exception:
            pass

    return gdir


@dataclass(frozen=True)
class DevCv2Options:
    tail_seconds: float = 4.0
    head_seconds: float = 4.0
    resize_w: int = 160
    resize_h: int = 90
    sample_every: int = 3
    min_match_frames: int = 8
    score_threshold: float = 12.0
    use_timestamp_hint: bool = True
    hint_slack_seconds: float = 1.5
    max_hint_seconds: float = 20.0


@dataclass(frozen=True)
class _DevOverlapResult:
    skip_frames: int
    score: float
    matched_frames: int
    used_hint: bool


def stitch_groups(
    out_base: Path,
    groups: Sequence[Group],
    *,
    exact: bool = True,
    dev: bool = False,
    dev_opts: Optional[DevCv2Options] = None,
    cleanup_on_failure: bool = False,
) -> None:
    if dev:
        _dev_cv2_stitch_groups(
            out_base,
            groups,
            dev_opts=dev_opts or DevCv2Options(),
            cleanup_on_failure=cleanup_on_failure,
        )
        return
    for g in groups:
        try:
            stitch_group(out_base, g, exact=exact, cleanup_on_failure=cleanup_on_failure)
        except Exception as exc:
            LOGGER.error("Failed stitching group %s: %s", getattr(g, "name", "<unknown>"), exc)
            if cleanup_on_failure:
                try:
                    shutil.rmtree(out_base / g.name, ignore_errors=True)
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# DEV-ONLY: OpenCV content-based overlap detection
# ---------------------------------------------------------------------------

def _dev_cv2_stitch_groups(
    out_base: Path,
    groups: Sequence[Group],
    *,
    dev_opts: DevCv2Options,
    cleanup_on_failure: bool = False,
) -> None:
    for g in groups:
        try:
            _dev_cv2_stitch_group(out_base, g, dev_opts=dev_opts)
        except Exception as exc:
            LOGGER.error("DEV stitch failed for group %s: %s", getattr(g, "name", "<unknown>"), exc)
            if cleanup_on_failure:
                try:
                    shutil.rmtree(out_base / g.name, ignore_errors=True)
                except Exception:
                    pass


def _dev_cv2_stitch_group(out_base: Path, group: Group, *, dev_opts: DevCv2Options) -> Path:
    try:
        import cv2
    except Exception as exc:  # pragma: no cover - depends on local environment
        raise RuntimeError("OpenCV dev stitch requested but 'cv2' is not installed.") from exc

    if not isinstance(group, GroupV1):
        raise RuntimeError("Only GroupV1 is supported currently.")
    group.validate()

    gdir = ensure_dir(out_base / group.name)
    cams = _group_clips_by_camera(group)

    for cam, clips in cams.items():
        # Content-first overlap detection with bounded search windows.
        clips = sorted(clips, key=lambda x: (x.get_date(), x.path.name))
        out_path = gdir / f"{cam}_dev.mp4"
        if not clips:
            continue

        # open first clip to learn video params
        first_cap = cv2.VideoCapture(str(clips[0].path))
        base_fps = first_cap.get(cv2.CAP_PROP_FPS) or 30.0
        width = int(first_cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(first_cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        first_cap.release()

        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        writer = cv2.VideoWriter(str(out_path), fourcc, base_fps, (width, height))

        # write first clip fully
        prev_clip: Optional[ClipV1] = None
        for idx, c in enumerate(clips):
            cap = cv2.VideoCapture(str(c.path))
            if idx == 0:
                while True:
                    ok, frame = cap.read()
                    if not ok:
                        break
                    writer.write(frame)
                prev_clip = c
                cap.release()
                continue

            assert prev_clip is not None
            hint_frames: Optional[int] = None
            if dev_opts.use_timestamp_hint:
                hint_frames = _timestamp_overlap_hint_frames(prev_clip, c, base_fps, max_hint_s=dev_opts.max_hint_seconds)
            ov = _dev_detect_content_overlap(
                prev_clip.path,
                c.path,
                target_fps=base_fps,
                opts=dev_opts,
                hint_frames=hint_frames,
            )
            overlap_frames = ov.skip_frames
            LOGGER.info(
                "[DEV] %s overlap %s -> %s = %d frames (score=%.3f matched=%d hint=%s)",
                cam,
                prev_clip.path.name,
                c.path.name,
                overlap_frames,
                ov.score,
                ov.matched_frames,
                "on" if ov.used_hint else "off",
            )

            # skip the overlapping frames in current clip
            if overlap_frames > 0:
                cap.set(cv2.CAP_PROP_POS_FRAMES, overlap_frames)

            while True:
                ok, frame = cap.read()
                if not ok:
                    break
                writer.write(frame)

            prev_clip = c
            cap.release()

        writer.release()
        LOGGER.info("[DEV] content-based stitched %s → %s", cam, out_path)

    return gdir


def _timestamp_overlap_hint_frames(prev_clip: ClipV1, curr_clip: ClipV1, fps: float, *, max_hint_s: float) -> int:
    prev_end = prev_clip.get_date().timestamp() + prev_clip.duration()
    curr_start = curr_clip.get_date().timestamp()
    overlap_s = max(0.0, prev_end - curr_start)
    overlap_s = min(overlap_s, max_hint_s)
    return int(round(overlap_s * max(1.0, fps)))


def _collect_sampled_gray_frames(
    cap,
    *,
    start_frame: int,
    frame_count: int,
    sample_every: int,
    resize_w: int,
    resize_h: int,
) -> List:
    import cv2

    out: List = []
    cap.set(cv2.CAP_PROP_POS_FRAMES, max(0, start_frame))
    for i in range(max(0, frame_count)):
        ok, frame = cap.read()
        if not ok:
            break
        if i % sample_every != 0:
            continue
        frame = cv2.resize(frame, (resize_w, resize_h))
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        out.append(frame)
    return out


def _dev_best_overlap_offset(
    prev_tail: List,
    curr_head: List,
    *,
    min_offset: int,
    max_offset: int,
    min_match_frames: int,
) -> Optional[Tuple[int, float, int]]:
    import numpy as np

    if not prev_tail or not curr_head:
        return None
    max_offset = min(max_offset, len(curr_head) - 1)
    if max_offset < min_offset:
        return None

    best: Optional[Tuple[int, float, int]] = None
    for off in range(min_offset, max_offset + 1):
        m = min(len(prev_tail), len(curr_head) - off)
        if m < min_match_frames:
            continue
        tail = prev_tail[-m:]
        head = curr_head[off:off + m]
        score = 0.0
        for a, b in zip(tail, head):
            score += float(np.abs(a.astype("int16") - b.astype("int16")).mean())
        score /= float(m)
        if best is None or score < best[1]:
            best = (off, score, m)
    return best


def _dev_detect_content_overlap(
    prev_path: Path,
    curr_path: Path,
    *,
    target_fps: float = 30.0,
    opts: DevCv2Options,
    hint_frames: Optional[int] = None,
) -> _DevOverlapResult:
    import cv2

    step = max(1, int(opts.sample_every))

    prev_cap = cv2.VideoCapture(str(prev_path))
    curr_cap = cv2.VideoCapture(str(curr_path))

    prev_fps = prev_cap.get(cv2.CAP_PROP_FPS) or target_fps
    curr_fps = curr_cap.get(cv2.CAP_PROP_FPS) or target_fps

    prev_total = int(prev_cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    curr_total = int(curr_cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)

    tail_raw = min(prev_total, int(prev_fps * opts.tail_seconds))
    head_raw = min(curr_total, int(curr_fps * opts.head_seconds))
    if tail_raw <= 0 or head_raw <= 0:
        prev_cap.release()
        curr_cap.release()
        return _DevOverlapResult(skip_frames=0, score=9999.0, matched_frames=0, used_hint=False)

    prev_tail = _collect_sampled_gray_frames(
        prev_cap,
        start_frame=max(0, prev_total - tail_raw),
        frame_count=tail_raw,
        sample_every=step,
        resize_w=opts.resize_w,
        resize_h=opts.resize_h,
    )
    curr_head = _collect_sampled_gray_frames(
        curr_cap,
        start_frame=0,
        frame_count=head_raw,
        sample_every=step,
        resize_w=opts.resize_w,
        resize_h=opts.resize_h,
    )
    prev_cap.release()
    curr_cap.release()

    if not prev_tail or not curr_head:
        return _DevOverlapResult(skip_frames=0, score=9999.0, matched_frames=0, used_hint=False)

    min_off = 0
    max_off = len(curr_head) - 1
    used_hint = False
    if hint_frames is not None:
        hint_off = max(0, int(round(hint_frames / step)))
        slack_off = max(1, int(round((opts.hint_slack_seconds * max(1.0, target_fps)) / step)))
        min_off = max(0, hint_off - slack_off)
        max_off = min(max_off, hint_off + slack_off)
        used_hint = True

    best = _dev_best_overlap_offset(
        prev_tail,
        curr_head,
        min_offset=min_off,
        max_offset=max_off,
        min_match_frames=max(1, int(opts.min_match_frames)),
    )
    if best is None:
        return _DevOverlapResult(skip_frames=0, score=9999.0, matched_frames=0, used_hint=used_hint)

    off_samples, score, matched = best
    if score > opts.score_threshold:
        return _DevOverlapResult(skip_frames=0, score=score, matched_frames=matched, used_hint=used_hint)

    skip_frames = max(0, off_samples * step)
    return _DevOverlapResult(skip_frames=skip_frames, score=score, matched_frames=matched, used_hint=used_hint)
