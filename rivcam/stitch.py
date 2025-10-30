
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

from rivcam.common import ClipV1, Group, GroupV1
from rivcam.ffmpeg_runner import ConcatEntry, Segment, concat_copy_demuxer, frame_duration_sec, trim_and_concat_encode
from rivcam.manifest import ManifestCamera, ManifestGroup, ManifestSegment, write_group_manifest
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


def stitch_group(out_base: Path, group: Group, *, exact: bool = True) -> Path:
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
        method, man_segments = _stitch_camera_fast_or_fallback(out_path, segs)
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

    return gdir


def stitch_groups(out_base: Path, groups: Sequence[Group], *, exact: bool = True) -> None:
    for g in groups:
        stitch_group(out_base, g, exact=exact)
