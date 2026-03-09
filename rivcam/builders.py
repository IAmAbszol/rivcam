from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union

from rivcam.build_dispatch import register, resolve
from rivcam.common import Clip, ClipV1, Group, GroupV1
from rivcam.parsers import Version, get_spec, latest_version
from rivcam.utils.logging import LOGGER
from rivcam.utils.time import UTC


__all__ = ["build_clip", "build_groups"]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _ensure_clips_from_root(root: Path, media_glob: str, *, spec_version: Version) -> List[Clip]:
    """
    Discover media files under `root` using the spec's media_glob and convert to clips
    via the versioned clip builder.
    """
    clips: List[Clip] = []
    for path in root.rglob(media_glob):
        if not path.is_file():
            continue
        c = build_clip(path, version=spec_version)
        if c is not None:
            clips.append(c)
    return clips


# ---------------------------------------------------------------------------
# V1 implementations
# ---------------------------------------------------------------------------

@register(event="build_clip", version=Version.V1)
def _build_clip_v1(path: Path, *, spec) -> Optional[ClipV1]:
    """
    V1 clip builder.

    The spec is expected to expose `parse_clip(path) -> Clip | None`.
    """
    clip = spec.parse_clip(path)
    if clip is None:
        LOGGER.debug("V1 clip builder: %s did not match spec %s", path, spec.name)
    else:
        LOGGER.debug(
            "V1 clip builder: built clip %s (camera=%s, start=%s)",
            clip.filename,
            getattr(clip, "camera_id", None),
            clip.get_date(),
        )
    return clip


@register(event="build_groups", version=Version.V1)
def _build_groups_v1(
    root_or_clips: Union[Path, Iterable[Clip]],
    *,
    spec,
    gap_tolerance_s: float = 60.0,
) -> List[GroupV1]:
    """
    V1 group builder.

    If given a Path, we discover media files under it and parse them to clips.
    If given an iterable of clips, we group those directly.
    """
    if isinstance(root_or_clips, Path):
        base_folder = root_or_clips.resolve()
        clips = _ensure_clips_from_root(base_folder, spec.media_glob, spec_version=spec.version)
    else:
        base_folder = Path(".").resolve()
        clips = list(root_or_clips)

    if not clips:
        LOGGER.info("V1 group builder: no clips to group")
        return []

    # spec is the thing that knows how to group by time window
    windows: List[Tuple[dt.datetime, dt.datetime, Tuple[Clip, ...]]] = spec.group_clips(
        clips,
        gap_tolerance_s=gap_tolerance_s,
    )

    groups: List[GroupV1] = []
    for start_dt, end_dt, window_clips in windows:
        name = spec.group_name(base_folder, start_dt, end_dt)
        grp = GroupV1(name=name, clips=list(window_clips), folder=base_folder)
        groups.append(grp)
        LOGGER.debug(
            "V1 group builder: built group %s (%d clips) [%s – %s]",
            name,
            len(window_clips),
            start_dt.isoformat(),
            end_dt.isoformat(),
        )

    return groups


# ---------------------------------------------------------------------------
# public API
# ---------------------------------------------------------------------------

def build_clip(path: Path, *, version: Optional[Version] = None) -> Optional[Clip]:
    """
    Build a Clip from a media path using the versioned builder.

    Args:
        path: Path to media file.
        version: Optional parser/builder version; if omitted, use the latest registered.

    Returns:
        Clip instance or None if the path does not match the spec.
    """
    v = version or latest_version()
    spec = get_spec(v)
    fn = resolve("build_clip", v)
    return fn(path, spec=spec)


def build_groups(
    root: Union[Path, Iterable[Clip]],
    *,
    version: Optional[Version] = None,
    gap_tolerance_s: float = 60.0,
) -> List[Group]:
    """
    Build groups from either:
      - a root directory of media files, or
      - a pre-parsed iterable of Clip objects.

    This is what the dev probe utilities call (scripts/devtools/grouping_probe.py
    and scripts/devtools/stitch_probe.py).

    Args:
        root: directory or iterable of Clip
        version: parser/builder version
        gap_tolerance_s: max gap (seconds) to stay in the same group

    Returns:
        list of Group (concrete: GroupV1 for V1)
    """
    v = version or latest_version()
    spec = get_spec(v)
    fn = resolve("build_groups", v)
    return fn(root, spec=spec, gap_tolerance_s=gap_tolerance_s)
