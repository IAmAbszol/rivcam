
from __future__ import annotations

import datetime as dt
from collections import defaultdict
from pathlib import Path
from typing import List, Optional, Sequence

from rivcam.common import Clip
from rivcam.common import ClipV1
from rivcam.common import Group
from rivcam.common import GroupV1
from rivcam.parsers import Version
from rivcam.parsers import get_spec
from rivcam.utils.logging import LOGGER


def _parse_filename_v1(path: Path, spec) -> Optional[tuple[dt.datetime, str]]:
    m = spec.pattern.search(path.name)
    if not m:
        return None
    yy = int(m["yy"])
    year = 2000 + yy if yy < 80 else 1900 + yy
    ts = dt.datetime(
        year, int(m["mm"]), int(m["dd"]),
        int(m["hh"]), int(m["mi"]), int(m["ss"]),
        tzinfo=dt.timezone.utc
    )
    cam = spec.postprocess_camera(m["cam"])
    return ts, cam


def build_clip(path: Path, *, version: Optional[Version] = None) -> Optional[Clip]:
    spec = get_spec(version)
    if spec.version == Version.V1:
        parsed = _parse_filename_v1(path, spec)
        if not parsed:
            LOGGER.debug("Skipping (filename did not match V1): %s", path.name)
            return None
        start_utc, camera = parsed
        return ClipV1(filename=path.name, path=path, start_utc=start_utc, camera_id=camera)
    LOGGER.error("Unsupported version requested: %s", spec.version)
    return None


def _derive_group_name_v1(folder: Path, start: dt.datetime, end: dt.datetime) -> str:
    def fmt(d: dt.datetime) -> str:
        return d.strftime("%Y%m%d_%H%M%S")
    return f"{folder.name}__{fmt(start)}__{fmt(end)}"


def build_groups(clips: Sequence[Clip], *, version: Optional[Version] = None, gap_tolerance_s: float = 60.0) -> List[Group]:
    spec = get_spec(version)
    if spec.version != Version.V1:
        raise RuntimeError(f"Unsupported version for grouping: {spec.version}")

    by_dir: dict[Path, list[ClipV1]] = defaultdict(list)
    for c in clips:
        if not isinstance(c, ClipV1):
            continue
        by_dir[c.path.parent].append(c)

    groups: List[Group] = []
    for folder, arr in sorted(by_dir.items()):
        arr.sort(key=lambda x: x.get_date())
        if not arr:
            continue
        current: list[ClipV1] = [arr[0]]
        win_start = arr[0].get_date()
        win_end_ts = arr[0].get_date().timestamp() + arr[0].duration()

        for c in arr[1:]:
            delta = c.get_date().timestamp() - win_end_ts
            if delta <= gap_tolerance_s:
                current.append(c)
                win_end_ts = max(win_end_ts, c.get_date().timestamp() + c.duration())
            else:
                name = _derive_group_name_v1(folder, win_start, dt.datetime.fromtimestamp(win_end_ts, tz=dt.timezone.utc))
                groups.append(GroupV1(name=name, clips=tuple(current), folder=folder))
                current = [c]
                win_start = c.get_date()
                win_end_ts = c.get_date().timestamp() + c.duration()

        if current:
            name = _derive_group_name_v1(folder, win_start, dt.datetime.fromtimestamp(win_end_ts, tz=dt.timezone.utc))
            groups.append(GroupV1(name=name, clips=tuple(current), folder=folder))

    return groups
