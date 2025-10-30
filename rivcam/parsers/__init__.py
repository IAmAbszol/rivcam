from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Callable, Dict, List, Optional, Pattern, Sequence, Tuple, Type

from rivcam.utils.time import UTC


class Version(Enum):
    V1 = auto()


@dataclass(frozen=True)
class IODoc:
    clip_fields: Dict[str, str]
    group_fields: Dict[str, str]
    notes: str = ""


@dataclass(frozen=True)
class ParserSpec:
    version: Version
    name: str
    description: str
    pattern: Pattern[str]
    postprocess_camera: Callable[[str], str]
    doc: str
    io: IODoc
    clip_type: Type
    group_type: Type

    def _parse_filename_bits(self, path: Path) -> Optional[Tuple[dt.datetime, str]]:
        m = self.pattern.match(path.name)
        if not m:
            return None
        gd = m.groupdict()
        mm = int(gd["mm"])
        dd = int(gd["dd"])
        yy = int(gd["yy"])
        hh = int(gd["hh"])
        mi = int(gd["mi"])
        ss = int(gd["ss"])
        year = 2000 + yy
        start_utc = dt.datetime(year, mm, dd, hh, mi, ss, tzinfo=UTC)
        camera_id = self.postprocess_camera(gd["cam"])
        return start_utc, camera_id

    def parse_clip(self, path: Path):
        bits = self._parse_filename_bits(path)
        if bits is None:
            return None
        start_utc, camera_id = bits
        return self.clip_type(
            filename=path.name,
            path=path,
            start_utc=start_utc,
            camera_id=camera_id,
            source="filename+ffprobe",
        )

    def group_clips(
        self,
        clips: Sequence,
        *,
        gap_tolerance_s: float = 60.0,
    ) -> List[Tuple[dt.datetime, dt.datetime, Tuple]]:
        if not clips:
            return []
        ordered = sorted(clips, key=lambda c: c.get_date().timestamp())
        windows: List[Tuple[dt.datetime, dt.datetime, Tuple]] = []
        current: List = []
        win_start_ts: Optional[float] = None
        win_end_ts: Optional[float] = None
        for c in ordered:
            c_start = c.get_date().timestamp()
            c_end = c_start + c.duration()
            if not current:
                current.append(c)
                win_start_ts = c_start
                win_end_ts = c_end
                continue
            assert win_start_ts is not None
            assert win_end_ts is not None
            gap = c_start - win_end_ts
            if gap <= gap_tolerance_s:
                current.append(c)
                if c_end > win_end_ts:
                    win_end_ts = c_end
            else:
                start_dt = dt.datetime.fromtimestamp(win_start_ts, tz=UTC)
                end_dt = dt.datetime.fromtimestamp(win_end_ts, tz=UTC)
                windows.append((start_dt, end_dt, tuple(current)))
                current = [c]
                win_start_ts = c_start
                win_end_ts = c_end
        if current and win_start_ts is not None and win_end_ts is not None:
            start_dt = dt.datetime.fromtimestamp(win_start_ts, tz=UTC)
            end_dt = dt.datetime.fromtimestamp(win_end_ts, tz=UTC)
            windows.append((start_dt, end_dt, tuple(current)))
        return windows

    def group_name(self, folder: Path, start: dt.datetime, end: dt.datetime) -> str:
        base = folder.name or "group"
        return f"{base}_{start.strftime('%Y%m%dT%H%M%SZ')}_{end.strftime('%Y%m%dT%H%M%SZ')}"

    @property
    def media_glob(self) -> str:
        return "*.mp4"


_REGISTRY: Dict[Version, ParserSpec] = {}


def register_spec(spec: ParserSpec) -> None:
    _REGISTRY[spec.version] = spec


def get_spec(version: Optional[Version] = None) -> ParserSpec:
    if not _REGISTRY:
        raise RuntimeError("No parser specs registered. Import rivcam to register defaults.")
    if version is None:
        version = latest_version()
    return _REGISTRY[version]


def latest_version() -> Version:
    if not _REGISTRY:
        raise RuntimeError("No parser specs registered. Import rivcam to register defaults.")
    return sorted(_REGISTRY.keys(), key=lambda v: v.value)[-1]
