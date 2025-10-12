
from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Optional, Sequence, Set, Tuple

from rivcam.ffmpeg_runner import probe_duration_seconds
from rivcam.parsers import Version
from rivcam.utils.logging import LOGGER
from rivcam.utils.time import UTC
from rivcam.utils.time import to_utc


class Clip:
    def __init__(self, version, filename: str, path: Path) -> None:
        self.version = version
        self.filename = filename
        self.path = path if path.is_absolute() else path.resolve()
        self._duration_cache: Optional[float] = None

    def get_date(self) -> dt.datetime:
        raise NotImplementedError

    def duration(self) -> float:
        if self._duration_cache is None:
            dur = probe_duration_seconds(self.path)
            if dur is None or dur <= 0:
                raise RuntimeError(f"Failed to probe duration for {self.path}")
            self._duration_cache = float(dur)
        return self._duration_cache

    def camera(self) -> Optional[str]:
        return None


class Group:
    def __init__(self, version, name: str, clips: Sequence[Clip]) -> None:
        self.version = version
        self.name = name
        self.clips: Tuple[Clip, ...] = tuple(clips)

    def approximate_length(self) -> float:
        if not self.clips:
            return 0.0
        starts = []
        ends = []
        for c in self.clips:
            s = c.get_date().timestamp()
            e = s + c.duration()
            starts.append(s)
            ends.append(e)
        start = min(starts)
        end = max(ends)
        length = max(0.0, end - start)
        return length

    def cameras(self) -> Set[str]:
        cams: Set[str] = set()
        for c in self.clips:
            cam = c.camera()
            if cam:
                cams.add(cam)
        return cams

    def validate(self) -> None:
        if not self.clips:
            raise ValueError("Group has no clips")
        v0 = self.clips[0].version
        for c in self.clips:
            if c.version != v0:
                raise ValueError("Group contains mixed clip versions")
        dates = [c.get_date() for c in self.clips]
        if dates != sorted(dates):
            LOGGER.warning("Group '%s' clips were not sorted by time; sorting in-memory for processing.", self.name)
            self.clips = tuple(sorted(self.clips, key=lambda x: x.get_date()))
        for c in self.clips:
            if c.duration() <= 0:
                raise ValueError(f"Clip has non-positive duration: {c.path}")
        unknown = [c for c in self.clips if not c.camera()]
        if unknown:
            LOGGER.warning("Group '%s' has clips with unknown camera ids (%d).", self.name, len(unknown))


class ClipV1(Clip):
    def __init__(self, filename: str, path: Path, start_utc: dt.datetime, camera_id: Optional[str], source: str = "filename+ffprobe") -> None:
        super().__init__(Version.V1, filename, path)
        self.start_utc = to_utc(start_utc)
        self.end_utc = self.start_utc + dt.timedelta(seconds=self.duration())
        self.camera_id = camera_id
        self.source = source

    def get_date(self) -> dt.datetime:
        return self.start_utc

    def duration(self) -> float:
        return super().duration()

    def camera(self) -> Optional[str]:
        return self.camera_id


class GroupV1(Group):
    def __init__(self, name: str, clips: Sequence[ClipV1], folder: Path) -> None:
        super().__init__(Version.V1, name, clips)
        self.folder = folder if folder.is_absolute() else folder.resolve()

    @property
    def start_utc(self) -> dt.datetime:
        return min(c.get_date() for c in self.clips)

    @property
    def end_utc(self) -> dt.datetime:
        latest = max(c.get_date().timestamp() + c.duration() for c in self.clips)
        return dt.datetime.fromtimestamp(latest, tz=UTC)
