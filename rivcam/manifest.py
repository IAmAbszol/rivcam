
from __future__ import annotations

import dataclasses
import datetime as dt
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


def _iso(dt_obj: dt.datetime) -> str:
    return dt_obj.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class ManifestSegment:
    path: str
    inpoint: Optional[float] = None
    outpoint: Optional[float] = None
    start_sec: Optional[float] = None
    dur_sec: Optional[float] = None


@dataclass
class ManifestCamera:
    camera: str
    output: str
    method: str  # "copy" or "encode"
    segments: List[ManifestSegment] = field(default_factory=list)


@dataclass
class ManifestGroup:
    version: str
    name: str
    folder: str
    start_utc: str
    end_utc: str
    approx_length_sec: float
    cameras: List[str]
    outputs: List[ManifestCamera] = field(default_factory=list)


def write_group_manifest(manifest_path: Path, data: ManifestGroup) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    obj = dataclasses.asdict(data)
    manifest_path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
