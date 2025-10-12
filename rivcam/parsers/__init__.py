
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Callable, Dict, Optional, Pattern, Type


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
