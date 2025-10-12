
from __future__ import annotations

import re

_ALIASES = {
    "frontcenter": "frontCenter",
    "rearcenter": "rearCenter",
    "sideleft": "sideLeft",
    "sideright": "sideRight",
    "gearguard": "gearGuard",
}

_SUFFIX_RE = re.compile(r"(?:_t)+$", re.IGNORECASE)


def _strip_suffix(raw: str) -> str:
    return _SUFFIX_RE.sub("", raw)


def _normalize(raw: str) -> str:
    base = _strip_suffix(raw)
    key = base.replace("-", "").replace("_", "").lower()
    return _ALIASES.get(key, base)


def POSTPROCESS_CAMERA_V1(token: str) -> str:
    return _normalize(token)
