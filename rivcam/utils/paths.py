
from __future__ import annotations

from pathlib import Path
from typing import List


def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p.absolute()


def list_videos(root: Path, exts=(".mp4", ".mov", ".m4v")) -> List[Path]:
    exts = tuple(e.lower() for e in exts)
    out: List[Path] = []
    for p in root.rglob("*"):
        if p.is_file() and p.suffix.lower() in exts and not p.name.startswith("._"):
            out.append(p)
    out.sort()
    return out
