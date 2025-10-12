
from __future__ import annotations

import logging
import sys

LOGGER = logging.getLogger("rivcam")


def setup_logger(level: str = "INFO") -> None:
    lvl = getattr(logging, level.upper(), logging.INFO)
    LOGGER.setLevel(lvl)
    if not LOGGER.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        LOGGER.addHandler(h)
