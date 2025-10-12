
from __future__ import annotations

import datetime as dt

UTC = dt.timezone.utc


def to_utc(d: dt.datetime) -> dt.datetime:
    if d.tzinfo is None:
        return d.replace(tzinfo=UTC)
    return d.astimezone(UTC)
