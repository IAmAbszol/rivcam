
from __future__ import annotations

import re

FILENAME_REGEX_V1 = re.compile(
    r"""(?x)
    (?P<mm>\d{2})_(?P<dd>\d{2})_(?P<yy>\d{2})_
    (?P<hh>\d{2})(?P<mi>\d{2})(?P<ss>\d{2})_video_(?P<cam>[^.]+)\.mp4$
    """
)
