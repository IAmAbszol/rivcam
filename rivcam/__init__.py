
from __future__ import annotations

import re

from rivcam.parsers import IODoc
from rivcam.parsers import ParserSpec
from rivcam.parsers import Version
from rivcam.parsers import get_spec
from rivcam.parsers import latest_version
from rivcam.parsers import register_spec
from rivcam.parsers.cameras import POSTPROCESS_CAMERA_V1
from rivcam.parsers.filenames import FILENAME_REGEX_V1
from rivcam.utils.logging import LOGGER
from rivcam.utils.logging import setup_logger
from rivcam.common import Clip
from rivcam.common import ClipV1
from rivcam.common import Group
from rivcam.common import GroupV1

# Initialize logging to INFO by default; can be changed by calling setup_logger().
if not LOGGER.handlers:
    setup_logger("INFO")

# Register V1 spec
register_spec(
    ParserSpec(
        version=Version.V1,
        name="RivianFilenameV1",
        description="Parse Rivian dashcam filenames (UTC timestamp), normalize camera id, probe duration via ffmpeg.",
        pattern=FILENAME_REGEX_V1,
        postprocess_camera=POSTPROCESS_CAMERA_V1,
        doc="V1 filename pattern: mm_dd_yy_hhmmss_video_<camera>[_t].mp4",
        io=IODoc(
            clip_fields={
                "version": "Version enum for this clip (V1)",
                "filename": "Original file name (str)",
                "path": "Absolute path (Path)",
                "start_utc": "UTC start time parsed from filename",
                "end_utc": "Computed as start_utc + probed duration (seconds)",
                "camera_id": "Canonical camera id (frontCenter, rearCenter, sideLeft, sideRight, gearGuard)",
                "source": "Provenance (e.g., 'filename+ffprobe')",
            },
            group_fields={
                "version": "Version enum for this group (V1)",
                "name": "Human-readable stable group name (used for output folder)",
                "clips": "Tuple[Clip, ...] members (same directory; time-ordered)",
                "folder": "Directory path these clips came from",
            },
            notes="Validation ensures non-empty, same version, durations present; approximate_length uses clip start/duration with half-open interval heuristics.",
        ),
        clip_type=ClipV1,
        group_type=GroupV1,
    )
)

__all__ = [
    "Version",
    "ParserSpec",
    "IODoc",
    "get_spec",
    "latest_version",
    "setup_logger",
    "Clip",
    "Group",
]
