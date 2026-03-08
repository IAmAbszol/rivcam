from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1] / "scripts"))
from common_utils import normalize_camera_id, parse_filename_timestamp  # noqa: E402


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("frontCenter", "frontCenter"),
        ("frontCenter_t", "frontCenter"),
        ("rear_center", "rearCenter"),
        ("sideRight_t", "sideRight"),
        ("SIDELEFT_T", "sideLeft"),
        ("gear-guard", "gearGuard"),
    ],
)
def test_scripts_camera_normalization(raw: str, expected: str) -> None:
    assert normalize_camera_id(raw) == expected


def test_rivcam_camera_normalization_parity() -> None:
    cameras = pytest.importorskip("rivcam.parsers.cameras")
    for raw in ["sideRight_t", "sideLeft_t", "frontCenter_t", "gear_guard"]:
        assert cameras.POSTPROCESS_CAMERA_V1(raw) == normalize_camera_id(raw)


def test_parse_filename_timestamp() -> None:
    ts = parse_filename_timestamp(Path("10_04_25_151214_video_frontCenter.mp4"))
    assert ts is not None
    assert ts.year == 2025
    assert ts.month == 10
    assert ts.day == 4
    assert ts.hour == 15
    assert ts.minute == 12
    assert ts.second == 14
