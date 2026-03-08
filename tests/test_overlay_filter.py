from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "scripts"))
from common_utils import build_filter_complex  # noqa: E402


def test_build_filter_complex_stretch_crop_semantics() -> None:
    template = {
        "canvas": {"w": 1920, "h": 1080},
        "layers": [
            {
                "key": "sideLeft",
                "w": 640,
                "h": 1080,
                "x": 0,
                "y": 0,
                "transpose": 2,
                "stretch_w": 2700,
                "pan_x": 1031,
                "auto_crop_y": 0,
            }
        ],
    }

    fc, _ = build_filter_complex(template)
    assert "transpose=2" in fc
    assert "scale=2700:1080" in fc
    assert "crop=640:1080:1031:0" in fc
    assert "overlay=x=0:y=0:shortest=1" in fc


def test_build_filter_complex_back_compat_without_pan_fields() -> None:
    template = {
        "canvas": {"w": 1280, "h": 720},
        "layers": [{"key": "frontCenter", "w": 640, "h": 360, "x": 0, "y": 0}],
    }
    fc, _ = build_filter_complex(template)
    assert "scale=640:360" in fc
    assert "crop=" not in fc


def test_build_filter_complex_mirror_option() -> None:
    template = {
        "canvas": {"w": 1920, "h": 1080},
        "layers": [
            {
                "key": "sideLeft",
                "w": 640,
                "h": 1080,
                "x": 0,
                "y": 0,
                "transpose": 2,
                "mirror": True,
                "stretch_w": 2700,
                "pan_x": 1056,
                "auto_crop_y": 0,
            }
        ],
    }
    fc, _ = build_filter_complex(template)
    assert "transpose=2,hflip,scale=2700:1080,crop=640:1080:1056:0" in fc
