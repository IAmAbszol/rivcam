from __future__ import annotations

import json
import re
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from rivcam.ffmpeg_runner import probe_duration_seconds
from rivcam.parsers.cameras import POSTPROCESS_CAMERA_V1
from rivcam.utils.logging import LOGGER

_AUTO_DISABLE_VIDEOTOOLBOX = False


@dataclass
class Layer:
    camera: str
    x: int
    y: int
    w: int
    h: int
    keep_aspect: bool = False
    transpose: Optional[int] = None
    mirror: bool = False
    stretch_w: Optional[int] = None
    pan_x: Optional[int] = None
    auto_crop_y: Optional[int] = None


@dataclass
class Template:
    width: int
    height: int
    background: str = "black"
    fps: Optional[float] = 30.0
    layers: List[Layer] = None


def _load_template_json(path: Path) -> Dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _normalize_camera(raw: str) -> str:
    return POSTPROCESS_CAMERA_V1(raw)


def _norm_stem(stem: str) -> str:
    return _normalize_camera(re.sub(r"(?:_t)+$", "", stem, flags=re.IGNORECASE))


def load_template(path: Path) -> Template:
    obj = _load_template_json(path)

    if "canvas" in obj and "layers" in obj:
        width = int(obj["canvas"]["w"])
        height = int(obj["canvas"]["h"])
        layers = [
            Layer(
                camera=_normalize_camera(str(layer["key"])),
                x=int(layer["x"]),
                y=int(layer["y"]),
                w=int(layer["w"]),
                h=int(layer["h"]),
                keep_aspect=bool(layer.get("keep_aspect", False)),
                transpose=int(layer["transpose"]) if layer.get("transpose") is not None else None,
                mirror=bool(layer.get("mirror", layer.get("hflip", False))),
                stretch_w=int(layer["stretch_w"]) if layer.get("stretch_w") is not None else None,
                pan_x=int(layer["pan_x"]) if layer.get("pan_x") is not None else None,
                auto_crop_y=int(layer["auto_crop_y"]) if layer.get("auto_crop_y") is not None else None,
            )
            for layer in obj.get("layers", [])
        ]
        return Template(
            width=width,
            height=height,
            background=obj.get("background", "black"),
            fps=float(obj["fps"]) if obj.get("fps") is not None else 30.0,
            layers=layers,
        )

    if "width" in obj and "height" in obj and "layers" in obj:
        layers = [
            Layer(
                camera=_normalize_camera(str(layer["camera"])),
                x=int(layer["x"]),
                y=int(layer["y"]),
                w=int(layer["w"]),
                h=int(layer["h"]),
                keep_aspect=bool(layer.get("keep_aspect", False)),
                transpose=int(layer["transpose"]) if layer.get("transpose") is not None else None,
                mirror=bool(layer.get("mirror", layer.get("hflip", False))),
                stretch_w=int(layer["stretch_w"]) if layer.get("stretch_w") is not None else None,
                pan_x=int(layer["pan_x"]) if layer.get("pan_x") is not None else None,
                auto_crop_y=int(layer["auto_crop_y"]) if layer.get("auto_crop_y") is not None else None,
            )
            for layer in obj.get("layers", [])
        ]
        return Template(
            width=int(obj["width"]),
            height=int(obj["height"]),
            background=obj.get("background", "black"),
            fps=float(obj["fps"]) if obj.get("fps") is not None else 30.0,
            layers=layers,
        )

    raise ValueError(f"Unsupported template schema in {path}")


def save_default_template(path: Path) -> None:
    tmpl = {
        "canvas": {"w": 1920, "h": 1080},
        "layers": [
            {"key": "rearCenter", "x": 0, "y": 540, "w": 1920, "h": 619},
            {"key": "sideLeft", "x": 0, "y": 0, "w": 640, "h": 1080, "transpose": 2, "mirror": False, "stretch_w": 2700, "pan_x": 1081, "auto_crop_y": 0},
            {"key": "sideRight", "x": 1280, "y": 0, "w": 640, "h": 1080, "transpose": 1, "stretch_w": 2392, "pan_x": 405, "auto_crop_y": 0},
            {"key": "frontCenter", "x": 640, "y": 0, "w": 640, "h": 540},
        ],
    }
    path.write_text(json.dumps(tmpl, indent=2), encoding="utf-8")


def _existing_camera_files(group_dir: Path) -> Dict[str, Path]:
    files: Dict[str, Path] = {}
    for p in sorted(group_dir.glob("*.mp4"), key=lambda q: q.name):
        key = _norm_stem(p.stem)
        files.setdefault(key, p)
    return files


def _build_filter_complex(tmpl: Template) -> str:
    chains: List[str] = ["[0:v]format=yuv420p[base]"]
    last = "base"

    for idx, layer in enumerate(tmpl.layers or [], start=1):
        in_lbl = f"{idx}:v"
        cur = f"v{idx-1}"

        ops: List[str] = []
        if layer.transpose is not None:
            ops.append(f"transpose={int(layer.transpose)}")
        if layer.mirror:
            ops.append("hflip")

        needs_pan_crop = layer.stretch_w is not None or layer.pan_x is not None or layer.auto_crop_y is not None
        scaled_w = layer.w
        if layer.stretch_w is not None:
            scaled_w = max(layer.w, int(layer.stretch_w))
            ops.append(f"scale={scaled_w}:{layer.h}")
        else:
            ops.append(f"scale={layer.w}:{layer.h}")

        if needs_pan_crop:
            crop_x = int(layer.pan_x) if layer.pan_x is not None else max(0, (scaled_w - layer.w) // 2)
            crop_y = int(layer.auto_crop_y) if layer.auto_crop_y is not None else 0
            max_x = max(0, scaled_w - layer.w)
            crop_x = max(0, min(crop_x, max_x))
            crop_y = max(0, min(crop_y, 0))
            ops.append(f"crop={layer.w}:{layer.h}:{crop_x}:{crop_y}")

        chains.append(f"[{in_lbl}]{','.join(ops)}[{cur}]")
        chains.append(f"[{last}][{cur}]overlay=x={layer.x}:y={layer.y}:shortest=1[ov{idx}]")
        last = f"ov{idx}"

    return ";".join(chains)


def _run_cmd(cmd: Sequence[str]) -> Tuple[int, str]:
    LOGGER.debug("RUN: %s", " ".join(shlex.quote(x) for x in cmd))
    cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    tail = cp.stderr[-2500:] if cp.stderr else ""
    return cp.returncode, tail


def _build_encode_cmd(
    *,
    inputs: Sequence[str],
    filter_complex: str,
    final_label: str,
    out_path: Path,
    fps: int,
    encoder: str,
    crf: int,
    preset: str,
) -> List[str]:
    cmd: List[str] = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "warning",
        *inputs,
        "-filter_complex",
        filter_complex,
        "-map",
        final_label,
        "-pix_fmt",
        "yuv420p",
        "-r",
        str(fps),
    ]
    if encoder == "videotoolbox":
        cmd += ["-c:v", "h264_videotoolbox", "-allow_sw", "1", "-b:v", "12M"]
    elif encoder == "libx264":
        cmd += ["-c:v", "libx264", "-crf", str(crf), "-preset", preset]
    else:
        raise ValueError(f"Unsupported encoder: {encoder}")

    cmd += ["-movflags", "+faststart", "-shortest", str(out_path)]
    return cmd


def compose_group(
    group_dir: Path,
    out_path: Path,
    template_path: Path,
    *,
    duration_sec: Optional[float] = None,
    encoder: str = "auto",
    crf: int = 18,
    preset: str = "veryfast",
) -> None:
    global _AUTO_DISABLE_VIDEOTOOLBOX

    if not template_path.exists():
        raise FileNotFoundError(f"Template not found: {template_path}")

    tmpl = load_template(template_path)
    if not tmpl.layers:
        raise RuntimeError("Template has no layers.")

    camera_files = _existing_camera_files(group_dir)
    fps = int(round(tmpl.fps or 30.0))

    inputs: List[str] = [
        "-f",
        "lavfi",
        "-i",
        f"color=size={tmpl.width}x{tmpl.height}:color={tmpl.background}:rate={fps}",
    ]
    real_durations: List[float] = []
    real_count = 0
    for layer in tmpl.layers:
        fp = camera_files.get(layer.camera)
        if fp and fp.exists():
            # Enforce template-driven orientation; do not auto-apply rotation metadata.
            inputs += ["-thread_queue_size", "2048", "-noautorotate", "-i", str(fp)]
            real_count += 1
            dur = probe_duration_seconds(fp)
            if dur and dur > 0:
                real_durations.append(float(dur))
        else:
            LOGGER.warning("Camera '%s' missing in %s; using black filler.", layer.camera, group_dir.name)
            inputs += ["-f", "lavfi", "-i", f"color=size={layer.w}x{layer.h}:color=black:rate={fps}"]

    if real_count == 0:
        raise RuntimeError(f"No stitched camera files found in {group_dir}")

    if duration_sec is None and real_durations:
        duration_sec = min(real_durations)

    filter_complex = _build_filter_complex(tmpl)
    final_label = f"[ov{len(tmpl.layers)}]"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    if encoder == "auto":
        encoders = ["libx264"] if _AUTO_DISABLE_VIDEOTOOLBOX else ["videotoolbox", "libx264"]
    else:
        encoders = [encoder]
    for enc in encoders:
        cmd = _build_encode_cmd(
            inputs=inputs,
            filter_complex=filter_complex,
            final_label=final_label,
            out_path=out_path,
            fps=fps,
            encoder=enc,
            crf=crf,
            preset=preset,
        )
        if duration_sec is not None and duration_sec > 0:
            cmd[1:1] = ["-t", f"{float(duration_sec):.3f}"]

        rc, err_tail = _run_cmd(cmd)
        if rc == 0:
            if enc != encoders[0]:
                LOGGER.info("Compose succeeded with encoder fallback '%s' for %s", enc, group_dir.name)
            return
        LOGGER.warning("Compose failed with encoder '%s' for %s (rc=%s)", enc, group_dir.name, rc)
        if err_tail:
            LOGGER.warning("ffmpeg stderr tail (%s):\n%s", enc, err_tail)
        if enc == "videotoolbox":
            tail = (err_tail or "").lower()
            if "cannot create compression session" in tail or "error initializing output stream" in tail:
                _AUTO_DISABLE_VIDEOTOOLBOX = True
                LOGGER.warning("Disabling videotoolbox for remaining groups in this run.")
        try:
            out_path.unlink()
        except Exception:
            pass

    try:
        out_path.unlink()
    except Exception:
        pass
    raise RuntimeError(f"Failed to compose {group_dir} with encoder strategy '{encoder}'")
