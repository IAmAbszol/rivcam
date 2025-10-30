
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import ffmpeg

from rivcam.ffmpeg_runner import compile_graph, run_with_progress
from rivcam.utils.logging import LOGGER


@dataclass
class Layer:
    camera: str
    x: int
    y: int
    w: int
    h: int
    keep_aspect: bool = True


@dataclass
class Template:
    width: int
    height: int
    background: str = "black"
    fps: Optional[float] = None
    layers: List[Layer] = None


def _load_template_json(path: Path) -> Dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_template(path: Path) -> Template:
    obj = _load_template_json(path)
    layers = [Layer(**L) for L in obj.get("layers", [])]
    return Template(
        width=int(obj["width"]),
        height=int(obj["height"]),
        background=obj.get("background", "black"),
        fps=float(obj["fps"]) if obj.get("fps") is not None else None,
        layers=layers,
    )


def save_default_template(path: Path) -> None:
    tmpl = {
        "width": 1920,
        "height": 1080,
        "background": "black",
        "fps": None,
        "layers": [
            {"camera": "frontCenter", "x": 320, "y": 0, "w": 1280, "h": 608, "keep_aspect": True},
            {"camera": "sideLeft", "x": 0, "y": 608, "w": 640, "h": 472, "keep_aspect": True},
            {"camera": "rearCenter", "x": 640, "y": 608, "w": 640, "h": 472, "keep_aspect": True},
            {"camera": "sideRight", "x": 1280, "y": 608, "w": 640, "h": 472, "keep_aspect": True},
            {"camera": "gearGuard", "x": 0, "y": 0, "w": 320, "h": 180, "keep_aspect": True},
        ],
    }
    path.write_text(json.dumps(tmpl, indent=2), encoding="utf-8")


def _existing_camera_files(group_dir: Path) -> Dict[str, Path]:
    cams = {}
    for cam in ("frontCenter", "rearCenter", "sideLeft", "sideRight", "gearGuard"):
        p = group_dir / f"{cam}.mp4"
        if p.exists():
            cams[cam] = p
    return cams


def _scale_filter(inp, target_w: int, target_h: int, keep_aspect: bool):
    if keep_aspect:
        v = inp.filter("scale", f"iw*min({target_w}/iw\,{target_h}/ih)", f"ih*min({target_w}/iw\,{target_h}/ih)")
        v = v.filter("pad", target_w, target_h, f"({target_w}-iw)/2", f"({target_h}-ih)/2", color="black")
        return v
    else:
        return inp.filter("scale", target_w, target_h)


def compose_group(group_dir: Path, out_path: Path, template_path: Path, *, duration_sec: Optional[float] = None) -> None:
    if not template_path.exists():
        raise FileNotFoundError(f"Template not found: {template_path}")
    tmpl = load_template(template_path)

    cam_files = _existing_camera_files(group_dir)
    if not cam_files:
        raise RuntimeError(f"No stitched camera files found in {group_dir}")

    used_layers: List[Tuple[Layer, Path]] = []
    for L in tmpl.layers or []:
        p = cam_files.get(L.camera)
        if p is not None:
            used_layers.append((L, p))
        else:
            LOGGER.debug("Camera '%s' not present in %s; skipping that layer.", L.camera, group_dir.name)

    if not used_layers:
        raise RuntimeError("None of the template layers matched existing camera files.")

    color_args = {"color": tmpl.background, "size": f"{tmpl.width}x{tmpl.height}"}
    if duration_sec is not None:
        color_args["duration"] = duration_sec
    bg = ffmpeg.input("color={color}:size={size}".format(**color_args), f="lavfi")

    if tmpl.fps:
        bg = bg.filter("fps", fps=tmpl.fps)

    overlays = bg
    streams = []
    inputs = []
    for idx, (L, path) in enumerate(used_layers):
        inp = ffmpeg.input(str(path))
        if tmpl.fps:
            inp = inp.filter("fps", fps=tmpl.fps)
        v = _scale_filter(inp.video, L.w, L.h, L.keep_aspect)
        streams.append((v, L.x, L.y))
        inputs.append(inp)

    node = overlays
    for v, x, y in streams:
        node = ffmpeg.overlay(node, v, x=x, y=y)

    out = ffmpeg.output(node, str(out_path), vcodec="libx264", preset="fast", crf=18, movflags="+faststart")
    argv = compile_graph(out, global_args=["-hide_banner", "-y"])
    run_with_progress(argv)
