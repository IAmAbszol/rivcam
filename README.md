# Rivcam

Rivian dashcam tooling to:

- parse Rivian clip filenames and normalize camera IDs,
- group clips by time window,
- stitch per-camera timelines with overlap trimming,
- compose group overlays from a JSON template,
- build filtered/labeled finals from group outputs,
- merge multiple finals into one output,
- optionally run OpenCV dev stitch mode for content-based overlap testing.

## Python Support

This repo is standardized on **Python 3.11**.

## Documentation

- High-level rivcam guide: `docs/rivcam-guide.md`

## Bootstrap (single command)

```bash
./scripts/bootstrap_python311.sh
```

Optional environment overrides:

- `PYTHON_BIN` (default `python3.11`)
- `VENV_DIR` (default `.venv311`)

## Script Pipeline

Stitch clips:

```bash
python3 scripts/video_stitch_processor.py recordings/OffRoading -y --out renders
```

Compose groups and generate final video (`final_composite.mp4` by default):

```bash
python3 scripts/super_compositor.py renders/OffRoading -y --template scripts/default_template.json
```

Key compositor options:

- `--encoder auto|videotoolbox|libx264` (default: `auto`)
- `--final-name <name>.mp4`
- `--no-final`

## Devtools

Helper probe utilities now live under `scripts/devtools/`:

- `python3 scripts/devtools/grouping_probe.py <root> [--gap ...]`
- `python3 scripts/devtools/stitch_probe.py <root> [--renders ...] [--dev]`

## `rivcam` CLI Pipeline

After bootstrap:

```bash
source .venv311/bin/activate
```

Run full pipeline:

```bash
rivcam all recordings/OffRoading --template scripts/default_template.json
```

Subcommands:

- `rivcam stitch <root> [--dev] [dev overlap options]`
- `rivcam compose <stitched-root> [--template ...] [--encoder ...] [--group-output-name ...] [--no-final]`
- `rivcam final <stitched-root> [--input-name ...] [--include-group ...] [--exclude-group ...] [--overlay-text ...]`
- `rivcam merge <root> [--input <file> ...] [--pattern ...] [--out ...]`
- `rivcam all <root> ...`

OpenCV dev overlap options are available via `rivcam stitch --help`.

Examples:

```bash
rivcam compose renders/OffRoading --template scripts/default_template.json --group-output-name composite.mp4 --no-final
rivcam final renders/OffRoading --input-name composite.mp4 --exclude-group 1 --overlay-text "Trail 1" --final-name trail_1.mp4
rivcam merge renders/OffRoading --input trail_1.mp4 --input trail_2.mp4 --out trails_combined.mp4
```

## Notes

- Missing camera files are rendered as black filler layers during composition.
- Template fields `stretch_w`, `pan_x`, and `auto_crop_y` are applied for stretch+crop overlays.
- Final concatenation tries stream-copy first and falls back to re-encode if needed.
