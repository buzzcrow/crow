#!/usr/bin/env bash
# Convert a screen recording to an optimized GIF for README embedding.
#
# Near-duplicate frames are automatically dropped (mpdecimate), so you can
# record slowly — static periods are skipped, only motion changes are kept.
#
# Usage:
#   tools/gif_convert.sh <input.mov> <output.gif> [width] [fps] [bayer_scale]
#
# Defaults: width=1350, fps=6, bayer_scale=5
#   width=orig  — keep the original mov resolution (no scaling)
# Requires: ffmpeg (install via `brew install ffmpeg` or `pixi install ffmpeg`)

set -euo pipefail

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <input.mov> <output.gif> [width] [fps] [bayer_scale]"
    echo "  width       — target width in pixels (default: 1350)"
    echo "  fps         — frames per second (default: 6)"
    echo "  bayer_scale — Bayer dither scale 0-5; lower is finer/clearer, higher compresses better (default: 5)"
    exit 1
fi

INPUT="$1"
OUTPUT="$2"
WIDTH="${3:-1350}"
FPS="${4:-6}"
DITHER="${5:-5}"

if [[ "$WIDTH" == "orig" ]]; then
    SCALE_FILTER=""
else
    SCALE_FILTER="scale=${WIDTH}:-1:flags=lanczos,"
fi

if [[ ! -f "$INPUT" ]]; then
    echo "error: input file not found: $INPUT"
    exit 1
fi

if ! command -v ffmpeg &>/dev/null; then
    echo "error: ffmpeg not found. Install with: brew install ffmpeg"
    exit 1
fi

tmp=$(mktemp "${TMPDIR:-/tmp}/palette_XXXXXX")
PALETTE="${tmp}.png"
rm -f "$tmp"

cleanup() { rm -f "$tmp" "$PALETTE"; }
trap cleanup EXIT

# Step 1: generate a custom palette for better color quality
#          (mpdecimate drops near-duplicate frames before palette sampling)
ffmpeg -y -v error -i "$INPUT" \
    -vf "fps=$FPS,${SCALE_FILTER}mpdecimate=hi=768:lo=320:frac=0.33,palettegen=stats_mode=diff" \
    "$PALETTE"

# Step 2: convert using the palette
#          mpdecimate removes static frames; paletteuse diff_mode stores only
#          changed regions per frame → small GIF even from a slow recording
ffmpeg -y -v error -i "$INPUT" -i "$PALETTE" \
    -lavfi "fps=$FPS,${SCALE_FILTER}mpdecimate=hi=768:lo=320:frac=0.33 [x]; [x][1:v] paletteuse=dither=bayer:bayer_scale=${DITHER}:diff_mode=rectangle" \
    -loop 0 \
    "$OUTPUT"

SIZE=$(du -h "$OUTPUT" | cut -f1)
echo "wrote $OUTPUT ($SIZE, width=${WIDTH}, ${FPS}fps, bayer_scale=${DITHER})"
