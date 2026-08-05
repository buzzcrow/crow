#!/usr/bin/env bash
# Convert a screen recording to an optimized MP4 (H.264) for README embedding.
#
# Near-duplicate frames are automatically dropped (mpdecimate), so you can
# record slowly — static periods are skipped, only motion changes are kept.
#
# Usage:
#   tools/mp4_convert.sh <input.mov> <output.mp4> [width] [fps] [crf]
#
# Defaults: width=1350, fps=6, crf=28
#   width=orig  — keep the original mov resolution (no scaling)
#   crf         — constant rate factor 0-51; lower is higher quality/larger,
#                 higher is smaller/more artifacts (default: 28)
# Requires: ffmpeg (install via `brew install ffmpeg` or `pixi install ffmpeg`)

set -euo pipefail

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <input.mov> <output.mp4> [width] [fps] [crf]"
    echo "  width — target width in pixels (default: 1350)"
    echo "  fps   — frames per second (default: 6)"
    echo "  crf   — constant rate factor 0-51; lower is higher quality/larger, higher is smaller/more artifacts (default: 28)"
    exit 1
fi

INPUT="$1"
OUTPUT="$2"
WIDTH="${3:-1350}"
FPS="${4:-6}"
CRF="${5:-28}"

if [[ "$WIDTH" == "orig" ]]; then
    SCALE_FILTER=""
else
    # -2 keeps height even (H.264 requires even dimensions)
    SCALE_FILTER="scale=${WIDTH}:-2:flags=lanczos,"
fi

if [[ ! -f "$INPUT" ]]; then
    echo "error: input file not found: $INPUT"
    exit 1
fi

if ! command -v ffmpeg &>/dev/null; then
    echo "error: ffmpeg not found. Install with: brew install ffmpeg"
    exit 1
fi

# mpdecimate drops near-duplicate frames → small MP4 even from a slow recording.
# +faststart moves the moov atom to the front for instant playback in browsers.
ffmpeg -y -v error -i "$INPUT" \
    -vf "fps=$FPS,${SCALE_FILTER}mpdecimate=hi=768:lo=320:frac=0.33" \
    -c:v libx264 -preset slow -crf "$CRF" -pix_fmt yuv420p -movflags +faststart \
    "$OUTPUT"

SIZE=$(du -h "$OUTPUT" | cut -f1)
echo "wrote $OUTPUT ($SIZE, width=${WIDTH}, ${FPS}fps, crf=${CRF})"
