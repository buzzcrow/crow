#!/usr/bin/env bash
# One-key batch conversion of all .mov files in a source directory to .gif.
#
# Usage:
#   tools/gen_demo_gifs.sh [src_dir] [dst_dir]
#
# Defaults: src_dir=../crow-demo, dst_dir=<src_dir>/gifs
#
# Preserves original screen resolution (no scaling) and still drops duplicate
# frames via mpdecimate. GIFs are written to the destination directory, not
# copied into doc/assets, so you can review them before committing.

set -euo pipefail
shopt -s nullglob

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEFAULT_SRC="$(cd "${REPO_ROOT}/../crow-demo" && pwd)"

SRC_DIR="${1:-${DEFAULT_SRC}}"
DST_DIR="${2:-${SRC_DIR}/gifs}"

mkdir -p "$DST_DIR"

found=0
for mov in "$SRC_DIR"/*.mov; do
    found=1
    base=$(basename "$mov" .mov)
    echo "Converting $mov -> ${DST_DIR}/${base}.gif"
    "$SCRIPT_DIR/gif_convert.sh" "$mov" "${DST_DIR}/${base}.gif" orig 10 3
done

if [[ "$found" -eq 0 ]]; then
    echo "error: no .mov files found in $SRC_DIR" >&2
    exit 1
fi

echo "Done. GIFs are in $DST_DIR"
