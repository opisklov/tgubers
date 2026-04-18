#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$SCRIPT_DIR/data"
OUT_DIR="$SCRIPT_DIR/exports"

mkdir -p "$OUT_DIR"

#for channel_dir in "$DATA_DIR"/*/; do
#    channel=$(basename "$channel_dir")
#    echo "Exporting $channel..."
#    python "$SCRIPT_DIR/export_xlsx.py" "$channel" --classify markers || echo "FAILED: $channel"
#done

echo ""
echo "Gathering xlsx files into $OUT_DIR..."
for channel_dir in "$DATA_DIR"/*/; do
    channel=$(basename "$channel_dir")
    src=$(find "$channel_dir" -maxdepth 1 -iname "${channel}.xlsx" | head -1)
    if [[ -n "$src" ]]; then
        cp "$src" "$OUT_DIR/${channel}.xlsx"
    fi
done

echo "Done. Files in $OUT_DIR:"
ls "$OUT_DIR"