#!/usr/bin/env bash
set -euo pipefail

PYTHON_SCRIPT_STAGE1="fetch_tiki_products.py"
PYTHON_SCRIPT_STAGE2="clean_tiki_products.py"
IDS_FILE="${IDS_FILE:-product_ids.txt}"
OUTPUT_DIR="${OUTPUT_DIR:-output}"
RETRY_DELAY="${RETRY_DELAY:-10}"

while true; do
  echo "[entrypoint] Bắt đầu Stage 1 (raw crawl)..."
  if python "$PYTHON_SCRIPT_STAGE1" --ids-file "$IDS_FILE" --output-dir "$OUTPUT_DIR"; then
    echo "[entrypoint] Stage 1 hoàn tất."
    break
  else
    echo "[entrypoint] Stage 1 lỗi, thử lại sau ${RETRY_DELAY}s..."
    sleep "$RETRY_DELAY"
  fi
done

echo "[entrypoint] Bắt đầu Stage 2 (clean & format)..."
python "$PYTHON_SCRIPT_STAGE2" --raw-dir "$OUTPUT_DIR" --output-dir "$OUTPUT_DIR"
echo "[entrypoint] Hoàn tất toàn bộ pipeline."

