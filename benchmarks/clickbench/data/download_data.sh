#!/usr/bin/env bash
# download_data.sh — Download ClickBench Parquet dataset (100 files, ~15GB).
source "$(dirname "$0")/../setup/setup_common.sh"

mkdir -p "$DATA_DIR"
cd "$DATA_DIR"

EXISTING=$(ls hits_*.parquet 2>/dev/null | wc -l)
if [ "$EXISTING" -eq "$NUM_PARQUET_FILES" ]; then
    log "All $NUM_PARQUET_FILES parquet files already downloaded."
    exit 0
fi

log "Downloading $NUM_PARQUET_FILES parquet files to $DATA_DIR..."
log "This is ~15GB and may take a while."

download_one() {
    local i="$1"
    local FILE="hits_${i}.parquet"
    local URL="${DATASET_BASE_URL}/hits_${i}.parquet"
    if [ -f "$FILE" ]; then
        echo "  [skip] $FILE already exists"
    else
        echo "  [download] $FILE"
        wget --quiet --continue "$URL" -O "$FILE.tmp" && mv "$FILE.tmp" "$FILE"
    fi
}
export -f download_one
export DATASET_BASE_URL
seq 0 $((NUM_PARQUET_FILES - 1)) | xargs -P 10 -I{} bash -c 'download_one {}'

FINAL_COUNT=$(ls hits_*.parquet 2>/dev/null | wc -l)
log "Download complete. $FINAL_COUNT / $NUM_PARQUET_FILES files present."

if [ "$FINAL_COUNT" -ne "$NUM_PARQUET_FILES" ]; then
    die "Expected $NUM_PARQUET_FILES files, got $FINAL_COUNT. Re-run to retry."
fi
