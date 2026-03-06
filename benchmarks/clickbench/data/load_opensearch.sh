#!/usr/bin/env bash
# load_opensearch.sh — Convert Parquet to JSON and bulk-load into OpenSearch.
source "$(dirname "$0")/../setup/setup_common.sh"

MAPPING_FILE="$REPO_DIR/integ-test/src/test/resources/clickbench/mappings/clickbench_index_mapping.json"
BULK_SIZE="${BULK_SIZE:-5000}"

# Check if data already loaded
ROW_COUNT=$(curl -sf "${OS_URL}/${INDEX_NAME}/_count" | jq -r '.count // 0')

if [ "$ROW_COUNT" -gt 0 ]; then
    log "OpenSearch '${INDEX_NAME}' already has $ROW_COUNT docs. Skipping load."
    exit 0
fi

# Ensure index exists
if ! curl -sf "${OS_URL}/${INDEX_NAME}" >/dev/null 2>&1; then
    log "Creating index '${INDEX_NAME}'..."
    curl -sf -XPUT "${OS_URL}/${INDEX_NAME}" \
        -H 'Content-Type: application/json' \
        -d @"$MAPPING_FILE" | jq .
fi

# Disable refresh during bulk load for performance
curl -sf -XPUT "${OS_URL}/${INDEX_NAME}/_settings" \
    -H 'Content-Type: application/json' \
    -d '{"index": {"refresh_interval": "-1", "number_of_replicas": 0}}'

log "Loading data into OpenSearch (this takes a while)..."
LOAD_START=$(date +%s)

TOTAL_INDEXED=0

for PARQUET_FILE in "$DATA_DIR"/hits_*.parquet; do
    FNAME=$(basename "$PARQUET_FILE")
    log "  Processing $FNAME..."

    # Use clickhouse-local to read parquet and output JSONEachRow
    # Pipe through a transformer that adds bulk action lines
    clickhouse-local -q "SELECT * FROM file('$PARQUET_FILE', Parquet) FORMAT JSONEachRow" | \
    while IFS= read -r line; do
        echo '{"index":{"_index":"'"$INDEX_NAME"'"}}'
        echo "$line"
    done | \
    split -l $((BULK_SIZE * 2)) -a 4 - /tmp/os_bulk_chunk_

    for CHUNK in /tmp/os_bulk_chunk_*; do
        RESPONSE=$(curl -sf -XPOST "${OS_URL}/_bulk" \
            -H 'Content-Type: application/x-ndjson' \
            --data-binary @"$CHUNK")
        ERRORS=$(echo "$RESPONSE" | jq -r '.errors')
        if [ "$ERRORS" = "true" ]; then
            log "    WARNING: Bulk request had errors in $CHUNK"
            echo "$RESPONSE" | jq '.items[] | select(.index.error != null) | .index.error' | head -5
        fi
        CHUNK_DOCS=$(wc -l < "$CHUNK")
        TOTAL_INDEXED=$((TOTAL_INDEXED + CHUNK_DOCS / 2))
        rm -f "$CHUNK"
    done
done

# Re-enable refresh and force merge
log "Restoring refresh interval and force merging..."
curl -sf -XPUT "${OS_URL}/${INDEX_NAME}/_settings" \
    -H 'Content-Type: application/json' \
    -d '{"index": {"refresh_interval": "1s"}}'
curl -sf -XPOST "${OS_URL}/${INDEX_NAME}/_refresh"
curl -sf -XPOST "${OS_URL}/${INDEX_NAME}/_forcemerge?max_num_segments=1"

LOAD_END=$(date +%s)
LOAD_TIME=$((LOAD_END - LOAD_START))

ROW_COUNT=$(curl -sf "${OS_URL}/${INDEX_NAME}/_count" | jq -r '.count')
DATA_SIZE=$(curl -sf "${OS_URL}/${INDEX_NAME}/_stats/store" | jq -r '.indices.hits.primaries.store.size_in_bytes // 0')

log "OpenSearch load complete: $ROW_COUNT docs, ${DATA_SIZE} bytes, ${LOAD_TIME}s"
echo "LOAD_TIME_OS=$LOAD_TIME" > "$RESULT_DIR/opensearch/load_meta.txt"
echo "DATA_SIZE_OS=$DATA_SIZE" >> "$RESULT_DIR/opensearch/load_meta.txt"
echo "ROW_COUNT_OS=$ROW_COUNT" >> "$RESULT_DIR/opensearch/load_meta.txt"
