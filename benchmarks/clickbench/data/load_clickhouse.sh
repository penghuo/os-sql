#!/usr/bin/env bash
# load_clickhouse.sh — Create table and load Parquet data into ClickHouse.
source "$(dirname "$0")/../setup/setup_common.sh"

SCHEMA_FILE="$QUERY_DIR/create_clickhouse.sql"

# Check if data already loaded
ROW_COUNT=$(clickhouse-client --host "$CH_HOST" --port "$CH_PORT" \
    -q "SELECT count() FROM hits" 2>/dev/null || echo "0")

if [ "$ROW_COUNT" -gt 0 ]; then
    log "ClickHouse 'hits' table already has $ROW_COUNT rows. Skipping load."
    exit 0
fi

# Create table
log "Creating ClickHouse 'hits' table..."
clickhouse-client --host "$CH_HOST" --port "$CH_PORT" -q "DROP TABLE IF EXISTS hits"
clickhouse-client --host "$CH_HOST" --port "$CH_PORT" < "$SCHEMA_FILE"

# Copy parquet files to ClickHouse user_files for file() access
log "Copying parquet files to ClickHouse user_files directory..."
sudo mkdir -p /var/lib/clickhouse/user_files/
sudo cp "$DATA_DIR"/hits_*.parquet /var/lib/clickhouse/user_files/
sudo chown -R clickhouse:clickhouse /var/lib/clickhouse/user_files/

# Load data
log "Loading data into ClickHouse (this takes a while)..."
LOAD_START=$(date +%s)
clickhouse-client --host "$CH_HOST" --port "$CH_PORT" \
    --max_insert_threads "$(( $(nproc) / 4 ))" \
    --time \
    -q "INSERT INTO hits SELECT * FROM file('hits_*.parquet')"
LOAD_END=$(date +%s)
LOAD_TIME=$((LOAD_END - LOAD_START))

ROW_COUNT=$(clickhouse-client --host "$CH_HOST" --port "$CH_PORT" -q "SELECT count() FROM hits")
DATA_SIZE=$(clickhouse-client --host "$CH_HOST" --port "$CH_PORT" \
    -q "SELECT total_bytes FROM system.tables WHERE name = 'hits' AND database = 'default'")

log "ClickHouse load complete: $ROW_COUNT rows, ${DATA_SIZE} bytes, ${LOAD_TIME}s"
echo "LOAD_TIME_CH=$LOAD_TIME" > "$RESULT_DIR/clickhouse/load_meta.txt"
echo "DATA_SIZE_CH=$DATA_SIZE" >> "$RESULT_DIR/clickhouse/load_meta.txt"
echo "ROW_COUNT_CH=$ROW_COUNT" >> "$RESULT_DIR/clickhouse/load_meta.txt"
