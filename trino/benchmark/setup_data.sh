#!/bin/bash
# Setup ClickBench data for Trino-OpenSearch benchmark
#
# This script:
#   1. Registers the raw hits.parquet as a Hive external table
#   2. Creates an Iceberg table from it via CTAS
#
# Prerequisites:
#   - OpenSearch running with Trino plugin enabled
#   - hits.parquet downloaded to trino/benchmark/data/
#
# Usage:
#   ./trino/benchmark/setup_data.sh [opensearch_url] [parquet_dir]

set -euo pipefail

OS_URL="${1:-http://localhost:9200}"
PARQUET_DIR="${2:-$(cd "$(dirname "$0")/data" && pwd)}"
ENDPOINT="${OS_URL}/_plugins/_trino_sql/v1/statement"

echo "=== ClickBench Data Setup ==="
echo "OpenSearch: ${OS_URL}"
echo "Parquet dir: ${PARQUET_DIR}"
echo ""

if [ ! -f "${PARQUET_DIR}/hits.parquet" ]; then
    echo "ERROR: ${PARQUET_DIR}/hits.parquet not found"
    echo "Download it: wget -O ${PARQUET_DIR}/hits.parquet https://datasets.clickhouse.com/hits_compatible/hits.parquet"
    exit 1
fi

run_sql() {
    local sql="$1"
    local catalog="${2:-iceberg}"
    local schema="${3:-clickbench}"
    local escaped_sql
    escaped_sql=$(echo -n "$sql" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')

    local response
    response=$(curl -s -X POST "${ENDPOINT}" \
        -H "Content-Type: application/json" \
        -H "X-Trino-Catalog: ${catalog}" \
        -H "X-Trino-Schema: ${schema}" \
        -d "{\"query\": ${escaped_sql}}" \
        --max-time 3600)

    if echo "$response" | grep -q '"state":"FAILED"'; then
        local msg
        msg=$(echo "$response" | python3 -c 'import json,sys; r=json.load(sys.stdin); print(r.get("error",{}).get("message","?"))' 2>/dev/null || echo "$response")
        echo "FAILED: $msg"
        return 1
    fi
    echo "OK"
    return 0
}

echo "Step 1: Create Hive schema pointing to Parquet directory..."
run_sql "CREATE SCHEMA IF NOT EXISTS hive.clickbench WITH (location = 'file://${PARQUET_DIR}')" "hive" "default"

echo "Step 2: Register Parquet file as Hive external table..."
# The Hive connector can read Parquet files from the schema location
# We create an external table pointing to the parquet file
run_sql "CREATE TABLE IF NOT EXISTS hive.clickbench.hits (
    WatchID BIGINT,
    JavaEnable SMALLINT,
    Title VARCHAR,
    GoodEvent SMALLINT,
    EventTime TIMESTAMP,
    EventDate DATE,
    CounterID INTEGER,
    ClientIP INTEGER,
    RegionID INTEGER,
    UserID BIGINT,
    CounterClass SMALLINT,
    OS SMALLINT,
    UserAgent SMALLINT,
    URL VARCHAR,
    Referer VARCHAR,
    IsRefresh SMALLINT,
    RefererCategoryID SMALLINT,
    RefererRegionID INTEGER,
    URLCategoryID SMALLINT,
    URLRegionID INTEGER,
    ResolutionWidth SMALLINT,
    ResolutionHeight SMALLINT,
    ResolutionDepth SMALLINT,
    FlashMajor SMALLINT,
    FlashMinor SMALLINT,
    FlashMinor2 VARCHAR,
    NetMajor SMALLINT,
    NetMinor SMALLINT,
    UserAgentMajor SMALLINT,
    UserAgentMinor VARCHAR,
    CookieEnable SMALLINT,
    JavascriptEnable SMALLINT,
    IsMobile SMALLINT,
    MobilePhone SMALLINT,
    MobilePhoneModel VARCHAR,
    Params VARCHAR,
    IPNetworkID INTEGER,
    TraficSourceID SMALLINT,
    SearchEngineID SMALLINT,
    SearchPhrase VARCHAR,
    AdvEngineID SMALLINT,
    IsArtifical SMALLINT,
    WindowClientWidth SMALLINT,
    WindowClientHeight SMALLINT,
    ClientTimeZone SMALLINT,
    ClientEventTime TIMESTAMP,
    SilverlightVersion1 SMALLINT,
    SilverlightVersion2 SMALLINT,
    SilverlightVersion3 INTEGER,
    SilverlightVersion4 SMALLINT,
    PageCharset VARCHAR,
    CodeVersion INTEGER,
    IsLink SMALLINT,
    IsDownload SMALLINT,
    IsNotBounce SMALLINT,
    FUniqID BIGINT,
    OriginalURL VARCHAR,
    HID INTEGER,
    IsOldCounter SMALLINT,
    IsEvent SMALLINT,
    IsParameter SMALLINT,
    DontCountHits SMALLINT,
    WithHash SMALLINT,
    HitColor VARCHAR,
    LocalEventTime TIMESTAMP,
    Age SMALLINT,
    Sex SMALLINT,
    Income SMALLINT,
    Interests SMALLINT,
    Robotness SMALLINT,
    RemoteIP INTEGER,
    WindowName INTEGER,
    OpenerName INTEGER,
    HistoryLength SMALLINT,
    BrowserLanguage VARCHAR,
    BrowserCountry VARCHAR,
    SocialNetwork VARCHAR,
    SocialAction VARCHAR,
    HTTPError SMALLINT,
    SendTiming INTEGER,
    DNSTiming INTEGER,
    ConnectTiming INTEGER,
    ResponseStartTiming INTEGER,
    ResponseEndTiming INTEGER,
    FetchTiming INTEGER,
    SocialSourceNetworkID SMALLINT,
    SocialSourcePage VARCHAR,
    ParamPrice BIGINT,
    ParamOrderID VARCHAR,
    ParamCurrency VARCHAR,
    ParamCurrencyID SMALLINT,
    OpenstatServiceName VARCHAR,
    OpenstatCampaignID VARCHAR,
    OpenstatAdID VARCHAR,
    OpenstatSourceID VARCHAR,
    UTMSource VARCHAR,
    UTMMedium VARCHAR,
    UTMCampaign VARCHAR,
    UTMContent VARCHAR,
    UTMTerm VARCHAR,
    FromTag VARCHAR,
    HasGCLID SMALLINT,
    RefererHash BIGINT,
    URLHash BIGINT,
    CLID INTEGER
) WITH (
    external_location = 'file://${PARQUET_DIR}',
    format = 'PARQUET'
)" "hive" "clickbench"

echo "Step 3: Verify Hive table has data..."
run_sql "SELECT COUNT(*) FROM hive.clickbench.hits" "hive" "clickbench"

echo "Step 4: Create Iceberg schema..."
run_sql "CREATE SCHEMA IF NOT EXISTS iceberg.clickbench" "iceberg" "clickbench"

echo "Step 5: Create Iceberg table from Hive source (CTAS — this takes a while)..."
echo "  Loading ~100M rows from Parquet into Iceberg format..."
time run_sql "CREATE TABLE iceberg.clickbench.hits WITH (format = 'PARQUET') AS SELECT * FROM hive.clickbench.hits" "iceberg" "clickbench"

echo "Step 6: Verify Iceberg table..."
run_sql "SELECT COUNT(*) FROM iceberg.clickbench.hits" "iceberg" "clickbench"

echo ""
echo "=== Data setup complete ==="
echo "Run benchmark: ./trino/benchmark/run_clickbench.sh ${OS_URL}"
