#!/bin/bash
# ============================================================================
# Split-Level Distribution Proof Test (ClickBench Iceberg)
#
# Starts a 3-node OpenSearch cluster and runs ClickBench queries against an
# Iceberg table (~100M rows). Checks node stats on EVERY node to prove that
# non-coordinator nodes actually received and processed splits.
#
# Evidence collected:
#   1. system.runtime.nodes — how many nodes Trino's scheduler sees
#   2. Node stats BEFORE queries (baseline)
#   3. Run 3 ClickBench queries (COUNT(*), GROUP BY, COUNT DISTINCT)
#   4. Node stats AFTER queries on ALL 3 nodes
#   5. Coordinator logs — "TransportRemoteTask for remote node" lines
#   6. Worker logs — "Task update handler" lines
#   7. PASS only if at least 1 worker node has tasksReceived delta > 0
#
# Usage:
#   ./trino/benchmark/test_split_distribution.sh
#
# Prerequisites:
#   - Plugin built (script will build if missing)
#   - hits.parquet in trino/benchmark/data/ (script will download if missing)
#   - Test cluster distro (script will create if missing)
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NODES=3
HEAP="${HEAP_PER_NODE:-16g}"
DATA_DIR="${SCRIPT_DIR}/data"
WAREHOUSE="/tmp/iceberg-clickbench-warehouse"

DISTRO_DIR="${REPO_DIR}/plugin/build/testclusters/integTest-0/distro/3.6.0-ARCHIVE"
BASE_CONFIG="${REPO_DIR}/plugin/build/testclusters/integTest-0/config"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

echo "============================================"
echo " Split-Level Distribution Proof Test"
echo " (ClickBench Iceberg — 100M rows)"
echo "============================================"
echo ""

# ---------- Step 0: Build prerequisites ----------
echo "Step 0: Checking prerequisites..."

# 0a. Build plugin if needed
if [ ! -f "${DISTRO_DIR}/bin/opensearch" ]; then
    echo "  Building plugin and test cluster distro..."
    echo "  (This runs the plugin once to create the distro, then stops it)"
    cd "${REPO_DIR}"
    ./gradlew :opensearch-sql-plugin:bundlePlugin -x test -x integTest -q
    # Start and immediately stop to create the distro
    timeout 120 ./gradlew :opensearch-sql-plugin:run 2>/dev/null &
    GRADLE_PID=$!
    echo "  Waiting for distro to be created..."
    for i in $(seq 1 60); do
        if [ -f "${DISTRO_DIR}/bin/opensearch" ]; then
            echo "  Distro created."
            break
        fi
        sleep 2
    done
    kill ${GRADLE_PID} 2>/dev/null || true
    wait ${GRADLE_PID} 2>/dev/null || true
    sleep 3
fi

if [ ! -f "${DISTRO_DIR}/bin/opensearch" ]; then
    echo -e "${RED}ERROR: Could not create OpenSearch distro.${NC}"
    echo "  Run manually:  ./gradlew :opensearch-sql-plugin:run  (then Ctrl-C after it starts)"
    exit 1
fi
echo -e "  ${GREEN}OpenSearch distro: OK${NC}"

# 0b. Download hits.parquet if needed
mkdir -p "${DATA_DIR}"
if [ ! -f "${DATA_DIR}/hits.parquet" ]; then
    echo "  Downloading hits.parquet (14GB)..."
    wget -q --show-progress -O "${DATA_DIR}/hits.parquet" \
        https://datasets.clickhouse.com/hits_compatible/hits.parquet
fi
echo -e "  ${GREEN}hits.parquet: OK ($(du -h "${DATA_DIR}/hits.parquet" | cut -f1))${NC}"

# ---------- Cleanup ----------
cleanup() {
    echo ""
    echo "Stopping nodes..."
    for NODE_DIR in /tmp/trino-disttest/node-*; do
        PID_FILE="${NODE_DIR}/pid"
        if [ -f "${PID_FILE}" ]; then
            PID=$(cat "${PID_FILE}" 2>/dev/null)
            if [ -n "${PID}" ] && kill -0 "${PID}" 2>/dev/null; then
                kill "${PID}" 2>/dev/null || true
            fi
        fi
    done
}
trap cleanup EXIT

# ---------- Helper: run SQL ----------
run_sql() {
    local sql="$1"
    local catalog="${2:-iceberg}"
    local schema="${3:-clickbench}"
    local port="${4:-${COORD_PORT}}"
    local timeout="${5:-600}"

    curl -s -X POST "http://localhost:${port}/_plugins/_trino_sql/v1/statement" \
        -H "Content-Type: application/json" \
        -H "X-Trino-Catalog: ${catalog}" \
        -H "X-Trino-Schema: ${schema}" \
        -d "${sql}" \
        --max-time "${timeout}" 2>/dev/null
}

get_state() {
    echo "$1" | python3 -c "import json,sys; print(json.load(sys.stdin).get('stats',{}).get('state','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN"
}

get_node_stat() {
    local port="$1"
    local field="$2"
    local stats
    stats=$(curl -s "http://localhost:${port}/_plugins/_trino_sql/v1/node/stats" --max-time 5 2>/dev/null)
    echo "${stats}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('${field}',0))" 2>/dev/null || echo "0"
}

# ---------- Step 1: Start 3-node cluster ----------
echo ""
echo "Step 1: Starting ${NODES}-node cluster (${HEAP}/node)..."

BASE_HTTP=9310
BASE_TRANSPORT=9410

SEED_HOSTS=""
for i in $(seq 0 $((NODES-1))); do
    TP=$((BASE_TRANSPORT + i))
    [ -n "${SEED_HOSTS}" ] && SEED_HOSTS="${SEED_HOSTS},"
    SEED_HOSTS="${SEED_HOSTS}127.0.0.1:${TP}"
done

mkdir -p /tmp/trino-spill

HTTP_PORTS=()
for i in $(seq 0 $((NODES-1))); do
    HP=$((BASE_HTTP + i))
    TP=$((BASE_TRANSPORT + i))
    HTTP_PORTS+=("${HP}")

    NODE_DIR="/tmp/trino-disttest/node-${i}"
    rm -rf "${NODE_DIR}"
    mkdir -p "${NODE_DIR}/data" "${NODE_DIR}/logs"
    cp -r "${BASE_CONFIG}" "${NODE_DIR}/config"

    cat > "${NODE_DIR}/config/opensearch.yml" << NODECONF
cluster.name: trino-disttest
node.name: node-${i}
discovery.seed_hosts: ${SEED_HOSTS}
cluster.initial_cluster_manager_nodes: [node-0]
http.port: ${HP}
transport.port: ${TP}
path.data: ${NODE_DIR}/data
path.logs: ${NODE_DIR}/logs
plugins.trino.enabled: true
cluster.routing.allocation.disk.watermark.low: 1b
cluster.routing.allocation.disk.watermark.high: 1b
cluster.routing.allocation.disk.watermark.flood_stage: 1b
indices.breaker.total.use_real_memory: false
NODECONF

    export OPENSEARCH_PATH_CONF="${NODE_DIR}/config"
    export OPENSEARCH_JAVA_OPTS="-Xms${HEAP} -Xmx${HEAP} -Dtrino.iceberg.warehouse=${WAREHOUSE} -XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:+ExitOnOutOfMemoryError"

    "${DISTRO_DIR}/bin/opensearch" -d -p "${NODE_DIR}/pid" > /dev/null 2>&1 &
    echo "  Node ${i}: HTTP=${HP}, Transport=${TP}"
done

COORD_PORT="${HTTP_PORTS[0]}"
echo "  Waiting for cluster..."
for i in $(seq 1 120); do
    HEALTH=$(curl -s "http://localhost:${COORD_PORT}/_cluster/health" --max-time 3 2>/dev/null || true)
    if echo "${HEALTH}" | grep -q "\"number_of_nodes\":${NODES}"; then
        echo -e "  ${GREEN}Cluster formed: ${NODES} nodes${NC}"
        break
    fi
    if [ $i -eq 120 ]; then
        echo -e "${RED}ERROR: Cluster did not form${NC}"
        exit 1
    fi
    sleep 2
done

echo "  Waiting 20s for Trino engines + node registration..."
sleep 20

# ---------- Step 2: Verify Trino + check node count ----------
echo ""
echo "Step 2: Verify Trino engine..."

R=$(run_sql "SELECT 1" "tpch" "tiny")
S=$(get_state "$R")
if [ "${S}" != "FINISHED" ]; then
    echo -e "  ${RED}SELECT 1 FAILED (state=${S})${NC}"
    echo "  ${R}" | head -3
    exit 1
fi
echo -e "  ${GREEN}SELECT 1: OK${NC}"

echo ""
echo -e "${BOLD}Step 3: How many nodes does Trino's scheduler see?${NC}"
R=$(run_sql "SELECT node_id, http_uri, state FROM system.runtime.nodes" "system" "runtime")
S=$(get_state "$R")
if [ "${S}" = "FINISHED" ]; then
    echo "${R}" | python3 -c "
import json,sys
data = json.load(sys.stdin)
rows = data.get('data',[])
print(f'  Trino sees {len(rows)} node(s):')
for row in rows:
    print(f'    node_id={row[0]}, http_uri={row[1]}, state={row[2]}')
" 2>/dev/null || echo "  (parse error)"
    TRINO_NODE_COUNT=$(echo "${R}" | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('data',[])))" 2>/dev/null || echo "1")
    if [ "${TRINO_NODE_COUNT}" -le 1 ]; then
        echo -e "  ${YELLOW}WARNING: Trino sees only ${TRINO_NODE_COUNT} node — distribution will NOT happen${NC}"
    else
        echo -e "  ${GREEN}Trino sees ${TRINO_NODE_COUNT} nodes — distribution possible${NC}"
    fi
else
    echo -e "  ${YELLOW}system.runtime.nodes query failed${NC}"
fi

# ---------- Step 4: Set up Iceberg table if needed ----------
echo ""
echo "Step 4: Set up Iceberg ClickBench table..."

# Check if table already exists
R=$(run_sql "SELECT COUNT(*) FROM iceberg.clickbench.hits" "iceberg" "clickbench")
S=$(get_state "$R")
if [ "${S}" = "FINISHED" ]; then
    COUNT=$(echo "$R" | python3 -c "import json,sys; print(json.load(sys.stdin)['data'][0][0])" 2>/dev/null || echo "0")
    echo -e "  ${GREEN}Iceberg table exists: ${COUNT} rows${NC}"
else
    echo "  Iceberg table not found. Creating from Parquet..."

    # Create Hive schema + external table
    echo "  Creating Hive external table..."
    R=$(run_sql "CREATE SCHEMA IF NOT EXISTS hive.clickbench WITH (location = 'file://${DATA_DIR}')" "hive" "default")
    S=$(get_state "$R")
    echo "  Hive schema: ${S}"

    R=$(run_sql "CREATE TABLE IF NOT EXISTS hive.clickbench.hits (
        WatchID BIGINT, JavaEnable SMALLINT, Title VARCHAR, GoodEvent SMALLINT,
        EventTime TIMESTAMP, EventDate DATE, CounterID INTEGER, ClientIP INTEGER,
        RegionID INTEGER, UserID BIGINT, CounterClass SMALLINT, OS SMALLINT,
        UserAgent SMALLINT, URL VARCHAR, Referer VARCHAR, IsRefresh SMALLINT,
        RefererCategoryID SMALLINT, RefererRegionID INTEGER, URLCategoryID SMALLINT,
        URLRegionID INTEGER, ResolutionWidth SMALLINT, ResolutionHeight SMALLINT,
        ResolutionDepth SMALLINT, FlashMajor SMALLINT, FlashMinor SMALLINT,
        FlashMinor2 VARCHAR, NetMajor SMALLINT, NetMinor SMALLINT,
        UserAgentMajor SMALLINT, UserAgentMinor VARCHAR, CookieEnable SMALLINT,
        JavascriptEnable SMALLINT, IsMobile SMALLINT, MobilePhone SMALLINT,
        MobilePhoneModel VARCHAR, Params VARCHAR, IPNetworkID INTEGER,
        TraficSourceID SMALLINT, SearchEngineID SMALLINT, SearchPhrase VARCHAR,
        AdvEngineID SMALLINT, IsArtifical SMALLINT, WindowClientWidth SMALLINT,
        WindowClientHeight SMALLINT, ClientTimeZone SMALLINT, ClientEventTime TIMESTAMP,
        SilverlightVersion1 SMALLINT, SilverlightVersion2 SMALLINT,
        SilverlightVersion3 INTEGER, SilverlightVersion4 SMALLINT,
        PageCharset VARCHAR, CodeVersion INTEGER, IsLink SMALLINT,
        IsDownload SMALLINT, IsNotBounce SMALLINT, FUniqID BIGINT,
        OriginalURL VARCHAR, HID INTEGER, IsOldCounter SMALLINT,
        IsEvent SMALLINT, IsParameter SMALLINT, DontCountHits SMALLINT,
        WithHash SMALLINT, HitColor VARCHAR, LocalEventTime TIMESTAMP,
        Age SMALLINT, Sex SMALLINT, Income SMALLINT, Interests SMALLINT,
        Robotness SMALLINT, RemoteIP INTEGER, WindowName INTEGER,
        OpenerName INTEGER, HistoryLength SMALLINT, BrowserLanguage VARCHAR,
        BrowserCountry VARCHAR, SocialNetwork VARCHAR, SocialAction VARCHAR,
        HTTPError SMALLINT, SendTiming INTEGER, DNSTiming INTEGER,
        ConnectTiming INTEGER, ResponseStartTiming INTEGER,
        ResponseEndTiming INTEGER, FetchTiming INTEGER,
        SocialSourceNetworkID SMALLINT, SocialSourcePage VARCHAR,
        ParamPrice BIGINT, ParamOrderID VARCHAR, ParamCurrency VARCHAR,
        ParamCurrencyID SMALLINT, OpenstatServiceName VARCHAR,
        OpenstatCampaignID VARCHAR, OpenstatAdID VARCHAR,
        OpenstatSourceID VARCHAR, UTMSource VARCHAR, UTMMedium VARCHAR,
        UTMCampaign VARCHAR, UTMContent VARCHAR, UTMTerm VARCHAR,
        FromTag VARCHAR, HasGCLID SMALLINT, RefererHash BIGINT,
        URLHash BIGINT, CLID INTEGER
    ) WITH (external_location = 'file://${DATA_DIR}', format = 'PARQUET')" "hive" "clickbench")
    S=$(get_state "$R")
    echo "  Hive table: ${S}"
    if [ "${S}" != "FINISHED" ]; then
        echo "  Error: ${R}" | head -3
        exit 1
    fi

    # Verify Hive table
    R=$(run_sql "SELECT COUNT(*) FROM hive.clickbench.hits" "hive" "clickbench")
    COUNT=$(echo "$R" | python3 -c "import json,sys; print(json.load(sys.stdin)['data'][0][0])" 2>/dev/null || echo "?")
    echo "  Hive row count: ${COUNT}"

    # Create Iceberg table via CTAS
    echo "  Creating Iceberg table from Hive (this takes several minutes)..."
    R=$(run_sql "CREATE SCHEMA IF NOT EXISTS iceberg.clickbench" "iceberg" "default")
    echo "  Iceberg schema: $(get_state "$R")"

    echo "  Running CTAS..."
    time R=$(run_sql "CREATE TABLE iceberg.clickbench.hits WITH (format = 'PARQUET') AS SELECT * FROM hive.clickbench.hits" "iceberg" "clickbench" "${COORD_PORT}" "3600")
    S=$(get_state "$R")
    echo "  CTAS result: ${S}"
    if [ "${S}" != "FINISHED" ]; then
        echo -e "  ${RED}CTAS FAILED${NC}"
        echo "  ${R}" | head -5
        exit 1
    fi

    R=$(run_sql "SELECT COUNT(*) FROM iceberg.clickbench.hits" "iceberg" "clickbench")
    COUNT=$(echo "$R" | python3 -c "import json,sys; print(json.load(sys.stdin)['data'][0][0])" 2>/dev/null || echo "?")
    echo -e "  ${GREEN}Iceberg table ready: ${COUNT} rows${NC}"
fi

# ---------- Step 5: Baseline node stats ----------
echo ""
echo "Step 5: Baseline node stats (BEFORE queries)..."

declare -A BEFORE_TASKS
declare -A BEFORE_SPLITS
declare -A BEFORE_UPDATES

for i in $(seq 0 $((NODES-1))); do
    HP="${HTTP_PORTS[$i]}"
    TASKS=$(get_node_stat "${HP}" "tasksReceived")
    SPLITS=$(get_node_stat "${HP}" "splitsProcessed")
    UPDATES=$(get_node_stat "${HP}" "taskUpdatesReceived")
    BEFORE_TASKS[$i]="${TASKS}"
    BEFORE_SPLITS[$i]="${SPLITS}"
    BEFORE_UPDATES[$i]="${UPDATES}"
    echo "  node-${i} (port ${HP}): tasks=${TASKS}, splits=${SPLITS}, updates=${UPDATES}"
done

# ---------- Step 6: Run ClickBench queries ----------
echo ""
echo -e "${BOLD}Step 6: Running ClickBench queries...${NC}"
echo ""

QUERIES=(
    "Q0:SELECT COUNT(*) FROM hits"
    "Q1:SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0"
    "Q7:SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC"
    "Q9:SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10"
    "Q15:SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10"
    "Q12:SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10"
)

for entry in "${QUERIES[@]}"; do
    QNAME="${entry%%:*}"
    SQL="${entry#*:}"

    START_MS=$(date +%s%3N)
    R=$(run_sql "${SQL}" "iceberg" "clickbench")
    END_MS=$(date +%s%3N)
    ELAPSED=$(( END_MS - START_MS ))
    S=$(get_state "$R")

    if [ "${S}" = "FINISHED" ]; then
        ROWS=$(echo "$R" | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('data',[])))" 2>/dev/null || echo "?")
        echo -e "  ${GREEN}${QNAME}: OK — ${ROWS} rows, ${ELAPSED}ms${NC}"
    else
        MSG=$(echo "$R" | python3 -c "import json,sys; print(json.load(sys.stdin).get('error',{}).get('message','?')[:80])" 2>/dev/null || echo "?")
        echo -e "  ${RED}${QNAME}: FAILED (${S}) — ${MSG}${NC}"
    fi
done

# ---------- Step 7: AFTER node stats + verdict ----------
echo ""
echo -e "${BOLD}Step 7: Node stats AFTER queries — distribution proof${NC}"
echo ""

WORKER_WITH_WORK=0
for i in $(seq 0 $((NODES-1))); do
    HP="${HTTP_PORTS[$i]}"
    TASKS=$(get_node_stat "${HP}" "tasksReceived")
    SPLITS=$(get_node_stat "${HP}" "splitsProcessed")
    UPDATES=$(get_node_stat "${HP}" "taskUpdatesReceived")
    RESULTS=$(get_node_stat "${HP}" "resultsFetched")

    DT=$(( TASKS - ${BEFORE_TASKS[$i]} ))
    DS=$(( SPLITS - ${BEFORE_SPLITS[$i]} ))
    DU=$(( UPDATES - ${BEFORE_UPDATES[$i]} ))

    LABEL="coordinator"
    [ $i -gt 0 ] && LABEL="worker    "

    if [ $i -gt 0 ] && [ "${DT}" -gt 0 ]; then
        echo -e "  ${GREEN}node-${i} (${LABEL}): +${DT} tasks, +${DS} splits, +${DU} updates, results=${RESULTS}  ← RECEIVED WORK${NC}"
        WORKER_WITH_WORK=$((WORKER_WITH_WORK + 1))
    else
        COLOR="${NC}"
        [ $i -gt 0 ] && [ "${DT}" -eq 0 ] && COLOR="${YELLOW}"
        echo -e "  ${COLOR}node-${i} (${LABEL}): +${DT} tasks, +${DS} splits, +${DU} updates, results=${RESULTS}${NC}"
    fi
done

# ---------- Step 8: Log evidence ----------
echo ""
echo "Step 8: Log evidence..."
echo ""
echo "  Coordinator (node-0) — TransportRemoteTask lines:"
grep -c "TransportRemoteTask for remote" /tmp/trino-disttest/node-0/logs/trino-disttest.log 2>/dev/null | \
    xargs -I{} echo "    {} lines found"
grep "TransportRemoteTask for remote" /tmp/trino-disttest/node-0/logs/trino-disttest.log 2>/dev/null | \
    tail -5 | sed 's/^/    /'
echo ""
echo "  Coordinator — split-level distribution status:"
grep -E "(split-level|Split-level|patched|NodeManager)" /tmp/trino-disttest/node-0/logs/trino-disttest.log 2>/dev/null | \
    tail -5 | sed 's/^/    /'
echo ""
echo "  Worker node-1 — task update handler:"
grep "Task update handler" /tmp/trino-disttest/node-1/logs/trino-disttest.log 2>/dev/null | \
    tail -5 | sed 's/^/    /'
echo ""
echo "  Worker node-2 — task update handler:"
grep "Task update handler" /tmp/trino-disttest/node-2/logs/trino-disttest.log 2>/dev/null | \
    tail -5 | sed 's/^/    /'

# ---------- Verdict ----------
echo ""
echo "============================================"
if [ "${WORKER_WITH_WORK}" -gt 0 ]; then
    echo -e " ${GREEN}${BOLD}PASS: Split-level distribution CONFIRMED${NC}"
    echo -e " ${WORKER_WITH_WORK} worker node(s) received tasks."
    echo " Splits were dispatched from coordinator to remote nodes"
    echo " via OpenSearch TransportActions."
else
    echo -e " ${RED}${BOLD}FAIL: No worker node received any tasks${NC}"
    echo " All work ran on the coordinator only."
    echo ""
    echo " Debug checklist:"
    echo "   1. Does system.runtime.nodes show ${NODES} nodes?"
    echo "   2. grep 'Split-level distribution enabled' node-0 log"
    echo "   3. grep 'TransportRemoteTask for remote' node-0 log"
    echo "   4. grep 'Registered Trino HTTP URL' node-0 log"
    echo "   5. grep 'Task update handler' node-1 log"
fi
echo "============================================"
