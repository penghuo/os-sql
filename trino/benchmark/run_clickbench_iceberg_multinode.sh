#!/bin/bash
# ClickBench Multi-Node Benchmark for Trino-OpenSearch
#
# Starts a 3-node OpenSearch cluster on localhost (different ports)
# and runs all 43 ClickBench queries. Each node runs the Trino plugin.
#
# Prerequisites:
#   - Iceberg table created: python3 trino/benchmark/create_iceberg_table.py
#   - Plugin built: ./gradlew :opensearch-sql-plugin:bundlePlugin
#   - Test cluster set up: ./gradlew :opensearch-sql-plugin:run (then Ctrl-C)
#
# Usage:
#   ./trino/benchmark/run_clickbench_iceberg_multinode.sh
#
# Environment:
#   NODES            — number of nodes (default: 3)
#   HEAP_PER_NODE    — JVM heap per node (default: 10g)
#   TRIES            — runs per query (default: 1)
#   ICEBERG_WAREHOUSE — Iceberg warehouse path

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
QUERIES_FILE="${REPO_DIR}/trino/trino-integ-test/src/test/resources/clickbench/queries.sql"
NODES="${NODES:-3}"
HEAP="${HEAP_PER_NODE:-24g}"
TRIES="${TRIES:-1}"
WAREHOUSE="${ICEBERG_WAREHOUSE:-/tmp/iceberg-clickbench-warehouse}"

DISTRO_DIR="${REPO_DIR}/plugin/build/testclusters/integTest-0/distro/3.6.0-ARCHIVE"
BASE_CONFIG="${REPO_DIR}/plugin/build/testclusters/integTest-0/config"

echo "=== ClickBench Multi-Node Benchmark (${NODES} nodes) ==="
echo "Warehouse: ${WAREHOUSE}"
echo "Heap/node: ${HEAP}"
echo "Tries:     ${TRIES}"
echo ""

# ---------- Validation ----------
if [ ! -d "${WAREHOUSE}" ]; then
    echo "ERROR: Iceberg warehouse not found at ${WAREHOUSE}"
    exit 1
fi
if [ ! -f "${DISTRO_DIR}/bin/opensearch" ]; then
    echo "ERROR: OpenSearch distro not found. Run: ./gradlew :opensearch-sql-plugin:run (then Ctrl-C)"
    exit 1
fi

# ---------- Step 1: Start cluster ----------
echo "Step 1: Starting ${NODES}-node cluster..."

PIDS=()
HTTP_PORTS=()
BASE_HTTP=9210
BASE_TRANSPORT=9310

SEED_HOSTS=""
NODE_NAMES=""
for i in $(seq 0 $((NODES-1))); do
    TP=$((BASE_TRANSPORT + i))
    [ -n "${SEED_HOSTS}" ] && SEED_HOSTS="${SEED_HOSTS},"
    SEED_HOSTS="${SEED_HOSTS}127.0.0.1:${TP}"
    [ -n "${NODE_NAMES}" ] && NODE_NAMES="${NODE_NAMES},"
    NODE_NAMES="${NODE_NAMES}node-${i}"
done

# Create spill directory for Trino's spill-to-disk feature
mkdir -p /tmp/trino-spill

for i in $(seq 0 $((NODES-1))); do
    HP=$((BASE_HTTP + i))
    TP=$((BASE_TRANSPORT + i))
    HTTP_PORTS+=("${HP}")

    # Create per-node config and data dirs
    NODE_DIR="/tmp/trino-multinode/node-${i}"
    rm -rf "${NODE_DIR}"
    mkdir -p "${NODE_DIR}/data" "${NODE_DIR}/logs"

    # Copy full config directory (includes jvm.options, log4j2.properties, etc.)
    cp -r "${BASE_CONFIG}" "${NODE_DIR}/config"

    cat > "${NODE_DIR}/config/opensearch.yml" << NODECONF
cluster.name: trino-multinode
node.name: node-${i}
discovery.seed_hosts: ${SEED_HOSTS}
cluster.initial_cluster_manager_nodes: [node-0]
http.port: ${HP}
transport.port: ${TP}
path.data: ${NODE_DIR}/data
path.logs: ${NODE_DIR}/logs
plugins.trino.enabled: true
plugins.trino.catalog.iceberg.warehouse: ${WAREHOUSE}
cluster.routing.allocation.disk.watermark.low: 1b
cluster.routing.allocation.disk.watermark.high: 1b
cluster.routing.allocation.disk.watermark.flood_stage: 1b
indices.breaker.total.use_real_memory: false
NODECONF

    export OPENSEARCH_PATH_CONF="${NODE_DIR}/config"
    export OPENSEARCH_JAVA_OPTS="-Xms${HEAP} -Xmx${HEAP} -Dtrino.iceberg.warehouse=${WAREHOUSE} -XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:+ExitOnOutOfMemoryError"

    "${DISTRO_DIR}/bin/opensearch" -d -p "${NODE_DIR}/pid" &
    PIDS+=("$!")
    echo "  Node ${i}: HTTP=${HP}, Transport=${TP}, PID=$!"
done

# Wait for cluster to form
echo "  Waiting for cluster..."
COORD_PORT="${HTTP_PORTS[0]}"
for i in $(seq 1 120); do
    HEALTH=$(curl -s "http://localhost:${COORD_PORT}/_cluster/health" --max-time 3 2>/dev/null || true)
    if echo "${HEALTH}" | grep -q "\"number_of_nodes\":${NODES}"; then
        echo "  Cluster GREEN with ${NODES} nodes"
        break
    fi
    if [ $i -eq 120 ]; then
        echo "ERROR: Cluster did not form in time"
        echo "  Last health: ${HEALTH}"
        for NODE_DIR in /tmp/trino-multinode/node-*; do
            echo "  --- $(basename ${NODE_DIR}) ---"
            tail -5 "${NODE_DIR}/logs/trino-multinode.log" 2>/dev/null || true
        done
        exit 1
    fi
    sleep 2
done

# ---------- Step 2: Verify Trino on all nodes ----------
echo ""
echo "Step 2: Verifying Trino on all nodes..."
for HP in "${HTTP_PORTS[@]}"; do
    STATS=$(curl -s "http://localhost:${HP}/_plugins/_trino_sql/v1/node/stats" --max-time 5 2>/dev/null)
    echo "  Port ${HP}: ${STATS}"
done

# Create Iceberg schema
curl -s -X POST "http://localhost:${COORD_PORT}/_plugins/_trino_sql/v1/statement" \
    -H "Content-Type: application/json" -H "X-Trino-Catalog: iceberg" -H "X-Trino-Schema: default" \
    -d "CREATE SCHEMA IF NOT EXISTS iceberg.clickbench WITH (location = 'file://${WAREHOUSE}/clickbench')" \
    --max-time 30 2>/dev/null > /dev/null

# Verify table
VERIFY=$(curl -s -X POST "http://localhost:${COORD_PORT}/_plugins/_trino_sql/v1/statement" \
    -H "Content-Type: application/json" -H "X-Trino-Catalog: iceberg" -H "X-Trino-Schema: clickbench" \
    -d "SELECT COUNT(*) FROM iceberg.clickbench.hits" --max-time 120 2>/dev/null)
COUNT=$(echo "${VERIFY}" | python3 -c "import json,sys; print(json.load(sys.stdin)['data'][0][0])" 2>/dev/null)
echo "  Rows: ${COUNT}"

# ---------- Step 3: Run benchmark ----------
echo ""
echo "Step 3: Running 43 ClickBench queries (${TRIES} tries, coordinator port ${COORD_PORT})..."
echo ""

ENDPOINT="http://localhost:${COORD_PORT}/_plugins/_trino_sql/v1/statement"

python3 - "${ENDPOINT}" "${TRIES}" "${QUERIES_FILE}" "${NODES}" << 'PYEOF'
import json, urllib.request, time, sys

endpoint = sys.argv[1]
tries = int(sys.argv[2])
queries_file = sys.argv[3]
num_nodes = sys.argv[4]

queries_raw = open(queries_file).readlines()
queries = [q.strip().rstrip(';') for q in queries_raw if q.strip() and not q.strip().startswith('--')]

def run_sql(sql, timeout=600):
    body = json.dumps({"query": sql}).encode()
    req = urllib.request.Request(endpoint, data=body,
        headers={"Content-Type": "application/json",
                 "X-Trino-Catalog": "iceberg", "X-Trino-Schema": "clickbench"})
    resp = urllib.request.urlopen(req, timeout=timeout).read().decode()
    return json.loads(resp)

print(f"{'Query':<7} ", end="")
for t in range(1, tries+1):
    print(f"{'Try'+str(t):>10}", end="")
if tries > 1:
    print(f"  {'Min':>8} {'Med':>8} {'Max':>8}", end="")
print(f"  {'Status':<6}")
print("-" * (7 + tries*10 + (24 if tries > 1 else 0) + 8))

passed = failed = 0
total = 0
results = []

for i, raw in enumerate(queries):
    sql = raw
    times = []
    ok = True
    err_msg = ""

    for t in range(tries):
        s = time.time()
        try:
            r = run_sql(sql, timeout=600)
            e = time.time() - s
            if r.get("stats",{}).get("state") == "FINISHED":
                times.append(e)
            else:
                ok = False
                err_msg = r.get("error",{}).get("message","?")[:60]
                times.append(-1)
        except Exception as ex:
            ok = False
            err_msg = str(ex)[:60]
            times.append(-1)

    print(f"Q{i:<5} ", end="")
    for t_val in times:
        if t_val >= 0:
            print(f"{t_val:>9.1f}s", end="")
        else:
            print(f"{'FAIL':>10}", end="")

    if ok:
        sorted_t = sorted(times)
        mn = sorted_t[0]
        if tries > 1:
            med, mx = sorted_t[len(sorted_t)//2], sorted_t[-1]
            print(f"  {mn:>7.1f}s {med:>7.1f}s {mx:>7.1f}s", end="")
        print(f"  OK")
        passed += 1
        total += (mn if tries > 1 else times[0])
        results.append((i, mn if tries > 1 else times[0], "OK"))
    else:
        if tries > 1:
            print(f"  {'':>8} {'':>8} {'':>8}", end="")
        print(f"  FAIL ({err_msg})")
        failed += 1
        results.append((i, -1, f"FAIL: {err_msg}"))

print("-" * (7 + tries*10 + (24 if tries > 1 else 0) + 8))
print(f"\nPassed: {passed}/43, Failed: {failed}/43")
print(f"Total time: {total:.0f}s")
print(f"Average: {total/max(passed,1):.1f}s per query")
print(f"Cluster: {num_nodes} nodes")

results_json = {
    "passed": passed, "failed": failed, "total_time": total,
    "nodes": int(num_nodes),
    "queries": [{"query": i, "time": t, "status": s} for i, t, s in results]
}
with open("/tmp/clickbench-multinode-results.json", "w") as f:
    json.dump(results_json, f, indent=2)
print(f"\nResults saved to /tmp/clickbench-multinode-results.json")
PYEOF

# ---------- Step 4: Node stats ----------
echo ""
echo "Step 4: Node stats after benchmark..."
for HP in "${HTTP_PORTS[@]}"; do
    STATS=$(curl -s "http://localhost:${HP}/_plugins/_trino_sql/v1/node/stats" --max-time 5 2>/dev/null)
    echo "  Port ${HP}: ${STATS}"
done

# ---------- Step 5: Cleanup ----------
echo ""
echo "Stopping nodes..."
for NODE_DIR in /tmp/trino-multinode/node-*; do
    PID=$(cat "${NODE_DIR}/pid" 2>/dev/null)
    if [ -n "${PID}" ]; then
        kill "${PID}" 2>/dev/null || true
    fi
done
echo "Done."
