#!/bin/bash
# ClickBench Benchmark for Trino-OpenSearch — Iceberg Table
#
# Runs all 43 ClickBench queries against an Iceberg table
# (created by create_iceberg_table.py from hits.parquet).
#
# Prerequisites:
#   - Iceberg table created: python3 trino/benchmark/create_iceberg_table.py /tmp/iceberg-clickbench-warehouse
#   - Plugin built: ./gradlew :opensearch-sql-plugin:bundlePlugin
#
# Usage:
#   ./trino/benchmark/run_clickbench_iceberg.sh
#
# Environment variables:
#   OPENSEARCH_HEAP  — JVM heap size (default: 32g)
#   TRIES            — runs per query (default: 1)
#   ICEBERG_WAREHOUSE — path to Iceberg warehouse (default: /tmp/iceberg-clickbench-warehouse)
#   SKIP_START       — set to 1 to skip starting OpenSearch (use existing instance)
#   OS_PORT          — port of existing OpenSearch (used with SKIP_START=1)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
QUERIES_FILE="${REPO_DIR}/trino/trino-integ-test/src/test/resources/clickbench/queries.sql"
HEAP="${OPENSEARCH_HEAP:-32g}"
TRIES="${TRIES:-1}"
WAREHOUSE="${ICEBERG_WAREHOUSE:-/tmp/iceberg-clickbench-warehouse}"
SKIP_START="${SKIP_START:-0}"

# ---------- Validation ----------
if [ ! -d "${WAREHOUSE}" ]; then
    echo "ERROR: Iceberg warehouse not found at ${WAREHOUSE}"
    echo "Create it: python3 trino/benchmark/create_iceberg_table.py ${WAREHOUSE}"
    exit 1
fi

if [ ! -f "${QUERIES_FILE}" ]; then
    echo "ERROR: ${QUERIES_FILE} not found"
    exit 1
fi

echo "=== ClickBench Benchmark (Iceberg) for Trino-OpenSearch ==="
echo "Warehouse: ${WAREHOUSE}"
echo "Heap:      ${HEAP}"
echo "Tries:     ${TRIES}"
echo ""

OS_PID=""

if [ "${SKIP_START}" = "1" ]; then
    OS_PORT="${OS_PORT:-}"
    if [ -z "${OS_PORT}" ]; then
        for port in 9201 9202 9200; do
            if curl -s "http://localhost:${port}/_cluster/health" 2>/dev/null | grep -q "status"; then
                OS_PORT="${port}"
                break
            fi
        done
    fi
    if [ -z "${OS_PORT}" ]; then
        echo "ERROR: No running OpenSearch found. Set OS_PORT or unset SKIP_START."
        exit 1
    fi
    echo "Using existing OpenSearch on port ${OS_PORT}"
else
    # ---------- Step 1: Start OpenSearch ----------
    echo "Step 1: Starting OpenSearch with Trino plugin (${HEAP} heap, Iceberg warehouse)..."

    if ! ls "${REPO_DIR}/plugin/build/distributions/opensearch-sql-"*.zip &>/dev/null; then
        echo "  Building plugin..."
        (cd "${REPO_DIR}" && ./gradlew :opensearch-sql-plugin:bundlePlugin -q)
    fi

    CLUSTER_DIR="${REPO_DIR}/plugin/build/testclusters/integTest-0"
    DISTRO_DIR="${CLUSTER_DIR}/distro/3.6.0-ARCHIVE"
    if [ ! -d "${DISTRO_DIR}" ]; then
        echo "ERROR: OpenSearch distribution not found at ${DISTRO_DIR}"
        echo "Run first: ./gradlew :opensearch-sql-plugin:run -DenableTrino (then Ctrl+C)"
        exit 1
    fi

    CONF="${CLUSTER_DIR}/config/opensearch.yml"
    grep -q "plugins.trino.enabled" "${CONF}" || echo "plugins.trino.enabled: true" >> "${CONF}"
    if grep -q "plugins.trino.catalog.iceberg.warehouse" "${CONF}"; then
        sed -i "s|plugins.trino.catalog.iceberg.warehouse:.*|plugins.trino.catalog.iceberg.warehouse: ${WAREHOUSE}|" "${CONF}"
    else
        echo "plugins.trino.catalog.iceberg.warehouse: ${WAREHOUSE}" >> "${CONF}"
    fi

    cp "${REPO_DIR}/trino/trino-opensearch/build/libs/trino-opensearch-"*.jar \
       "${DISTRO_DIR}/plugins/opensearch-sql/" 2>/dev/null || true

    rm -rf "${CLUSTER_DIR}/data"
    export OPENSEARCH_JAVA_OPTS="-Xms${HEAP} -Xmx${HEAP}"

    pkill -f "penghuo.*opensearch.*bootstrap" 2>/dev/null || true
    sleep 2

    nohup "${DISTRO_DIR}/bin/opensearch" > /tmp/clickbench-os-iceberg.log 2>&1 &
    OS_PID=$!
    echo "  OpenSearch PID: ${OS_PID}"

    OS_PORT=""
    for i in $(seq 1 120); do
        OS_PORT=$(grep "publish_address.*http" /tmp/clickbench-os-iceberg.log 2>/dev/null | \
                  grep -oP '127\.0\.0\.1:\K\d+' | tail -1)
        if [ -n "${OS_PORT}" ]; then
            if curl -s "http://localhost:${OS_PORT}/_plugins/_trino_sql/v1/statement" \
                -X POST -H "Content-Type: application/json" \
                -H "X-Trino-Catalog: tpch" -H "X-Trino-Schema: tiny" \
                -d '{"query":"SELECT 1"}' 2>/dev/null | grep -q "FINISHED"; then
                break
            fi
        fi
        if [ $i -eq 120 ]; then
            echo "ERROR: OpenSearch failed to start. Check /tmp/clickbench-os-iceberg.log"
            exit 1
        fi
        sleep 2
    done
    echo "  Port: ${OS_PORT}"
fi

ENDPOINT="http://localhost:${OS_PORT}/_plugins/_trino_sql/v1/statement"
echo "  Endpoint: ${ENDPOINT}"
echo ""

# ---------- Step 2: Verify Iceberg table ----------
echo "Step 2: Verifying Iceberg table..."
VERIFY=$(curl -s -X POST "${ENDPOINT}" \
    -H "Content-Type: application/json" \
    -H "X-Trino-Catalog: iceberg" -H "X-Trino-Schema: clickbench" \
    -d '{"query":"SELECT COUNT(*) FROM iceberg.clickbench.hits"}' \
    --max-time 120)

if echo "$VERIFY" | grep -q "FINISHED"; then
    COUNT=$(echo "$VERIFY" | python3 -c "import json,sys; print(json.load(sys.stdin)['data'][0][0])")
    echo "  iceberg.clickbench.hits: ${COUNT} rows"
else
    echo "ERROR: Cannot access iceberg.clickbench.hits"
    echo "Response: ${VERIFY}"
    exit 1
fi
echo ""

# ---------- Step 3: Run queries ----------
echo "Step 3: Running 43 ClickBench queries against Iceberg table..."
echo ""

python3 - "${ENDPOINT}" "${TRIES}" "${QUERIES_FILE}" << 'PYEOF'
import json, urllib.request, time, sys

endpoint = sys.argv[1]
tries = int(sys.argv[2])
queries_file = sys.argv[3]

queries_raw = open(queries_file).readlines()
queries = [q.strip().rstrip(';') for q in queries_raw if q.strip() and not q.strip().startswith('--')]

def run_sql(sql, timeout=600):
    body = json.dumps({"query": sql}).encode()
    req = urllib.request.Request(endpoint, data=body,
        headers={"Content-Type": "application/json",
                 "X-Trino-Catalog": "iceberg", "X-Trino-Schema": "clickbench"})
    resp = urllib.request.urlopen(req, timeout=timeout).read().decode()
    return json.loads(resp)

# Header
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
if tries > 1:
    print(f"Total time: {total:.0f}s (best of {tries})")
else:
    print(f"Total time: {total:.0f}s")
print(f"Average: {total/max(passed,1):.1f}s per query")

results_json = {
    "passed": passed, "failed": failed, "total_time": total,
    "queries": [{"query": i, "time": t, "status": s} for i, t, s in results]
}
with open("/tmp/clickbench-iceberg-results.json", "w") as f:
    json.dump(results_json, f, indent=2)
print(f"\nResults saved to /tmp/clickbench-iceberg-results.json")
PYEOF

# ---------- Step 4: Cleanup ----------
if [ -n "${OS_PID}" ]; then
    echo ""
    echo "Stopping OpenSearch (PID: ${OS_PID})..."
    kill ${OS_PID} 2>/dev/null || true
fi
echo "Done."
