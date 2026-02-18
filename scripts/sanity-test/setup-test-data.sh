#!/usr/bin/env bash
# -------------------------------------------------------------------
# Phase 1 Distributed Engine — Manual Sanity Test Data Setup
#
# Prerequisites:
#   - OpenSearch running locally on port 9200
#   - SQL plugin installed with distributed engine support
#
# Usage:
#   ./setup-test-data.sh [HOST]
#   HOST defaults to localhost:9200
# -------------------------------------------------------------------
set -euo pipefail

HOST="${1:-localhost:9200}"

echo "=== Distributed Engine Sanity Test Setup ==="
echo "Target: $HOST"
echo ""

# -------------------------------------------------------------------
# 1. Enable Calcite + Distributed Engine + Strict Mode
# -------------------------------------------------------------------
echo "[1/7] Enabling Calcite, distributed engine, and STRICT MODE..."
curl -s -X PUT "$HOST/_cluster/settings" \
  -H 'Content-Type: application/json' \
  -d '{
    "transient": {
      "plugins.calcite.enabled": true,
      "plugins.sql.distributed_engine.enabled": true,
      "plugins.sql.distributed_engine.strict_mode": true
    }
  }' | python3 -m json.tool 2>/dev/null || true
echo ""

echo "  *** strict_mode=true: any silent fallback to DSL will FAIL the query ***"
echo "  *** This proves the distributed engine is genuinely executing queries ***"
echo ""

# -------------------------------------------------------------------
# 2. Verify settings took effect
# -------------------------------------------------------------------
echo "[2/7] Verifying cluster settings..."
curl -s "$HOST/_cluster/settings?flat_settings=true" | python3 -c "
import sys, json
settings = json.load(sys.stdin)
transient = settings.get('transient', {})
checks = {
    'plugins.calcite.enabled': 'true',
    'plugins.sql.distributed_engine.enabled': 'true',
    'plugins.sql.distributed_engine.strict_mode': 'true',
}
all_ok = True
for key, expected in checks.items():
    actual = transient.get(key, 'NOT SET')
    status = 'OK' if actual == expected else 'FAIL'
    if status == 'FAIL':
        all_ok = False
    print(f'  {key} = {actual} [{status}]')
if not all_ok:
    print()
    print('  ERROR: Some settings did not apply. Is the plugin installed?')
    sys.exit(1)
" 2>/dev/null
echo ""

# -------------------------------------------------------------------
# 3. Create sanity_employees — 5 shards, mixed types
# -------------------------------------------------------------------
INDEX_EMPLOYEES="sanity_employees"
echo "[3/7] Creating index: $INDEX_EMPLOYEES (5 shards)..."

curl -s -X DELETE "$HOST/$INDEX_EMPLOYEES" 2>/dev/null || true

curl -s -X PUT "$HOST/$INDEX_EMPLOYEES" \
  -H 'Content-Type: application/json' \
  -d '{
    "settings": {
      "number_of_shards": 5,
      "number_of_replicas": 0
    },
    "mappings": {
      "properties": {
        "emp_id":      { "type": "integer" },
        "name":        { "type": "keyword" },
        "dept":        { "type": "keyword" },
        "city":        { "type": "keyword" },
        "age":         { "type": "integer" },
        "salary":      { "type": "double" },
        "is_active":   { "type": "boolean" },
        "hire_date":   { "type": "date", "format": "yyyy-MM-dd" },
        "description": { "type": "text" }
      }
    }
  }' | python3 -m json.tool 2>/dev/null || true
echo ""

# -------------------------------------------------------------------
# 4. Bulk-load 50 deterministic documents into sanity_employees
# -------------------------------------------------------------------
echo "[4/7] Loading 50 documents into $INDEX_EMPLOYEES..."

BULK_BODY=""

NAMES=("Alice" "Bob" "Carol" "Dave" "Eve" "Frank" "Grace" "Heidi" "Ivan" "Judy"
       "Karl" "Liam" "Mona" "Nick" "Olivia" "Pat" "Quinn" "Rosa" "Sam" "Tina"
       "Uma" "Vince" "Wendy" "Xander" "Yuki" "Zara" "Aaron" "Beth" "Craig" "Dana"
       "Eli" "Faye" "Gus" "Hope" "Ian" "Jade" "Kyle" "Luna" "Max" "Nina"
       "Omar" "Pia" "Reed" "Sara" "Tom" "Uri" "Val" "Walt" "Xena" "Yuri")

DEPTS=("Engineering" "Sales" "Marketing" "Support" "HR")
CITIES=("Seattle" "Portland" "SanFrancisco" "NewYork" "Austin")

for i in $(seq 0 49); do
  age=$((25 + i % 35))
  salary=$(echo "40000 + $i * 1000" | bc)
  dept_idx=$((i % 5))
  city_idx=$(( (i / 5) % 5 ))
  is_active="true"
  if (( i % 7 == 0 )); then is_active="false"; fi

  # Hire dates spread across 2020-2024
  year=$((2020 + i % 5))
  month=$(printf "%02d" $(( (i % 12) + 1 )))
  day=$(printf "%02d" $(( (i % 28) + 1 )))

  BULK_BODY+='{"index":{"_id":"'"$i"'"}}'$'\n'
  BULK_BODY+='{"emp_id":'"$i"',"name":"'"${NAMES[$i]}"'","dept":"'"${DEPTS[$dept_idx]}"'","city":"'"${CITIES[$city_idx]}"'","age":'"$age"',"salary":'"$salary"'.0,"is_active":'"$is_active"',"hire_date":"'"$year-$month-$day"'","description":"Employee '"${NAMES[$i]}"' in '"${DEPTS[$dept_idx]}"' department"}'$'\n'
done

echo "$BULK_BODY" | curl -s -X POST "$HOST/$INDEX_EMPLOYEES/_bulk" \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @- | python3 -c "
import sys, json
resp = json.load(sys.stdin)
print(f'  Loaded: {len(resp.get(\"items\",[]))} docs, errors: {resp.get(\"errors\",False)}')
" 2>/dev/null || echo "  Bulk load sent"

curl -s -X POST "$HOST/$INDEX_EMPLOYEES/_refresh" > /dev/null
echo ""

# -------------------------------------------------------------------
# 5. Create sanity_logs — time-series data, 5 shards
# -------------------------------------------------------------------
INDEX_LOGS="sanity_logs"
echo "[5/7] Creating index: $INDEX_LOGS (5 shards)..."

curl -s -X DELETE "$HOST/$INDEX_LOGS" 2>/dev/null || true

curl -s -X PUT "$HOST/$INDEX_LOGS" \
  -H 'Content-Type: application/json' \
  -d '{
    "settings": {
      "number_of_shards": 5,
      "number_of_replicas": 0
    },
    "mappings": {
      "properties": {
        "timestamp":    { "type": "date" },
        "level":        { "type": "keyword" },
        "service":      { "type": "keyword" },
        "status_code":  { "type": "integer" },
        "latency_ms":   { "type": "double" },
        "message":      { "type": "text" },
        "bytes_sent":   { "type": "long" },
        "client_ip":    { "type": "ip" }
      }
    }
  }' | python3 -m json.tool 2>/dev/null || true
echo ""

# -------------------------------------------------------------------
# 6. Bulk-load 100 log documents
# -------------------------------------------------------------------
echo "[6/7] Loading 100 documents into $INDEX_LOGS..."

LEVELS=("INFO" "WARN" "ERROR" "DEBUG" "INFO")
SERVICES=("api-gateway" "auth-service" "user-service" "order-service" "payment-service")
STATUS_CODES=(200 200 200 301 400 404 500 200 200 200)
IPS=("10.0.1.1" "10.0.1.2" "10.0.2.1" "10.0.2.2" "192.168.1.1"
     "192.168.1.2" "172.16.0.1" "172.16.0.2" "10.0.3.1" "10.0.3.2")

BULK_BODY=""
BASE_TS=1700000000000  # 2023-11-14T22:13:20Z

for i in $(seq 0 99); do
  ts=$((BASE_TS + i * 60000))
  level_idx=$((i % 5))
  svc_idx=$(( (i / 3) % 5 ))
  sc_idx=$((i % 10))
  latency=$(echo "5.0 + $i * 2.5 + ($i % 7) * 10" | bc)
  bytes=$((100 + i * 50 + (i % 13) * 100))
  ip_idx=$((i % 10))

  BULK_BODY+='{"index":{"_id":"'"$i"'"}}'$'\n'
  BULK_BODY+='{"timestamp":'"$ts"',"level":"'"${LEVELS[$level_idx]}"'","service":"'"${SERVICES[$svc_idx]}"'","status_code":'"${STATUS_CODES[$sc_idx]}"',"latency_ms":'"$latency"',"message":"Request processed by '"${SERVICES[$svc_idx]}"'","bytes_sent":'"$bytes"',"client_ip":"'"${IPS[$ip_idx]}"'"}'$'\n'
done

echo "$BULK_BODY" | curl -s -X POST "$HOST/$INDEX_LOGS/_bulk" \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @- | python3 -c "
import sys, json
resp = json.load(sys.stdin)
print(f'  Loaded: {len(resp.get(\"items\",[]))} docs, errors: {resp.get(\"errors\",False)}')
" 2>/dev/null || echo "  Bulk load sent"

curl -s -X POST "$HOST/$INDEX_LOGS/_refresh" > /dev/null
echo ""

# -------------------------------------------------------------------
# 7. Verify setup + smoke test with strict mode
# -------------------------------------------------------------------
echo "[7/7] Verifying setup + strict mode smoke test..."

for idx in "$INDEX_EMPLOYEES" "$INDEX_LOGS"; do
  count=$(curl -s "$HOST/$idx/_count" | python3 -c "import sys,json; print(json.load(sys.stdin).get('count','?'))" 2>/dev/null || echo "?")
  shards=$(curl -s "$HOST/$idx/_settings" | python3 -c "
import sys,json
d=json.load(sys.stdin)
k=list(d.keys())[0]
print(d[k]['settings']['index']['number_of_shards'])
" 2>/dev/null || echo "?")
  echo "  $idx: $count docs, $shards shards"
done

echo ""
echo "  Running smoke test (strict_mode=true, any fallback = error)..."

PASS=0
FAIL=0

run_test() {
  local label="$1"
  local query="$2"
  local tmpfile
  tmpfile=$(mktemp)

  curl -s -X POST "$HOST/_plugins/_ppl" \
    -H 'Content-Type: application/json' \
    -d "{\"query\": \"$query\"}" > "$tmpfile" 2>&1

  local verdict
  verdict=$(python3 -c "
import sys, json
with open('$tmpfile') as f:
    r = json.load(f)
if 'error' in r:
    details = r['error'].get('details', 'unknown error')[:200]
    print(f'FAIL:{details}')
else:
    rows = len(r.get('datarows', []))
    print(f'PASS:{rows} rows')
" 2>&1)

  if [[ "$verdict" == PASS:* ]]; then
    echo "    PASS: $label — ${verdict#PASS:}"
    PASS=$((PASS + 1))
  else
    echo "    FAIL: $label — ${verdict#FAIL:}"
    FAIL=$((FAIL + 1))
  fi
  rm -f "$tmpfile"
}

run_test "filter"          "source=sanity_employees | where dept = 'Engineering'"
run_test "projection"      "source=sanity_employees | fields name, dept, salary"
run_test "filter+project"  "source=sanity_employees | where age > 50 | fields name, age"
run_test "count by"        "source=sanity_employees | stats count() by dept"
run_test "avg by"          "source=sanity_employees | stats avg(salary) by dept"
run_test "global count"    "source=sanity_employees | stats count() as total"
run_test "min/max"         "source=sanity_employees | stats min(age), max(age) by city"
run_test "sort desc+head"  "source=sanity_employees | sort - salary | head 5"
run_test "sort asc"        "source=sanity_employees | sort age | fields name, age"
run_test "dedup"           "source=sanity_employees | dedup dept | fields dept"
run_test "filter+agg"      "source=sanity_employees | where is_active = true | stats count() by dept"
run_test "filter+agg+sort" "source=sanity_employees | where age >= 30 | stats avg(salary) as avg_sal by dept | sort - avg_sal"
run_test "logs filter"     "source=sanity_logs | where level = 'ERROR'"
run_test "logs stats"      "source=sanity_logs | stats count() as requests, avg(latency_ms) as avg_lat by service"
run_test "logs topN"       "source=sanity_logs | sort - latency_ms | head 10 | fields service, latency_ms"
run_test "empty result"    "source=sanity_employees | where age > 999"
run_test "empty agg"       "source=sanity_employees | where age > 999 | stats count() as cnt"

echo ""
echo "  Smoke test results: $PASS passed, $FAIL failed (out of 17)"
echo ""

if [ "$FAIL" -gt 0 ]; then
  echo "  WARNING: $FAIL queries failed with strict_mode=true."
  echo "  This means those queries fell back to DSL or hit an engine error."
  echo "  The distributed engine did NOT handle them."
  echo ""
fi

echo "=== Setup complete ==="
echo ""
echo "strict_mode is ON. Every query in queries.md that succeeds is"
echo "genuinely executed by the distributed engine (no silent fallback)."
echo ""
echo "If a query returns HTTP 400 with StrictModeViolationException,"
echo "it means that pattern tried to fall back — the engine cannot handle it."
echo ""
echo "Endpoints:"
echo "  PPL execute:  POST $HOST/_plugins/_ppl"
echo "  PPL explain:  POST $HOST/_plugins/_ppl/_explain"
echo "  Stats:        GET  $HOST/_plugins/_sql/stats"
