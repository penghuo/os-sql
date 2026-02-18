#!/usr/bin/env bash
# =============================================================================
# DQE Phase 2 — Test Data Setup & Verification Script
#
# Usage:
#   ./scripts/dqe-phase2-test.sh setup       # Create indices + load data
#   ./scripts/dqe-phase2-test.sh settings     # Configure DQE settings
#   ./scripts/dqe-phase2-test.sh verify       # Run verification queries
#   ./scripts/dqe-phase2-test.sh teardown     # Delete test indices
#   ./scripts/dqe-phase2-test.sh all          # setup + settings + verify
# =============================================================================

set -euo pipefail

ES_URL="${ES_URL:-http://localhost:9200}"
CURL="curl -s -w \n"

# ---- colours (disabled if not a tty) ----------------------------------------
if [ -t 1 ]; then
  GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
else
  GREEN=''; RED=''; YELLOW=''; CYAN=''; NC=''
fi

info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $*"; }
header(){ echo; echo -e "${CYAN}=== $* ===${NC}"; }

# ---- health check -----------------------------------------------------------
check_cluster() {
  info "Checking cluster at ${ES_URL} ..."
  local health
  health=$($CURL "${ES_URL}/_cluster/health?timeout=5s" 2>/dev/null) || {
    fail "Cannot reach ${ES_URL}. Start OpenSearch first:"
    echo "  ./gradlew :opensearch-sql:run &"
    exit 1
  }
  ok "Cluster reachable: $(echo "$health" | grep -o '"status":"[^"]*"')"
}

# =============================================================================
# TEST DATA
# =============================================================================

setup_orders_index() {
  header "Creating 'dqe_orders' index (for join + aggregation tests)"
  $CURL -X PUT "${ES_URL}/dqe_orders" -H 'Content-Type: application/json' -d '{
    "settings": { "number_of_shards": 3, "number_of_replicas": 0 },
    "mappings": {
      "properties": {
        "order_id":    { "type": "integer" },
        "customer_id": { "type": "integer" },
        "amount":      { "type": "double"  },
        "region":      { "type": "keyword" },
        "status":      { "type": "keyword" },
        "order_date":  { "type": "date"    }
      }
    }
  }' > /dev/null

  $CURL -X POST "${ES_URL}/dqe_orders/_bulk?refresh=wait_for" \
    -H 'Content-Type: application/x-ndjson' -d '
{"index":{"_id":"1"}}
{"order_id":1,"customer_id":101,"amount":250.00,"region":"us-east","status":"completed","order_date":"2025-01-15"}
{"index":{"_id":"2"}}
{"order_id":2,"customer_id":102,"amount":125.50,"region":"us-west","status":"completed","order_date":"2025-01-16"}
{"index":{"_id":"3"}}
{"order_id":3,"customer_id":101,"amount":89.99,"region":"us-east","status":"pending","order_date":"2025-01-17"}
{"index":{"_id":"4"}}
{"order_id":4,"customer_id":103,"amount":450.00,"region":"eu-west","status":"completed","order_date":"2025-01-18"}
{"index":{"_id":"5"}}
{"order_id":5,"customer_id":104,"amount":32.75,"region":"us-west","status":"cancelled","order_date":"2025-01-19"}
{"index":{"_id":"6"}}
{"order_id":6,"customer_id":102,"amount":175.25,"region":"us-west","status":"completed","order_date":"2025-01-20"}
{"index":{"_id":"7"}}
{"order_id":7,"customer_id":105,"amount":600.00,"region":"eu-west","status":"completed","order_date":"2025-01-21"}
{"index":{"_id":"8"}}
{"order_id":8,"customer_id":101,"amount":55.00,"region":"us-east","status":"completed","order_date":"2025-01-22"}
{"index":{"_id":"9"}}
{"order_id":9,"customer_id":103,"amount":320.00,"region":"eu-west","status":"pending","order_date":"2025-01-23"}
{"index":{"_id":"10"}}
{"order_id":10,"customer_id":106,"amount":99.99,"region":"ap-south","status":"completed","order_date":"2025-01-24"}
{"index":{"_id":"11"}}
{"order_id":11,"customer_id":104,"amount":210.00,"region":"us-west","status":"completed","order_date":"2025-01-25"}
{"index":{"_id":"12"}}
{"order_id":12,"customer_id":107,"amount":780.00,"region":"ap-south","status":"completed","order_date":"2025-01-26"}
' > /dev/null
  ok "dqe_orders: 12 docs across 3 shards"
}

setup_customers_index() {
  header "Creating 'dqe_customers' index (join target)"
  $CURL -X PUT "${ES_URL}/dqe_customers" -H 'Content-Type: application/json' -d '{
    "settings": { "number_of_shards": 2, "number_of_replicas": 0 },
    "mappings": {
      "properties": {
        "customer_id": { "type": "integer" },
        "name":        { "type": "keyword" },
        "tier":        { "type": "keyword" },
        "country":     { "type": "keyword" }
      }
    }
  }' > /dev/null

  $CURL -X POST "${ES_URL}/dqe_customers/_bulk?refresh=wait_for" \
    -H 'Content-Type: application/x-ndjson' -d '
{"index":{"_id":"1"}}
{"customer_id":101,"name":"Alice","tier":"gold","country":"US"}
{"index":{"_id":"2"}}
{"customer_id":102,"name":"Bob","tier":"silver","country":"US"}
{"index":{"_id":"3"}}
{"customer_id":103,"name":"Charlie","tier":"gold","country":"DE"}
{"index":{"_id":"4"}}
{"customer_id":104,"name":"Diana","tier":"bronze","country":"US"}
{"index":{"_id":"5"}}
{"customer_id":105,"name":"Erik","tier":"gold","country":"FR"}
{"index":{"_id":"6"}}
{"customer_id":106,"name":"Fatima","tier":"silver","country":"IN"}
{"index":{"_id":"7"}}
{"customer_id":107,"name":"George","tier":"bronze","country":"IN"}
' > /dev/null
  ok "dqe_customers: 7 docs across 2 shards"
}

setup_metrics_index() {
  header "Creating 'dqe_metrics' index (for STDDEV/VAR + window tests)"
  $CURL -X PUT "${ES_URL}/dqe_metrics" -H 'Content-Type: application/json' -d '{
    "settings": { "number_of_shards": 3, "number_of_replicas": 0 },
    "mappings": {
      "properties": {
        "service":   { "type": "keyword" },
        "host":      { "type": "keyword" },
        "latency":   { "type": "double"  },
        "cpu_pct":   { "type": "double"  },
        "mem_mb":    { "type": "integer" },
        "timestamp": { "type": "date"    }
      }
    }
  }' > /dev/null

  $CURL -X POST "${ES_URL}/dqe_metrics/_bulk?refresh=wait_for" \
    -H 'Content-Type: application/x-ndjson' -d '
{"index":{"_id":"1"}}
{"service":"api","host":"node-1","latency":12.5,"cpu_pct":45.2,"mem_mb":512,"timestamp":"2025-01-20T10:00:00"}
{"index":{"_id":"2"}}
{"service":"api","host":"node-2","latency":15.3,"cpu_pct":52.1,"mem_mb":480,"timestamp":"2025-01-20T10:01:00"}
{"index":{"_id":"3"}}
{"service":"api","host":"node-1","latency":11.8,"cpu_pct":43.5,"mem_mb":520,"timestamp":"2025-01-20T10:02:00"}
{"index":{"_id":"4"}}
{"service":"web","host":"node-3","latency":25.1,"cpu_pct":71.3,"mem_mb":1024,"timestamp":"2025-01-20T10:00:00"}
{"index":{"_id":"5"}}
{"service":"web","host":"node-3","latency":28.7,"cpu_pct":75.8,"mem_mb":1100,"timestamp":"2025-01-20T10:01:00"}
{"index":{"_id":"6"}}
{"service":"web","host":"node-4","latency":22.0,"cpu_pct":68.0,"mem_mb":980,"timestamp":"2025-01-20T10:02:00"}
{"index":{"_id":"7"}}
{"service":"db","host":"node-5","latency":3.2,"cpu_pct":30.0,"mem_mb":2048,"timestamp":"2025-01-20T10:00:00"}
{"index":{"_id":"8"}}
{"service":"db","host":"node-5","latency":4.1,"cpu_pct":32.5,"mem_mb":2100,"timestamp":"2025-01-20T10:01:00"}
{"index":{"_id":"9"}}
{"service":"db","host":"node-6","latency":3.8,"cpu_pct":28.9,"mem_mb":2000,"timestamp":"2025-01-20T10:02:00"}
{"index":{"_id":"10"}}
{"service":"api","host":"node-2","latency":18.9,"cpu_pct":60.0,"mem_mb":490,"timestamp":"2025-01-20T10:03:00"}
{"index":{"_id":"11"}}
{"service":"web","host":"node-4","latency":30.5,"cpu_pct":80.1,"mem_mb":1150,"timestamp":"2025-01-20T10:03:00"}
{"index":{"_id":"12"}}
{"service":"db","host":"node-6","latency":5.0,"cpu_pct":35.0,"mem_mb":2050,"timestamp":"2025-01-20T10:03:00"}
{"index":{"_id":"13"}}
{"service":"api","host":"node-1","latency":14.0,"cpu_pct":48.0,"mem_mb":500,"timestamp":"2025-01-20T10:04:00"}
{"index":{"_id":"14"}}
{"service":"web","host":"node-3","latency":26.3,"cpu_pct":73.0,"mem_mb":1050,"timestamp":"2025-01-20T10:04:00"}
{"index":{"_id":"15"}}
{"service":"db","host":"node-5","latency":3.5,"cpu_pct":31.0,"mem_mb":2080,"timestamp":"2025-01-20T10:04:00"}
' > /dev/null
  ok "dqe_metrics: 15 docs across 3 shards (5 per service: api, web, db)"
}

do_setup() {
  check_cluster
  setup_orders_index
  setup_customers_index
  setup_metrics_index
  echo
  ok "All test data loaded."
}

# =============================================================================
# SETTINGS
# =============================================================================

do_settings() {
  check_cluster
  header "Configuring DQE settings"

  info "Enabling Calcite engine..."
  $CURL -X PUT "${ES_URL}/_cluster/settings" -H 'Content-Type: application/json' -d '{
    "transient": { "plugins.calcite.enabled": true }
  }' > /dev/null
  ok "plugins.calcite.enabled = true"

  info "Disabling legacy pushdown (DQE active)..."
  $CURL -X PUT "${ES_URL}/_cluster/settings" -H 'Content-Type: application/json' -d '{
    "transient": { "plugins.calcite.legacy_pushdown.enabled": false }
  }' > /dev/null
  ok "plugins.calcite.legacy_pushdown.enabled = false"

  echo
  info "Current settings:"
  $CURL "${ES_URL}/_cluster/settings?flat_settings=true&filter_path=**.calcite*" \
    | python3 -m json.tool 2>/dev/null || $CURL "${ES_URL}/_cluster/settings?flat_settings=true&filter_path=**.calcite*"
}

# =============================================================================
# VERIFICATION QUERIES
# =============================================================================

ppl_explain() {
  local desc="$1"; local query="$2"
  echo
  info "${desc}"
  echo -e "  PPL: ${YELLOW}${query}${NC}"
  echo "  --- explain ---"
  $CURL -X POST "${ES_URL}/_plugins/_ppl/_explain" \
    -H 'Content-Type: application/json' \
    -d "{\"query\": \"${query}\"}" | python3 -m json.tool 2>/dev/null \
    || $CURL -X POST "${ES_URL}/_plugins/_ppl/_explain" \
      -H 'Content-Type: application/json' \
      -d "{\"query\": \"${query}\"}"
}

ppl_query() {
  local desc="$1"; local query="$2"
  echo
  info "${desc}"
  echo -e "  PPL: ${YELLOW}${query}${NC}"
  echo "  --- result ---"
  $CURL -X POST "${ES_URL}/_plugins/_ppl" \
    -H 'Content-Type: application/json' \
    -d "{\"query\": \"${query}\"}" | python3 -m json.tool 2>/dev/null \
    || $CURL -X POST "${ES_URL}/_plugins/_ppl" \
      -H 'Content-Type: application/json' \
      -d "{\"query\": \"${query}\"}"
}

do_verify() {
  check_cluster

  # ---- Phase 1 baseline (should still work) --------------------------------
  header "Phase 1 — Baseline (scatter-gather)"

  ppl_explain "Simple aggregation (PartialAggregate + MergeAggregateExchange)" \
    "source=dqe_orders | stats count() by region"

  ppl_query "Execute: count by region" \
    "source=dqe_orders | stats count() by region"

  ppl_explain "AVG decomposition (SUM+COUNT on shards, merge on coordinator)" \
    "source=dqe_orders | stats avg(amount) by region"

  ppl_query "Execute: avg amount by region" \
    "source=dqe_orders | stats avg(amount) by region"

  # ---- Phase 2: STDDEV/VAR via Welford -------------------------------------
  header "Phase 2 — STDDEV/VAR (Welford merge)"

  ppl_explain "STDDEV_POP (should decompose into COUNT+SUM+SUM_SQUARES)" \
    "source=dqe_metrics | stats stddev_pop(latency) by service"

  ppl_query "Execute: stddev_pop latency by service" \
    "source=dqe_metrics | stats stddev_pop(latency) by service"

  ppl_query "Execute: stddev_samp of cpu_pct by service" \
    "source=dqe_metrics | stats stddev_samp(cpu_pct) by service"

  # ---- Phase 2: Distributed window ----------------------------------------
  header "Phase 2 — Distributed Window (HashExchange by partition key)"

  ppl_explain "Window with PARTITION BY (should use HashExchange)" \
    "source=dqe_metrics | eventstats avg(latency) by service"

  ppl_query "Execute: eventstats avg latency by service" \
    "source=dqe_metrics | eventstats avg(latency) by service"

  ppl_explain "Window WITHOUT PARTITION BY (should use ConcatExchange)" \
    "source=dqe_metrics | eventstats avg(latency)"

  # ---- Phase 2: Distributed join -------------------------------------------
  header "Phase 2 — Distributed Join (HashExchange by join key)"

  ppl_explain "Equi-join (should use HashExchange on both sides)" \
    "source=dqe_orders | join left=o right=c ON o.customer_id = c.customer_id dqe_customers"

  ppl_query "Execute: join orders with customers" \
    "source=dqe_orders | join left=o right=c ON o.customer_id = c.customer_id dqe_customers | fields o.order_id, c.name, o.amount, c.tier"

  # ---- Phase 2: Combined ---------------------------------------------------
  header "Phase 2 — Combined queries"

  ppl_query "Join + aggregation: total spend by customer tier" \
    "source=dqe_orders | join left=o right=c ON o.customer_id = c.customer_id dqe_customers | stats sum(o.amount) by c.tier"

  ppl_query "STDDEV_POP + group by: latency variance across hosts" \
    "source=dqe_metrics | stats stddev_pop(latency), avg(latency), count() by host"

  echo
  header "Verification complete"
  info "Look for these in explain output to confirm DQE Phase 2:"
  echo "  - Exchange / ConcatExchange .............. Phase 1 scatter-gather"
  echo "  - HashExchange ........................... Phase 2 shuffle"
  echo "  - PartialAggregate (partial=true) ........ shard-side partial agg"
  echo "  - MergeAggregateExchange ................. coordinator merge"
  echo "  - mergeStrategies=[WELFORD] .............. Welford STDDEV/VAR"
  echo "  - mergeStrategies=[BINARY_STATE] ......... HLL/tdigest merge"
}

# =============================================================================
# TEARDOWN
# =============================================================================

do_teardown() {
  check_cluster
  header "Deleting test indices"
  $CURL -X DELETE "${ES_URL}/dqe_orders"    > /dev/null 2>&1 && ok "Deleted dqe_orders"    || warn "dqe_orders not found"
  $CURL -X DELETE "${ES_URL}/dqe_customers" > /dev/null 2>&1 && ok "Deleted dqe_customers" || warn "dqe_customers not found"
  $CURL -X DELETE "${ES_URL}/dqe_metrics"   > /dev/null 2>&1 && ok "Deleted dqe_metrics"   || warn "dqe_metrics not found"
}

# =============================================================================
# MAIN
# =============================================================================

case "${1:-help}" in
  setup)    do_setup ;;
  settings) do_settings ;;
  verify)   do_verify ;;
  teardown) do_teardown ;;
  all)      do_setup; do_settings; do_verify ;;
  *)
    echo "Usage: $0 {setup|settings|verify|teardown|all}"
    echo
    echo "  setup     Create test indices and load sample data"
    echo "  settings  Configure Calcite + DQE cluster settings"
    echo "  verify    Run PPL explain + execute queries to verify DQE"
    echo "  teardown  Delete test indices"
    echo "  all       setup + settings + verify"
    echo
    echo "Environment:"
    echo "  ES_URL    OpenSearch URL (default: http://localhost:9200)"
    exit 1
    ;;
esac
