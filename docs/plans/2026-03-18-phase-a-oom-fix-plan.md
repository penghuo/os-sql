# Phase A: Fix OOM Crashes on 100M Dataset — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** All 43 ClickBench queries complete on 100M rows without OOM crashes, circuit breaker trips, or JVM death.

**Architecture:** Two spike tasks explore approaches for bounding memory in COUNT(DISTINCT) HashSets and GROUP BY hash maps. Then implement the chosen approaches, verify on 100M, and confirm no regression on 1M.

**Tech Stack:** Java 21, Lucene DocValues, OpenSearch 3.6.0-SNAPSHOT, ClickBench 100M dataset (4 shards)

**Key Context:**
- OOM queries: Q14 (`COUNT(DISTINCT UserID) GROUP BY SearchPhrase`), Q33 (`GROUP BY WatchID, ClientIP`)
- Circuit breaker: Q19 (`GROUP BY UserID, minute, SearchPhrase` — fills 15.3GB heap)
- No existing unit tests for `LongOpenHashSet`, `FusedGroupByAggregate`, or `TransportShardExecuteAction`
- Test via ClickBench integration: `cd benchmarks/clickbench && bash run/run_all.sh correctness` (1M) and manual Q14/Q33 on 100M

---

### Task 1: Spike — COUNT(DISTINCT) Memory Bounding Approaches

**Goal:** Explore approaches to prevent `executeVarcharCountDistinctWithHashSets()` and `executeCountDistinctWithHashSets()` from OOMing on high-cardinality data. Produce a written recommendation.

**Files to study:**
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java:1245-1411` (varchar path)
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java:789-880` (numeric path)
- `dqe/src/main/java/org/opensearch/sql/dqe/operator/LongOpenHashSet.java` (unbounded hash set)
- `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/transport/TransportTrinoSqlAction.java:2268-2550` (coordinator merge)

**Step 1: Analyze memory usage of Q14 on 100M**

Run Q14 on the 100M index and observe memory behavior before crash. Key question: how many groups exist, how large are the HashSets per group, and what's the total memory footprint?

```bash
# Check cardinality of SearchPhrase on full dataset
curl -sf -XPOST "http://localhost:9200/_plugins/_trino_sql" \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT COUNT(DISTINCT SearchPhrase) FROM hits WHERE SearchPhrase <> '\'''\''"}' \
  --max-time 120
```

Also check: `SELECT COUNT(DISTINCT UserID) FROM hits` to understand distinct value count.

Estimate: `num_groups × avg_hashset_size × 16 bytes/entry = total memory`.

**Step 2: Evaluate approach candidates**

For each approach, assess: memory bound guarantee, accuracy, implementation complexity, coordinator merge compatibility.

| Approach | Description | Memory Bound | Accuracy | Complexity |
|----------|-------------|:------------:|:--------:|:----------:|
| **A. HyperLogLog fallback** | Switch from HashSet to HLL sketch when total entries across all groups exceed threshold (e.g., 10M). HLL uses ~16KB per group regardless of cardinality. | Yes (fixed per group) | ~0.8% error | Medium — need HLL impl + coordinator merge |
| **B. Bounded HashSet + early-stop** | Track `totalEntries` counter. When exceeded, stop adding new values to any HashSet. Return lower-bound count. | Yes | Lower bound (undercount) | Low — add counter + check |
| **C. Sampling** | Process only every Nth doc (e.g., N=10) and multiply count. | Yes | ~10% error | Low |
| **D. Fallback to decomposed plan** | When cardinality exceeds threshold, abandon HashSet path and fall through to the generic `GROUP BY (SearchPhrase, UserID), COUNT(*)` operator pipeline which uses the existing TopN-inflated limit. | Yes (uses existing bounded path) | Exact | Low — just add cardinality check before entering HashSet path |
| **E. Per-group cap** | Cap each individual HashSet at K entries. Groups exceeding K get approximate count = K × (total_docs / scanned_docs). | Yes | Approximate | Medium |

**Step 3: Prototype most promising approach(es)**

Write a minimal prototype of the top 1-2 approaches. Measure:
- Memory usage (via `-verbose:gc` or `jstat`)
- Accuracy (compare output against ClickHouse expected result for Q14)
- Latency impact

**Step 4: Write recommendation**

Save to `docs/plans/2026-03-18-spike-count-distinct-recommendation.md`:
- Chosen approach with rationale
- Memory budget and threshold values
- Accuracy tradeoff
- Impact on coordinator merge path
- Any changes needed to `ShardExecuteResponse` serialization

---

### Task 2: Spike — GROUP BY Memory Bounding Approaches

**Goal:** Explore approaches to prevent `FlatTwoKeyMap.resize()` and `FlatThreeKeyMap.resize()` from OOMing on high-cardinality GROUP BY. Produce a written recommendation.

**Files to study:**
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java:8796-8915` (FlatTwoKeyMap)
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java:8857-8879` (existing `findOrInsertCapped`)
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java:8917+` (FlatThreeKeyMap — Q19 path)
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java:353-468` (GROUP BY dispatch)

**Step 1: Analyze memory usage of Q33 on 100M**

Q33: `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10`

Key question: how many unique (WatchID, ClientIP) pairs exist per shard? How large does FlatTwoKeyMap grow before OOM?

```bash
# Check cardinality
curl -sf -XPOST "http://localhost:9200/_plugins/_trino_sql" \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT COUNT(*) FROM hits WHERE CounterID = 62"}' \
  --max-time 120
```

Wait — Q33 has `WHERE CounterID = 62`, so it's filtered. Check how many docs match and how many unique groups.

Also check Q19 (3-key GROUP BY, no WHERE filter — the circuit breaker one):
- Estimate: `unique(UserID) × 60 minutes × unique(SearchPhrase)` — could be billions of groups.

**Step 2: Evaluate approach candidates**

| Approach | Description | Memory Bound | Accuracy | Complexity |
|----------|-------------|:------------:|:--------:|:----------:|
| **A. Hard cap + graceful error** | Cap FlatTwoKeyMap at N entries (e.g., 5M). If `resize()` would exceed cap, throw `DqeMemoryLimitException`. Caught in `TransportShardExecuteAction`, returned as HTTP 400. | Yes | N/A (error) | Low |
| **B. Use existing `findOrInsertCapped`** | The cap mechanism already exists in `FlatTwoKeyMap.findOrInsertCapped()`. Extend to all GROUP BY paths, not just LIMIT-without-ORDER-BY. When capped, skip new groups. | Yes | Partial (undercounts some groups) | Low — extend existing mechanism |
| **C. Streaming partial flush** | When map reaches N entries, emit current partial results, reset map, continue scanning. Coordinator merges partials. | Yes | Exact (with coordinator merge) | High — changes shard response format |
| **D. Pre-check cardinality** | Before executing fused GROUP BY, sample 1% of docs to estimate group count. If estimated > threshold, fall back to generic operator pipeline with bounded memory. | Yes | Depends on fallback | Medium |
| **E. Resize with OOM guard** | Wrap `resize()` in try-catch for `OutOfMemoryError`. On OOM, return partial results collected so far. | Yes | Partial | Low but fragile |

Note: `findOrInsertCapped` already exists at line 8857. The question is whether partial results (skipped groups) are acceptable.

**Step 3: Prototype most promising approach(es)**

Test on Q33 (100M, `WHERE CounterID = 62`) and Q19 (100M, no WHERE).

**Step 4: Write recommendation**

Save to `docs/plans/2026-03-18-spike-groupby-memory-recommendation.md`:
- Chosen approach with rationale
- Cap value and configuration
- Behavior when cap is hit (error vs partial results)
- Impact on correctness
- Changes needed to fused paths

---

### Task 3: Implement COUNT(DISTINCT) Memory Fix

**Depends on:** Task 1 spike recommendation.

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java`
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/operator/LongOpenHashSet.java` (if needed)
- Possibly modify: `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/transport/TransportTrinoSqlAction.java` (coordinator merge)

**Step 1: Implement chosen approach from Task 1 recommendation**

Follow the recommendation doc. The exact code depends on the spike findings.

**Step 2: Verify Q14 completes on 100M without OOM**

```bash
# Restart OpenSearch fresh
sudo -u opensearch kill "$(cat /opt/opensearch/opensearch.pid)" 2>/dev/null
sleep 5
sudo -u opensearch /opt/opensearch/bin/opensearch -d -p /opt/opensearch/opensearch.pid
while ! curl -sf http://localhost:9200 >/dev/null 2>&1; do sleep 3; done

# Run Q14
curl -s -XPOST "http://localhost:9200/_plugins/_trino_sql" \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '\'''\'' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10"}' \
  --max-time 120 | jq .
```

Expected: Returns 10 rows without OOM. OS stays alive.

**Step 3: Verify no regression on 1M correctness**

```bash
cd /home/ec2-user/oss/wukong/benchmarks/clickbench
bash run/run_all.sh correctness
```

Expected: 32/43 pass (same as baseline). Q14 must still pass on 1M.

**Step 4: Commit**

```bash
git add -A
git commit -m "fix(dqe): bound COUNT(DISTINCT) memory to prevent OOM on high-cardinality data"
```

---

### Task 4: Implement GROUP BY Memory Fix

**Depends on:** Task 2 spike recommendation.

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`
- Possibly modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java`

**Step 1: Implement chosen approach from Task 2 recommendation**

Follow the recommendation doc.

**Step 2: Verify Q33 completes on 100M without OOM**

```bash
curl -s -XPOST "http://localhost:9200/_plugins/_trino_sql" \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10"}' \
  --max-time 120 | jq .
```

Expected: Returns 10 rows or a clear error message. No OOM crash.

**Step 3: Verify Q19 doesn't trip circuit breaker**

```bash
curl -s -XPOST "http://localhost:9200/_plugins/_trino_sql" \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10"}' \
  --max-time 120 | jq .status
```

Expected: Returns result or error. No circuit breaker trip. Subsequent queries still work.

**Step 4: Verify no regression on 1M correctness**

```bash
cd /home/ec2-user/oss/wukong/benchmarks/clickbench
bash run/run_all.sh correctness
```

Expected: 32/43 pass (same as baseline).

**Step 5: Commit**

```bash
git add -A
git commit -m "fix(dqe): bound GROUP BY memory to prevent OOM on high-cardinality data"
```

---

### Task 5: Full 100M Verification

**Depends on:** Tasks 3 and 4.

**Step 1: Restart OpenSearch fresh**

```bash
sudo -u opensearch kill "$(cat /opt/opensearch/opensearch.pid)" 2>/dev/null
sleep 5
sudo -u opensearch /opt/opensearch/bin/opensearch -d -p /opt/opensearch/opensearch.pid
while ! curl -sf http://localhost:9200 >/dev/null 2>&1; do sleep 3; done
```

**Step 2: Run all 43 queries on 100M**

```bash
cd /home/ec2-user/oss/wukong/benchmarks/clickbench

QUERY_FILE="queries/queries_trino.sql"
OS_URL="http://localhost:9200"

for q in $(seq 1 43); do
    query=$(sed -n "${q}p" "$QUERY_FILE" | sed 's/;[[:space:]]*$//')
    escaped=$(printf '%s' "$query" | jq -Rs '.')

    if ! curl -sf "$OS_URL" >/dev/null 2>&1; then
        echo "Q${q}: OS CRASHED — FAIL"
        sudo -u opensearch /opt/opensearch/bin/opensearch -d -p /opt/opensearch/opensearch.pid 2>/dev/null
        while ! curl -sf "$OS_URL" >/dev/null 2>&1; do sleep 3; done
        continue
    fi

    START_NS=$(date +%s%N)
    HTTP_CODE=$(curl -s -o /tmp/os_q${q}.json -w '%{http_code}' \
        -XPOST "$OS_URL/_plugins/_trino_sql" \
        -H 'Content-Type: application/json' \
        -d "{\"query\": $escaped}" \
        --max-time 120) || true
    END_NS=$(date +%s%N)
    ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))

    if [ "$HTTP_CODE" -ge 200 ] && [ "$HTTP_CODE" -lt 300 ]; then
        echo "Q${q}: OK (${ELAPSED_MS}ms)"
    elif [ "$HTTP_CODE" = "429" ]; then
        echo "Q${q}: CIRCUIT BREAKER (${ELAPSED_MS}ms) — FAIL"
    elif [ "$HTTP_CODE" = "400" ]; then
        echo "Q${q}: MEMORY LIMIT ERROR (expected for bounded queries)"
    else
        echo "Q${q}: HTTP ${HTTP_CODE} (${ELAPSED_MS}ms)"
    fi
done
```

**Step 3: Verify success criteria**

- 0 OOM crashes (OS never goes down)
- 0 circuit breaker trips that block subsequent queries
- Q14, Q33: either return results (exact or approximate) or return HTTP 400 with clear message
- Q19: either completes or returns error without blocking subsequent queries
- All other queries: return same results as before

**Step 4: Run 1M correctness as final regression check**

```bash
cd /home/ec2-user/oss/wukong/benchmarks/clickbench
bash run/run_all.sh correctness
```

Expected: 32/43 pass.

**Step 5: Commit verification results**

```bash
git add -A
git commit -m "test(dqe): verify all 43 queries complete on 100M without OOM"
```

---

## Execution Order

```
Task 1: Spike COUNT(DISTINCT) ──┐
                                 ├─ Task 3: Implement COUNT(DISTINCT) fix ──┐
Task 2: Spike GROUP BY ─────────┤                                           ├─ Task 5: Full verification
                                 └─ Task 4: Implement GROUP BY fix ─────────┘
```

Tasks 1 and 2 can run in parallel. Tasks 3 and 4 can run in parallel (after their respective spikes). Task 5 runs after both fixes are in place.

## Time Estimates

| Task | Duration |
|------|----------|
| Task 1: Spike COUNT(DISTINCT) | ~3-4 hours |
| Task 2: Spike GROUP BY | ~3-4 hours |
| Task 3: Implement COUNT(DISTINCT) fix | ~4-8 hours |
| Task 4: Implement GROUP BY fix | ~4-8 hours |
| Task 5: Full verification | ~1 hour |
| **Total** | **~2-3 days** |
