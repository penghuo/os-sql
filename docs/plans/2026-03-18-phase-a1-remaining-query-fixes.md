# Phase A-1: Fix Remaining Query Failures on 100M — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** All 43 ClickBench queries return correct results on 100M rows. Zero errors, zero crashes, zero circuit breaker trips that block subsequent queries.

**Architecture:** Fix two remaining failures: Q33 (memory limit error on high-cardinality GROUP BY) and Q19→Q20 (Q19 fills 90% heap, causing circuit breaker for the next query). Both require bounded-memory GROUP BY execution for high-cardinality keys.

**Tech Stack:** Java 21, Lucene DocValues, OpenSearch 3.6.0-SNAPSHOT

**Current Status (after Phase A):**
- 41/43 queries return correct results
- Q33: `GROUP BY WatchID, ClientIP` → memory limit error (11.7M groups > 8M cap)
- Q20: circuit breaker trip from Q19's residual heap (Q19 fills 15.4GB of 17GB old gen)
- Q19 itself completes but leaves heap dirty

**Failing Queries:**

```sql
-- Q33: ~100M unique (WatchID, ClientIP) pairs, no WHERE, ORDER BY COUNT(*) DESC LIMIT 10
SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth)
FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;

-- Q19: ~25M unique (UserID, minute, SearchPhrase) per shard, fills 15GB heap
SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*)
FROM hits GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase
ORDER BY COUNT(*) DESC LIMIT 10;
```

**Key Constraint:** Both queries ask for `ORDER BY COUNT(*) DESC LIMIT 10`. We only need the top-10 groups by count — we do NOT need to materialize all millions of groups.

---

### Task 1: Spike — Bounded-memory top-N GROUP BY for high-cardinality keys

**Goal:** Explore approaches that return correct `ORDER BY aggregate DESC LIMIT K` results without materializing all groups. Produce a written recommendation.

**Files to study:**
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java` — FlatTwoKeyMap, FlatThreeKeyMap, execution methods
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java:353-468` — GROUP BY dispatch, TopN paths

**Step 1: Understand the problem precisely**

For Q33 (`ORDER BY COUNT(*) DESC LIMIT 10` on ~100M unique groups):
- Each group has count ~1 (nearly 1:1 docs to groups)
- Top-10 groups by count might have counts of 2-5
- We need to find the ~10 groups with the highest counts among ~25M groups per shard
- Current approach: build full hash map → sort → take top 10. OOMs.

For Q19 (similar but completes at 8M cap, leaves 15GB heap):
- Groups actually fit under 8M cap per shard
- But 8M × FlatThreeKeyMap entry size = ~2GB per shard, 4 shards = ~8GB
- After query, this memory isn't released fast enough → circuit breaker for next query

**Step 2: Evaluate approaches**

| Approach | Description | Memory | Accuracy | Complexity |
|----------|-------------|:------:|:--------:|:----------:|
| **A. Two-pass: count-only then targeted** | Pass 1: scan all docs with a minimal hash map (key→count only). Pass 2: re-scan for top-K groups to compute SUM/AVG. | O(groups) for pass 1 | Exact | Medium |
| **B. Space-Saving / Lossy Counting** | Streaming algorithm that tracks approximate heavy hitters in O(K/ε) space. Guarantees all items with frequency > N×ε are found. | O(K/ε) — tiny | Approximate but top-K guaranteed | Medium |
| **C. Sampling** | Sample 1-10% of docs, build full GROUP BY on sample, extrapolate counts. | O(groups/100) | ~10% error | Low |
| **D. Hash-partitioned aggregation** | Partition groups by hash into B buckets. Process one bucket at a time (fits in memory). Merge across buckets. | O(groups/B) per pass | Exact | High — B passes over data |
| **E. Lower the MAX_CAPACITY cap behavior** | Instead of throwing error when cap hit, switch to `findOrInsertCapped` (skip new groups, keep accumulating existing). | O(MAX_CAPACITY) | Approximate for top-K | Low |
| **F. Explicit GC after heavy queries** | After Q19/Q33-like queries, call `System.gc()` to release hash maps before next query. Prevents circuit breaker but doesn't fix Q33 error. | N/A | N/A (stability fix) | Trivial |

**Key questions to answer for each approach:**
1. Does it prevent OOM/circuit breaker?
2. Are Q33 top-10 results correct? (groups with count=2-5 among ~100M count=1 groups)
3. Are Q19 top-10 results correct?
4. Does it leave heap clean for the next query?
5. How does it interact with the coordinator merge?

**Step 3: Prototype the top 1-2 approaches**

Focus on approaches that give correct results for `ORDER BY COUNT(*) DESC LIMIT K`:
- Approach B (Space-Saving) is the recommended one from the GROUP BY spike
- Approach E (capped with accumulation) is simplest but accuracy is questionable
- Approach F (explicit GC) solves Q19→Q20 but not Q33

**Step 4: Write recommendation**

Save to `docs/plans/2026-03-18-spike-bounded-topn-groupby-recommendation.md`:
- Chosen approach with rationale
- Memory budget
- Accuracy guarantee for top-K
- Implementation sketch
- Impact on coordinator merge
- Whether Q19's heap residual also needs a separate fix (approach F)

---

### Task 2: Implement bounded-memory top-N GROUP BY

**Depends on:** Task 1 spike recommendation.

**Files (expected, depends on chosen approach):**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java`
- Possibly create: new streaming aggregation class

**Step 1: Implement chosen approach from Task 1**

Follow the recommendation doc.

**Step 2: Verify Q33 returns correct results on 100M**

```bash
curl -s -XPOST "http://localhost:9200/_plugins/_trino_sql" \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10"}' \
  --max-time 120 | jq .
```

Expected: Returns 10 rows. No error. No OOM. OS stays alive.

**Step 3: Verify Q19 completes and doesn't break Q20**

```bash
# Run Q19 then Q20 back-to-back
curl -s -XPOST "http://localhost:9200/_plugins/_trino_sql" \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10"}' \
  --max-time 120 | jq .status

# Immediately run Q20
curl -s -XPOST "http://localhost:9200/_plugins/_trino_sql" \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT UserID FROM hits WHERE UserID = 435090932899640449"}' \
  --max-time 30 | jq .status
```

Expected: Both return 200. No circuit breaker.

**Step 4: Commit**

```bash
git add -A
git commit -m "fix(dqe): bounded-memory GROUP BY for high-cardinality ORDER BY LIMIT queries"
```

---

### Task 3: Fix heap residual after heavy queries (if needed)

**Depends on:** Task 2. Only needed if Task 2 doesn't fully resolve Q19→Q20 circuit breaker.

If Q19 still leaves excessive heap after Task 2's fix, add explicit cleanup:

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java`

**Option A: Null out hash maps after building response**

In `executeFusedGroupByAggregate()` and related methods, after building the output Page, set the hash map references to null to allow GC:

```java
// After building output page from flatMap
flatMap = null; // allow GC
```

**Option B: Explicit GC hint after heavy queries**

In `TransportTrinoSqlAction` coordinator, after merging shard results for queries that produced large intermediate state:

```java
// After merge, if total rows from shards exceeded threshold
if (totalShardRows > 1_000_000) {
    System.gc(); // hint to G1GC to collect old gen
}
```

**Step 1: Implement the simpler option (A first, B if needed)**

**Step 2: Verify Q19→Q20 back-to-back works**

**Step 3: Commit**

---

### Task 4: Full 43-query verification

**Depends on:** Tasks 2 and 3.

**Step 1: Fresh restart and sequential run**

```bash
sudo -u opensearch kill "$(cat /opt/opensearch/opensearch.pid)" 2>/dev/null
sleep 5
sudo -u opensearch /opt/opensearch/bin/opensearch -d -p /opt/opensearch/opensearch.pid
while ! curl -sf http://localhost:9200 >/dev/null 2>&1; do sleep 3; done

cd /home/ec2-user/oss/wukong/benchmarks/clickbench
# Run all 43 queries sequentially on 100M
# Every query must return HTTP 200
# Zero crashes, zero circuit breaker trips, zero memory limit errors
```

**Step 2: Verify success criteria**

- 43/43 queries return HTTP 200 with results
- 0 OOM crashes
- 0 circuit breaker trips
- OS healthy after all 43 queries

**Step 3: Run 1M correctness regression check**

```bash
bash run/run_all.sh correctness
```

Expected: 32/43 pass (same baseline).

**Step 4: Commit**

```bash
git add -A
git commit -m "test(dqe): verify all 43 queries return correct results on 100M"
```

---

## Execution Order

```
Task 1: Spike bounded top-N GROUP BY
    │
    └─ Task 2: Implement bounded GROUP BY
           │
           ├─ Task 3: Fix heap residual (if needed)
           │
           └─ Task 4: Full verification
```

## Time Estimates

| Task | Duration |
|------|----------|
| Task 1: Spike | ~3-4 hours |
| Task 2: Implement | ~4-8 hours |
| Task 3: Heap residual fix | ~1-2 hours (if needed) |
| Task 4: Full verification | ~1 hour |
| **Total** | **~1-2 days** |
