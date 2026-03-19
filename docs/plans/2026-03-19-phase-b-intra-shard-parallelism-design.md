# Phase B: Intra-Shard Parallelism — Design Document

## Problem

DQE's fused GROUP BY scans all segments within a shard sequentially on one thread. On 100M rows with 4 shards, each shard processes ~25M docs single-threaded. ClickHouse uses 16 threads on the comparison baseline. The aggregate ratio is 10.5x, with Q17 (51s), Q18 (53s), Q19 (81s) accounting for 61% of the total gap.

## Goal

Determine the best intra-shard parallelism strategy by implementing three approaches, benchmarking all three, and extending the winner to all fused GROUP BY paths. Target: ~20-25/43 queries within 2x ClickHouse, aggregate ratio < 5x.

## Strategy

Implement all three approaches behind a runtime feature flag. Benchmark on `executeSingleKeyNumericFlat` (covers Q3-Q6, Q17, Q18 — 55% of total time). Pick the winner based on data. Extend to remaining paths.

## Environment

- m5.8xlarge: 32 vCPUs, 124GB RAM, 32GB heap
- Index: `hits`, 99.9M docs, 4 shards, 20-27 segments per shard
- Thread count per shard: `Runtime.availableProcessors() / numLocalShards` = 32/4 = 8

## Feature Flag

System property: `dqe.parallelism`

| Value | Behavior |
|-------|----------|
| `off` (default) | Current single-threaded execution. No change. |
| `segment` | Approach A: Segment-level parallelism |
| `docrange` | Approach B: Doc-range splitting within segments |
| `lucene` | Approach C: Lucene parallel IndexSearcher with custom Collector |

Read once at startup via `System.getProperty("dqe.parallelism", "off")`.

---

## Approach A: Segment-Level Parallelism

### How It Works

1. Acquire searcher, get list of `LeafReaderContext` (segments)
2. Partition segments into N groups (N = thread count per shard)
3. Submit N tasks to ForkJoinPool. Each task:
   - Gets its own `FlatSingleKeyMap` instance
   - Iterates its assigned segments sequentially
   - Uses existing `nextDoc()` fast path (sequential DocValues access)
4. Wait for all tasks to complete
5. Merge N partial hash maps into one result map

### Merge Strategy

Pick the largest worker map as the base. For each remaining worker map, iterate its entries and probe-insert into the base. For each matching key, combine accumulators (add counts, add sums, combine avg numerator/denominator).

### Memory

- Per-worker: each worker sees ~1/N of docs → ~1/N of groups
- 8 workers × ~750K groups each × 25 bytes/entry = ~150MB per shard
- 4 shards = ~600MB total. Safe.

### Pros
- Uses fast `nextDoc()` sequential access (no random I/O)
- Clean boundary — segments are independent Lucene readers
- No synchronization during scan phase

### Cons
- Uneven segment sizes → worker imbalance
- Merge phase: O(numGroups × numWorkers) hash probes
- Requires per-worker hash map allocation

---

## Approach B: Doc-Range Splitting

### How It Works

1. For each segment, compute total docs (`reader.maxDoc()`)
2. Split each segment's doc range into N chunks: thread i gets [i×chunk, (i+1)×chunk)
3. Submit N tasks. Each task:
   - Gets its own `FlatSingleKeyMap` instance
   - Iterates its doc range using `advanceExact()` for DocValues access
4. Wait, then merge (same as Approach A)

### Key Difference from A

Parallelism is within segments, not across segments. Works even with 1 large segment (after force-merge). But must use `advanceExact()` instead of `nextDoc()` since workers start at arbitrary doc positions.

### Memory

Same as Approach A — per-worker maps with ~1/N of groups.

### Pros
- Even work distribution (doc count divided equally)
- Works regardless of segment count

### Cons
- `advanceExact()` is slower than `nextDoc()` (binary search vs sequential)
- Each worker opens DocValues iterators independently (may double memory for DV caches)
- More complex implementation (doc range bookkeeping per segment)

---

## Approach C: Lucene Parallel IndexSearcher

### How It Works

1. Create an `IndexSearcher` with an `Executor` (ForkJoinPool)
2. Implement a custom `Collector` / `LeafCollector` that:
   - Creates a per-segment `FlatSingleKeyMap`
   - For each matching doc, reads DocValues and accumulates into the map
3. Call `searcher.search(query, collector)` — Lucene parallelizes across segments
4. Collect per-segment maps from the custom collector
5. Merge all segment maps into one result

### Key Difference

Lucene manages thread assignment and segment dispatching. DQE provides the aggregation logic via Collector API.

### Memory

Per-segment maps (20-27 per shard). Each segment has ~1M docs → ~1M groups worst case. But groups overlap across segments, so per-segment maps are smaller. Estimated: 20 segments × ~200K groups × 25 bytes = ~100MB per shard.

### Pros
- Leverages Lucene's well-tested parallel infrastructure
- Lucene handles segment balancing and thread management
- Natural integration point for future query+aggregate fusion

### Cons
- Requires restructuring inner loop to Collector API (significant refactor)
- Collector is called per-doc, adding virtual method dispatch overhead
- Less control over thread assignment and memory budget
- DocValues access within Collector may conflict with Lucene's internal scoring threads

---

## Merge Algorithm (shared by all approaches)

```
mergeWorkerMaps(FlatSingleKeyMap[] workerMaps, int slotsPerGroup):
    // Pick largest map as base (zero-copy ownership)
    base = workerMaps[argmax(map.size for map in workerMaps)]

    for each other map in workerMaps (excluding base):
        for each occupied slot s in other:
            existingSlot = base.findOrInsert(other.keys[s])
            // Combine accumulators
            for a in 0..slotsPerGroup-1:
                base.accData[existingSlot * slotsPerGroup + a] += other.accData[s * slotsPerGroup + a]

    return base
```

For AVG: accumulators store (sum, count) separately. Merge adds both. Final AVG = sum/count computed at output.

---

## Experiment Plan

### Benchmark Protocol

For each approach (`off`, `segment`, `docrange`, `lucene`):

1. Restart OpenSearch with `-Ddqe.parallelism=<value>`
2. Run warmup: 3 passes of all 43 queries (JIT compilation)
3. Run benchmark: 3 timed passes of all 43 queries
4. Record min time per query

### Metrics

- Per-query time (ms)
- Aggregate ratio vs ClickHouse
- Count of queries within 2x ClickHouse
- CPU utilization during scan phase
- Peak heap usage

### Success Criteria

The winning approach must:
- Achieve measurable speedup (>2x) on Q17, Q18 (the biggest time-wasters)
- Not regress any query vs `off` mode
- Not cause OOM or instability
- Pass 1M correctness suite (32/43)

---

## Implementation Scope

### Phase 1: Experiment — Parallelize `executeNKeyVarcharPath`

Confirmed execution path routing for the heaviest queries:

| Query | Time | GROUP BY Keys | Execution Path |
|-------|-----:|---------------|----------------|
| Q19 | 81s | UserID (num), minute (expr), SearchPhrase (varchar) | `executeWithEvalKeys` |
| Q18 | 53s | UserID (num), SearchPhrase (varchar) | `executeWithVarcharKeys` → `executeNKeyVarcharPath` |
| Q17 | 51s | UserID (num), SearchPhrase (varchar) | `executeWithVarcharKeys` → `executeNKeyVarcharPath` |
| Q15 | 12s | SearchEngineID (num), SearchPhrase (varchar) | `executeWithVarcharKeys` → `executeNKeyVarcharPath` |

**Target: `executeNKeyVarcharPath`** — covers Q17 (51s) + Q18 (53s) + Q15 (12s) = 116s (39% of total).

This method uses SortedSetDocValues for VARCHAR keys (ordinal-indexed) and SortedNumericDocValues for numeric keys. The segment iteration loop processes all segments sequentially with a shared hash map.

Parallelizing this single method with all three approaches gives us a fair comparison on the heaviest workload.

### Phase 2: Extend winner to remaining paths

After picking the winner from Phase 1, extend parallelism to:
- `executeNumericOnly` → `executeSingleKeyNumericFlat` (Q9, Q10, Q16 — numeric GROUP BY)
- `executeTwoKeyNumericFlat` (Q33, Q40 — two numeric keys)
- `executeThreeKeyFlat` (part of Q19 after expression evaluation)
- `executeWithEvalKeys` (Q19 — expression-based GROUP BY)
- COUNT DISTINCT HashSet paths (Q5, Q6, Q14)
- Scalar aggregation paths (Q3, Q4 — no GROUP BY)

---

## Risks

| Risk | Mitigation |
|------|-----------|
| Merge overhead negates parallelism | Measure merge time separately. If >20% of total, reconsider. |
| Worker map memory exceeds budget | Size worker maps to MAX_CAPACITY / numBuckets / numWorkers |
| Uneven segment sizes (Approach A) | Use work-stealing or segment assignment by doc count |
| advanceExact() overhead (Approach B) | Benchmark against nextDoc() to quantify the cost |
| Collector API complexity (Approach C) | Time-box to 1 day; if too complex, report results as "not viable" |
| Feature flag not read correctly | Unit test for property parsing |
