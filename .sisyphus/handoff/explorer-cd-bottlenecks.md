# COUNT(DISTINCT) Execution Path Bottleneck Analysis

## File: dqe/.../TransportShardExecuteAction.java

---

## 1. Method-by-Method Analysis

### executeCountDistinctWithHashSets (L1001-L1367) — Q04/Q08

- **Doc iteration**: Parallel segment scanning via ForkJoinPool (CountDownLatch pattern)
- **Columnar loading**: YES — `FusedGroupByAggregate.loadNumericColumn()` for both key0/key1 (L1161-1162), but ONLY for MatchAll + no liveDocs
- **Cache-friendly path**: YES — 3-pass partitioned approach when grpSize < 10,000: (1) discover groups, (2) scatter k1 into per-group arrays, (3) sequential hash set insertion per group
- **High-cardinality fallback**: Round-robin insertion (interleaved hash set access = cache thrashing)
- **Segment merge**: Union LongOpenHashSets with smaller-into-larger optimization

### executeNKeyCountDistinctWithHashSets (L1390-L1660) — Q11/Q13

- **Doc iteration**: Parallel segment scanning (same CountDownLatch pattern)
- **Columnar loading**: NO — uses per-doc `nextDoc()` iteration even for MatchAll (L1600-1620)
- **Hash map**: java.util.HashMap<LongArrayKey, LongOpenHashSet> — boxed keys, object overhead
- **LongArrayKey**: Allocates `tmpGroupKey.clone()` per doc (L1625) — massive GC pressure
- **Segment merge**: Same union pattern as 2-key path

### executeMixedDedupWithHashSets (L1992-L2470) — Q09

- **Doc iteration**: Parallel via CompletableFuture for MatchAll with numWorkers > 1
- **Columnar loading**: NO — uses per-doc `nextDoc()` DV iteration in all paths
- **Worker partitioning**: Greedy largest-first segment assignment (good)
- **Hash map**: Custom open-addressing (good), but per-doc DV reads are the bottleneck
- **Accumulator merge**: Sequential merge of worker results into main hash map

### executeDistinctValuesScanWithRawSet (L3084-L3113) — Q04/Q05

- **Delegates to**: `FusedScanAggregate.collectDistinctValuesRaw()`
- **Two-phase approach**: (1) parallel columnar load into long[], (2) sequential hash set insertion
- **Pre-sizing**: Estimates distinct count as min(totalDocs/4, 8M) to avoid resizes
- **Single segment**: Fused single-pass with run-length dedup

---

## 2. Coordinator Merge Logic (TransportTrinoSqlAction.java)

### mergeCountDistinctValuesViaRawSets (L2026) — scalar CD
- Finds largest shard set, merges smaller sets into temp, parallel contains() check against largest
- **Good**: read-only parallel counting, no mutation of largest set

### mergeDedupCountDistinctViaSets (L2527) — grouped CD (Q04/Q08)
- Groups per-shard sets by group key, then calls `countMergedGroupSets()` per group
- **countMergedGroupSets** (L2661): finds largest set, builds `extras` LongOpenHashSet for elements not in largest
- **TopN optimization**: if topN*10 < totalGroups, uses min-heap to prune candidate groups by upper bound (sum of shard sizes)
- **Bottleneck**: Each group merge is sequential — no parallelism across groups

### mergeMixedDedupViaSets (L3074) — Q09
- Iterates shard Pages to merge accumulators + union HashSets per group key
- **Bottleneck**: HashSet merge uses `existing.add(v)` in a loop over srcKeys — mutates the target set, no parallel merge

---

## 3. Identified Bottlenecks & Optimization Opportunities

### BOTTLENECK 1: NKey path (Q11/Q13) lacks columnar loading
- **Location**: `scanSegmentForNKeyCountDistinct` L1590-1640
- **Issue**: Uses per-doc `dvs[i].nextDoc()` for all N keys — sequential DV access
- **Fix**: Load all N columns via `loadNumericColumn()` into flat arrays, then iterate arrays
- **Impact**: HIGH — eliminates per-doc DV overhead for 3+ key queries

### BOTTLENECK 2: NKey path allocates LongArrayKey per doc
- **Location**: `scanSegmentForNKeyCountDistinct` L1625 — `new LongArrayKey(tmpGroupKey.clone())`
- **Issue**: Every doc allocates a long[] + LongArrayKey object → massive GC pressure on millions of docs
- **Fix**: Use open-addressing with composite key hashing (like the 2-key path does with `Long.hashCode(k0)`) or encode N keys into a single long hash for the group map
- **Impact**: HIGH — eliminates millions of allocations per segment

### BOTTLENECK 3: NKey path uses HashMap instead of open-addressing
- **Location**: `scanSegmentForNKeyCountDistinct` L1588
- **Issue**: `java.util.HashMap<LongArrayKey, LongOpenHashSet>` — boxed keys, Entry objects, poor cache locality
- **Fix**: Custom open-addressing map with long[] keys (like 2-key path's grpKeys/grpOcc/grpSets arrays)
- **Impact**: MEDIUM — reduces cache misses and GC pressure

### BOTTLENECK 4: Mixed dedup (Q09) lacks columnar loading
- **Location**: `executeMixedDedupWithHashSets` worker lambda ~L2130-2180
- **Issue**: Per-doc `nextDoc()` DV iteration for key0, key1, and all aggregate columns
- **Fix**: Load columns into flat arrays first, then iterate — same pattern as 2-key CD path
- **Impact**: HIGH — sequential memory access vs random DV probing

### BOTTLENECK 5: Mixed dedup (Q09) parallel path still uses per-doc DV
- **Location**: Worker lambda in parallel MatchAll path ~L2130
- **Issue**: Even in parallel workers, each worker iterates DV per-doc instead of columnar
- **Fix**: Each worker should `loadNumericColumn()` for its segments first
- **Impact**: MEDIUM-HIGH — compounds with BOTTLENECK 4

### BOTTLENECK 6: Coordinator group merge is sequential
- **Location**: `mergeDedupCountDistinctViaSets` L2615-2625
- **Issue**: Iterates candidateKeys sequentially, calling `countMergedGroupSets()` per group
- **Fix**: Parallel stream or ForkJoinPool over candidate groups
- **Impact**: LOW-MEDIUM — depends on number of groups (typically ~400 for Q04/Q08)

### BOTTLENECK 7: No cache-friendly partitioned path for NKey/Mixed
- **Location**: 2-key path has it at L1190-1240 (grpSize < 10000), NKey and Mixed paths don't
- **Issue**: Round-robin hash set insertion across groups causes L2 cache thrashing
- **Fix**: Port the 3-pass partitioned approach (discover groups → scatter → sequential insert) to NKey and Mixed paths
- **Impact**: MEDIUM-HIGH for high-cardinality dedup keys

### BOTTLENECK 8: collectDistinctValuesRaw merges per-segment sets sequentially
- **Location**: FusedScanAggregate.java L1674+
- **Issue**: After parallel columnar load, hash set insertion into single `distinctSet` is sequential
- **Fix**: Build per-segment LongOpenHashSets in parallel, then merge (like the 2-key CD path does)
- **Impact**: MEDIUM — already has parallel load, but insertion is the bottleneck

---

## 4. Feature Comparison Matrix

| Feature                        | 2-key CD (Q04/Q08) | NKey CD (Q11/Q13) | Mixed Dedup (Q09) | Scalar CD (Q04/Q05) |
|-------------------------------|---------------------|--------------------|--------------------|----------------------|
| Parallel segment scan          | ✅ CountDownLatch   | ✅ CountDownLatch  | ✅ CompletableFuture| ✅ CompletableFuture |
| Columnar loading (flat array)  | ✅ loadNumericColumn| ❌ per-doc nextDoc | ❌ per-doc nextDoc | ✅ parallel load     |
| Open-addressing group map      | ✅ grpKeys/grpOcc   | ❌ HashMap         | ✅ grpKeys/grpOcc  | N/A (no grouping)    |
| Cache-friendly partitioned     | ✅ (grpSize<10K)    | ❌                 | ❌                 | N/A                  |
| Allocation-free inner loop     | ✅                  | ❌ clone() per doc | ✅                 | ✅                   |
| Pre-sized hash sets            | ✅ 1024 initial     | ✅ 1024 initial    | ⚠️ 16 initial      | ✅ estimated         |
| Coordinator parallel merge     | ❌ sequential       | N/A (tuple output) | ❌ sequential      | ✅ parallel contains |

---

## 5. Priority Ranking (by expected impact)

1. **P0 — NKey columnar loading** (Q11/Q13): Port `loadNumericColumn()` to scanSegmentForNKeyCountDistinct
2. **P0 — NKey allocation elimination** (Q11/Q13): Replace HashMap<LongArrayKey> with open-addressing arrays
3. **P1 — Mixed dedup columnar loading** (Q09): Port `loadNumericColumn()` to worker lambdas
4. **P1 — Mixed dedup initial set size** (Q09): Change from 16 to 1024 (L2161) — trivial fix, avoids 6 resizes
5. **P2 — Cache-friendly partitioned path** for NKey and Mixed paths
6. **P2 — Coordinator parallel group merge** for grouped CD queries
7. **P3 — Scalar CD parallel set insertion** (already has parallel load)
