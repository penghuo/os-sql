# Phase C: Global Ordinals in DQE — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Achieve 43/43 ClickBench queries within 2x ClickHouse by adopting Lucene Global Ordinals in DQE's FusedGroupByAggregate, matching native DSL aggregation speed.

**Architecture:** Build OrdinalMap (Lucene API) at query start to map segment-local ordinals to global ordinals. Replace HashMap-based multi-segment GROUP BY with global-ordinal-indexed arrays. Eliminate per-doc string hashing and cross-segment merge overhead.

**Tech Stack:** Java 21, Lucene OrdinalMap, OpenSearch 3.6.0-SNAPSHOT, FusedGroupByAggregate.java

**Key Data:** Native DSL aggregation returns Q17 in 13ms (warm). DQE returns Q17 in 28,878ms. The gap is entirely in DQE's execution approach, not Lucene's scan speed.

**Commit convention:** Every commit message ends with `(x/43 within 2x)` showing current progress.

---

### Task 1: Spike — Build and Test OrdinalMap in DQE

**Goal:** Prove OrdinalMap works in DQE. Measure build cost and memory. Prototype on Q34 (GROUP BY URL COUNT(*)).

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`

**Step 1: Add OrdinalMap builder helper**

Add a helper method that builds a Lucene OrdinalMap for a given VARCHAR field across all segments:

```java
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.PackedInts;

/**
 * Build a Lucene OrdinalMap for a VARCHAR field across all segments.
 * Maps segment-local ordinals to a global ordinal space, enabling
 * ordinal-indexed aggregation without cross-segment HashMap merge.
 *
 * @return OrdinalMap, or null if any segment lacks the field
 */
private static OrdinalMap buildGlobalOrdinalMap(
    List<LeafReaderContext> leaves, String fieldName) throws IOException {
  SortedSetDocValues[] subs = new SortedSetDocValues[leaves.size()];
  for (int i = 0; i < leaves.size(); i++) {
    subs[i] = leaves.get(i).reader().getSortedSetDocValues(fieldName);
    if (subs[i] == null) return null;
  }
  return OrdinalMap.build(null, subs, PackedInts.DEFAULT);
}
```

Place this near the other helper methods, before the inner class definitions (around line 9800).

**Step 2: Add global ordinal lookup helper**

```java
/**
 * Look up the term bytes for a global ordinal.
 * Uses OrdinalMap to find which segment owns this ordinal and resolves it.
 */
private static BytesRef lookupGlobalOrd(
    OrdinalMap ordinalMap, SortedSetDocValues[] segmentDvs, long globalOrd) throws IOException {
  int segmentIndex = ordinalMap.getFirstSegmentNumber(globalOrd);
  long segmentOrd = ordinalMap.getFirstSegmentOrd(globalOrd);
  return segmentDvs[segmentIndex].lookupOrd(segmentOrd);
}
```

**Step 3: Prototype in `executeSingleVarcharCountStar`**

In the `executeSingleVarcharCountStar` method, replace the multi-segment path (around line 1663 `// === Multi-segment path ===`) with a global ordinals path. Add a new branch BEFORE the existing multi-segment path:

```java
      // === Global ordinals multi-segment path ===
      OrdinalMap ordinalMap = buildGlobalOrdinalMap(leaves, columnName);
      if (ordinalMap != null) {
        long globalOrdCount = ordinalMap.getValueCount();
        // For manageable cardinality, use ordinal-indexed array
        if (globalOrdCount > 0 && globalOrdCount <= 50_000_000) {
          long[] globalCounts = new long[(int) globalOrdCount];

          // Scan all segments using global ordinals
          for (int segIdx = 0; segIdx < leaves.size(); segIdx++) {
            LeafReaderContext leafCtx = leaves.get(segIdx);
            SortedSetDocValues dv = leafCtx.reader().getSortedSetDocValues(columnName);
            if (dv == null) continue;
            LongValues segmentToGlobal = ordinalMap.getGlobalOrds(segIdx);

            // Use Scorer for filtered queries, direct iteration for MatchAll
            if (query instanceof MatchAllDocsQuery) {
              while (dv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                long segOrd = dv.nextOrd();
                long globalOrd = segmentToGlobal.get(segOrd);
                globalCounts[(int) globalOrd]++;
              }
            } else {
              // Need scorer for filtered queries
              IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
              Weight weight = luceneSearcher.createWeight(
                  luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
              Scorer scorer = weight.scorer(leafCtx);
              if (scorer == null) continue;
              DocIdSetIterator docIt = scorer.iterator();
              int doc;
              while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (dv.advanceExact(doc)) {
                  long segOrd = dv.nextOrd();
                  long globalOrd = segmentToGlobal.get(segOrd);
                  globalCounts[(int) globalOrd]++;
                }
              }
            }
          }

          // Count non-zero groups
          int groupCount = 0;
          for (int i = 0; i < globalCounts.length; i++) {
            if (globalCounts[i] > 0) groupCount++;
          }
          if (groupCount == 0) return List.of();

          // Top-N selection
          SortedSetDocValues[] segDvs = new SortedSetDocValues[leaves.size()];
          for (int i = 0; i < leaves.size(); i++) {
            segDvs[i] = leaves.get(i).reader().getSortedSetDocValues(columnName);
          }

          if (sortAggIndex >= 0 && topN > 0 && topN < groupCount) {
            int n = (int) Math.min(topN, groupCount);
            int[] heap = new int[n];
            long[] heapVals = new long[n];
            int heapSize = 0;

            for (int i = 0; i < globalCounts.length; i++) {
              if (globalCounts[i] == 0) continue;
              long cnt = globalCounts[i];
              if (heapSize < n) {
                heap[heapSize] = i;
                heapVals[heapSize] = cnt;
                heapSize++;
                int k = heapSize - 1;
                while (k > 0) {
                  int parent = (k - 1) >>> 1;
                  boolean swap = sortAscending
                      ? (heapVals[k] > heapVals[parent])
                      : (heapVals[k] < heapVals[parent]);
                  if (swap) {
                    int tmpI = heap[parent]; heap[parent] = heap[k]; heap[k] = tmpI;
                    long tmpV = heapVals[parent]; heapVals[parent] = heapVals[k]; heapVals[k] = tmpV;
                    k = parent;
                  } else break;
                }
              } else {
                boolean better = sortAscending ? (cnt < heapVals[0]) : (cnt > heapVals[0]);
                if (better) {
                  heap[0] = i; heapVals[0] = cnt;
                  int k = 0;
                  while (true) {
                    int left = 2 * k + 1;
                    if (left >= heapSize) break;
                    int right = left + 1;
                    int target = left;
                    if (right < heapSize) {
                      boolean pickRight = sortAscending
                          ? (heapVals[right] > heapVals[left])
                          : (heapVals[right] < heapVals[left]);
                      if (pickRight) target = right;
                    }
                    boolean swap = sortAscending
                        ? (heapVals[target] > heapVals[k])
                        : (heapVals[target] < heapVals[k]);
                    if (swap) {
                      int tmpI = heap[k]; heap[k] = heap[target]; heap[target] = tmpI;
                      long tmpV = heapVals[k]; heapVals[k] = heapVals[target]; heapVals[target] = tmpV;
                      k = target;
                    } else break;
                  }
                }
              }
            }

            BlockBuilder keyBuilder = VarcharType.VARCHAR.createBlockBuilder(null, heapSize);
            BlockBuilder countBuilder = BigintType.BIGINT.createBlockBuilder(null, heapSize);
            for (int i = 0; i < heapSize; i++) {
              BytesRef bytes = lookupGlobalOrd(ordinalMap, segDvs, heap[i]);
              VarcharType.VARCHAR.writeSlice(keyBuilder,
                  Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              BigintType.BIGINT.writeLong(countBuilder, heapVals[i]);
            }
            return List.of(new Page(keyBuilder.build(), countBuilder.build()));
          }

          // No top-N: output all groups
          BlockBuilder keyBuilder = VarcharType.VARCHAR.createBlockBuilder(null, groupCount);
          BlockBuilder countBuilder = BigintType.BIGINT.createBlockBuilder(null, groupCount);
          for (int i = 0; i < globalCounts.length; i++) {
            if (globalCounts[i] > 0) {
              BytesRef bytes = lookupGlobalOrd(ordinalMap, segDvs, i);
              VarcharType.VARCHAR.writeSlice(keyBuilder,
                  Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              BigintType.BIGINT.writeLong(countBuilder, globalCounts[i]);
            }
          }
          return List.of(new Page(keyBuilder.build(), countBuilder.build()));
        }
      }
```

**Step 4: Compile and verify**

```bash
cd /home/ec2-user/oss/wukong && ./gradlew :dqe:compileJava
```

**Step 5: Build plugin, deploy, test Q34**

```bash
./gradlew :opensearch-sql-plugin:bundlePlugin -x test -x integTest -x yamlRestTest
# Stop OS, reinstall plugin, restart
# Test Q34:
curl -s -XPOST "http://localhost:9200/_plugins/_trino_sql" \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10"}' \
  --max-time 120 | jq '{status, rows: (.datarows | length), first: .datarows[0]}'
```

Measure timing. Compare with baseline (20s). Expected: <5s.

**Step 6: Run 1M correctness**

```bash
cd benchmarks/clickbench && bash run/run_all.sh correctness
```

Expected: 32/43 (no regression).

**Step 7: Commit**

```bash
git add -A && git commit -m "perf(dqe): global ordinals for single-VARCHAR COUNT(*) GROUP BY (x/43 within 2x)"
```

---

### Task 2: Single-VARCHAR Global Ordinals for Generic Aggregates

**Goal:** Extend global ordinals to `executeSingleVarcharGeneric` (Q13, Q14, Q34 with SUM/AVG/MIN/MAX).

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`

**Step 1: Add global ordinals path in `executeSingleVarcharGeneric`**

Before the multi-segment path (around line 2243), add a global ordinals branch. Same pattern as Task 1 but using `AccumulatorGroup[]` indexed by global ordinal instead of `long[]`:

```java
      // === Global ordinals multi-segment path ===
      OrdinalMap ordinalMap = buildGlobalOrdinalMap(leaves, columnName);
      if (ordinalMap != null) {
        long globalOrdCount = ordinalMap.getValueCount();
        if (globalOrdCount > 0 && globalOrdCount <= 10_000_000) {
          AccumulatorGroup[] globalGroups = new AccumulatorGroup[(int) globalOrdCount];

          IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
          Weight weight = luceneSearcher.createWeight(
              luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);

          for (int segIdx = 0; segIdx < leaves.size(); segIdx++) {
            LeafReaderContext leafCtx = leaves.get(segIdx);
            SortedSetDocValues dv = leafCtx.reader().getSortedSetDocValues(columnName);
            if (dv == null) continue;
            LongValues segmentToGlobal = ordinalMap.getGlobalOrds(segIdx);

            // Open agg DocValues
            SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
            SortedSetDocValues[] varcharAggDvs = new SortedSetDocValues[numAggs];
            for (int i = 0; i < numAggs; i++) {
              if (!isCountStar[i]) {
                AggSpec spec = specs.get(i);
                if (isVarcharArg[i]) {
                  varcharAggDvs[i] = leafCtx.reader().getSortedSetDocValues(spec.arg);
                } else {
                  numericAggDvs[i] = leafCtx.reader().getSortedNumericDocValues(spec.arg);
                }
              }
            }

            Scorer scorer = weight.scorer(leafCtx);
            if (scorer == null) continue;
            DocIdSetIterator docIt = scorer.iterator();
            int doc;
            while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
              if (!dv.advanceExact(doc)) continue;
              long segOrd = dv.nextOrd();
              int globalOrd = (int) segmentToGlobal.get(segOrd);
              AccumulatorGroup accGroup = globalGroups[globalOrd];
              if (accGroup == null) {
                accGroup = createAccumulatorGroup(specs);
                globalGroups[globalOrd] = accGroup;
              }
              collectVarcharGenericAccumulate(doc, accGroup, numAggs,
                  isCountStar, isVarcharArg, isDoubleArg, accType,
                  numericAggDvs, varcharAggDvs);
            }
          }

          // Count non-null groups, build output (same as single-segment path)
          // ... (reuse existing output-building code pattern with lookupGlobalOrd for key resolution)
        }
      }
```

**Step 2: Compile, deploy, test Q13/Q14**

```bash
./gradlew :dqe:compileJava
# rebuild, deploy, test
```

**Step 3: Run correctness + benchmark affected queries**

**Step 4: Commit**

```bash
git commit -m "perf(dqe): global ordinals for single-VARCHAR generic aggregates (x/43 within 2x)"
```

---

### Task 3: Multi-Key Global Ordinals for NKeyVarchar Path

**Goal:** Replace HashMap-based NKeyVarchar multi-segment path with global ordinals. Covers Q15, Q17, Q18, Q31, Q32, Q36.

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`

**Step 1: Spike — determine composite key strategy**

For Q17 (UserID:numeric + SearchPhrase:varchar), the composite key is (long, globalOrd). Two strategies:

- **A) FlatTwoKeyMap with (numericKey, globalOrd)**: Reuse existing flat map, treat global ordinal as second numeric key. This works because global ordinals ARE longs.
- **B) Ordinal-indexed + nested**: If one key is low-cardinality, use ordinal-indexed outer array with hash map inner. Unlikely to help for Q17 (high-cardinality UserID).

Try strategy A first. In `executeNKeyVarcharPath`, before the multi-segment path (line ~8721):

1. Build OrdinalMap for each VARCHAR key
2. In the Collector, convert segment ordinals to global ordinals
3. Use (numericKey, globalOrd) as composite key in existing per-doc hash map (but now all values are longs — no string allocation)

The key change: instead of `SegmentGroupKey` → `MergedGroupKey` (which allocates BytesRefKey), use `SegmentGroupKey` with global ordinals directly. No merge phase needed because global ordinals are consistent across segments.

```java
      // === Global ordinals multi-segment path ===
      // Build OrdinalMap for each VARCHAR key
      OrdinalMap[] ordinalMaps = new OrdinalMap[keyInfos.size()];
      boolean canUseGlobalOrdinals = true;
      for (int k = 0; k < keyInfos.size(); k++) {
        if (keyInfos.get(k).isVarchar) {
          ordinalMaps[k] = buildGlobalOrdinalMap(leaves, keyInfos.get(k).name);
          if (ordinalMaps[k] == null) { canUseGlobalOrdinals = false; break; }
        }
      }

      if (canUseGlobalOrdinals) {
        // Single global HashMap using global ordinals as keys (no cross-segment merge needed)
        Map<SegmentGroupKey, AccumulatorGroup> globalGroups = new HashMap<>();

        for (int segIdx = 0; segIdx < leaves.size(); segIdx++) {
          // ... build per-segment LongValues[] for ordinal mapping
          // ... in collect(): convert segment ordinal to global ordinal before hashing
          // ... accumulate directly into globalGroups (no finish/merge phase)
        }

        // Build output: resolve global ordinals to strings only for output rows
        // ... use lookupGlobalOrd() for VARCHAR keys in output entries
      }
```

**Step 2: Implement, compile, test Q17/Q18**

**Step 3: Run full benchmark, correctness**

**Step 4: Commit**

```bash
git commit -m "perf(dqe): global ordinals for multi-key VARCHAR GROUP BY (x/43 within 2x)"
```

---

### Task 4: Spike — Profile Remaining Bottlenecks

**Goal:** After global ordinals, measure what's still slow. Identify next optimization.

**Step 1: Run full 43-query benchmark**

```bash
cd benchmarks/clickbench && bash run/run_opensearch.sh --timeout 120 --warmup 1 --num-tries 3 \
  --output-dir results/performance/opensearch_global_ordinals
```

**Step 2: Compare with ClickHouse, identify remaining gaps**

```bash
python3 compare_results.py  # compare all 43 queries
```

**Step 3: For each query still >2x CH, categorize:**
- Is it a scan speed issue? (full table, no WHERE)
- Is it a filter issue? (WHERE clause not selective enough)
- Is it an expression issue? (REGEXP, EXTRACT, CASE)
- Is it a fixed overhead issue? (<10ms in CH)

**Step 4: Write spike findings and next steps**

---

### Task 5: Numeric-Only GROUP BY Optimization

**Goal:** Optimize Q3/Q4/Q5/Q6/Q9/Q10/Q16/Q30/Q36.

**Depends on:** Task 4 profiling results to prioritize.

**Spike:** Profile `executeNumericOnly` and `FusedScanAggregate` paths. Measure per-doc overhead.

**Possible optimizations:**
- Q3/Q4 (scalar agg): Single-pass DocValues scan without GROUP BY overhead. Should be <50ms.
- Q5/Q6 (COUNT DISTINCT): Parallelize LongOpenHashSet building across segments
- Q9/Q10/Q16 (numeric GROUP BY): Already uses FlatSingleKeyMap — profile to find bottleneck
- Q30 (89 SUMs): Batch SUM computation, read DocValues once

**Step 1: Profile, identify bottleneck**
**Step 2: Implement fix**
**Step 3: Benchmark, correctness check**
**Step 4: Commit with progress**

---

### Task 6: WHERE-Filtered Query Optimization

**Goal:** Optimize Q8/Q11-Q15/Q21-Q24/Q28/Q37-Q43.

**Depends on:** Task 4 profiling results.

**Key insight:** These queries have selective WHERE clauses. Q37-Q43 filter to CounterID=62 (~50K docs out of 100M). Native DSL handles these in <50ms because Lucene's BKD tree skips non-matching segments.

**Spike:** Measure how many segments/docs match for each query.

**Possible optimizations:**
- Ensure DQE generates efficient Lucene queries (PointRangeQuery for numeric ranges)
- Skip segments where Scorer returns null (already done in parallel paths)
- Reduce per-segment LeafCollector overhead for tiny result sets
- Apply global ordinals even for filtered queries (smaller ordinal space after filter)

**Step 1: Measure segment hit rates per query**
**Step 2: Implement optimization**
**Step 3: Benchmark, correctness**
**Step 4: Commit**

---

### Task 7: Expression GROUP BY Optimization

**Goal:** Optimize Q19 (51s), Q29 (26s), Q40 (2.9s).

**Depends on:** Tasks 3 and 4.

**Spike needed:** Can global ordinals combine with ordinal caching for expressions?

- Q19: `GROUP BY UserID, EXTRACT(minute FROM EventTime), SearchPhrase` — SearchPhrase can use global ordinals. EXTRACT can be pre-computed per unique EventTime ordinal. UserID is numeric.
- Q29: `GROUP BY REGEXP_REPLACE(Referer, ...)` — already uses ordinal caching (~16K ordinals vs ~921K docs). Profile to find remaining bottleneck.
- Q40: `GROUP BY ... CASE WHEN ... END, URL` — expression + VARCHAR key

**Step 1: Profile Q19 with global ordinals on SearchPhrase**
**Step 2: Implement combined global-ordinals + expression-caching**
**Step 3: Benchmark, correctness**
**Step 4: Commit**

---

### Task 8: Fixed Overhead Optimization

**Goal:** Get Q1 (7ms→<2ms) and Q20 (9ms→<6ms) within 2x CH.

**Q1: COUNT(*)** — Read from index metadata. No scan needed.
```java
// In executeWithTopN or canFuse check:
if (groupByKeys.isEmpty() && aggregates == ["COUNT(*)"]) {
  long count = engineSearcher.getIndexReader().numDocs();
  // Return single-row Page with count
}
```

**Q20: WHERE UserID = value** — Ensure Lucene uses PointQuery on long field for O(log n) lookup.

**Step 1: Implement metadata COUNT(*)**
**Step 2: Profile Q20 Lucene query plan**
**Step 3: Benchmark, correctness**
**Step 4: Commit**

---

## Execution Order

```
Task 1 (Spike: OrdinalMap) ──→ Task 2 (Single-VARCHAR Generic) ──→ Task 3 (Multi-Key)
                                                                         │
                                                                    Task 4 (Profile)
                                                                         │
                                                          ┌──────────────┼──────────────┐
                                                     Task 5          Task 6          Task 7
                                                   (Numeric)     (WHERE-filter)   (Expression)
                                                          └──────────────┼──────────────┘
                                                                         │
                                                                    Task 8 (Overhead)
```

Tasks 1→2→3→4 are sequential. Tasks 5-7 can interleave based on Task 4's profiling. Task 8 is independent.

## Success Criteria

- 43/43 queries within 2x ClickHouse on 100M ClickBench
- 32/43 correctness on 1M (no regression)
- 0 OOM, 0 circuit breaker, 0 crashes on 100M
- Each commit tracks progress: `(x/43 within 2x)`
