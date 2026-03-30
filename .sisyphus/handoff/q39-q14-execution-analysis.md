# Q39 & Q14 Execution Path Analysis

## Q39: CASE WHEN + 5 GROUP BY keys + OFFSET 1000

### Query Pattern
```sql
SELECT TraficSourceID, SearchEngineID, AdvEngineID, 
       CASE WHEN ... END AS Src, URL AS Dst, COUNT(*) 
FROM hits WHERE CounterID=62 AND EventDate BETWEEN ... AND IsRefresh=0 
GROUP BY ... ORDER BY PageViews DESC OFFSET 1000 LIMIT 10
```

### Execution Path

1. **TransportShardExecuteAction.executePlan()** (line ~170-230):
   - Plan structure: `LimitNode -> ProjectNode -> SortNode -> AggregationNode(EvalNode -> TableScanNode)`
   - Hits `extractAggFromSortedLimit()` which unwraps to the inner AggregationNode
   - The AggregationNode has an EvalNode child (for CASE WHEN expression)

2. **canFuse() check** (FusedGroupByAggregate.java, line ~130-180):
   - Detects EvalNode-computed keys via `evalOutputNames.contains(key)`
   - For the CASE WHEN key ("Src"), it's recognized as an eval expression
   - For URL ("Dst"), it's a plain VARCHAR column
   - Returns `true` — the fused path is eligible

3. **Dispatch in executeInternal()** (FusedGroupByAggregate.java, line ~580-640):
   - `hasVarchar = true` (URL is VARCHAR)
   - `hasEvalKey = true` (CASE WHEN expression detected)
   - **Routes to `executeWithEvalKeys()`** (line ~640)

4. **executeWithEvalKeys()** (FusedGroupByAggregate.java, line ~2100+):
   - **Phase 1: Expression compilation**
     - Compiles CASE WHEN expression using ExpressionCompiler
     - Detects inline CASE WHEN optimization: `CASE WHEN (col1=const AND col2=const) THEN varchar_col ELSE '' END`
     - If pattern matches → uses ordinal-based inline evaluation (no Block materialization)
     - If not → falls back to Block-based per-micro-batch evaluation
   
   - **Phase 2: Per-segment doc collection + eval**
     - Collects matching doc IDs into int[] array via Scorer
     - Processes in micro-batches of 256 docs
     - **CASE WHEN is evaluated per-doc** (or per-micro-batch via Block expressions)
     - Non-eval keys (numeric) read directly from DocValues
     - VARCHAR keys (URL) read from SortedSetDocValues using ordinals (single-segment) or BytesRefKey (multi-segment)
   
   - **Phase 3: Grouping**
     - Uses `LinkedHashMap<Object, AccumulatorGroup>` or flat long[] map (single-segment, all-long-keys)
     - For Q39 with URL (varchar), uses the generic HashMap path with ProbeGroupKey

5. **OFFSET handling**:
   - `topN = limitNode.getCount() + limitNode.getOffset()` = 10 + 1000 = **1010**
   - Passed to `executeFusedGroupByAggregateWithTopN()` or the sort+limit fallback
   - The fused path computes **all groups** first, then applies top-N heap selection for 1010 groups
   - **OFFSET is NOT pushed down** — all groups are materialized, then top-1010 selected, then coordinator applies OFFSET 1000

### Q39 Performance Bottlenecks

| Bottleneck | Impact | Location |
|---|---|---|
| **CASE WHEN per-doc evaluation** | High — evaluated for every matching doc, not cached per ordinal | `executeWithEvalKeys()` micro-batch loop |
| **URL (VARCHAR) as GROUP BY key** | High — URL has ~700K+ unique values, each requiring BytesRefKey allocation | `executeWithEvalKeys()` grouping phase |
| **5 GROUP BY keys** | Medium — falls into N-key generic path, not the optimized 2-key FlatTwoKeyMap | `executeWithEvalKeys()` uses HashMap |
| **All groups materialized** | Medium — must compute all groups before top-1010 selection | No early termination possible with ORDER BY |
| **No ordinal caching for CASE WHEN** | High — unlike `executeWithExpressionKey()` which caches per ordinal, the eval path evaluates per doc | Design limitation |

### Q39 Optimization Opportunities

1. **Ordinal-cache CASE WHEN results**: The CASE WHEN expression depends on numeric columns (TraficSourceID, SearchEngineID, AdvEngineID) and URL. If the condition columns are GROUP BY keys, the CASE WHEN result is deterministic per group — could be computed once per group instead of per doc.

2. **Inline CASE WHEN detection** (partially implemented): Lines ~2200-2280 detect simple `CASE WHEN (col=const) THEN varchar_col ELSE '' END` patterns and convert to ordinal-based inline evaluation. Q39's CASE WHEN with multiple WHEN branches may not match this pattern.

3. **Top-N push-down into grouping**: Currently all groups are materialized. For ORDER BY COUNT(*) DESC OFFSET 1000 LIMIT 10, could use a heap of size 1010 during accumulation to prune low-count groups early.

---

## Q14: SearchEngineID + SearchPhrase GROUP BY with WHERE filter

### Query Pattern
```sql
SELECT SearchEngineID, SearchPhrase, COUNT(*) 
FROM hits WHERE SearchPhrase<>'' 
GROUP BY SearchEngineID, SearchPhrase 
ORDER BY c DESC LIMIT 10
```

### Execution Path

1. **TransportShardExecuteAction.executePlan()** (line ~170-230):
   - Plan structure: `LimitNode -> ProjectNode -> SortNode -> AggregationNode -> TableScanNode`
   - `extractAggFromSortedLimit()` extracts the AggregationNode
   - `canFuse()` returns true (SearchEngineID=numeric, SearchPhrase=VARCHAR)

2. **Dispatch decision** (line ~200-230):
   - `sortIndices.size() == 1 && sortIndices.get(0) >= numGroupByCols` → true (sorting by COUNT(*))
   - **Routes to `executeFusedGroupByAggregateWithTopN()`** with sortAggIndex=0, sortAsc=false, topN=10

3. **FusedGroupByAggregate.executeWithTopN()** → **executeInternal()** (line ~560):
   - `hasVarchar = true` (SearchPhrase is VARCHAR)
   - `keyInfos.size() == 2` → enters single-segment 2-key fast path check
   - **For single-segment**: tries `tryOrdinalIndexedTwoKeyCountStar()` first
     - Checks: allCountStar=true, one varchar key, one numeric key
     - SearchPhrase has ~18K ordinals, SearchEngineID has ~39 distinct values
     - **Uses ordinal-indexed path**: per-ordinal small array for numeric key → O(1) lookup
   - **For multi-segment**: falls through to `executeWithVarcharKeys()` → flat 2-key path

4. **Specific method for Q14** (single-segment case):
   - `tryOrdinalIndexedTwoKeyCountStar()` (line ~3800+)
   - Allocates `numericKeys[numOrds * KEYS_PER_ORD]` and `counts[numOrds * KEYS_PER_ORD]`
   - KEYS_PER_ORD = 64 (max numeric keys per varchar ordinal)
   - For each doc: `varcharDv.advanceExact(doc)` → get ordinal → linear scan of numeric keys array
   - **Filtered query path**: uses Lucene Collector with advanceExact per doc

5. **Parallelism**: 
   - Single-segment: **NOT parallel** (sequential scan)
   - Multi-segment: parallel via ForkJoinPool workers in `executeWithVarcharKeys()`

### Q14 Performance Characteristics

| Aspect | Detail |
|---|---|
| **Filter** | `SearchPhrase<>''` — compiled to Lucene query, reduces doc set |
| **Group cardinality** | ~18K SearchPhrase ordinals × ~39 SearchEngineID values |
| **Key types** | numeric (SearchEngineID) + VARCHAR (SearchPhrase) |
| **Aggregation** | COUNT(*) only — uses ordinal-indexed fast path |
| **Top-N** | topN=10, applied via heap selection on ordinal counts |
| **Parallelism** | Single-segment: none. Multi-segment: parallel workers |

### Q14 Optimization Opportunities

1. **Filter selectivity**: `SearchPhrase<>''` likely matches a large fraction of docs. The filtered path uses `advanceExact()` per doc which is slower than sequential `nextDoc()`. Could use bitset-then-sequential approach (already implemented for some paths).

2. **Global ordinals for multi-segment**: For filtered queries with ≤1M ordinals, the code already builds global ordinals (line ~800). Q14 with ~18K ordinals should hit this path on multi-segment indices.

3. **Parallel filtered scan**: The single-segment filtered path is sequential. For Q14's ~0.15s improvement target, adding doc-range parallelism within the single segment could help.

4. **Forward-only DV iteration**: The filtered path currently uses `advanceExact()` per doc. Converting to collect-then-sequential (like the VARCHAR COUNT(DISTINCT) path at line ~2400) would avoid binary search overhead.

---

## Key Differences: Q39 vs Q14

| Aspect | Q39 | Q14 |
|---|---|---|
| **Path** | `executeWithEvalKeys()` | `tryOrdinalIndexedTwoKeyCountStar()` or flat 2-key |
| **# GROUP BY keys** | 5 (including CASE WHEN + URL) | 2 (numeric + varchar) |
| **Expression eval** | CASE WHEN per-doc | None |
| **VARCHAR key cardinality** | ~700K (URL) | ~18K (SearchPhrase) |
| **Hash map type** | Generic HashMap with Object[] keys | Ordinal-indexed array or FlatTwoKeyMap |
| **Top-N** | 1010 (OFFSET 1000 + LIMIT 10) | 10 |
| **Regression factor** | 27.36x | 2.20x |
| **Primary bottleneck** | Per-doc CASE WHEN eval + high-cardinality URL | Filter advanceExact overhead |
