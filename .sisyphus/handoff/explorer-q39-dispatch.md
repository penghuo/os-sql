# Q39 Dispatch Path Analysis: executeWithEvalKeys

## 1. Exact Dispatch Path

```
executeInternal (line 1280)
  → classifies keys via KeyInfo loop (line 1330+)
  → Q39 keys: TraficSourceID (numeric), SearchEngineID (numeric), AdvEngineID (numeric),
               CASE WHEN... (eval/VARCHAR), URL AS Dst (eval→column-ref optimization)
  → hasVarchar = true (CASE WHEN produces VARCHAR)
  → NOT single VARCHAR key (5 keys total)
  → hasEvalKey = true (CASE WHEN detected as "eval" expression)
  → DISPATCHES TO: executeWithEvalKeys (line 1430 → line 7170)
```

## 2. Inline CASE WHEN Optimization — YES, Q39 ACTIVATES IT

The code at lines 7310-7410 detects Q39's exact pattern:
```
CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END
```

This matches the inline optimization because:
- Single WHEN clause ✓
- Output type is VARCHAR ✓  
- ELSE is constant empty string ✓
- THEN is a ColumnReference to VARCHAR column (Referer) ✓
- Condition columns (SearchEngineID, AdvEngineID) are non-eval GROUP BY keys ✓

**Result**: `inlineEvalKey[3] = true`, `isEvalKey[3] = false`
- The CASE WHEN is NOT evaluated via Block/CaseBlockExpression
- Instead, it checks `resolvedKeys[condIdx[c]]` (already-resolved SearchEngineID/AdvEngineID values) against 0L
- If condition true → reads Referer from DocValues ordinal
- If condition false → uses sentinel ordinal (-2L) for empty string

## 3. URL AS Dst — Column Reference Optimization

At line 7244-7258, `URL AS Dst` is detected as a simple ColumnReference:
```java
if (evalExprs[k] instanceof ColumnReference colRef) {
    // Convert to regular non-eval key → direct DocValues reading
    keyInfos.set(k, new KeyInfo(colName, colType, colIsVarchar, null, null));
    isEvalKey[k] = false;
    evalExprs[k] = null;
}
```
**Result**: URL is read directly via SortedSetDocValues, no Block materialization.

## 4. After Optimizations: hasRemainingEvalKeys = FALSE

Both eval keys are optimized away:
- CASE WHEN → inlined (no Block eval)
- URL AS Dst → column reference (no Block eval)

This means `evalColumns` is empty after the rebuild at line 7420+, and the Block materialization loop at line 7685-7740 is **SKIPPED entirely** (`hasRemainingEvalKeys = false`).

## 5. Flat Long Map — YES, Q39 ACTIVATES IT

At line 7620-7630, the check `allKeysLong` evaluates:
- TraficSourceID: numeric → long ✓
- SearchEngineID: numeric → long ✓
- AdvEngineID: numeric → long ✓
- CASE WHEN (inlineEvalKey): VARCHAR ordinal → long ✓
- URL (keyIsVarchar): ordinal → long ✓

**Result**: `useFlatLongMap = true`, initial capacity 32768.
Uses open-addressing hash with `long[]` parallel arrays — zero per-group object allocation.

## 6. The Hot Loop (per-doc processing)

For each doc in each micro-batch of 256:

```
Per doc (6 advanceExact calls in worst case):
  1. Read TraficSourceID from SortedNumericDocValues.advanceExact(docId)
  2. Read SearchEngineID from SortedNumericDocValues.advanceExact(docId)
  3. Read AdvEngineID from SortedNumericDocValues.advanceExact(docId)
  4. Read URL ordinal from SortedSetDocValues.advanceExact(docId)
  5. Inline CASE WHEN: check SearchEngineID==0 && AdvEngineID==0
     - If true: Read Referer ordinal from SortedSetDocValues.advanceExact(docId) [6th call]
     - If false: Use sentinel -2L (no DocValues read)
  6. Hash 5 long keys → probe flat map → CountStarAccum.count++
```

**Key cost**: 5-6 `advanceExact` calls per doc, including 1-2 VARCHAR DocValues lookups (URL always, Referer conditionally).

## 7. NO PARALLEL EXECUTION — CONFIRMED

Searched lines 7170-8218 for: `parallel`, `ForkJoin`, `CompletableFuture`, `submit`, `invokeAll`, `PARALLEL_POOL`.
**Zero matches.** The entire method runs single-threaded.

Meanwhile, other paths in the same file use `PARALLEL_POOL` with `CompletableFuture.supplyAsync` at:
- Line 881 (expression-key parallel path)
- Line 2022 (single VARCHAR COUNT(*) parallel)
- Line 2913 (single VARCHAR generic parallel)
- Line 4277 (numeric-only parallel)
- Line 5248, 5309, 6096, 6454 (various other parallel paths)

## 8. No Bitset Filter Optimization

Other paths (lines 5829, 6830, 8698, 9470) have selective filter detection:
```java
// Check selectivity: if filter matches <50% of docs, use bitset+nextDoc lockstep
```
executeWithEvalKeys does NOT have this. It collects ALL matching doc IDs into `int[]` array upfront (line 7582-7590), then iterates with `advanceExact` per doc. For Q39's selective filter (~1-5% of 100M rows), this means:
- ~1-5M docs collected into array
- Each doc does 5-6 random `advanceExact` seeks

## 9. Group Count Estimate

Q39 groups by (TraficSourceID, SearchEngineID, AdvEngineID, CASE_WHEN_result, URL).
- CounterID=62 filters to ~1-5% of 100M = 1-5M rows
- URL has very high cardinality (unique URLs)
- Estimated groups: likely 100K-1M+ unique groups (dominated by URL cardinality)
- Flat map starts at 32768, will resize multiple times

## 10. Specific Optimization Opportunities

### P0: Add Parallel Execution (biggest win)
- Other paths split doc ranges across `PARALLEL_POOL` workers
- executeWithEvalKeys processes ALL segments sequentially, ALL docs single-threaded
- For 1-5M docs, parallelizing across 4-8 workers could give 3-6x speedup
- Pattern: split `segDocIds` into ranges, each worker has local flat map, merge at end

### P1: Eliminate Micro-Batch Overhead
- EVAL_BATCH_SIZE=256 creates batch loop overhead even when `hasRemainingEvalKeys=false`
- When all eval keys are inlined/column-ref optimized, the batch loop is unnecessary
- Should detect `hasRemainingEvalKeys=false` and use a direct doc loop without batching

### P2: Reduce advanceExact Calls
- For selective filters with sorted doc IDs, could use `advance(docId)` instead of `advanceExact(docId)` for forward-only iteration
- DocValues are already opened per-segment; doc IDs are collected in order
- Could use lockstep iteration pattern from other paths

### P3: Consider Routing to executeWithVarcharKeys
- After inline CASE WHEN + column-ref optimizations, Q39 has NO remaining eval keys
- It's effectively a multi-key VARCHAR query that could use executeWithVarcharKeys
- That path HAS parallel execution support

### P4: High-Cardinality URL Grouping
- URL creates massive group count, flat map resizes repeatedly
- Consider pre-sizing flat map based on estimated selectivity
- Or use a different hash map strategy for high-cardinality VARCHAR keys
