# Dispatch Path Analysis: Q15, Q16, Q18, Q32, Q35

## Plan Structure Context

All 5 queries have `ORDER BY COUNT(*) DESC LIMIT 10`, which means the shard plan is:
```
LimitNode -> [ProjectNode] -> SortNode -> AggregationNode -> TableScanNode
```

The key dispatch entry point in `TransportShardExecuteAction.executePlan()` is `extractAggFromSortedLimit(plan)`, which extracts the inner `AggregationNode` from this pattern.

---

## Q15: `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10`

**Keys:** `[UserID]` — BIGINT (single numeric key, no VARCHAR)
**Aggs:** `[COUNT(*)]` — single COUNT(*)
**Plan match:** `extractAggFromSortedLimit()` → YES (LimitNode → SortNode → AggregationNode)

### Dispatch in TransportShardExecuteAction (line ~280-340):
1. `extractAggFromSortedLimit(plan)` returns the inner `AggregationNode`
2. `FusedGroupByAggregate.canFuse(innerAgg, colTypeMap)` → YES (single BIGINT key, COUNT(*) supported)
3. Sort is on single aggregate column (COUNT(*)), `sortIndices.size() == 1 && sortIndices.get(0) >= numGroupByCols` → YES
4. Calls `executeFusedGroupByAggregateWithTopN()` → `FusedGroupByAggregate.executeWithTopN()`

### Dispatch in FusedGroupByAggregate.executeInternal():
1. `hasVarchar` = false (UserID is BIGINT)
2. Routes to `executeNumericOnly()`
3. `keyInfos.size() == 1 && !anyTrunc` → YES
4. Routes to `executeSingleKeyNumeric()`
5. `canUseFlatAccumulators` = true (COUNT(*) uses 1 slot)
6. Routes to `executeSingleKeyNumericFlat()`

| Property | Value |
|---|---|
| **extractAggFromSortedLimit match** | YES |
| **FusedGroupByAggregate method** | `executeSingleKeyNumericFlat()` |
| **Execution** | PARALLEL (doc-range parallel via ForkJoinPool for MatchAll COUNT(*)) |
| **Key data structure** | `FlatSingleKeyMap` (open-addressing, long[] keys + long[] accData) |
| **Top-N** | Inline heap selection on flat accData array |

---

## Q16: `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10`

**Keys:** `[UserID, SearchPhrase]` — BIGINT + VARCHAR (mixed)
**Aggs:** `[COUNT(*)]` — single COUNT(*)
**Plan match:** `extractAggFromSortedLimit()` → YES

### Dispatch in TransportShardExecuteAction:
1. `extractAggFromSortedLimit(plan)` returns inner AggregationNode
2. `FusedGroupByAggregate.canFuse()` → YES (BIGINT + VARCHAR keys, COUNT(*))
3. Sort on single aggregate column → YES
4. Calls `executeFusedGroupByAggregateWithTopN()` → `FusedGroupByAggregate.executeWithTopN()`

### Dispatch in FusedGroupByAggregate.executeInternal():
1. `hasVarchar` = true (SearchPhrase is VARCHAR)
2. `keyInfos.size() == 1` → NO (2 keys)
3. `hasEvalKey` = false
4. Routes to `executeWithVarcharKeys()`
5. `singleSegment && keyInfos.size() == 2` → YES (single-segment path)
6. Tries `tryOrdinalIndexedTwoKeyCountStar()` — checks if one varchar + one numeric + all COUNT(*)
   - `varcharKeyIdx=1` (SearchPhrase), `numericKeyIdx=0` (UserID) → matches
   - If ordinal count ≤ 500K and numeric key cardinality ≤ 64 per ordinal → uses ordinal-indexed path
   - Otherwise falls through to `FlatTwoKeyMap` path
7. If ordinal-indexed fails: `canUseFlatVarchar` = true (COUNT(*) only)
8. Routes to flat two-key varchar path with `FlatTwoKeyMap`

| Property | Value |
|---|---|
| **extractAggFromSortedLimit match** | YES |
| **FusedGroupByAggregate method** | `executeWithVarcharKeys()` → `tryOrdinalIndexedTwoKeyCountStar()` or flat `FlatTwoKeyMap` path |
| **Execution** | SEQUENTIAL (single-segment, direct doc iteration) |
| **Key data structure** | Ordinal-indexed arrays (numericKeys[] + counts[] per varchar ordinal) OR `FlatTwoKeyMap` (open-addressing, long[] keys0/keys1 + long[] accData) |
| **Top-N** | Inline heap selection on ordinal counts or flat accData |

---

## Q18: `SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10`

**Keys:** `[UserID, EXTRACT(MINUTE FROM EventTime), SearchPhrase]` — BIGINT + INT expression + VARCHAR
**Aggs:** `[COUNT(*)]`
**Plan match:** `extractAggFromSortedLimit()` → YES

### Dispatch in TransportShardExecuteAction:
1. `extractAggFromSortedLimit(plan)` returns inner AggregationNode
2. `FusedGroupByAggregate.canFuse()` → YES
   - UserID: BIGINT ✓
   - `EXTRACT(MINUTE FROM EventTime)`: matches `EXTRACT_PATTERN`, source is TimestampType ✓
   - SearchPhrase: VARCHAR ✓
   - COUNT(*) ✓
3. Sort on single aggregate column → YES
4. Calls `executeFusedGroupByAggregateWithTopN()`

### Dispatch in FusedGroupByAggregate.executeInternal():
1. `hasVarchar` = true (SearchPhrase)
2. `keyInfos.size() == 1` → NO (3 keys)
3. `hasEvalKey` = false
4. Routes to `executeWithVarcharKeys()`
5. `singleSegment && keyInfos.size() == 2` → NO (3 keys)
6. Falls through to `executeNKeyVarcharPath()`
7. In `executeNKeyVarcharPath()`:
   - `keyInfos.size() == 3` → YES
   - `anyVarcharKey` = true (SearchPhrase)
   - `singleSegment` = true → `canUseFlatThreeKey` check
   - `canUseFlatThreeKey` = true (COUNT(*) only, flat-compatible)
   - Routes to `executeThreeKeyFlat()` (single-segment, single bucket)

| Property | Value |
|---|---|
| **extractAggFromSortedLimit match** | YES |
| **FusedGroupByAggregate method** | `executeNKeyVarcharPath()` → `executeThreeKeyFlat()` |
| **Execution** | SEQUENTIAL (single-segment, 3-key flat path; parallel only for all-numeric keys) |
| **Key data structure** | `FlatThreeKeyMap` (open-addressing, long[] keys0/keys1/keys2 + long[] accData). EXTRACT(MINUTE) applied inline via `applyExtract()` during key extraction. VARCHAR key stored as segment-local ordinal. |
| **Top-N** | Inline heap selection on flat accData |

---

## Q32: `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10`

**Keys:** `[WatchID, ClientIP]` — BIGINT + INTEGER (both numeric)
**Aggs:** `[COUNT(*), SUM(IsRefresh), AVG(ResolutionWidth)]`
**Plan match:** `extractAggFromSortedLimit()` → YES

### Dispatch in TransportShardExecuteAction:
1. `extractAggFromSortedLimit(plan)` returns inner AggregationNode
2. `FusedGroupByAggregate.canFuse()` → YES (2 numeric keys, COUNT/SUM/AVG all supported)
3. Sort on single aggregate column (COUNT(*)) → YES
4. Calls `executeFusedGroupByAggregateWithTopN()`

### Dispatch in FusedGroupByAggregate.executeInternal():
1. `hasVarchar` = false (both keys numeric)
2. Routes to `executeNumericOnly()`
3. `keyInfos.size() == 1` → NO
4. `keyInfos.size() == 2 && !anyTrunc` → YES
5. Routes to `executeTwoKeyNumeric()`
6. `canUseFlatAccumulators` check:
   - COUNT(*): accType=0, slot=1 ✓
   - SUM(IsRefresh): IsRefresh is numeric (not double), accType=1, slot=1 ✓
   - AVG(ResolutionWidth): ResolutionWidth is numeric (not double), accType=2, slots=2 (sum+count) ✓
   - No VARCHAR args, no DISTINCT, no MIN/MAX → `canUseFlatAccumulators` = **true**
7. Routes to `executeTwoKeyNumericFlat()`

**AVG slot layout:** AVG requires 2 slots (sum + count), so `slotsPerGroup = 1 + 1 + 2 = 4`

| Property | Value |
|---|---|
| **extractAggFromSortedLimit match** | YES |
| **FusedGroupByAggregate method** | `executeTwoKeyNumericFlat()` |
| **Execution** | PARALLEL (segment-parallel via ForkJoinPool workers, each with own FlatTwoKeyMap) |
| **Key data structure** | `FlatTwoKeyMap` (open-addressing, long[] keys0/keys1 + long[] accData with 4 slots/group) |
| **Top-N** | Inline heap selection on flat accData[sortAccOff] |
| **AVG handling** | 2 slots: accData[off]=sum, accData[off+1]=count. Output: (double)sum/count |

---

## Q35: `SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10`

**Keys:** `[ClientIP, (ClientIP - 1), (ClientIP - 2), (ClientIP - 3)]` — 4 expression keys derived from INTEGER
**Aggs:** `[COUNT(*)]`
**Plan match:** `extractAggFromSortedLimit()` → YES

### Dispatch in TransportShardExecuteAction:
1. `extractAggFromSortedLimit(plan)` returns inner AggregationNode
2. `FusedGroupByAggregate.canFuse()` → YES
   - ClientIP: INTEGER (numeric) ✓
   - `(ClientIP - 1)`: matches `ARITH_EXPR_PATTERN`, source is numeric ✓
   - `(ClientIP - 2)`: matches `ARITH_EXPR_PATTERN` ✓
   - `(ClientIP - 3)`: matches `ARITH_EXPR_PATTERN` ✓
   - COUNT(*) ✓
3. Sort on single aggregate column → YES
4. Calls `executeFusedGroupByAggregateWithTopN()`

### Dispatch in FusedGroupByAggregate.executeInternal():
1. `hasVarchar` = false (all numeric/arith)
2. Routes to `executeNumericOnly()`
3. `keyInfos.size() == 1` → NO (4 keys)
4. `keyInfos.size() == 2` → NO
5. **Derived single-key optimization check** (`anyTrunc && keyInfos.size() > 1`):
   - `anyTrunc` = true (arith expressions detected)
   - `sharedCol` = "ClientIP" for all 4 keys → `allSameCol` = true
   - All keys are either plain or arith → `allArithOrPlain` = true
   - Routes to `executeDerivedSingleKeyNumeric()`
6. `canUseFlat` = true (COUNT(*) only)
7. Uses `FlatSingleKeyMap` with source key = ClientIP only
8. Derived keys (ClientIP-1, ClientIP-2, ClientIP-3) computed at output time via `applyArith()`

| Property | Value |
|---|---|
| **extractAggFromSortedLimit match** | YES |
| **FusedGroupByAggregate method** | `executeDerivedSingleKeyNumeric()` (reduces 4-key to 1-key) |
| **Execution** | PARALLEL (segment-parallel or doc-range parallel via ForkJoinPool) |
| **Key data structure** | `FlatSingleKeyMap` (single source key = ClientIP). Derived keys computed at output time. |
| **Top-N** | Inline heap selection on flat accData |
| **Key optimization** | 4 keys → 1 key. Hash computation and comparison cost reduced ~4x. |

---

## Summary Table

| Query | extractAggFromSortedLimit | FusedGroupByAggregate Method | Parallel? | Key Data Structure | Slots/Group |
|-------|--------------------------|------------------------------|-----------|-------------------|-------------|
| Q15 | YES | `executeSingleKeyNumericFlat` | YES (doc-range) | `FlatSingleKeyMap` | 1 |
| Q16 | YES | `executeWithVarcharKeys` → ordinal-indexed or `FlatTwoKeyMap` | NO (single-seg) | Ordinal arrays or `FlatTwoKeyMap` | 1 |
| Q18 | YES | `executeNKeyVarcharPath` → `executeThreeKeyFlat` | NO (single-seg, has varchar) | `FlatThreeKeyMap` | 1 |
| Q32 | YES | `executeTwoKeyNumericFlat` | YES (segment-parallel) | `FlatTwoKeyMap` | 4 (COUNT:1 + SUM:1 + AVG:2) |
| Q35 | YES | `executeDerivedSingleKeyNumeric` | YES (segment/doc-range) | `FlatSingleKeyMap` (1 key, not 4) | 1 |
