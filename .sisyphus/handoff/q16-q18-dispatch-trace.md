# Q16 & Q18 Dispatch Path Trace

## 1. Dispatch Entry Point (TransportShardExecuteAction.java)

Both Q16 and Q18 have ORDER BY COUNT(*) DESC LIMIT 10, so they match the pattern:
`LimitNode -> [ProjectNode] -> SortNode -> AggregationNode` (line 412-417)

Extracted via `extractAggFromSortedLimit()` (line 3277).

### Decision tree (lines 440-584):
1. **HAVING filter?** No for both → skip HAVING path (line 446)
2. **Single sort key on aggregate column?** Yes — `sortIndices.size() == 1 && sortIndices.get(0) >= numGroupByCols` (line 510)
   - sortAggIndex = index of COUNT(*) in agg output columns
   - Both Q16 and Q18 sort by COUNT(*) DESC → single agg sort key
3. **Calls `executeFusedGroupByAggregateWithTopN()`** (line 515) with sortAggIndex pointing to COUNT(*), sortAsc=false, topN=10

### executeFusedGroupByAggregateWithTopN (line 2880):
Delegates to `FusedGroupByAggregate.executeWithTopN()` which calls `executeInternal()` with topN params.

## 2. FusedGroupByAggregate Dispatch (FusedGroupByAggregate.java)

### Key Classification (executeInternal, line 1290+):
- **Q16 keys**: UserID (BigintType → numeric), SearchPhrase (VarcharType → varchar)
  - `hasVarchar = true`, 2 keys
- **Q18 keys**: UserID (BigintType → numeric), `EXTRACT(MINUTE FROM EventTime)` (matched by EXTRACT_PATTERN → KeyInfo with exprFunc="extract", exprUnit="MINUTE", type=BigintType, source=EventTime), SearchPhrase (VarcharType → varchar)
  - `hasVarchar = true`, 3 keys

### Dispatch (line 1386+):
Both have varchar → enter `executeWithVarcharKeys()` (line 8210).

#### Q16 (2 keys) — line 8278:
- Single segment: tries `tryOrdinalIndexedTwoKeyCountStar()` first (specialized for COUNT(*) only)
- If not applicable, tries **flat two-key varchar path** (FlatTwoKeyVarcharMap) — line 8330+
  - COUNT(*) qualifies for flat storage
  - **TopN heap selection at line 8873**: min-heap of size 10 on accData[sortAccOff], avoids full Page construction
- Multi-segment: tries `executeMultiSegGlobalOrdFlatTwoKey()`, then falls to parallel doc-range

#### Q18 (3 keys) — line 10378:
- Enters **3-key flat fast path** check: `keyInfos.size() == 3`
  - SearchPhrase is varchar → `anyVarcharKey = true`
  - Requires `singleSegment || !anyVarcharKey` → only works on single-segment indexes
  - If single segment + COUNT(*) only → uses `FlatThreeKeyMap` with **TopN heap at line 10010**
  - If multi-segment with varchar → **falls through to N-key path** (line 10484)
- N-key path: `executeNKeyVarcharPath()` (line 10359)
  - Single segment: HashMap<SegmentGroupKey, AccumulatorGroup> with **TopN heap at line 10681**
  - Multi-segment: `executeNKeyVarcharParallelDocRange()` (line 11720) with parallel workers

## 3. TopN Optimization Summary

| Path | TopN? | Mechanism | Line |
|------|-------|-----------|------|
| 2-key flat varchar (Q16) | YES | min-heap on accData[] | 8873 |
| 3-key flat (Q18 single-seg) | YES | min-heap on accData[] | 10010 |
| N-key single-seg (Q18 fallback) | YES | min-heap on HashMap entries | 10681 |
| N-key parallel doc-range (Q18 multi-seg) | Partial | early-term cap for LIMIT-only; heap in merge | 11750+ |

**All paths have TopN optimization** — bounded heap of size topN instead of full materialization. The heap selects top-N groups by sort aggregate value, then only those groups get ordinal resolution and Page construction.

## 4. EXTRACT(MINUTE FROM EventTime) Evaluation

- **Pushed down to per-doc inline computation** — NOT a separate expression evaluation step
- Recognized by `EXTRACT_PATTERN` regex (line 112): `EXTRACT(MINUTE FROM EventTime)`
- Creates `KeyInfo(sourceCol="EventTime", type=BigintType, exprFunc="extract", exprUnit="MINUTE")`
- Encoded as `arithUnits[k] = "E:MINUTE"` in executeWithVarcharKeys (line 8236)
- In hot loop: reads EventTime from SortedNumericDocValues, calls `applyArith(val, "E:MINUTE")` (line 10549)
- `applyArith()` detects "E:" prefix → delegates to `applyExtract()` (line 1529)
- `applyExtract("MINUTE")` uses **pure arithmetic**: `((epochSeconds / 60) % 60 + 60) % 60` (line 1570)
- No ZonedDateTime allocation, no expression compilation — just integer math per doc
