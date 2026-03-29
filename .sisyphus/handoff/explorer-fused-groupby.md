# FusedGroupByAggregate.java — Single Numeric Key GROUP BY + COUNT(*) + Top-N Analysis

File: `/local/home/penghuo/oss/os-sql/dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java` (13198 lines)

## Findings JSON

```json
{
  "patterns": [
    {
      "name": "Heap-based top-N selection (numeric flat path)",
      "description": "Manual min/max-heap over FlatSingleKeyMap slots. For DESC sort, uses min-heap: fills first N slots, then replaces root when a larger value is found. Sift-up on insert, sift-down on replace. Outputs only heap entries.",
      "files": ["FusedGroupByAggregate.java:4572-4627"],
      "example": "if (sortAggIndex >= 0 && topN > 0 && topN < flatMap.size) {\n  int n = (int) Math.min(topN, flatMap.size);\n  int[] heap = new int[n]; // stores slot indices\n  // ... sift-up/sift-down logic ...\n  outputSlots = heap; outputCount = heapSize;\n}"
    },
    {
      "name": "Heap-based top-N selection (varchar COUNT(*) path)",
      "description": "Same heap pattern but over ordinal indices with separate heapVals[] array for counts. This is the EXISTING pattern the task description references.",
      "files": ["FusedGroupByAggregate.java:1365-1444"],
      "example": "int[] heap = new int[n]; // ordinal indices\nlong[] heapVals = new long[n]; // count values\n// ... same sift-up/sift-down pattern ..."
    },
    {
      "name": "FlatSingleKeyMap open-addressing hash map",
      "description": "Custom hash map with parallel arrays: long[] keys, boolean[] occupied, long[] accData (contiguous slots per group). Uses Murmur3 hash. No per-group object allocation.",
      "files": ["FusedGroupByAggregate.java:12460-12530"],
      "example": "long[] keys; boolean[] occupied; long[] accData;\nint findOrInsert(long key) { /* linear probing */ }"
    }
  ],
  "files": [
    {
      "path": "FusedGroupByAggregate.java",
      "purpose": "Fused scan-group-aggregate operator using Lucene DocValues directly",
      "relevance": "Contains all GROUP BY execution paths including numeric key + COUNT(*) + top-N"
    }
  ],
  "implementations": [
    {
      "name": "executeWithTopN (public entry point)",
      "location": "FusedGroupByAggregate.java:907-916",
      "description": "Public API. Delegates to executeInternal with sortAggIndex, sortAscending, topN params.",
      "dependencies": ["executeInternal"]
    },
    {
      "name": "executeInternal (dispatch)",
      "location": "FusedGroupByAggregate.java:938-1120",
      "description": "Parses AggregationNode, builds KeyInfo/AggSpec lists, dispatches to: executeSingleVarcharCountStar (line 1055), executeSingleVarcharGeneric (line 1068), executeNumericOnly (line 1111), or executeWithVarcharKeys.",
      "dependencies": ["executeSingleVarcharCountStar", "executeSingleVarcharGeneric", "executeNumericOnly", "executeWithVarcharKeys"]
    },
    {
      "name": "executeNumericOnly (numeric dispatch)",
      "location": "FusedGroupByAggregate.java:2812-2900+",
      "description": "For numeric-only keys. Classifies agg types into accType[]. If canUseFlatAccumulators (COUNT/SUM-long/AVG-long only), dispatches to executeSingleKeyNumericFlat. Otherwise falls back to SingleKeyHashMap path.",
      "dependencies": ["executeSingleKeyNumericFlat", "executeTwoKeyNumeric"]
    },
    {
      "name": "executeSingleKeyNumericFlat (THE key method for Q15)",
      "location": "FusedGroupByAggregate.java:4438-4676",
      "description": "Single numeric key with flat accumulators. Uses FlatSingleKeyMap (open-addressing, long[] keys + long[] accData). Supports parallel segment processing. Result building at line 4568: has FULL top-N heap selection (lines 4572-4627). Builds Page with BlockBuilder at end.",
      "dependencies": ["FlatSingleKeyMap", "scanSegmentFlatSingleKey", "BigintType", "BlockBuilder"]
    },
    {
      "name": "scanSegmentFlatSingleKey (per-segment scan)",
      "location": "FusedGroupByAggregate.java:4798-4900+",
      "description": "Reads SortedNumericDocValues for key column, iterates docs (MatchAll fast path or Weight/Scorer path), calls flatMap.findOrInsert(key) and increments accData slots.",
      "dependencies": ["FlatSingleKeyMap", "SortedNumericDocValues"]
    },
    {
      "name": "executeSingleVarcharCountStar (reference heap pattern)",
      "location": "FusedGroupByAggregate.java:1274-1460",
      "description": "VARCHAR key + COUNT(*) only. Single-segment: ordinal array. Multi-segment: global HashMap<String, Long>. Has heap-based top-N at lines 1365-1444 (ordinal path) and lines 1570-1630 (multi-segment path).",
      "dependencies": ["SortedSetDocValues", "BlockBuilder"]
    },
    {
      "name": "FlatSingleKeyMap (data structure)",
      "location": "FusedGroupByAggregate.java:12460-12530+",
      "description": "Open-addressing hash map. INITIAL_CAPACITY=4096, LOAD_FACTOR=0.7, MAX_CAPACITY=16M. Fields: long[] keys, boolean[] occupied, long[] accData (slotsPerGroup longs per entry). findOrInsert() uses Murmur3 hash + linear probing.",
      "dependencies": ["SingleKeyHashMap.hash1"]
    }
  ],
  "recommendations": [
    "The numeric flat path (executeSingleKeyNumericFlat) ALREADY HAS a complete heap-based top-N selection at lines 4572-4627. For Q15 (GROUP BY UserID COUNT(*) ORDER BY COUNT(*) DESC LIMIT 10), this path should already be active IF executeWithTopN is called with sortAggIndex=0, sortAscending=false, topN=10.",
    "Verify that the query planner actually calls executeWithTopN (not execute) for Q15. If it calls execute(), topN=0 and the heap is skipped — all groups are materialized.",
    "The heap pattern in the numeric path (line 4572) stores slot indices into FlatSingleKeyMap and reads values via flatMap.accData[slot * slotsPerGroup + sortAccOff]. The varchar path (line 1365) stores ordinal indices and uses a separate heapVals[] array. Both use identical sift-up/sift-down logic.",
    "No PriorityQueue (java.util) is used anywhere — all heaps are manual int[] arrays for zero-allocation top-N."
  ]
}
```

## Key Call Chain for Q15

```
executeWithTopN(aggNode, shard, query, columnTypeMap, sortAggIndex=0, sortAscending=false, topN=10)
  → executeInternal(...)                                    [line 916]
    → executeNumericOnly(...)                               [line 1111] (UserID is BIGINT, no VARCHAR keys)
      → executeSingleKeyNumericFlat(...)                    [line 2849] (single key, canUseFlatAccumulators=true for COUNT(*))
        → scanSegmentFlatSingleKey(...)                     [line 4555] (per-segment: reads SortedNumericDocValues, populates FlatSingleKeyMap)
        → top-N heap selection                              [line 4572-4627] (min-heap of size 10 over all slots)
        → build Page with BlockBuilder                      [line 4640-4676] (only 10 rows output)
```

## Heap Selection Detail (lines 4572-4627)

- Condition: `sortAggIndex >= 0 && topN > 0 && topN < flatMap.size`
- `int[] heap = new int[n]` — stores FlatSingleKeyMap slot indices
- Values read inline: `flatMap.accData[heap[k] * slotsPerGroup + sortAccOff]`
- For DESC (sortAscending=false): min-heap — root is smallest, replaced when larger value found
- For ASC (sortAscending=true): max-heap — root is largest, replaced when smaller value found
- After heap built: `outputSlots = heap; outputCount = heapSize;`
- Result: only `heapSize` rows written to BlockBuilder → Page
