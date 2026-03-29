# DQE ClickBench Handover — Extracted Sections 7, 9, 10, 12

Source: `/home/penghuo/oss/os-sql/docs/handover/2026-03-24-phase-d-handover.md`

---

## Section 7: Current Status (25/43 Within 2x)

### WITHIN 2x (25 queries)

```
Q00(0.3x) Q01(0.1x) Q03(1.1x) Q06(0.1x) Q07(0.8x) Q10(1.5x) Q12(0.5x)
Q14(1.6x) Q17(0.0x) Q19(0.1x) Q20(0.9x) Q21(0.8x) Q22(0.6x) Q23(0.2x)
Q24(0.0x) Q25(1.2x) Q26(0.0x) Q27(1.4x) Q29(1.2x) Q33(0.3x) Q34(0.3x)
Q38(0.6x) Q40(0.2x) Q41(0.7x) Q42(0.6x)
```

### ABOVE 2x (18 queries, sorted by DQE time)

| Query | Ratio | DQE Time | CH-Parquet Time | Description |
|-------|-------|----------|-----------------|-------------|
| Q28 | 2.7x | 25.5s | 9.5s | REGEXP_REPLACE GROUP BY HAVING |
| Q18 | 5.9x | 21.7s | 3.7s | GROUP BY UserID, minute, SearchPhrase ORDER BY COUNT(*) LIMIT 10 |
| Q32 | 3.2x | 14.3s | 4.5s | GROUP BY WatchID, ClientIP full-table |
| Q16 | 4.1x | 7.3s | 1.8s | GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) LIMIT 10 |
| Q13 | 6.2x | 6.0s | 1.0s | GROUP BY SearchPhrase + COUNT(DISTINCT UserID) |
| Q09 | 8.0x | 4.9s | 0.6s | GROUP BY RegionID + mixed aggs + COUNT(DISTINCT) |
| Q05 | 5.8x | 4.0s | 0.7s | COUNT(DISTINCT SearchPhrase) global |
| Q08 | 3.4x | 1.9s | 0.5s | GROUP BY RegionID + COUNT(DISTINCT UserID) |
| Q11 | 9.5x | 2.6s | 0.3s | GROUP BY MobilePhone,Model + COUNT(DISTINCT) |
| Q15 | 4.1x | 2.1s | 0.5s | GROUP BY UserID ORDER BY COUNT(*) LIMIT 10 |
| Q39 | 16.9x | 2.4s | 0.1s | CounterID=62 filtered, OFFSET, 5 keys + CASE WHEN |
| Q04 | 5.4x | 2.3s | 0.4s | COUNT(DISTINCT UserID) global |
| Q31 | 2.0x | 2.2s | 1.1s | GROUP BY WatchID, ClientIP (borderline — needs 3ms!) |
| Q35 | 5.0x | 1.9s | 0.4s | GROUP BY 1, URL (full-table, high-card) |
| Q30 | 2.3x | 1.8s | 0.8s | GROUP BY SearchEngineID, ClientIP |
| Q36 | 4.0x | 0.5s | 0.1s | GROUP BY ClientIP expressions (4 keys) |
| Q37 | 2.7x | 0.3s | 0.1s | CounterID=62 filtered URL GROUP BY |
| Q02 | 2.2x | 0.2s | 0.1s | SUM+COUNT+AVG, borderline |

Correctness: 33/43 pass on 1M. Do NOT regress below this.

---

## Section 9: Root Cause Analysis for All 18 Failing Queries

### Category A: COUNT(DISTINCT) Decomposition (6 queries — Q04, Q05, Q08, Q09, Q11, Q13)

Calcite decomposes `GROUP BY x, COUNT(DISTINCT y)` into two-level aggregation, doubling key space. Fix: intercept at `TransportShardExecuteAction` dispatch (line ~280-360, `AggDedupNode` detection), route to fused GROUP BY with per-group `LongOpenHashSet`.

### Category B: High-Cardinality GROUP BY (4 queries — Q15, Q16, Q18, Q32)

HashMap with 17M-50M entries doesn't fit in CPU cache. Fix: parallelize single-key path (Q15), hash-partitioned aggregation (Q16/Q18/Q32).

### Category C: Full-Table VARCHAR GROUP BY (3 queries — Q35, Q36, Q39)

Millions of unique groups from URL/IP keys. Fundamentally limited by per-doc hash overhead.

### Category D: Borderline (5 queries — Q02, Q28, Q30, Q31, Q37)

- Q31 needs 3ms.
- Q28 needs regex Pattern caching.
- Q02/Q30/Q37 need small targeted fixes.

---

## Section 10: Key Code Map

### Execution Flow

```
POST /_plugins/_trino_sql {"query": "SELECT ..."}
  → TransportTrinoSqlAction (coordinator)
    → Parse → Calcite plan → PlanOptimizer → PlanFragmenter
    → Dispatch to shards via TransportShardExecuteAction
      → Route to fused path:
          FusedScanAggregate.execute()          — scalar aggs
          FusedScanAggregate.executeWithEval()   — SUM(col+k) shortcut
          FusedGroupByAggregate.execute()         — GROUP BY
          FusedGroupByAggregate.executeWithTopN() — GROUP BY + ORDER BY + LIMIT
          executeCountDistinctWithHashSets()      — COUNT(DISTINCT)
    → Merge at coordinator → JSON response
```

### Key Files (relative to repo root)

Full path prefix: `dqe/src/main/java/org/opensearch/sql/dqe/`

| File | ~Lines | Purpose |
|------|--------|---------|
| `dqe/.../shard/transport/TransportShardExecuteAction.java` | 2,200 | Shard dispatch, pattern detection |
| `dqe/.../shard/source/FusedGroupByAggregate.java` | 11,700 | All GROUP BY execution |
| `dqe/.../shard/source/FusedScanAggregate.java` | 1,600 | Scalar aggregation |
| `dqe/.../coordinator/transport/TransportTrinoSqlAction.java` | 2,000 | Coordinator, result merging |
| `dqe/.../planner/optimizer/PlanOptimizer.java` | 500 | PARTIAL vs SINGLE mode |
| `dqe/.../coordinator/fragment/PlanFragmenter.java` | 800 | Shard plan construction |

### FusedGroupByAggregate Key Methods

| Method | ~Line | Purpose |
|--------|-------|---------|
| `executeInternal` | 932 | Main dispatch |
| `executeSingleVarcharCountStar` | 1268 | VARCHAR + COUNT(*) with global ordinals |
| `executeSingleVarcharGeneric` | 1937 | VARCHAR + any aggregates |
| `executeNumericOnly` | 2786 | Numeric GROUP BY |
| `executeSingleKeyNumericFlat` | ~4269 | Single numeric key |
| `executeTwoKeyNumericFlat` | ~4626 | Two numeric keys |
| `executeNKeyVarcharPath` | 8657 | Multi-key VARCHAR dispatch |
| `executeNKeyVarcharParallelDocRange` | ~9946 | Parallel multi-key |

---

## Section 12: Recommended Next Steps (Priority Order)

1. **COUNT(DISTINCT) Fusion** — 6 queries, highest leverage (Q04, Q05, Q08, Q09, Q11, Q13)
2. **Parallelize executeSingleKeyNumericFlat** — Q15 and similar
3. **Hash-Partitioned Aggregation** — Q16, Q18, Q32
4. **Borderline fixes** — Q02, Q30, Q31, Q37
5. **Q28 REGEXP_REPLACE** — cache Pattern, hoist regex
