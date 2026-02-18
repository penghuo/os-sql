# Phase 2 Task Breakdown: Full Shuffle + Joins

## Prerequisites

Phase 1 complete: IC-3 (PPL Calcite IT Regression) passes.

## Organization

```
Exchange Extensions ──> Join Operators ──> IC-4 (Full Shuffle E2E)
                   ──> Window Operator ──┘         │
                                                   │
Lucene Advanced Ops (parallel) ────────────────────┘
Spill-to-Disk (parallel) ─────────────────────────┘
                                                   │
                                              IC-5 (Full Regression + Performance)
                                                   │
                                            PHASE 2 COMPLETE
```

---

## Exchange Extensions

| ID | Task | Description | Classes | Dependencies | Owner |
|----|------|-------------|---------|-------------|-------|
| P2.1 | HashExchange operator | Hash-partitions Pages by key columns. Sends each partition to the assigned target node via TransportService. Receives Pages from all sources for its partition | ~4 | E.1, E.2 | Team Planning |
| P2.2 | BroadcastExchange operator | Sends full copy of Pages to all participating nodes. Memory-bounded buffer | ~2 | E.1, E.2 | Team Planning |
| P2.3 | AddExchanges Phase 2 | Extend AddExchanges for hash-partition (joins, distributed agg) and broadcast (small-table join). Join strategy: broadcast if build side < threshold, else hash-partition both sides | ~1 (extend L3.3) | P2.1, P2.2 | Team Planning |

**Exchange extensions total: ~7 classes**

---

## Join Operators

| ID | Task | Description | Classes | Dependencies | Owner |
|----|------|-------------|---------|-------------|-------|
| P2.4 | LookupJoinOperator (port) | Port `LookupJoinOperator` + `LookupJoinOperatorFactory` from Trino. Support INNER, LEFT, RIGHT, FULL, SEMI, ANTI join types. No spill in initial port | ~8 | L1.4, P2.1 | Team Operators |
| P2.5 | HashBuilderOperator (port) | Port `HashBuilderOperator` + `JoinHash` + `PagesHash`. Builds hash table from build side for join probe. Memory-tracked via OperatorContext | ~6 | L1.4, L1.6 | Team Operators |

**Join operators total: ~14 classes**

---

## Window Operator

| ID | Task | Description | Classes | Dependencies | Owner |
|----|------|-------------|---------|-------------|-------|
| P2.6 | WindowOperator (port) | Port `WindowOperator` + `RegularWindowPartition`. Support ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, SUM/AVG/MIN/MAX OVER. ROWS and RANGE frames | ~5 | L1.4, P2.1 | Team Operators |

**Window operator total: ~5 classes**

---

## Lucene Advanced Operators

| ID | Task | Description | Classes | Dependencies | Owner |
|----|------|-------------|---------|-------------|-------|
| P2.7 | LuceneAggScan | Lucene-native aggregation: `Collector`/`LeafCollector` with DocValues accumulation. Faster than scan+agg for simple aggregates | ~2 | L2.1 | Team Operators |
| P2.8 | LuceneSortScan | `IndexSearcher.search(Query, int, Sort)` with early termination top-K. Faster than full scan + sort | ~2 | L2.1 | Team Operators |

**Lucene advanced total: ~4 classes**

---

## Spill-to-Disk

| ID | Task | Description | Classes | Dependencies | Owner |
|----|------|-------------|---------|-------------|-------|
| P2.9 | Spill-to-disk support | Add `SpillableHashAggregationBuilder`, spill for OrderByOperator. Local temp files, configurable spill path. Port `FileSingleStreamSpiller` (simplified) | ~6 | L1.6, L2.4, L2.6 | Team Runtime |

**Spill total: ~6 classes**

---

## Phase 2 Unit Tests

| ID | Task | Description | Dependencies | Owner |
|----|------|-------------|-------------|-------|
| P2.10 | Phase 2 unit tests | HashExchange, BroadcastExchange, join operators, window operator, Lucene advanced ops, spill tests | P2.1-P2.9 | Tester + all teams |

**Test classes: ~12**

---

### INTEGRATION CHECKPOINT 4: Full Shuffle End-to-End

**Prerequisites**: P2.1-P2.10 all pass
**Scope**: Multi-stage distributed queries (joins, window functions, distributed aggregation)

| IC-4 Test | Description | Pass Criteria |
|-----------|-------------|---------------|
| `ShuffleJoinTest` | 2-index hash join via distributed engine | Results match expected join output |
| `ShuffleWindowTest` | Window function across distributed data | Correct row numbering/ranking |
| `ShuffleMultiStageAggTest` | High-cardinality agg with hash exchange | Partial -> exchange -> final correct |
| `ShuffleExplainTest` | `_explain` shows multi-stage plan | All stages, exchanges, assignments visible |
| `ShuffleMemoryPressureTest` | Query that requires spill | Completes correctly with spill |
| `ShuffleConcurrentTest` | 5 concurrent multi-stage queries | All complete, no deadlock |

**Gate**: ALL IC-4 tests pass before proceeding to regression.

---

### INTEGRATION CHECKPOINT 5: Full Regression + Performance (PHASE 2 GATE)

**Prerequisites**: IC-4 passes
**Scope**: All existing tests pass, performance validation, production readiness.

**Regression:**

| ID | Task | Description | Dependencies | Owner |
|----|------|-------------|-------------|-------|
| R.1 | Calcite integTest regression | ALL `*Calcite*IT` tests with distributed engine enabled AND disabled. 0 failures | IC-4 | Tester + all teams |
| R.2 | yamlRestTest regression | ALL YAML REST tests. 0 failures | IC-4 | Tester |
| R.3 | doctest regression | ALL doctests. 0 failures | IC-4 | Tester |
| R.4 | Feature flag tests | Verify enable/disable/fallback for all query patterns | R.1 | Tester |
| R.5 | Dual-mode comparison | Every IT query through both paths, compare row-for-row | R.1 | Tester |
| R.6 | Security validation | DLS/FLS enforced with direct Lucene access. Index-level permissions | IC-4 | Team Operators |

**Performance + Stability:**

| ID | Task | Description | Dependencies | Owner |
|----|------|-------------|-------------|-------|
| P.1 | Performance benchmark suite | JMH micro-benchmarks. E2E latency for 50 representative queries | IC-4 | Tester |
| P.2 | Latency regression validation | p50/p99 of distributed vs DSL for simple queries | P.1 | Tester |
| P.3 | Memory benchmark | Peak memory under load, circuit breaker behavior | IC-4 | Team Runtime |
| P.4 | Scalability test | Same queries on 3/5/10-node clusters | IC-4 | Tester |
| P.5 | Stability test | 1000 sequential queries, node failure during query | IC-4 | Tester |

**Final Sign-Off Checklist:**
- [ ] ALL Calcite integTests pass (distributed enabled)
- [ ] ALL Calcite integTests pass (distributed disabled)
- [ ] ALL yamlRestTests pass
- [ ] ALL doctests pass
- [ ] `_explain` API returns distributed plan info
- [ ] No latency regression (p50/p99 within threshold)
- [ ] Feature flag works correctly
- [ ] Memory cleanup verified (no leaks)
- [ ] Security: DLS/FLS enforced
- [ ] Thread context propagated across exchanges
- [ ] Spill-to-disk works under memory pressure

---

## Phase 2 Summary

| Component | New Classes | Test Classes |
|-----------|-------------|-------------|
| Exchange Extensions | ~7 | ~3 |
| Join Operators | ~14 | ~4 |
| Window Operator | ~5 | ~2 |
| Lucene Advanced | ~4 | ~2 |
| Spill-to-Disk | ~6 | ~1 |
| **Total** | **~36** | **~12** |

**Integration Checkpoints:**
1. **IC-4**: Full shuffle E2E (6 tests)
2. **IC-5**: Full regression + performance — **Phase 2 delivery gate**
