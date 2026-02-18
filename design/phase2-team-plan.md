# Phase 2 Team Plan: Full Shuffle + Joins

## Prerequisites

Phase 1 complete (IC-3 passes). Same team structure continues.

## Team Allocation

| Team | Engineers | Focus |
|------|----------|-------|
| Runtime | R1, R2 | Spill-to-disk, memory revocation |
| Operators | O1, O2 | Join ops, window ops, Lucene agg/sort scan |
| Planning | P1, P2 | HashExchange, BroadcastExchange, join strategy in AddExchanges |
| Test | T1 | Phase 2 operator tests, IC-4, IC-5 |
| Tech Lead | TL | Join planning, spill architecture |
| **Total** | **8** | |

---

## Task Assignments

### Team Runtime (R1, R2)

| Engineer | Task | Description |
|----------|------|-------------|
| R1 | P2.9 (part 1) | SpillableHashAggregationBuilder — extend HashAgg with spill |
| R2 | P2.9 (part 2) | FileSingleStreamSpiller, spill for OrderBy, memory revocation integration |

### Team Operators (O1, O2)

| Engineer | Task | Description |
|----------|------|-------------|
| O1 | P2.7, P2.8 | LuceneAggScan, LuceneSortScan (Lucene-native advanced operators) |
| O2 | P2.4, P2.5, P2.6 | LookupJoinOperator, HashBuilderOperator, WindowOperator (ported from Trino) |

### Team Planning (P1, P2)

| Engineer | Task | Description |
|----------|------|-------------|
| P1 | P2.3 | AddExchanges Phase 2 (hash/broadcast strategy for joins) |
| P2 | P2.1, P2.2 | HashExchange, BroadcastExchange operators |

### Team Test (T1)

| Phase | Tasks |
|-------|-------|
| First | P2.10 — Phase 2 operator unit tests |
| Then | IC-4 — Full shuffle E2E test suite |
| Then | IC-5 — Full regression + performance benchmarks |

---

## Key Deliverables

| Deliverable | Depends on | Owner |
|-------------|------------|-------|
| HashExchange + BroadcastExchange tested | E.1, E.2 (Phase 1) | Team Planning |
| AddExchanges with join strategy | P2.1, P2.2 | Team Planning (P1) |
| LookupJoinOperator + HashBuilderOperator tested | L1.4, P2.1 | Team Operators (O2) |
| WindowOperator tested | L1.4, P2.1 | Team Operators (O2) |
| LuceneAggScan + LuceneSortScan tested | L2.1 | Team Operators (O1) |
| Spill-to-disk support tested | L1.6, L2.4, L2.6 | Team Runtime |
| IC-4 (Full shuffle E2E) passes | All Phase 2 ops | Tester |
| IC-5 (Full regression + perf) passes | IC-4 | Tester + all teams |

---

## Risk Mitigation

| Risk | Owning Team | Mitigation |
|------|-------------|------------|
| Join operator complexity (38 files in Trino's join package) | Operators (O2) | Start with INNER join only, add other types incrementally |
| Hash exchange hot spots with skewed data | Planning (P2) | Test with skewed distributions early, consider range-based fallback |
| Spill-to-disk I/O bottleneck | Runtime | Benchmark spill throughput early, use async I/O if needed |
| WindowOperator partition memory | Operators (O2) | Port Trino's partition-aware memory management, spill large partitions |
| Regression failures from join path changes | All teams | Run Calcite ITs continuously during development, not just at checkpoints |
