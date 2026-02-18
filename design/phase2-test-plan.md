# Phase 2 Test Plan: Full Shuffle + Joins

## Overview

Phase 2 testing covers exchange extensions, join operators, window operators, Lucene advanced operators, and spill-to-disk. Two integration checkpoints gate Phase 2 completion.

```
Stage 5: Phase 2 Operators ──> IC-4 (Full Shuffle E2E)
                                  │
                                IC-5 (Full Regression + Performance)
                                  │
                            PHASE 2 COMPLETE
```

**Prerequisite**: Phase 1 IC-3 passes.

---

## Stage 5: Phase 2 Operator Tests

### S5.1 HashExchange Tests
| Test | Description |
|------|-------------|
| `HashExchangePartitioningTest` | Pages hash-partitioned to N targets by key column |
| `HashExchangeSkewTest` | Skewed key distribution, verify all partitions receive data |
| `HashExchangeNullKeyTest` | Null keys in partition column |

### S5.2 BroadcastExchange Tests
| Test | Description |
|------|-------------|
| `BroadcastExchangeBasicTest` | All targets receive identical Pages |
| `BroadcastExchangeMemoryTest` | Verify memory bounded for broadcast copies |

### S5.3 Join Operator Tests
| Test Suite | Key Test Cases |
|-----------|----------------|
| `LookupJoinOperatorTest` | INNER join, LEFT join, RIGHT join, FULL join, SEMI join, ANTI join, multi-column join key, null join keys, empty build side, empty probe side |
| `HashBuilderOperatorTest` | Build hash table, verify lookup, duplicate keys, memory tracking |

### S5.4 Window Operator Tests
| Test Suite | Key Test Cases |
|-----------|----------------|
| `WindowOperatorTest` | ROW_NUMBER, RANK, DENSE_RANK, LAG/LEAD, SUM OVER, ROWS frame, RANGE frame, single partition, multiple partitions |

### S5.5 Lucene Advanced Operator Tests
| Test Suite | Key Test Cases |
|-----------|----------------|
| `LuceneAggScanTest` | COUNT, SUM, AVG over DocValues, GROUP BY keyword field, verify partial aggregates correct |
| `LuceneSortScanTest` | Sort by long field ASC/DESC, sort by keyword, top-K with early termination, verify sort order correct |

### S5.6 Spill-to-Disk Tests
| Test | Description |
|------|-------------|
| `AggregationSpillTest` | High-cardinality GROUP BY exceeds memory -> spill |
| `JoinSpillTest` | Large build side exceeds memory -> spill |
| `SortSpillTest` | Large ORDER BY exceeds memory -> spill |
| `SpillCleanupTest` | Verify spill files deleted after query completes |

**Run command**: `./gradlew :distributed-engine:test --tests '*HashExchange*' --tests '*Broadcast*' --tests '*Join*' --tests '*Window*' --tests '*Spill*' --tests '*LuceneAgg*' --tests '*LuceneSort*'`

---

## Integration Checkpoint 4: Full Shuffle End-to-End

**Gate**: Multi-stage distributed queries execute correctly.

| IC-4 Test | Description | Pass Criteria |
|-----------|-------------|---------------|
| `ShuffleJoinTest` | 2-index hash join via distributed engine | Results match expected join output |
| `ShuffleWindowTest` | Window function across distributed data | Correct row numbering/ranking |
| `ShuffleMultiStageAggTest` | High-cardinality agg with hash exchange | Partial -> exchange -> final correct |
| `ShuffleExplainTest` | `_explain` shows multi-stage plan | All stages, exchanges, assignments visible |
| `ShuffleMemoryPressureTest` | Query that requires spill | Completes correctly with spill |
| `ShuffleConcurrentTest` | 5 concurrent multi-stage queries | All complete, no deadlock |

**Run command**: `./gradlew :distributed-engine:integTest --tests '*Shuffle*'`

**Decision point**: If IC-4 fails, fix Phase 2 issues before proceeding to regression.

---

## Stage 6: Full Regression Suite

**Scope**: All existing tests pass with distributed engine enabled.
**Prerequisite**: IC-4 passes.

### S6.1 Existing Test Suites

| Test Suite | Command | Pass Criteria |
|-----------|---------|---------------|
| Calcite Integration Tests | `./gradlew :integ-test:integTest --tests '*Calcite*IT' -DignorePrometheus` | ALL pass (0 failures) |
| YAML REST Tests | `./gradlew yamlRestTest` | ALL pass (0 failures) |
| Documentation Tests | `./gradlew doctest -DignorePrometheus` | ALL pass (0 failures) |
| Core Unit Tests | `./gradlew :core:test` | ALL pass (0 failures) |
| OpenSearch Module Tests | `./gradlew :opensearch:test` | ALL pass (0 failures) |

### S6.2 Dual-Mode Comparison Tests

| Test | Description | Pass Criteria |
|------|-------------|---------------|
| `DualModeCalciteITTest` | Each Calcite IT query via DSL and distributed | Results identical |
| `DualModeYamlTest` | Each YAML test query via DSL and distributed | Results identical |

### S6.3 Feature Flag Tests

| Test | Description | Pass Criteria |
|------|-------------|---------------|
| `FeatureFlagDisabledTest` | All tests pass with distributed engine disabled | 100% pass, DSL path used |
| `FeatureFlagEnabledTest` | All tests pass with distributed engine enabled | 100% pass, distributed path used where supported |
| `FeatureFlagFallbackTest` | Unsupported patterns fall back to DSL | Graceful fallback, warning logged |

---

## Integration Checkpoint 5 (FINAL): Production Readiness

**Gate**: Full regression + performance validation.

### S7.1 Performance Tests

| Test | Description | Pass Criteria |
|------|-------------|---------------|
| `LatencyRegressionTest` | 50 representative simple queries (filter+agg) | p50 within 10% of DSL path, p99 within 20% |
| `ThroughputTest` | 100 concurrent queries | No timeouts, no OOM |
| `MemoryBenchmarkTest` | Profile memory under load | Peak stays within circuit breaker budget |
| `ScalabilityTest` | Same query on 3-node, 5-node, 10-node clusters | Latency decreases with more nodes for parallelizable queries |
| `PlanningOverheadTest` | Measure time in converter + AddExchanges + fragmenter | < 10ms for typical queries |

### S7.2 Stability Tests

| Test | Description | Pass Criteria |
|------|-------------|---------------|
| `LongRunningStabilityTest` | 1000 queries in sequence | No memory leaks, no crashes |
| `NodeFailureDuringQueryTest` | Kill a data node mid-query | Query fails gracefully with error message |
| `CircuitBreakerTripTest` | Queries that exceed memory budget | Circuit breaker trips, query fails cleanly, system recovers |

### Final Sign-Off Checklist

- [ ] ALL Calcite integTests pass (distributed enabled)
- [ ] ALL Calcite integTests pass (distributed disabled)
- [ ] ALL yamlRestTests pass
- [ ] ALL doctests pass
- [ ] `_explain` API returns distributed plan info
- [ ] No latency regression for simple queries (p50/p99)
- [ ] Feature flag toggles correctly
- [ ] Memory cleanup verified (no leaks)
- [ ] Security: DLS/FLS enforced with direct Lucene access
- [ ] Thread context propagated across exchanges
- [ ] Spill-to-disk works under memory pressure
- [ ] Join queries no longer fall back to DSL
- [ ] Window function queries no longer fall back to DSL
