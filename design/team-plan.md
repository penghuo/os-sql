# Team Plan: Native Distributed Query Engine

## Team Structure

4 dedicated teams + 1 tech lead + 1 dedicated tester = **8 people total**

```
                        ┌──────────────┐
                        │  Tech Lead   │
                        │  (1 person)  │
                        └──────┬───────┘
                               │
          ┌────────────┬───────┼───────┬────────────┐
          │            │       │       │            │
    ┌─────┴─────┐ ┌────┴────┐ │ ┌─────┴─────┐ ┌───┴────┐
    │   Team    │ │  Team   │ │ │   Team    │ │  Team  │
    │  Runtime  │ │Operators│ │ │ Planning  │ │ Test & │
    │ (2 eng)   │ │(2 eng)  │ │ │ (2 eng)   │ │Quality │
    │           │ │         │ │ │           │ │(1 eng) │
    │ Layer 1   │ │ Layer 2 │ │ │ Layer 3 + │ │  All   │
    │           │ │         │ │ │ Exchange  │ │ layers │
    └───────────┘ └─────────┘ │ └───────────┘ └────────┘
                               │
                    Cross-team integration
```

---

## Role Descriptions

### Tech Lead (1 person)

**Responsibilities:**
- Overall architecture ownership, cross-layer integration decisions
- Code review across all layers (especially at integration boundaries)
- Trino source code expertise — guide porting decisions
- Calcite-to-Trino handoff design (RelNodeToPlanNodeConverter)
- Integration checkpoint sign-off
- Unblock teams when cross-layer dependencies arise
- Performance tuning guidance

**Required skills:**
- Deep knowledge of Apache Calcite internals (existing codebase)
- Strong understanding of Trino execution model (new, from source study)
- OpenSearch plugin architecture

**Assigned tasks:** Architecture decisions, code review, IC sign-off

---

### Team Runtime — Layer 1 (2 engineers)

**Owns:** Page/Block data types, Operator interface, Driver/Pipeline, Context hierarchy, MemoryPool

**Engineer R1 — Data Format Specialist:**
- L1.1: Page/Block data types (port from Trino)
- L1.2: BlockBuilder variants (port from Trino)
- L1.3: PagesSerde (port + adapt for StreamOutput/StreamInput)
- L1.4: Operator interface (port from Trino)
- P2.9: Spill-to-disk support (Phase 2)

**Engineer R2 — Runtime Framework Specialist:**
- L1.5: Context hierarchy (custom, simplified from Trino)
- L1.6: MemoryPool (custom, circuit breaker integration)
- L1.7: Driver/Pipeline (port + adapt for OpenSearch thread pool)
- P.3: Memory benchmark (Phase 3, with Tester)

**Shared work:** L1.8 (Layer 1 unit tests)

**Required skills:**
- Strong Java fundamentals (generics, concurrency, memory management)
- Trino's `io.trino.spi.block` and `io.trino.spi.Page` source code understanding
- OpenSearch circuit breaker APIs
- Cooperative scheduling concepts

**Key deliverables:**
| Deliverable | Milestone | Depends on |
|-------------|-----------|-----------|
| Page/Block types compilable + tested | Week 1 end | Nothing |
| Operator interface + BlockBuilders | Week 2 end | L1.1 |
| Context hierarchy + MemoryPool | Week 3 end | L1.4 |
| Driver/Pipeline + all L1 tests pass | Week 4 end | L1.5, L1.6 |

---

### Team Operators — Layer 2 (2 engineers)

**Owns:** Lucene leaf operators, ported Trino generic operators

**Engineer O1 — Lucene Specialist:**
- L2.1: LuceneFullScan (DocValues -> Block conversion)
- L2.2: LuceneFilterScan (Weight/Scorer/DocIdSetIterator)
- P2.7: LuceneAggScan (Collector/LeafCollector, Phase 2)
- P2.8: LuceneSortScan (IndexSearcher.search with Sort, Phase 2)
- R.6: Security validation (DLS/FLS with direct Lucene access)

**Engineer O2 — Trino Operator Porter:**
- L2.3: FilterAndProjectOperator (port)
- L2.4: HashAggregationOperator (port, basic GroupByHash)
- L2.5: TopNOperator (port)
- L2.6: OrderByOperator (port)
- L2.7: MergeSortedPages (port)
- P2.4: LookupJoinOperator (port, Phase 2)
- P2.5: HashBuilderOperator (port, Phase 2)
- P2.6: WindowOperator (port, Phase 2)

**Shared work:** L2.8 (Layer 2a unit tests), P2.10 (Phase 2 operator tests)

**Required skills:**

*Engineer O1:*
- Deep Lucene expertise: IndexSearcher, Weight, Scorer, DocIdSetIterator, LeafReaderContext, DocValues (NumericDocValues, SortedDocValues, BinaryDocValues)
- OpenSearch shard internals: IndexShard.acquireSearcher(), LeafReaderContext enumeration
- Existing per-segment code patterns (CalciteFilterScriptLeafFactory, CalciteAggregationScriptLeafFactory)

*Engineer O2:*
- Trino operator source code expertise (io.trino.operator.*)
- Hash table implementations, join algorithms, window function semantics
- Memory revocation patterns (Operator.startMemoryRevoke/finishMemoryRevoke)
- Type system bridging (Trino TypeOperators -> OpenSearch type system)

**Key deliverables:**
| Deliverable | Milestone | Depends on |
|-------------|-----------|-----------|
| LuceneFullScan + LuceneFilterScan tested | Week 3 end | L1.4, L1.5 |
| All Phase 1 operators tested | Week 4 end | L1.4 |
| IC-1 (Local execution) passes | Week 5 end | L1.*, L2.1-L2.7 |
| Join operators ported + tested | Phase 2 start + 3 weeks | IC-2 |
| Window operator ported + tested | Phase 2 start + 4 weeks | P2.1 |

---

### Team Planning — Layer 3 + Exchange (2 engineers)

**Owns:** RelNodeToPlanNodeConverter, AddExchanges, PlanFragmenter, StageScheduler, Exchange operators, TransportActions

**Engineer P1 — Planner Specialist:**
- L3.1: PlanNode hierarchy
- L3.2: RelNodeToPlanNodeConverter (the Calcite-to-Trino bridge)
- L3.3: AddExchanges (adapted from Trino)
- L3.4: PlanFragmenter (adapted from Trino)
- P2.3: AddExchanges Phase 2 extensions (hash/broadcast strategy)
- L3.7: _explain API integration

**Engineer P2 — Distribution Specialist:**
- L3.5: StageScheduler (ClusterState-based shard routing)
- L3.6: Execution mode router (feature flag, fallback logic)
- E.1: ShardQueryAction (TransportAction for fragment execution)
- E.2: OutputBuffer (bounded Page buffer with backpressure)
- E.3: GatherExchange operator (coordinator-side collection)
- P2.1: HashExchange operator (Phase 2)
- P2.2: BroadcastExchange operator (Phase 2)

**Shared work:** L3.8 (Layer 3 tests), E.4 (Exchange tests)

**Required skills:**

*Engineer P1:*
- Apache Calcite RelNode tree structure (LogicalFilter, LogicalAggregate, LogicalJoin, etc.)
- Trino's AddExchanges and PlanFragmenter source code
- Query plan optimization concepts (distribution properties, exchange insertion)
- PPL-specific AST nodes that need converter support

*Engineer P2:*
- OpenSearch TransportService, HandledTransportAction pattern
- OpenSearch ClusterState, RoutingTable, IndexShardRoutingTable
- Distributed systems: backpressure, buffering, timeout handling
- Network serialization (StreamOutput/StreamInput)

**Key deliverables:**
| Deliverable | Milestone | Depends on |
|-------------|-----------|-----------|
| PlanNode hierarchy + Converter | Week 3 end | Nothing (parallel with L1) |
| AddExchanges + Fragmenter | Week 4 end | L3.1, L3.2 |
| ShardQueryAction + GatherExchange | Week 5 end | L1.3, L3.4 |
| IC-2 (Scatter-gather E2E) passes | Week 6-7 | IC-1, L3.*, E.* |
| HashExchange + BroadcastExchange | Phase 2 start + 2 weeks | IC-2 |

---

### Team Test & Quality (1 dedicated tester)

**Owns:** Integration checkpoints, regression suite, performance benchmarks, dual-mode comparison

**Responsibilities:**
- Write and maintain integration checkpoint test suites (IC-1 through IC-4)
- Run full regression suite at each checkpoint
- Dual-mode comparison testing (distributed vs DSL path)
- Performance benchmarking and latency regression validation
- Feature flag testing (enabled/disabled/fallback)
- Coordinate with all teams on test failures
- CI/CD pipeline setup for distributed engine test targets

**Assigned tasks:**
| Phase | Tasks |
|-------|-------|
| Phase 1 | IC-1 tests, IC-2 tests, support L1.8/L2.8/L3.8/E.4 |
| Phase 2 | P2.10 (Phase 2 operator tests), IC-3 tests |
| Phase 3 | R.1-R.5 (full regression), P.1-P.2 (perf benchmarks), P.4-P.5 (scalability/stability) |

**Required skills:**
- OpenSearch integration test framework (`OpenSearchIntegTestCase`)
- JUnit 5, JMH for micro-benchmarks
- YAML REST test framework
- Query result comparison and validation
- Performance profiling (async-profiler)

**Key deliverables:**
| Deliverable | Milestone | Depends on |
|-------------|-----------|-----------|
| IC-1 test suite ready | Week 4 | Layer 1 + 2a tests passing |
| IC-2 test suite ready | Week 6 | IC-1 passes |
| IC-3 test suite ready | Phase 2 + 4 weeks | IC-2 passes |
| Full regression pass | Phase 3 start | IC-3 passes |
| Performance benchmark report | Phase 3 + 2 weeks | IC-3 passes |

---

## Phase-by-Phase Team Allocation

### Phase 1: Scatter-Gather MVP

| Team | Engineers | Focus |
|------|----------|-------|
| Runtime | R1, R2 | Page/Block, Driver/Pipeline, MemoryPool |
| Operators | O1, O2 | Lucene leaf ops, basic ported operators |
| Planning | P1, P2 | Converter, Fragmenter, GatherExchange |
| Test | T1 | IC-1 + IC-2 test suites |
| Tech Lead | TL | Cross-team integration, architecture |
| **Total** | **8** | |

### Phase 2: Full Shuffle + Joins

| Team | Engineers | Focus |
|------|----------|-------|
| Runtime | R1, R2 | Spill-to-disk, memory revocation |
| Operators | O1, O2 | Join ops, window ops, Lucene agg/sort scan |
| Planning | P1, P2 | HashExchange, BroadcastExchange, join strategy |
| Test | T1 | Phase 2 operator tests, IC-3 suite |
| Tech Lead | TL | Join planning, spill architecture |
| **Total** | **8** | |

### Phase 3: Regression + Production Readiness

| Team | Engineers | Focus |
|------|----------|-------|
| All teams | All | Fix regression failures, performance tuning |
| Test | T1 (lead) | Full regression, performance benchmarks, stability |
| Tech Lead | TL | Final sign-off, documentation |
| **Total** | **8** (some may rotate to other projects after sign-off) | |

---

## Cross-Team Integration Points

These are moments where multiple teams must collaborate closely. Schedule sync meetings at these points.

| Integration Point | Teams Involved | Description |
|-------------------|---------------|-------------|
| **Operator interface contract** | Runtime + Operators | Agree on exact Operator interface, OperatorContext API before implementation |
| **Page/Block type mapping** | Runtime + Operators | Agree on DocValues -> Block mapping, null representation, multi-valued fields |
| **PlanNode -> OperatorFactory** | Planning + Operators | Converter produces PlanNodes; something must translate PlanNodes to OperatorFactories for Driver creation |
| **Exchange serialization** | Runtime + Planning | PagesSerde must work over TransportService; agree on wire format |
| **Memory budget partitioning** | Runtime + Planning | How is MemoryPool budget split across stages? Per-query or per-stage limits? |
| **IC-1 wiring** | All teams | First time all layers connect; expect integration issues |
| **IC-2 multi-node** | All teams | First time exchange actually transfers Pages between nodes |
| **Security context propagation** | Operators + Planning | ThreadContext with credentials must flow from coordinator to data nodes via ShardQueryAction |

---

## Communication Plan

| Cadence | Format | Participants | Purpose |
|---------|--------|-------------|---------|
| Daily | Standup (15 min) | All teams | Blockers, progress, cross-team needs |
| Weekly | Design sync (60 min) | Tech Lead + 1 per team | Architecture decisions, integration issues |
| Per IC | IC review (90 min) | All teams + Tester | Review IC results, go/no-go for next phase |
| As needed | Pairing sessions | 2 engineers cross-team | Resolve integration issues at code level |

---

## Risk Mitigation by Team

| Risk | Owning Team | Mitigation |
|------|-------------|------------|
| Trino operator dependencies deeper than expected | Operators (O2) | Start with simplest operator (FilterAndProject), discover patterns before tackling HashAgg/Join |
| RelNode-to-PlanNode converter misses edge cases | Planning (P1) | Start with PPL's most common RelNode types first; enumerate all from CalciteRelNodeVisitor |
| Memory accounting mismatch between MemoryPool and circuit breaker | Runtime (R2) | Early integration test with realistic memory pressure at IC-1 |
| Exchange performance bottleneck | Planning (P2) | Benchmark Page serialization size early; consider compression if needed |
| Bytecode generation (FlatHashStrategyCompiler) too complex to port | Operators (O2) + Tech Lead | Fallback: replace with reflection-based hashing; benchmark the performance delta |
| PPL-specific nodes not handled by converter | Planning (P1) | Enumerate all PPL commands that produce non-standard RelNodes; fallback to DSL for unhandled ones |
