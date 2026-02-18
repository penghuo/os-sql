# Phase 1 Team Plan: Scatter-Gather MVP

## Team Structure

5 specialized agents + 1 tech lead (Claude)

```
                         ┌──────────────┐
                         │  Tech Lead   │
                         │  (Claude)    │
                         └──────┬───────┘
                                │
       ┌──────────┬─────────────┼──────────┬──────────┐
       │          │             │          │          │
  ┌────┴────┐ ┌───┴───┐ ┌──────┴──────┐ ┌─┴──────┐ ┌┴────────┐
  │ runtime │ │lucene │ │ trino-ops   │ │planner │ │exchange │
  │         │ │-ops   │ │             │ │        │ │         │
  │ L1.1-8  │ │L2.1-2 │ │ L2.3-7     │ │L3.1-8  │ │E.1-4   │
  │ ~38 cls │ │+types │ │ +tests     │ │~27 cls │ │V.1-4   │
  │         │ │~4 cls │ │ ~17 cls    │ │        │ │~14 cls │
  └─────────┘ └───────┘ └────────────┘ └────────┘ └─────────┘
```

### Why 5 Agents

**Critical path analysis** showed that splitting the old `planning` agent into `planner` + `exchange` saves 3 turns on the critical path (27 → 24 turns to IC-2). Splitting the old `operators` agent into `lucene-ops` + `trino-ops` doesn't reduce the critical path further, but provides significant value for IC-3:

1. **IC-3 debugging parallelism**: When 30+ integration tests fail, lucene-ops fixes DocValues bugs while trino-ops fixes aggregation bugs simultaneously.
2. **Knowledge isolation**: LuceneFullScan (DocValues, LeafReaderContext, Scorer) and HashAggregationOperator (GroupByHash, accumulators) require completely different domain knowledge. One agent holding both in context is wasteful.
3. **lucene-ops idle time is productive**: While waiting for L1.4+L1.5 dependencies, lucene-ops studies OpenSearch's DocValues usage patterns and prepares type mapping design.
4. **IC-3 failure routing**: "DocValues type mapping failures → lucene-ops", "Aggregation edge cases → trino-ops", "Converter missing pattern errors → planner".

---

## Role Descriptions

### Tech Lead (Claude — you)

**Responsibilities:**
- Overall architecture ownership, cross-layer integration decisions
- Code review across all layers (especially at integration boundaries)
- Trino source code expertise — guide porting decisions
- Calcite-to-Trino handoff design (RelNodeToPlanNodeConverter)
- Integration checkpoint sign-off
- Unblock teams when cross-layer dependencies arise

**Assigned tasks:** Architecture decisions, code review, IC sign-off

---

### runtime — Layer 1 (Execution Runtime)

**Owns:** Page/Block data types, Operator interface, Driver/Pipeline, Context hierarchy, MemoryPool

**Tasks:**
- L1.1: Page/Block data types (port from Trino)
- L1.2: BlockBuilder variants (port from Trino)
- L1.3: PagesSerde (port + adapt for StreamOutput/StreamInput)
- L1.4: Operator interface (port from Trino)
- L1.5: Context hierarchy (custom, simplified from Trino)
- L1.6: MemoryPool (custom, circuit breaker integration)
- L1.7: Driver/Pipeline (port + adapt for OpenSearch thread pool)
- L1.8: Layer 1 unit tests

**Scheduling (critical: delivers L1.1 and L1.4 ASAP to unblock others):**
```
Turn  1-3:  L1.1 (Page/Block)         ← FIRST: unblocks everything
Turn  4:    L1.4 (Operator interface)  ← SECOND: unblocks L2 agents
Turn  5-6:  L1.3 (PagesSerde)         ← THIRD: unblocks exchange E.1
Turn  7-8:  L1.2 (BlockBuilder)
Turn  9-11: L1.5 (Context)
Turn 12-13: L1.6 (MemoryPool)
Turn 14-16: L1.7 (Driver/Pipeline)
Turn 17-19: L1.8 (Unit tests)
```

**Key deliverables:**
| Deliverable | Target Turn | Depends on |
|-------------|------------|------------|
| Page/Block types compilable + tested | Turn 3 | Nothing |
| Operator interface | Turn 4 | L1.1 |
| PagesSerde | Turn 6 | L1.1 |
| Context hierarchy + MemoryPool | Turn 13 | L1.4 |
| Driver/Pipeline + all L1 tests pass | Turn 19 | L1.5, L1.6 |

---

### lucene-ops — Layer 2: Lucene Leaf Operators

**Owns:** LuceneFullScan, LuceneFilterScan, DocValues→Block type mapping

**Tasks:**
- L2.1: LuceneFullScan (DocValues → Block conversion, per LeafReaderContext)
- L2.2: LuceneFilterScan (Weight/Scorer/DocIdSetIterator → filtered Pages)
- Type mapping tests (S2.3 from test plan)
- Lucene-side L2.8 tests

**Specialty:** DocValues→Block conversion, Weight/Scorer, LeafReaderContext. Deep Lucene knowledge.

**Scheduling (waits for L1.4+L1.5, uses idle time productively):**
```
Turn  1-4:  [Study OpenSearch DocValues patterns, prepare type mapping design]
Turn  5-7:  [Study IndexSearcher/LeafReaderContext patterns in codebase]
Turn  8-11: [IDLE or write type mapping tests scaffolding]
Turn 12-13: L2.1 (LuceneFullScan)     ← needs L1.4 + L1.5
Turn 14-15: L2.2 (LuceneFilterScan)   ← needs L2.1
Turn 16-17: Type mapping tests (S2.3)
Turn 18-19: Lucene operator tests
```

**Key deliverables:**
| Deliverable | Target Turn | Depends on |
|-------------|------------|------------|
| LuceneFullScan tested | Turn 13 | L1.4, L1.5 |
| LuceneFilterScan tested | Turn 15 | L2.1 |
| All Lucene operator tests pass | Turn 19 | L2.1, L2.2 |

---

### trino-ops — Layer 2: Ported Trino Operators

**Owns:** FilterAndProjectOperator, HashAggregationOperator, TopNOperator, OrderByOperator, MergeSortedPages

**Tasks:**
- L2.3: FilterAndProjectOperator (port from Trino)
- L2.4: HashAggregationOperator (port, basic GroupByHash, no bytecode gen)
- L2.5: TopNOperator (port, heap-based)
- L2.6: OrderByOperator (port, in-memory)
- L2.7: MergeSortedPages (port, K-way merge)
- Trino-side L2.8 tests

**Specialty:** Porting Trino operators. Needs Trino codebase familiarity.

**Scheduling (grabs what it can early, does HashAgg last):**
```
Turn  1-3:  [Study Trino operator source files]
Turn  4:    L2.7 (MergeSorted)            ← only needs L1.1
Turn  5:    L2.5 (TopN)                   ← needs L1.4
Turn  6:    L2.6 (OrderBy)               ← needs L1.4
Turn  7-11: [IDLE waiting for L1.5, or write test scaffolding]
Turn 12-13: L2.3 (FilterAndProject)      ← needs L1.4 + L1.5
Turn 14-16: L2.4 (HashAgg)              ← needs L1.4 + L1.5 + L1.6, biggest task
Turn 17-18: Trino operator tests
```

**Key deliverables:**
| Deliverable | Target Turn | Depends on |
|-------------|------------|------------|
| MergeSortedPages | Turn 4 | L1.1 |
| TopN + OrderBy | Turn 6 | L1.4 |
| FilterAndProject | Turn 13 | L1.4, L1.5 |
| HashAggregation | Turn 16 | L1.4, L1.5, L1.6 |
| All Trino operator tests pass | Turn 18 | L2.3-L2.7 |

---

### planner — Layer 3: Physical Planning

**Owns:** PlanNode hierarchy, RelNodeToPlanNodeConverter, AddExchanges, PlanFragmenter

**Tasks:**
- L3.1: PlanNode hierarchy (PlanNode, FilterNode, ProjectNode, AggregationNode, etc.)
- L3.2: RelNodeToPlanNodeConverter (visitor over Calcite RelNode → PlanNode)
- L3.3: AddExchanges (Phase 1: GatherExchange only)
- L3.4: PlanFragmenter (cut at ExchangeNode → 2 fragments)
- L3.8: Layer 3 unit tests

**Specialty:** PlanNode hierarchy, Converter, AddExchanges, Fragmenter. Knows both Calcite and Trino.

**Scheduling (starts immediately, no L1 dependencies):**
```
Turn  1-2:  L3.1 (PlanNode hierarchy)
Turn  3-5:  L3.2 (RelNodeToPlanNodeConverter — the key bridge)
Turn  6-7:  L3.3 (AddExchanges)
Turn  8-9:  L3.4 (PlanFragmenter)
Turn 10-11: L3.8 (Unit tests)
[FREE from turn 12 — available for IC-1 support]
```

**Key deliverables:**
| Deliverable | Target Turn | Depends on |
|-------------|------------|------------|
| PlanNode hierarchy + Converter | Turn 5 | Nothing (parallel with L1) |
| AddExchanges + Fragmenter | Turn 9 | L3.1, L3.2 |
| Layer 3 unit tests pass | Turn 11 | L3.1-L3.4 |

---

### exchange — Exchange, Scheduling, Router, Verification

**Owns:** ShardQueryAction, OutputBuffer, GatherExchange, StageScheduler, Router, _explain, Verification (V.1-V.4)

**Tasks:**
- E.1: ShardQueryAction (TransportAction for fragment execution)
- E.2: OutputBuffer (bounded Page buffer with backpressure)
- E.3: GatherExchange operator (coordinator-side collection)
- E.4: Exchange unit tests
- L3.5: StageScheduler (ClusterState-based shard routing)
- L3.6: Execution mode router (feature flag, fallback logic)
- L3.7: _explain API integration
- V.1: Engine tag in response
- V.2: Execution counters (DistributedEngineMetrics)
- V.3: Strict mode
- V.4: Test harness integration

**Specialty:** Transport layer, GatherExchange, Router, verification infrastructure. OpenSearch transport layer specialist.

**Scheduling (waits for L1.3 + L3.4, does what it can):**
```
Turn  1-6:  [Study OpenSearch transport actions, prepare scaffolding]
Turn  7-8:  E.2 (OutputBuffer)            ← needs L1.1 + L1.3
Turn  9:    [IDLE waiting for L3.4]
Turn 10-11: E.1 (ShardQueryAction)        ← needs L1.3 + L3.4
Turn 12-13: E.3 (GatherExchange)          ← needs E.1 + E.2
Turn 14-15: L3.5 (StageScheduler)         ← needs L3.4
Turn 16-17: L3.6 (Router)                 ← needs L3.5
Turn 18:    L3.7 (Explain)                ← needs L3.5
Turn 19-20: E.4 (Exchange tests)
Turn 21-23: V.1-V.4 (Verification)        ← needs L3.6
```

**Key deliverables:**
| Deliverable | Target Turn | Depends on |
|-------------|------------|------------|
| OutputBuffer | Turn 8 | L1.1, L1.3 |
| ShardQueryAction + GatherExchange | Turn 13 | L1.3, L3.4 |
| StageScheduler + Router | Turn 17 | L3.4 |
| Exchange tests pass | Turn 20 | E.1-E.3 |
| Verification complete (V.1-V.4) | Turn 23 | L3.6 |

---

## Integration Checkpoints

```
IC-1 (turn ~20): runtime(19) + lucene-ops(19) + trino-ops(18) → run by planner (free since turn 12)
IC-2 (turn ~24): IC-1(~21) + planner(11) + exchange(23)       → run by runtime (free since turn 20)
IC-3 (turn 25+): All teams fix failures in parallel by specialty
```

**Makespan to IC-2: ~24 turns**

---

## Cross-Team Integration Points

| Integration Point | Teams Involved | Description |
|-------------------|---------------|-------------|
| **Operator interface contract** | runtime + lucene-ops + trino-ops | Agree on exact Operator interface, OperatorContext API before implementation |
| **Page/Block type mapping** | runtime + lucene-ops | Agree on DocValues → Block mapping, null representation, multi-valued fields |
| **PlanNode → OperatorFactory** | planner + trino-ops + lucene-ops | Converter produces PlanNodes; something must translate PlanNodes to OperatorFactories for Driver creation |
| **Exchange serialization** | runtime + exchange | PagesSerde must work over TransportService; agree on wire format |
| **Memory budget partitioning** | runtime + planner | How is MemoryPool budget split across stages? Per-query or per-stage limits? |
| **IC-1 wiring** | runtime + lucene-ops + trino-ops + planner | First time all layers connect; expect integration issues |
| **IC-2 multi-node** | All agents | First time exchange actually transfers Pages between nodes |
| **IC-3 regression** | All agents | All teams fix failures in their specialty areas |

---

## IC-3 Failure Routing by Specialty

| Failure Category | Assigned Agent | Examples |
|-----------------|---------------|----------|
| DocValues type mapping | lucene-ops | Wrong Block type for field, null handling in DocValues |
| Aggregation edge cases | trino-ops | GroupByHash collision, accumulator overflow, empty group |
| Operator pipeline issues | trino-ops | TopN tie-breaking, OrderBy null ordering |
| Converter missing patterns | planner | Unhandled RelNode type, PPL-specific node |
| Exchange/transport errors | exchange | Serialization mismatch, backpressure deadlock |
| Memory/context issues | runtime | Circuit breaker trip, context propagation failure |

---

## Risk Mitigation

| Risk | Owning Agent | Mitigation |
|------|-------------|------------|
| Trino operator dependencies deeper than expected | trino-ops | Start with simplest operator (TopN), discover patterns before tackling HashAgg |
| RelNode-to-PlanNode converter misses edge cases | planner | Enumerate all RelNode types from CalciteRelNodeVisitor; fallback to DSL for unhandled ones |
| Memory accounting mismatch between MemoryPool and circuit breaker | runtime | Early integration test with realistic memory pressure at IC-1 |
| Exchange performance bottleneck | exchange | Benchmark Page serialization size early; consider compression if needed |
| Bytecode generation (FlatHashStrategyCompiler) too complex to port | trino-ops + Tech Lead | Fallback: replace with reflection-based hashing; benchmark the performance delta |
| PPL-specific nodes not handled by converter | planner | Enumerate all PPL commands that produce non-standard RelNodes; fallback to DSL for unhandled ones |
| IC-3 reveals many failing Calcite ITs | All agents | Triage failures by category (see IC-3 Failure Routing) and assign to owning agent |
| DocValues → Block conversion correctness | lucene-ops | Test every DocValues type against known data; compare with DSL path results |
