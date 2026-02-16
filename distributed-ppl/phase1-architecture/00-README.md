# Phase 1: Architecture Design — Distributed PPL Execution

## Status: COMPLETE (pending review)

## Documents

| # | Document | Contents |
|---|----------|----------|
| 01 | [Architecture Decision Record](01-architecture-decision-record.md) | Core design decisions: fragment model, transport, operators, routing, memory, lifecycle |
| 02 | [Fragment Planner Design](02-fragment-planner-design.md) | How RelNode trees are split into fragments at exchange boundaries |
| 03 | [Transport Protocol Spec](03-transport-protocol-spec.md) | 3 transport actions, request/response schemas, serialization, security |
| 04 | [Operator Interface Design](04-operator-interface-design.md) | 5 distributed operators: partial agg, final agg, exchange, hash join, merge sort |
| 05 | [Execution Engine Design](05-distributed-execution-engine-design.md) | Orchestration, shard dispatch, exchange service, query lifecycle, routing |
| 06 | [Command Routing Matrix](06-command-routing-matrix.md) | Per-PPL-command distributed strategy (Tier 1/2/3) with fragment plan examples |
| 07 | [Implementation Roadmap](07-implementation-roadmap.md) | Phase 2/3 work breakdown, file lists, acceptance criteria |

## Architecture Summary

```
PPL text
  → ANTLR Parser → AST                              (existing, unchanged)
  → Analyzer                                         (existing, unchanged)
  → CalciteRelNodeVisitor → Calcite RelNode tree     (existing, unchanged)
  → QueryRouter                                      (NEW: decides distributed vs single-node)
  → FragmentPlanner                                  (NEW: splits RelNode at exchange boundaries)
  → DistributedExecutionEngine                       (NEW: dispatches fragments via transport)
      ├─ SOURCE fragments → data nodes (shard-local execution)
      ├─ HASH fragments → worker nodes (hash-partitioned joins/aggs)
      └─ SINGLE fragment → coordinator (final merge)
  → Results → PPL response format                    (existing, unchanged)
```

## Key Decisions

1. **Fragment-based execution** — queries split into fragment subtrees connected by exchanges
2. **3 transport actions** — shard execution, exchange data, query cancel
3. **5 distributed operators** — partial agg, final agg, exchange, hash join, merge sort
4. **All operators extend PhysicalPlan** — same Iterator<ExprValue> contract as single-node
5. **Feature flag gated** — `plugins.ppl.distributed.enabled=false` by default
6. **Automatic routing** — QueryRouter decides based on index size and query pattern
7. **Graceful fallback** — unsupported queries route to single-node engine
8. **Circuit breaker integration** — all operators respect OpenSearch memory limits

## What's NOT Changed

- PPL grammar, parser, AST
- Analyzer, type system
- CalciteRelNodeVisitor (all 45+ visitors)
- Existing optimization rules
- Response formatting (JSON, JDBC, CSV)
- SQL execution path
- Async query / Spark path

## Next Steps

Phase 2 implementation can begin with two parallel workstreams:
- **Workstream A:** Fragment planner + shard split manager (9 steps)
- **Workstream B:** Transport actions + execution engine (12 steps)

See [Implementation Roadmap](07-implementation-roadmap.md) for details.
