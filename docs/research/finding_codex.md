# Native Trino Integration in OpenSearch: Multi-Agent Findings

Date: 2026-02-21

## Scope
- Objective: investigate a **native Trino integration** in OpenSearch by bringing in Trino engine components (SQL parser, AST, optimizer, physical operators, functions, scheduler, shuffle/exchange).
- Constraint: treat this as **greenfield** for a new REST API (`/_plugins/_trino_sql`), and do not depend on existing OpenSearch SQL plugin internals as implementation building blocks.
- Focus: technical feasibility and engineering challenges.

## Investigation Tracks (Parallel Agents)
1. Trino distributed query engine key components and execution flow.
2. Current Trino-to-OpenSearch connector integration and limits.
3. OpenSearch runtime constraints relevant to a new native Trino SQL API.

## 1. Trino Key Components Required for Native Embedding

A native integration needs these engine layers, end-to-end:

1. SQL parse + AST
- Trino parser (`SqlParser`) and Trino AST nodes (`Statement`, `Expression`) are required for dialect fidelity.
- Must preserve Trino semantics (identifier resolution, function syntax, type coercion rules).

2. Analysis layer
- Name/type resolution against OpenSearch index metadata.
- Scope building, expression typing, implicit cast insertion, function signature resolution.

3. Optimizer (rule + cost based)
- Logical plan rewrites (predicate pushdown, projection pruning, join ordering, limit/aggregation pushdown).
- Cost model that understands shard/segment statistics and OpenSearch execution primitives.

4. Physical planning + operators
- Fragmented plan stages and operators (scan/filter/project/aggregate/join/sort/window/exchange).
- Operator model must run safely inside OpenSearch JVM and thread-pool model.

5. Function subsystem
- Trino function registry and execution semantics (scalar/aggregate/window).
- Pushdown analyzer for which functions can be translated to OpenSearch DSL/aggs/scripts.

6. Scheduler + split management + shuffle/exchange
- Coordinator-side stage scheduler, shard-aware split assignment, data-local preference.
- Exchange transport with partitioned/broadcast/gather patterns.
- Backpressure, bounded buffers, retries, and cancellation.

7. Resource governance
- Per-query memory, global memory guards, spill-to-disk, timeout/cancellation.
- Strong isolation from search/indexing workloads.

## 2. How Trino Integrates with OpenSearch Today (Connector Model)

Current integration is connector-based, not native engine embedding:

1. Connector shape
- `plugin/trino-opensearch` is a thin wrapper over Trino Elasticsearch connector logic.
- Connector SPI surface: metadata, split manager, page source provider.

2. Data path
- Metadata via index mappings APIs.
- One split per shard via `_search_shards`.
- Data retrieval via REST/JSON (scroll/PIT-style retrieval path depending on implementation).

3. Pushdown profile
- Basic predicate/projection/limit pushdown.
- Major gaps: no full aggregation pushdown parity, limited text/complex predicate pushdown, join execution remains Trino-side.

4. Core limitation
- HTTP/JSON boundary dominates cost: serialization, network hop, and restricted storage-engine visibility.
- No direct access to Lucene/OpenSearch internals for richer planning/execution decisions.

Conclusion: connector mode is useful for federation, but it cannot provide deep native performance or full feature parity for OpenSearch-native analytics workloads.

## 3. Greenfield Native Architecture (`/_plugins/_trino_sql`)

Target execution flow:

1. REST ingress
- New endpoint: `POST /_plugins/_trino_sql`.
- Request includes Trino SQL text, session settings, timeout/memory hints, optional async mode.

2. Parse and analyze
- Parse into Trino AST.
- Resolve metadata against OpenSearch catalog abstraction (cluster/index/field).

3. Optimize
- Trino logical optimizer + OpenSearch-specific rules.
- Produce fragment graph with explicit exchanges.

4. Physical execution in cluster
- Coordinator schedules fragments to data nodes near shards.
- Workers run Trino physical operators in-process.
- Intermediate exchange uses OpenSearch transport layer (not external HTTP).

5. Return
- Stream pages to client (cursor/async optional).
- Expose explain/analyze output with plan and runtime stats.

## 4. Top Technology Challenges

1. SQL dialect and semantic compatibility
- Trino grammar/AST must stay version-aligned with Trino behavior.
- Risk: semantic drift if parser/analyzer forks from upstream.

2. Metadata model mismatch
- Trino expects catalog/schema/table abstractions; OpenSearch is index/mapping-centric.
- Nested/object/array/multi-field handling needs strict, deterministic mapping rules.

3. Cost model quality
- Join ordering and pushdown decisions depend on accurate stats.
- OpenSearch stats are shard/segment-oriented and may be insufficient for Trino CBO without enrichment.

4. Pushdown correctness frontier
- Translating expressions/functions to Query DSL/aggs/scripts is non-trivial.
- Need precise equivalence guarantees (NULL semantics, collation/case, timezone, decimal precision).

5. Scheduler coexistence with OpenSearch workloads
- Query execution must not starve search/index thread pools.
- Requires dedicated pools, admission control, query queues, and fairness policies.

6. Shuffle/exchange pressure
- Exchange traffic can congest transport and heap quickly.
- Need bounded exchange buffers, backpressure protocol, and spill-aware flow control.

7. Memory management and spill
- Hash join/sort/window operators can exceed heap.
- Requires query-level memory accounting integrated with circuit breakers plus robust spill implementation.

8. Function parity at scale
- Trino has a large function surface area.
- Must define staged parity: native pushdown functions first, JVM fallback second, unsupported explicit errors third.

9. Security and multi-index authorization
- Query planning must enforce field/document/index-level security before optimization shortcuts.
- Cross-index joins require strict auth propagation and row/field filtering correctness.

10. Fault tolerance and retries
- Node failures mid-stage need deterministic retry rules and idempotent exchange semantics.
- Coordinator recovery model (or lack of it) must be explicit.

11. Operability and observability
- Must expose per-stage/operator metrics, memory/spill stats, skew diagnostics, and explain plans.
- Without this, production tuning and incident response will be weak.

## 5. Recommended Implementation Strategy

1. Phase 1: Parser/analyzer + single-node execution
- Validate Trino SQL compatibility and semantics.
- No distributed shuffle yet.

2. Phase 2: Pushdown-first execution
- Implement high-impact pushdowns (filters/projections/aggregations/limits).
- Add conformance test matrix for semantic parity.

3. Phase 3: Distributed fragments + exchange
- Introduce stage scheduler, partitioned/broadcast exchanges, and cancellation.
- Harden backpressure and memory isolation.

4. Phase 4: Joins/windows + spill
- Add hash join/window operators with spill-to-disk and skew handling.

5. Phase 5: Production hardening
- Security enforcement audits, chaos/failure testing, SLO-based performance tuning.

## 6. Decision Summary

1. Native integration is technically feasible, but the hardest parts are not parsing; they are distributed runtime control (scheduler + exchange + memory), semantic correctness of pushdown translation, and production-grade operability.
2. The connector approach is a useful baseline but fundamentally constrained by REST/JSON boundaries and limited pushdown.
3. A realistic path is incremental: parser/analyzer compatibility first, then pushdown depth, then full distributed execution.

## Evidence Reviewed
- `docs/prompts/02-trino-opensearch-connector.md`
- `docs/prompts/03-native-integration-architecture.md`
- `plugin/src/main/java/org/opensearch/sql/plugin/SQLPlugin.java`
- `sql/src/main/java/org/opensearch/sql/sql/SQLService.java`
- `sql/src/main/java/org/opensearch/sql/sql/domain/SQLQueryRequest.java`
- `core/src/main/java/org/opensearch/sql/executor/QueryService.java`
- `core/src/main/java/org/opensearch/sql/executor/execution/QueryPlanFactory.java`
- `core/src/main/java/org/opensearch/sql/executor/ExecutionEngine.java`
- `opensearch/src/main/java/org/opensearch/sql/opensearch/executor/OpenSearchExecutionEngine.java`
- `opensearch/src/main/java/org/opensearch/sql/opensearch/executor/OpenSearchQueryManager.java`
- `opensearch/src/main/java/org/opensearch/sql/opensearch/client/OpenSearchClient.java`
- `async-query/src/main/java/org/opensearch/sql/spark/rest/RestAsyncQueryManagementAction.java`
- `async-query-core/src/main/java/org/opensearch/sql/spark/asyncquery/AsyncQueryExecutorServiceImpl.java`
