# Design Review: Native Trino Integration in OpenSearch

Date: 2026-02-21
Reviewed document: `docs/research/finding_codex.md`

## Executive Summary

Four independent review angles examined the proposed native Trino integration in OpenSearch. The document under review is technically sound in identifying the right problem space and challenges, but the reviewers found significant gaps in several areas: the distributed execution design underestimates OpenSearch runtime constraints, the phased implementation strategy has ordering issues, alternative approaches deserve deeper evaluation before committing to native embedding, and the operational burden is substantially larger than presented. The consensus is that the native integration is feasible but carries high execution risk, and a hybrid or enhanced-connector approach may deliver better value with lower risk in the near term.

---

## 1. Architecture and Systems Design Review

### 1.1 Layered Architecture Assessment

The proposed pipeline (parse -> analyze -> optimize -> execute -> exchange) follows the standard query engine layering and is fundamentally sound. However, several architectural gaps emerge on closer examination:

**Missing catalog/metadata service layer.** The document jumps from "resolve metadata against OpenSearch catalog abstraction" to optimization without addressing how the catalog service itself works. In a running OpenSearch cluster, index metadata changes frequently (new indices, mapping updates, alias changes). The design needs an explicit metadata caching and invalidation layer between analysis and optimization. Without it, query plans can be compiled against stale metadata, leading to runtime failures or incorrect results.

**No explicit plan caching or compilation model.** Trino compiles plans per-query with no plan caching. For an OpenSearch-embedded engine serving repeated analytical queries, this is wasteful. The architecture should address whether compiled plans (or plan fragments) can be cached and under what invalidation conditions.

**Incomplete data-path specification.** The document says workers run "Trino physical operators in-process" but does not specify how scan operators access Lucene segments. There are two fundamentally different approaches: (a) going through OpenSearch's search infrastructure (QueryPhase/FetchPhase), which adds overhead but preserves compatibility; or (b) reading Lucene segments directly, which is faster but couples tightly to internal APIs. This is a first-order architectural decision that shapes every subsequent layer.

### 1.2 Distributed Execution Design Gaps

The document identifies the right components (scheduler, exchange, backpressure) but lacks depth on several critical distributed systems concerns:

**Split assignment and data locality.** The document mentions "shard-aware split assignment" but does not address shard relocation, replica selection, or how to handle indices with uneven shard sizes. In OpenSearch, shards can relocate during query execution, and primary vs. replica selection affects both performance and consistency.

**Exchange topology choices.** The document lists "partitioned/broadcast/gather" patterns but does not discuss when each applies or how the topology is determined. In Trino, the optimizer selects exchange types based on the physical plan. The design must specify how this selection works when the underlying storage is OpenSearch rather than HDFS/S3.

**No discussion of pipeline vs. stage-based execution.** Trino uses a stage-based execution model where entire stages run concurrently. The document does not address whether this model is compatible with OpenSearch's resource governance. An alternative pipeline model (processing tuples as they flow) might coexist better with search workloads but fundamentally changes the execution semantics.

**Coordinator single-point-of-failure.** The document mentions coordinator recovery must be "explicit" but does not propose a model. In a cluster where any node can be the coordinating node for search requests, the design needs to specify whether there is a single designated Trino coordinator or whether any node can coordinate, and what happens to in-flight queries during node failures.

### 1.3 Phased Implementation Realism

The five-phase plan has reasonable directionality but contains critical ordering issues:

**Phase 1 (single-node) is misleading.** A "single-node execution" that parses Trino SQL, analyzes it, and runs operators locally is essentially building a new single-node query engine. This is useful for validating SQL compatibility but delivers zero value for the use case that motivates native integration (distributed in-situ analytics). The phase should be framed honestly as a "compatibility validation" phase, not as delivering a usable feature.

**Phase 2 (pushdown) before Phase 3 (distributed) is correct but incomplete.** Pushdown-first is the right strategy because it maximizes the work done by OpenSearch natively. However, Phase 2 needs explicit acceptance criteria: what coverage percentage of Trino SQL constructs must be pushdown-able before Phase 3 begins? Without this, Phase 2 could expand indefinitely.

**Phase 4 (joins/windows + spill) is where real complexity lives.** Hash join and window operators with spill-to-disk are the hardest operators to implement correctly and efficiently. Placing them in Phase 4 means the design defers the hardest engineering problems to the latest phase, when accumulated technical debt from Phases 1-3 will already be substantial.

**Missing Phase 0: connector enhancement baseline.** Before Phase 1, the project should invest in measuring what an enhanced connector can achieve. This provides a quantitative baseline against which native integration progress can be measured, and may reveal that pushdown improvements alone close most of the performance gap.

### 1.4 JVM Embedding Risks

Running Trino engine components inside the OpenSearch JVM introduces risks the document underestimates:

**Dependency conflicts.** Trino and OpenSearch both depend on Guava, Jackson, Netty, and other libraries, often at different versions. Trino uses its own classloader isolation (`PluginClassLoader`), but embedding Trino components inside OpenSearch's plugin classloader hierarchy creates version conflicts that are notoriously difficult to resolve. Shading can help but introduces its own debugging difficulties.

**GC pressure.** Trino's operator model allocates and releases memory in patterns optimized for throughput (large batch allocations, operator-scoped lifecycles). OpenSearch's GC is tuned for search workloads (many small, short-lived allocations). Combining both workload patterns in a single JVM will likely require GC tuning compromises that degrade both workloads.

**Thread model mismatch.** Trino uses a cooperative multi-tasking model with driver loops and split-level scheduling. OpenSearch uses thread-per-request with thread pools. Embedding Trino's scheduling model inside OpenSearch requires either (a) adapting Trino to use OpenSearch's thread pools (losing Trino's scheduling semantics) or (b) running Trino's own scheduler alongside OpenSearch's (creating resource contention and priority inversion risks).

### 1.5 Architectural Alternatives

The document presents native embedding as the primary path without evaluating alternatives with comparable rigor:

**Calcite bridge.** OpenSearch SQL already uses Apache Calcite. Extending Calcite with a Trino-compatible SQL dialect (grammar + validation rules) and Trino-compatible function signatures, while keeping Calcite's optimizer and OpenSearch-native execution, could deliver Trino SQL compatibility without the embedding risks. The existing `CalcitePlanContext`, `CalciteRelNodeVisitor`, and related infrastructure in the codebase (`core/src/main/java/org/opensearch/sql/calcite/`) provide a concrete starting point.

**Sidecar process model.** Running Trino as a sidecar process on each OpenSearch node, communicating via shared memory or local Unix sockets, avoids JVM embedding entirely. Data can be transferred in Arrow format. This preserves Trino's native execution semantics while gaining data locality.

**Enhanced connector with Arrow Flight.** Implementing Arrow Flight in the OpenSearch connector eliminates the HTTP/JSON serialization overhead that the document correctly identifies as the connector's main limitation. Combined with richer pushdown (aggregations, complex predicates), this could close a significant portion of the performance gap.

---

## 2. OpenSearch Integration and Compatibility Review

### 2.1 Transport Layer for Exchange

The proposal to use OpenSearch's transport layer for inter-node exchange is feasible but carries significant constraints:

**Serialization format mismatch.** OpenSearch transport uses its own `StreamInput`/`StreamOutput` binary serialization. Trino's exchange protocol uses a page-based format (`SerializedPage`) optimized for columnar data. Either Trino's page format must be adapted to OpenSearch's serialization, or custom transport actions must bypass the standard serialization, both of which add integration complexity and risk.

**Backpressure limitations.** OpenSearch's transport layer is fundamentally request/response oriented. It does not natively support streaming with backpressure. Trino's exchange requires the ability to pause producers when consumers fall behind. Implementing this on top of OpenSearch transport would require a custom flow-control protocol layered on top of transport actions, similar to what OpenSearch does for cross-cluster replication but more complex.

**Resource accounting.** OpenSearch transport actions have no built-in mechanism for tracking memory consumption by ongoing exchanges. Circuit breakers in OpenSearch operate at a coarser granularity (request-level, not stream-level). A sustained exchange flow could accumulate significant memory without triggering circuit breakers until it's too late.

**Network bandwidth competition.** The transport layer is shared with cluster state updates, shard recovery, search inter-node communication, and replication traffic. Heavy exchange traffic from analytical queries could degrade operational cluster communication. The design needs traffic prioritization or separate network channels.

**Recommendation:** Consider implementing exchange on a dedicated Netty channel or using a custom TCP transport separate from the main OpenSearch transport. This avoids interference with cluster operations and allows purpose-built backpressure and serialization.

### 2.2 Thread Pool and Memory Coexistence

This is one of the most critical integration challenges:

**Thread pool model.** The current codebase shows that OpenSearch SQL queries run through `OpenSearchQueryManager` (at `opensearch/src/main/java/org/opensearch/sql/opensearch/executor/OpenSearchQueryManager.java`) which uses the `GENERIC` thread pool. The `GENERIC` thread pool is unbounded (scaling) and shared with many cluster operations. Long-running analytical queries from Trino operators would monopolize threads.

**Dedicated thread pool necessity.** The design must introduce a dedicated, bounded thread pool for Trino query execution, similar to how the `SEARCH` and `WRITE` pools are configured. This pool needs:
  - Configurable size (likely percentage of available cores)
  - Queue depth limits with rejection policies
  - Integration with OpenSearch's `ThreadPoolStats` for monitoring

**Memory isolation.** The existing codebase uses OpenSearch's circuit breaker framework (`CircuitBreakerService`) for memory protection. The `REQUEST` circuit breaker tracks per-request heap usage. However, Trino's memory model is more granular: per-operator, per-query, and per-node memory pools with revocable/non-revocable categories. The integration must either:
  - Adapt Trino's memory tracking to report into OpenSearch's circuit breaker framework
  - Or run a separate memory accounting system that coordinates with circuit breakers at a higher level

The current `OpenSearchExecutionEngine` implementation is relatively simple, primarily delegating to OpenSearch client for query execution. A Trino integration would need a far more sophisticated execution engine with lifecycle management, memory tracking, and cancellation support.

**GC impact.** Analysis of the existing code shows that OpenSearch SQL/PPL currently creates relatively short-lived objects (query plans, result sets). Trino operators maintain long-lived state (hash tables for joins, sort buffers, window function state). This fundamentally changes the object lifetime distribution in the heap, likely requiring G1 or ZGC tuning adjustments that affect all OpenSearch workloads.

### 2.3 Metadata Model Mapping

The catalog/schema/table vs. index/mapping mismatch is more severe than the document suggests:

**Current SQL plugin approach.** The existing codebase maps OpenSearch concepts to a SQL-compatible model:
  - Cluster -> catalog (implicit, single catalog)
  - No schema concept (flat namespace)
  - Index/alias -> table
  - Field mappings -> columns

Trino expects a richer model with explicit catalogs and schemas, and its optimizer makes decisions based on this hierarchy. The mapping must handle:

**Multi-type fields.** OpenSearch allows a field to be indexed as both `text` and `keyword` (multi-fields). Trino has no direct equivalent. The current SQL plugin handles this by exposing them as separate columns, but Trino's type system would need explicit rules for which sub-field to use in which context.

**Nested and object types.** OpenSearch's nested type creates independent Lucene documents for array elements. Trino's `ROW` and `ARRAY` types have different semantics. Mapping between them requires careful attention to query semantics, especially for joins and aggregations on nested fields.

**Dynamic mappings.** OpenSearch indices can have dynamic mappings where new fields appear as documents are indexed. Trino's analysis phase expects stable schemas. The metadata layer must handle schema drift gracefully, either by snapshotting at analysis time or by implementing dynamic schema evolution.

**Index patterns and aliases.** Trino queries reference specific tables. OpenSearch commonly uses index patterns (`logs-*`) and aliases that resolve to multiple indices with potentially different mappings. The catalog implementation must handle union schemas and mapping conflicts.

### 2.4 Security Model Integration

The security integration is perhaps the most underestimated challenge:

**Current security model.** The existing SQL plugin code includes security access patterns (e.g., `SecurityAccess` utility class, `doPrivileged` blocks). The OpenSearch security plugin enforces:
  - Index-level permissions (who can access which indices)
  - Document-level security (DLS: row-level filters applied transparently)
  - Field-level security (FLS: column masking/exclusion)

**Impact on query planning.** DLS and FLS must be applied before optimization, because:
  - DLS filters must be injected before predicate pushdown decisions (a pushed-down predicate might expose filtered rows through side channels)
  - FLS exclusions must be known before projection pruning (an optimizer cannot prune a security-filtered column and then fail to apply the filter)

**Cross-index joins.** If a Trino query joins two indices with different security policies, the execution engine must ensure that:
  - Each side of the join only sees authorized rows/fields
  - The join result does not leak information across security boundaries
  - Intermediate exchange data maintains security context

This is a notoriously difficult problem in database security and requires careful design. The document mentions it but provides no architectural approach.

**Recommendation:** Security enforcement should be a first-class architectural concern from Phase 1, not a hardening step in Phase 5. Building query planning and execution without security integration from the start will result in either expensive retrofitting or security vulnerabilities.

### 2.5 Comparison with Existing SQL Plugin Architecture

The existing OpenSearch SQL plugin provides a concrete reference point:

**Current Calcite-based architecture.** The codebase shows a mature Calcite integration:
  - `CalcitePlanContext` for planning context
  - `CalciteRelNodeVisitor` for converting logical plans to OpenSearch operations
  - `CalciteRelBuilder` and related infrastructure for building relational algebra
  - Type conversion between Calcite and OpenSearch types
  - Function registration and pushdown rules

**Duplication risk.** A native Trino integration would duplicate much of this infrastructure: type mapping, function pushdown rules, security integration, result formatting, REST endpoint handling. The codebase already has these patterns established in the SQL plugin; building parallel infrastructure for Trino creates a maintenance burden.

**Migration complexity.** If the Trino integration eventually replaces the Calcite-based engine, migrating existing SQL/PPL users requires backward compatibility for both SQL dialects during a transition period. The document does not address this transition.

**Alternative: extend Calcite.** The existing Calcite infrastructure could be extended to support Trino SQL syntax by adding Calcite's `SqlConformance` customization or a translation layer from Trino AST to Calcite RelNode. This reuses the existing pushdown, type mapping, and security infrastructure while adding Trino dialect support.

---

## 3. Trino Ecosystem and Semantic Correctness Review

### 3.1 Parser/Analyzer Fork Risks

The document correctly identifies semantic drift as a risk but significantly underestimates its severity:

**Trino release cadence.** Trino releases a new version roughly every 1-2 weeks. Each release may include grammar changes, new syntax, function additions, and semantic adjustments. Over a 12-month period, this amounts to 25-50 releases, many of which touch the parser or analyzer.

**AST stability.** Trino's AST (`io.trino.sql.tree`) is internal API with no backward-compatibility guarantees. Classes are regularly renamed, refactored, or restructured. A fork would need continuous rebasing against these changes.

**Semantic testing burden.** To verify that a forked parser/analyzer matches upstream Trino behavior, the project would need to run Trino's own test suites (which depend on Trino's full runtime) or maintain a parallel conformance test suite. Either approach is a substantial ongoing cost.

**Concrete mitigation proposals lacking.** The document should specify:
  - A target Trino version to pin against (e.g., Trino 4xx)
  - A cadence for upstream synchronization (quarterly? yearly?)
  - A conformance test framework that can be run without the full Trino runtime
  - Clear boundaries on which Trino features are in-scope vs. explicitly unsupported

**Recommendation:** Rather than forking Trino's parser, consider consuming it as a library dependency (Trino's `trino-parser` module is published as a Maven artifact). This avoids the fork entirely for parsing, though the analysis layer would still need custom implementation.

### 3.2 Function Parity Challenges

The staged approach (pushdown -> JVM fallback -> error) is a reasonable framework, but the scale of the challenge is larger than presented:

**Function inventory.** Trino has approximately 500+ built-in functions across categories: string, math, date/time, JSON, array, map, aggregate, window, conditional, URL, IP, UUID, color, geospatial, ML, and more. Each function has specific NULL-handling semantics, type coercion rules, and edge-case behaviors.

**Pushdown complexity.** Only a small fraction of Trino functions have direct OpenSearch equivalents:
  - Basic comparisons and boolean logic -> Query DSL
  - Simple aggregations (COUNT, SUM, AVG, MIN, MAX) -> OpenSearch aggregations
  - Some string functions -> OpenSearch scripting (Painless)

  Many Trino functions have no OpenSearch equivalent and cannot be pushed down:
  - Window functions (ROW_NUMBER, RANK, LAG/LEAD) have no aggregation equivalent
  - Array/map manipulation functions
  - Type conversion functions with specific precision rules
  - Regular expression functions with Trino-specific regex flavor

**JVM fallback correctness.** When a function cannot be pushed down, the data must be fetched and the function evaluated in the JVM. This changes the query's performance characteristics unpredictably. A query that looks simple (single table scan with filter) might require full table scan if the filter uses a non-pushdown function.

**Testing matrix.** For each function, correctness must be verified across:
  - Normal inputs, NULL inputs, edge cases (empty strings, boundary values)
  - Interaction with pushdown (does pushdown produce the same result as JVM evaluation?)
  - Type coercion combinations (what happens when argument types differ from declared signature?)

  This matrix grows multiplicatively and represents a large ongoing testing investment.

### 3.3 Pushdown Correctness

This is the most technically challenging aspect of the entire integration:

**NULL semantics mismatch.** Trino uses standard SQL three-valued logic (TRUE, FALSE, NULL). OpenSearch's query DSL uses a two-valued model (match/no-match) with separate `missing` handling. Specific mismatches:
  - `WHERE x != 5` in Trino excludes NULLs; in OpenSearch's `must_not` with `term`, missing-field documents may or may not match depending on configuration
  - `WHERE x IN (1, 2, NULL)` in Trino never matches NULL; the OpenSearch `terms` query has no NULL concept
  - `COALESCE`, `NULLIF`, and `IS [NOT] DISTINCT FROM` have specific three-valued-logic semantics that must be preserved in DSL translation

**Timezone handling.** Trino's `TIMESTAMP WITH TIME ZONE` carries explicit timezone information and performs timezone-aware comparisons. OpenSearch stores dates in UTC internally and applies timezone conversion at query time. Mismatches can occur when:
  - Date range filters cross DST boundaries
  - Aggregation buckets (date_histogram) use different timezone rules
  - Cast between timestamp types has different precision behavior

**Decimal precision.** Trino supports `DECIMAL(p, s)` with arbitrary precision (up to 38 digits). OpenSearch's `scaled_float` and `double` types have limited precision. Aggregation operations (SUM, AVG) on decimal types can produce different results due to precision loss in OpenSearch.

**String collation.** Trino string comparisons are binary by default but can be configured with collation. OpenSearch's keyword field comparisons depend on the analyzer chain and normalizer. Text fields use full-text matching semantics that are fundamentally different from SQL string comparison. The integration must clearly define which field types map to Trino VARCHAR and how comparison semantics are preserved.

**Recommendation:** Build a semantic conformance test suite from day one. Each pushdown rule should have corresponding tests that verify the pushed-down result matches the JVM-evaluated result for representative inputs including NULLs, edge cases, and type boundaries.

### 3.4 Cost Model Quality

The document correctly identifies this as a challenge but does not propose a solution:

**Required statistics for Trino CBO.** Trino's cost-based optimizer uses:
  - Table row count (total and per-partition)
  - Column statistics: distinct value count (NDV), NULL fraction, min/max values, histogram
  - Data size estimates
  - Correlation between columns (limited)

**Available OpenSearch statistics.** OpenSearch provides:
  - Shard-level document counts (`_cat/shards`)
  - Index-level document counts (`_cat/indices`)
  - Field data/doc_values usage statistics
  - Segment-level information (`_segments`)

  Critically missing: column cardinality, NULL fraction, value distribution histograms, and data size per column.

**Gap analysis.** Without column-level statistics, the cost model cannot make informed decisions about:
  - Join ordering (which table should be the build side of a hash join?)
  - Aggregation strategy (hash vs. sort aggregation based on group cardinality)
  - Filter selectivity (how much data does a predicate eliminate?)

**Approaches to close the gap:**
  - **ANALYZE command:** Implement a Trino-compatible `ANALYZE` command that samples data and computes statistics, stored in a metadata index.
  - **Runtime adaptive optimization:** Use runtime feedback from initial pages to adjust the execution plan (similar to adaptive query execution in Spark).
  - **Heuristic-based fallback:** Use simple heuristics when statistics are unavailable (e.g., assume uniform distribution, fixed selectivity factors).

### 3.5 Connector Approach Evaluation

The document dismisses the connector approach in one paragraph. This is premature:

**Connector improvement potential.** Several enhancements could significantly close the gap:

1. **Arrow-based data transfer.** Replace JSON serialization with Apache Arrow IPC format. This eliminates the serialization overhead that the document identifies as the primary connector limitation. Arrow-based Trino connectors exist for other systems and demonstrate 5-10x throughput improvements over JSON.

2. **Richer pushdown.** The current connector has "basic predicate/projection/limit pushdown." Adding aggregation pushdown (which OpenSearch's aggregation framework supports natively), TopN pushdown, and complex expression pushdown would push more work to OpenSearch.

3. **Partition-aware splits.** Instead of one split per shard, use OpenSearch's `_search_shards` API with routing to create splits that align with data locality, and use `slice` queries for intra-shard parallelism.

4. **Asynchronous pre-fetching.** Pipeline data retrieval so that the connector pre-fetches the next batch while Trino processes the current one, hiding network latency.

**Cost-benefit comparison.** An enhanced connector approach:
  - Pros: maintains clear separation of concerns, leverages upstream Trino improvements automatically, lower engineering risk, incremental deployment
  - Cons: still has network hop overhead (mitigated by Arrow), cannot access Lucene directly, limited to connector SPI capabilities

The document should include a quantitative comparison: what percentage of the target workload's performance gap does the enhanced connector close? If it's 70-80%, the remaining gap may not justify the risk and cost of native integration.

---

## 4. Production Operations and Risk Review

### 4.1 Operability and Observability Gaps

The document lists observability as challenge #11 but provides essentially no architectural detail. A production-grade system needs:

**Query lifecycle tracking.**
  - Query submission tracking with unique query IDs
  - Query state machine: QUEUED -> PLANNING -> RUNNING -> FINISHING -> COMPLETED/FAILED
  - Per-stage progress tracking (rows processed, bytes scanned, percentage complete)
  - Integration with `_plugins/_query/stats` or a similar stats API

**Operator-level metrics.**
  - Per-operator CPU time, wall clock time, rows input/output, bytes processed
  - Memory usage per operator (current, peak, revocable)
  - Spill statistics per operator (bytes spilled, spill events)
  - Blocked time (waiting for exchange data, waiting for memory)

**System-level integration.**
  - Expose Trino query metrics via OpenSearch's `_nodes/stats` API
  - Integration with OpenSearch's slow log facility for queries exceeding thresholds
  - JMX/MBeans for monitoring integration with external tools
  - Structured JSON logging for query events

**Explain/analyze output.** The document mentions "explain/analyze output" in passing. The design needs:
  - `EXPLAIN` that shows the logical and physical plan with pushdown annotations
  - `EXPLAIN ANALYZE` that shows actual runtime statistics after execution
  - Plan visualization compatible with existing Trino UI tools (or a new OpenSearch Dashboards plugin)

**Missing: query management APIs.**
  - List running queries
  - Cancel a running query
  - Set per-query resource limits (memory, timeout, CPU)
  - Query queueing and admission control

Without these operational capabilities, the system cannot be run in production. They should be explicitly scoped into Phase 1 or Phase 2, not deferred to Phase 5.

### 4.2 Fault Tolerance and Recovery

The document's treatment of fault tolerance is insufficient for production deployment:

**Trino's own fault tolerance model.** Trino historically has limited fault tolerance: if any task in a stage fails, the entire query fails. Recent versions (Trino 400+) introduced a "fault-tolerant execution" mode with task-level retries and intermediate data spooling, but this is:
  - Optional and not enabled by default
  - Adds significant complexity and overhead
  - Requires external storage for intermediate data

**Data node failure during query.** When a data node hosting shards fails mid-query:
  - If the node was running scan operators: Can retry on replica shards (if available)
  - If the node was running intermediate operators (join/aggregate): Must re-execute the stage or fail the query
  - If exchange buffers on the failed node are lost: Must re-produce upstream data

The design must specify which of these scenarios are handled and which cause query failure.

**Coordinator failure.** If the coordinating node fails:
  - All in-flight queries managed by that coordinator are lost
  - No mechanism for query state transfer to another node
  - Client must retry the query from scratch

This is acceptable for short queries but not for long-running analytical queries. The design should state this limitation explicitly and consider:
  - Query state checkpointing (heavy implementation, significant overhead)
  - Client-side retry with idempotent query semantics
  - Multiple coordinator model with shared state (extremely complex)

**Practical recommendation:** Adopt Trino's approach: fail-fast for task failures, client-side retry, with optional task-level retry for idempotent operations. Do not attempt full query recovery in early phases.

### 4.3 Engineering Effort Assessment

**Phase 1 (Parser/analyzer + single-node):**
  - Complexity: Medium-High
  - Key challenge: Getting the analysis layer right without Trino's full runtime context
  - Expertise needed: Trino internals (parser, type system), OpenSearch metadata model
  - Uncertainty: Medium (well-defined scope, but semantic edge cases are numerous)

**Phase 2 (Pushdown execution):**
  - Complexity: High
  - Key challenge: Pushdown correctness across the full expression/function/type space
  - Expertise needed: OpenSearch Query DSL, aggregation framework, Painless scripting
  - Uncertainty: High (the "long tail" of pushdown edge cases is hard to scope)

**Phase 3 (Distributed fragments + exchange):**
  - Complexity: Very High
  - Key challenge: Building a distributed execution runtime that coexists with OpenSearch
  - Expertise needed: Distributed systems, OpenSearch transport layer, resource management
  - Uncertainty: Very High (this is essentially building a new distributed query engine)

**Phase 4 (Joins/windows + spill):**
  - Complexity: Very High
  - Key challenge: Memory management, spill implementation, operator correctness
  - Expertise needed: Query engine internals, memory management, storage I/O
  - Uncertainty: High (spill-to-disk implementation is notoriously difficult to get right)

**Phase 5 (Production hardening):**
  - Complexity: High
  - Key challenge: Security audit, chaos testing, performance tuning across workload combinations
  - Expertise needed: Security, operations, performance engineering
  - Uncertainty: Medium (scope is defined by production requirements)

**Team composition.** This project requires a team with a rare combination of deep Trino engine expertise and deep OpenSearch internals knowledge. These skill sets rarely overlap. The project likely needs:
  - 2-3 engineers with Trino/query-engine experience
  - 2-3 engineers with OpenSearch internals experience
  - 1 distributed systems specialist
  - 1 performance/reliability engineer

### 4.4 Alternative Approaches

The document presents native embedding as the primary path without rigorously evaluating alternatives:

**Alternative 1: Enhanced Connector with Arrow Flight**
  - Approach: Improve the existing Trino-OpenSearch connector with Arrow-based data transfer, richer pushdown, and partition-aware splits
  - Trade-offs:
    - Pro: Low risk, incremental, leverages upstream Trino development
    - Pro: No JVM embedding, no OpenSearch runtime interference
    - Con: Network hop remains (though Arrow mitigates serialization cost)
    - Con: Limited to connector SPI capabilities
  - Best for: Workloads where 80% of execution can be pushed to OpenSearch

**Alternative 2: Sidecar Trino Process**
  - Approach: Deploy a lightweight Trino worker as a sidecar process on each OpenSearch node, communicating via local IPC (shared memory or Unix sockets)
  - Trade-offs:
    - Pro: Full Trino execution semantics without JVM conflict
    - Pro: Data locality for scan operations
    - Con: Operational complexity of managing a second process
    - Con: Data transfer between processes still has cost
  - Best for: Workloads requiring full Trino execution capabilities with data locality

**Alternative 3: Calcite-Based Hybrid**
  - Approach: Extend the existing Calcite-based SQL engine with Trino-compatible SQL syntax and function signatures
  - Trade-offs:
    - Pro: Leverages existing infrastructure (type mapping, pushdown, security)
    - Pro: No new execution engine to build
    - Con: Not true Trino compatibility (Calcite and Trino have different optimizer behaviors)
    - Con: May not satisfy users who need exact Trino SQL dialect
  - Best for: Users who want "Trino-like" SQL without strict Trino compatibility

**Alternative 4: Federated Engine (External Process)**
  - Approach: Run Trino as a standalone service alongside OpenSearch, with an optimized connector. OpenSearch provides a plugin endpoint that proxies to the external Trino service.
  - Trade-offs:
    - Pro: Clean separation, full Trino capabilities
    - Pro: Can federate across multiple data sources
    - Con: Additional infrastructure to deploy and manage
    - Con: No data locality benefit
  - Best for: Multi-source analytics workloads

### 4.5 Top 5 Project Risks

**Risk 1: Semantic drift from Trino upstream** (Likelihood: High, Impact: High)
  - The forked parser/analyzer will diverge from upstream Trino, creating incompatibilities that erode the value proposition of "Trino compatibility."
  - Mitigation: Use Trino's published parser artifact as a dependency; invest in a comprehensive conformance test suite; define explicit compatibility version targets.

**Risk 2: Unbounded pushdown scope** (Likelihood: High, Impact: Medium)
  - Pushdown translation is a long tail problem. Each new function, type combination, or edge case requires custom implementation and testing. The project could spend years on pushdown coverage without reaching acceptable parity.
  - Mitigation: Define a minimum viable pushdown set based on actual workload analysis; implement graceful fallback to JVM execution for unsupported expressions; publish a compatibility matrix.

**Risk 3: Resource isolation failure** (Likelihood: Medium, Impact: High)
  - Analytical queries consuming resources needed for search/indexing, degrading OpenSearch's primary function.
  - Mitigation: Dedicated thread pools with hard limits; query admission control; circuit breaker integration; ability to kill runaway queries; configurable resource limits per query.

**Risk 4: Engineering capacity exhaustion** (Likelihood: Medium-High, Impact: High)
  - The project requires rare expertise (Trino + OpenSearch + distributed systems) and spans 5 phases of increasing complexity. Key personnel departures could stall the project.
  - Mitigation: Thorough documentation of architectural decisions; modular design that allows independent progress; early investment in automated testing; consider alternative approaches that require less specialized expertise.

**Risk 5: Insufficient production readiness at GA** (Likelihood: Medium, Impact: High)
  - Deferring operability (observability, query management, security) to late phases means the system may be technically functional but not production-ready when leadership expects delivery.
  - Mitigation: Integrate observability and security from Phase 1; define production-readiness criteria upfront; run production-like workload testing continuously, not just in Phase 5.

---

## 5. Cross-Cutting Synthesis

### Key Themes Across All Reviews

1. **The document correctly identifies the problem space** but underestimates execution complexity in several areas: distributed runtime integration, pushdown correctness, security enforcement, and operational tooling.

2. **The phased strategy needs revision.** Security and observability should be promoted from Phase 5 to Phase 1. A Phase 0 (enhanced connector baseline) should be added. Phase 4 (joins/spill) deserves more risk attention as the hardest engineering phase.

3. **Alternative approaches deserve quantitative evaluation.** Before committing to native embedding, measure what an enhanced connector or Calcite hybrid approach can achieve. If either closes 70-80% of the performance gap with 20% of the risk, it may be the better investment.

4. **The JVM embedding model is the highest-risk architectural decision.** Dependency conflicts, GC interference, thread model mismatch, and resource isolation are all hard problems. A sidecar or separate-process model avoids most of these at the cost of data transfer overhead.

5. **Pushdown correctness is a long-tail problem** that requires sustained investment in testing infrastructure. Each semantic mismatch between Trino and OpenSearch (NULL handling, timezone, precision, collation) creates a potential correctness bug that is hard to find and expensive to fix.

### Recommendations

1. **Add a Phase 0** that benchmarks an enhanced connector (Arrow transport, richer pushdown) against the target workloads to establish a quantitative baseline.

2. **Move security and observability to Phase 1** as architectural foundations, not Phase 5 afterthoughts.

3. **Evaluate the Calcite bridge alternative** seriously. The existing codebase has substantial Calcite infrastructure that could be extended with Trino-compatible syntax at lower risk.

4. **If proceeding with native integration**, use Trino's published `trino-parser` Maven artifact rather than forking the parser. Build the analysis layer from scratch against OpenSearch metadata.

5. **Define explicit compatibility boundaries**: which Trino SQL features are in-scope, which are explicitly unsupported, and what the conformance testing strategy is.

6. **Invest in a semantic conformance test suite** from day one that validates pushdown correctness against JVM-evaluated baselines for all supported expressions.

7. **Design the resource isolation model before any code**: dedicated thread pools, circuit breaker integration, admission control, and query killing must be architectural foundations, not bolted on later.
