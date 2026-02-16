# Architecture Decision Record: Native Distributed PPL Execution

**Status:** DRAFT
**Date:** 2026-02-15
**Author:** distributed-ppl project lead

---

## 1. Context

OpenSearch PPL currently executes entirely on a single coordinating node. The pipeline:

```
PPL → ANTLR Parser → AST → Analyzer → CalciteRelNodeVisitor → RelNode
  → Planner → PhysicalPlan → OpenSearchExecutionEngine (single-node)
```

This bottlenecks on the coordinator for joins, large aggregations, and sorts on big datasets.
The goal is native distributed execution inside OpenSearch's JVM — no external processes.

## 2. Decision: Fragment-Based Execution Model

### 2.1 Core Concept

A distributed query is decomposed into **fragments** — subtrees of the Calcite RelNode plan
that execute together on one or more nodes. Fragments communicate via **exchanges**.

```
PPL text
  → ANTLR Parser → AST                          (existing, unchanged)
  → Analyzer                                     (existing, unchanged)
  → CalciteRelNodeVisitor → RelNode tree         (existing, unchanged)
  → FragmentPlanner                              (NEW)
      Splits RelNode at exchange boundaries
      Assigns fragment types (SOURCE, HASH, SINGLE)
  → DistributedPhysicalPlan                      (NEW)
      Fragment tree with distributed operators
  → DistributedExecutionEngine                   (NEW)
      Dispatches SOURCE fragments to shard nodes via transport
      Executes SINGLE fragment on coordinator
      Manages exchange data flow between fragments
  → Results → PPL response format                (existing, unchanged)
```

### 2.2 Fragment Types

| Type | Execution Location | Multiplicity | Purpose |
|------|--------------------|-------------|---------|
| SOURCE | On shard-owning data nodes | 1 per shard | Scan data, apply filters, partial aggs |
| HASH | On worker nodes (hash-partitioned) | N (configurable) | Intermediate ops: repartitioned join, repartitioned agg |
| SINGLE | On coordinator only | 1 | Final aggregation, merge sort, result output |

### 2.3 Fragment Boundary Rules

Exchange boundaries (where fragments split) are inserted at these RelNode patterns:

| RelNode Pattern | Exchange Type | Fragment Below | Fragment Above |
|----------------|---------------|----------------|----------------|
| `LogicalAggregate` | GATHER or HASH | SOURCE (partial agg) | SINGLE (final agg) |
| `LogicalSort` with LIMIT | GATHER (ordered) | SOURCE (local sort + limit) | SINGLE (merge sort) |
| `LogicalJoin` | HASH (both sides by join key) | SOURCE (scan each side) | HASH (hash join) |
| `LogicalSort` without LIMIT | GATHER (ordered) | SOURCE (local sort) | SINGLE (merge sort) |
| Simple filter/eval/project | No exchange (stays in SOURCE) | — | — |

### 2.4 Decision Rationale

**Why fragments (vs. operator-level distribution)?**
- Fragment-level granularity maps naturally to shard-level execution
- Minimizes transport actions (one per fragment, not one per operator)
- Matches Trino's proven pattern — battle-tested at scale
- Clean serialization boundary: each fragment is a self-contained PhysicalPlan

**Why not distributed Calcite (e.g., Apache Calcite's Enumerable)?**
- Calcite's built-in distribution uses JDBC/ODBC — wrong transport for OpenSearch
- We already have CalciteRelNodeVisitor translating to RelNode — fragmenting RelNode is natural
- Custom fragment planner gives full control over shard-level optimization

## 3. Decision: Transport Protocol

### 3.1 Transport Actions

All distributed PPL transport actions use OpenSearch's existing `TransportService` framework.
Actions are registered in `SQLPlugin.getActions()`.

| Action Name | Class | Purpose |
|-------------|-------|---------|
| `cluster:internal/ppl/shard/execute` | `TransportShardExecutionAction` | Execute a plan fragment on a remote data node |
| `cluster:internal/ppl/exchange/data` | `TransportExchangeDataAction` | Transfer exchange data between nodes |
| `cluster:internal/ppl/query/cancel` | `TransportQueryCancelAction` | Cancel all fragments of a distributed query |

### 3.2 Why 3 Actions (Not 5)

The reference design listed 5 transport actions. We consolidate to 3:

- **Shard execution + status** merged: The shard execution response includes status.
  No separate status polling — use streaming responses instead.
- **Exchange write + pull** merged into single bidirectional exchange action.
  The coordinator orchestrates push/pull direction based on exchange type.

### 3.3 Serialization

All requests/responses use OpenSearch's `StreamOutput`/`StreamInput` wire protocol.

```java
// Shard Execution Request
public class ShardExecutionRequest extends TransportRequest {
    String queryId;           // Unique distributed query ID
    int fragmentId;           // Which fragment to execute
    byte[] serializedPlan;    // Fragment's PhysicalPlan (serialized)
    String indexName;         // Target index
    int shardId;              // Target shard
    Map<String, String> settings;  // Query-level settings
}

// Shard Execution Response
public class ShardExecutionResponse extends TransportResponse {
    String queryId;
    int fragmentId;
    List<ExprValue> results;         // For GATHER exchange: final results
    byte[] partialAggregationState;  // For partial aggregation
    boolean hasMore;                 // For streaming large results
    String errorMessage;             // null on success
}

// Exchange Data Request
public class ExchangeDataRequest extends TransportRequest {
    String queryId;
    int sourceFragmentId;
    int targetFragmentId;
    int partitionId;          // For HASH exchange
    int batchSize;            // Max rows to transfer
}

// Exchange Data Response
public class ExchangeDataResponse extends TransportResponse {
    List<ExprValue> rows;
    boolean hasMore;
    long totalRowsTransferred;
}
```

### 3.4 PhysicalPlan Serialization

Plan fragments must be serializable for transport. The existing codebase already has
`SerializablePlan` (used by `OpenSearchIndexScan` for cursor pagination). We extend this:

- Each distributed operator implements `SerializablePlan`
- Serialization uses `StreamOutput`/`StreamInput` (existing OpenSearch pattern)
- Child operators are serialized recursively
- `OpenSearchIndexScan` already serializes — no change needed for leaf scans

## 4. Decision: Operator Interface Design

### 4.1 All Operators Extend PhysicalPlan

Every distributed operator extends `PhysicalPlan` and implements `Iterator<ExprValue>`.
This maintains compatibility with the existing execution model.

```java
// Base contract (existing, unchanged)
public abstract class PhysicalPlan extends PlanNode<PhysicalPlan>
    implements Iterator<ExprValue>, AutoCloseable {

    public abstract <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context);
    public void add(Split split) { getChild().forEach(c -> c.add(split)); }
}
```

### 4.2 New Distributed Operators

| Operator | Extends | Iterator Semantics |
|----------|---------|-------------------|
| `PartialAggregationOperator` | `PhysicalPlan` | Reads child, emits `(group_key, partial_state)` tuples |
| `FinalAggregationOperator` | `PhysicalPlan` | Reads partial states from exchange, emits `(group_key, result)` |
| `DistributedExchangeOperator` | `PhysicalPlan` | On consumer: pulls from transport buffer. On producer: writes to transport buffer |
| `DistributedHashJoinOperator` | `PhysicalPlan` | Build phase: accumulates build side. Probe phase: streams probe side, emits matches |
| `DistributedSortOperator` | `PhysicalPlan` | K-way merge sort across shard results using priority queue |

### 4.3 Operator Lifecycle

```
1. open()       — initialize resources (buffers, hash tables, transport connections)
2. hasNext()    — check if more results available
3. next()       — return next ExprValue
4. close()      — release resources, cancel pending transport, clean buffers
```

All operators must:
- Integrate with OpenSearch's circuit breaker (memory accounting)
- Support query cancellation (check cancelled flag in hasNext())
- Clean up on exception (close() called even on error)

### 4.4 PhysicalPlanNodeVisitor Extensions

```java
// Add to existing PhysicalPlanNodeVisitor:
public R visitPartialAggregation(PartialAggregationOperator node, C context) {
    return visitNode(node, context);
}
public R visitFinalAggregation(FinalAggregationOperator node, C context) {
    return visitNode(node, context);
}
public R visitDistributedExchange(DistributedExchangeOperator node, C context) {
    return visitNode(node, context);
}
public R visitDistributedHashJoin(DistributedHashJoinOperator node, C context) {
    return visitNode(node, context);
}
public R visitDistributedSort(DistributedSortOperator node, C context) {
    return visitNode(node, context);
}
```

## 5. Decision: Shard Split Management

### 5.1 ShardSplit

```java
public class ShardSplit implements Split {
    private final String indexName;
    private final int shardId;
    private final String preferredNodeId;  // Node owning the primary shard
    private final List<String> replicaNodeIds;  // Fallback nodes
    private final boolean isLocal;  // On coordinator?

    @Override
    public String getSplitId() {
        return indexName + ":" + shardId;
    }
}
```

### 5.2 ShardSplitManager

Resolves Calcite table references to concrete shard splits using `ClusterState`:

```java
public class ShardSplitManager {
    private final ClusterService clusterService;

    public List<ShardSplit> getSplits(String indexName) {
        ClusterState state = clusterService.state();
        IndexRoutingTable routing = state.routingTable().index(indexName);
        // For each shard: find primary → preferred node
        // Return List<ShardSplit> with node affinity
    }
}
```

**Performance budget:** <2ms (in-memory ClusterState lookup, no network).

### 5.3 Split Assignment Strategy

1. **Primary preference:** Assign fragment to node owning the primary shard
2. **Replica fallback:** If primary node is overloaded, use replica node
3. **Local optimization:** If shard is on coordinator, skip transport (execute locally)
4. **Multi-index joins:** For cross-index queries, co-locate by hash of join key

## 6. Decision: Command Routing Strategy

### 6.1 Feature Flag

```yaml
plugins.ppl.distributed.enabled: false        # Master switch (default OFF)
plugins.ppl.distributed.auto_route: true       # Auto-detect when to distribute
plugins.ppl.distributed.threshold_docs: 100000 # Min docs for distributed path
```

### 6.2 Routing Decision Tree

```
1. Is distributed mode enabled? No → single-node path (existing)
2. Does the query contain only shard-local operations? Yes → single-node path
3. Is the index < threshold_docs? Yes → single-node path
4. Does the query contain unsupported commands? Yes → single-node path (fallback)
5. Otherwise → distributed path
```

### 6.3 Shard-Local vs Distributed Operations

| Operation Type | Shard-Local | Needs Exchange |
|---------------|-------------|----------------|
| where (filter) | Yes | No |
| eval (expression) | Yes | No |
| fields (projection) | Yes | No |
| rename | Yes | No |
| fillnull | Yes | No |
| stats (aggregation) | Partial: Yes | Final: Yes (GATHER) |
| sort + limit | Local sort: Yes | Merge sort: Yes (GATHER ordered) |
| join | No | Yes (HASH both sides) |
| dedup | Partial: possible | Final: Yes (HASH by dedup key) |
| top/rare | Partial: Yes | Final: Yes (GATHER) |

### 6.4 Fallback Commands (Phase 1 — single-node only)

These commands are NOT routed to distributed path initially:
- `expand`, `flatten` — complex unnesting
- `parse`, `grok`, `rex`, `patterns` — regex operations (shard-local candidate for future)
- `fieldsummary` — multi-aggregation
- `append`, `appendcol` — subsearch variants
- `correlation` — time-series correlation
- ML commands (`kmeans`, `AD`) — permanently out of scope

## 7. Decision: Memory Management

### 7.1 Circuit Breaker Integration

All distributed operators register with OpenSearch's `CircuitBreakerService`:

```java
// In DistributedHashJoinOperator
circuitBreaker.addEstimateBytesAndMaybeBreak(hashTableSizeBytes, "ppl_hash_join");
// On close:
circuitBreaker.addWithoutBreaking(-hashTableSizeBytes);
```

### 7.2 Memory Budgets

| Component | Default Limit | Configuration |
|-----------|--------------|---------------|
| Hash join build table | 100MB per query | `plugins.ppl.distributed.join.memory_limit` |
| Exchange buffer | 10MB per exchange | `plugins.ppl.distributed.exchange.buffer_size` |
| Partial agg state | 50MB per shard | `plugins.ppl.distributed.agg.partial_memory_limit` |
| Sort buffer | 50MB per query | `plugins.ppl.distributed.sort.memory_limit` |

### 7.3 Backpressure

Exchange operators implement backpressure:
- Producer blocks when remote buffer is full (buffer at capacity)
- Consumer signals available capacity via transport response
- Configurable batch size controls granularity

## 8. Decision: Query Lifecycle

### 8.1 Query ID

Each distributed query gets a unique `queryId` (UUID). All fragments, exchanges, and
transport messages reference this ID for:
- Correlation across nodes
- Cancellation propagation
- Resource cleanup

### 8.2 Lifecycle States

```
PLANNING → DISPATCHING → EXECUTING → GATHERING → COMPLETE
                                                → FAILED
                                                → CANCELLED
```

### 8.3 Cancellation

Cancellation propagates via `TransportQueryCancelAction`:
1. Coordinator sends cancel to all nodes with active fragments
2. Each node's fragment executor checks cancellation flag in operator loop
3. Exchange buffers are drained and closed
4. Resources freed (circuit breaker released, buffers deallocated)

### 8.4 Error Handling

- Fragment failure → coordinator receives error in shard execution response
- Transport failure → timeout + retry (configurable, default 1 retry)
- Partial failure → entire query fails (no partial results in v1)
- Error includes: node ID, fragment ID, exception message, stack trace

## 9. File Layout

### New Files

```
core/src/main/java/org/opensearch/sql/planner/distributed/
├── FragmentPlanner.java          # Splits RelNode into fragments
├── Fragment.java                 # Fragment data structure
├── FragmentType.java             # SOURCE, HASH, SINGLE enum
├── DistributedPhysicalPlan.java  # Fragment tree with operators
├── ShardSplit.java               # Shard-level split implementation
├── ShardSplitManager.java        # ClusterState → ShardSplit resolution
└── QueryLifecycle.java           # Query state machine

core/src/main/java/org/opensearch/sql/planner/physical/distributed/
├── PartialAggregationOperator.java
├── FinalAggregationOperator.java
├── DistributedExchangeOperator.java
├── DistributedHashJoinOperator.java
├── DistributedSortOperator.java
└── ExchangeBuffer.java           # In-memory buffer with backpressure

opensearch/src/main/java/org/opensearch/sql/opensearch/executor/distributed/
├── DistributedExecutionEngine.java   # Orchestrates fragment execution
├── FragmentExecutor.java             # Executes a single fragment on a node
└── ExchangeService.java             # Manages exchange data flow

plugin/src/main/java/org/opensearch/sql/plugin/transport/distributed/
├── TransportShardExecutionAction.java
├── ShardExecutionRequest.java
├── ShardExecutionResponse.java
├── TransportExchangeDataAction.java
├── ExchangeDataRequest.java
├── ExchangeDataResponse.java
├── TransportQueryCancelAction.java
├── QueryCancelRequest.java
└── QueryCancelResponse.java
```

### Modified Files

```
plugin/src/main/java/org/opensearch/sql/plugin/SQLPlugin.java
  → Register new transport actions in getActions()

core/src/main/java/org/opensearch/sql/planner/physical/PhysicalPlanNodeVisitor.java
  → Add visit methods for distributed operators

ppl/src/main/java/org/opensearch/sql/ppl/PPLService.java
  → Add routing logic: distributed vs single-node

core/src/main/java/org/opensearch/sql/executor/ExecutionEngine.java
  → Add distributed execution method (or use existing with context)
```

## 10. Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| PhysicalPlan serialization complexity | Blocks fragment dispatch | Start with simple operators; leverage existing SerializablePlan |
| Memory pressure from hash joins | OOM on large joins | Circuit breaker + configurable limits + spill-to-disk (v2) |
| Transport overhead for small queries | Performance regression | Hard gate: skip distributed for <100K docs |
| Partial aggregation accuracy | Incorrect results | Extensive correctness testing vs single-node (Phase 5) |
| Fragment planner edge cases | Wrong plan for complex queries | Fallback to single-node for unsupported patterns |
| Cluster topology changes during query | Fragment execution failure | Retry failed fragments; cancel + re-plan if topology change is major |

## 11. Success Criteria

Phase 1 is complete when:
1. This ADR is reviewed and approved
2. Fragment model design is detailed enough for implementation
3. Transport protocol is specified with request/response schemas
4. Operator interfaces are defined with lifecycle contracts
5. Command routing strategy covers all Tier 1 commands
6. All Phase 2 engineers can start implementation without design ambiguity
