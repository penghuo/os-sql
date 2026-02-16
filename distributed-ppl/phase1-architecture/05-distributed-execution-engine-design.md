# Distributed Execution Engine Design

**Status:** DRAFT
**Implements:** ADR Sections 5, 6, 8 (Shard Splits, Routing, Query Lifecycle)

---

## 1. Overview

The `DistributedExecutionEngine` orchestrates the execution of a distributed query.
It receives a `Fragment` tree from the `FragmentPlanner` and:

1. Resolves shard splits for SOURCE fragments
2. Dispatches SOURCE fragments to data nodes via transport
3. Manages exchange data flow between fragments
4. Executes the root SINGLE fragment locally on the coordinator
5. Collects results into a `QueryResponse`

## 2. Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Coordinator Node                                            │
│                                                              │
│  ┌────────────────────┐                                      │
│  │ PPLService          │  → decides single-node vs distributed│
│  └────────┬───────────┘                                      │
│           ↓                                                  │
│  ┌────────────────────┐     ┌──────────────────┐             │
│  │ FragmentPlanner     │────→│ ShardSplitManager │             │
│  │ (RelNode → Fragments)│     │ (ClusterState)    │             │
│  └────────┬───────────┘     └──────────────────┘             │
│           ↓                                                  │
│  ┌────────────────────────────────────────────────┐          │
│  │ DistributedExecutionEngine                      │          │
│  │                                                 │          │
│  │  ┌─────────────┐  ┌──────────────┐             │          │
│  │  │ Dispatcher   │  │ ExchangeService│             │          │
│  │  │ (transport)  │  │ (buffers)     │             │          │
│  │  └──────┬──────┘  └──────┬───────┘             │          │
│  │         │                 │                     │          │
│  │  ┌──────┴─────────────────┴──────┐             │          │
│  │  │ FragmentExecutor (local)       │             │          │
│  │  │ Executes SINGLE fragment       │             │          │
│  │  │ (final agg, merge sort, etc.)  │             │          │
│  │  └────────────────────────────────┘             │          │
│  └────────────────────────────────────────────────┘          │
│                                                              │
│        Transport                                             │
│  ───────┬──────────────────────────────────────────          │
│         ↓                                                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ Data Node 1   │  │ Data Node 2   │  │ Data Node 3   │     │
│  │ Shard 0,1     │  │ Shard 2,3     │  │ Shard 4       │     │
│  │               │  │               │  │               │     │
│  │ FragmentExec  │  │ FragmentExec  │  │ FragmentExec  │     │
│  │ (SOURCE frag) │  │ (SOURCE frag) │  │ (SOURCE frag) │     │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
└──────────────────────────────────────────────────────────────┘
```

## 3. DistributedExecutionEngine

### 3.1 Class Definition

```java
/**
 * Executes distributed PPL queries by orchestrating fragment execution
 * across OpenSearch data nodes.
 *
 * Implements the same ExecutionEngine interface as OpenSearchExecutionEngine,
 * allowing transparent routing between single-node and distributed paths.
 */
public class DistributedExecutionEngine implements ExecutionEngine {

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ShardSplitManager splitManager;
    private final FragmentPlanner fragmentPlanner;
    private final ExchangeService exchangeService;
    private final CircuitBreakerService circuitBreakerService;
    private final DistributedQuerySettings settings;

    /** Active distributed queries for lifecycle management */
    private final ConcurrentMap<String, DistributedQueryContext> activeQueries;

    @Override
    public void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener) {
        // This method is called when the query has been planned by FragmentPlanner
        // and the plan is a DistributedPhysicalPlan wrapping a Fragment tree.
        String queryId = UUID.randomUUID().toString();
        DistributedQueryContext queryContext = new DistributedQueryContext(
            queryId, plan, listener, settings);
        activeQueries.put(queryId, queryContext);

        try {
            executeDistributed(queryContext);
        } catch (Exception e) {
            queryContext.fail(e);
            activeQueries.remove(queryId);
        }
    }
}
```

### 3.2 Execution Flow

```java
private void executeDistributed(DistributedQueryContext ctx) {
    Fragment rootFragment = ctx.getRootFragment();

    // Step 1: Resolve shard splits
    Map<String, List<ShardSplit>> splits = resolveSplits(rootFragment);

    // Step 2: Dispatch SOURCE fragments to data nodes
    dispatchSourceFragments(ctx, rootFragment, splits);

    // Step 3: Execute root (SINGLE) fragment locally
    // This blocks until exchange data arrives from SOURCE fragments
    executeLocalFragment(ctx, rootFragment);
}
```

### 3.3 Split Resolution

```java
private Map<String, List<ShardSplit>> resolveSplits(Fragment root) {
    Map<String, List<ShardSplit>> result = new HashMap<>();
    // Walk fragment tree, find all SOURCE fragments
    collectSourceTables(root).forEach(tableName -> {
        List<ShardSplit> splits = splitManager.getSplits(tableName);
        result.put(tableName, splits);
    });
    return result;
}
```

### 3.4 Fragment Dispatch

```java
private void dispatchSourceFragments(DistributedQueryContext ctx,
                                      Fragment root,
                                      Map<String, List<ShardSplit>> splits) {
    List<Fragment> sourceFragments = collectSourceFragments(root);

    for (Fragment source : sourceFragments) {
        for (ShardSplit split : splits.get(source.getTableName())) {
            if (split.isLocal()) {
                // Execute locally — skip transport
                ctx.getLocalExecutor().submit(() ->
                    executeLocalShard(ctx, source, split));
            } else {
                // Dispatch to remote node via transport
                dispatchToNode(ctx, source, split);
            }
        }
    }
}

private void dispatchToNode(DistributedQueryContext ctx,
                             Fragment fragment,
                             ShardSplit split) {
    ShardExecutionRequest request = new ShardExecutionRequest(
        ctx.getQueryId(),
        fragment.getFragmentId(),
        serializePlan(fragment.getRoot()),
        split.getIndexName(),
        split.getShardId(),
        ctx.getSettings()
    );

    DiscoveryNode targetNode = clusterService.state().nodes()
        .get(split.getPreferredNodeId());

    transportService.sendRequest(
        targetNode,
        ShardExecutionAction.NAME,
        request,
        new TransportResponseHandler<ShardExecutionResponse>() {
            @Override
            public void handleResponse(ShardExecutionResponse response) {
                ctx.onShardComplete(fragment.getFragmentId(),
                    split.getShardId(), response);
            }

            @Override
            public void handleException(TransportException e) {
                ctx.onShardFailed(fragment.getFragmentId(),
                    split.getShardId(), e);
            }
        }
    );
}
```

### 3.5 Local Fragment Execution

```java
private void executeLocalFragment(DistributedQueryContext ctx,
                                   Fragment rootFragment) {
    // Root fragment runs on coordinator.
    // Its child is typically an exchange operator that pulls from
    // the SOURCE fragment results.
    PhysicalPlan rootPlan = rootFragment.getRoot();
    List<ExprValue> results = new ArrayList<>();

    try {
        rootPlan.open();
        while (rootPlan.hasNext()) {
            ctx.checkCancelled();
            results.add(rootPlan.next());
        }
        rootPlan.close();

        ctx.complete(results);
    } catch (Exception e) {
        rootPlan.close();
        ctx.fail(e);
    } finally {
        activeQueries.remove(ctx.getQueryId());
    }
}
```

## 4. DistributedQueryContext

```java
/**
 * Tracks the lifecycle of a single distributed query.
 */
public class DistributedQueryContext {
    private final String queryId;
    private final Fragment rootFragment;
    private final ResponseListener<QueryResponse> listener;
    private final DistributedQuerySettings settings;

    /** Query state machine */
    private final AtomicReference<QueryState> state;

    /** Track shard execution completion */
    private final ConcurrentMap<String, ShardExecutionResponse> shardResults;
    private final AtomicInteger pendingShards;

    /** Cancellation token shared with all operators */
    private final AtomicBoolean cancelled;

    /** Timeout future */
    private final ScheduledFuture<?> timeoutFuture;

    public void onShardComplete(int fragmentId, int shardId,
                                 ShardExecutionResponse response) {
        if (response.getStatus() == Status.FAILED) {
            fail(new ShardExecutionException(response.getErrorMessage()));
            return;
        }
        shardResults.put(fragmentId + ":" + shardId, response);

        // Feed results into exchange buffer for the parent fragment
        exchangeService.feedResults(queryId, fragmentId, response.getResults(),
            response.isHasMore());

        if (!response.isHasMore() && pendingShards.decrementAndGet() == 0) {
            // All shards complete — exchange buffers will close
            exchangeService.markAllSourcesComplete(queryId);
        }
    }

    public void onShardFailed(int fragmentId, int shardId, Exception e) {
        // Any shard failure fails the entire query (v1 — no partial results)
        fail(e);
    }

    public void complete(List<ExprValue> results) {
        if (state.compareAndSet(QueryState.EXECUTING, QueryState.COMPLETE)) {
            timeoutFuture.cancel(false);
            listener.onResponse(buildQueryResponse(results));
        }
    }

    public void fail(Exception e) {
        if (state.compareAndSet(QueryState.EXECUTING, QueryState.FAILED)) {
            timeoutFuture.cancel(false);
            cancelAllFragments();
            listener.onFailure(e);
        }
    }

    public void cancel(String reason) {
        cancelled.set(true);
        if (state.compareAndSet(QueryState.EXECUTING, QueryState.CANCELLED)) {
            timeoutFuture.cancel(false);
            cancelAllFragments();
            listener.onFailure(new QueryCancelledException(queryId, reason));
        }
    }

    private void cancelAllFragments() {
        // Send cancel to all nodes with active fragments
        Set<String> activeNodes = getActiveNodeIds();
        for (String nodeId : activeNodes) {
            transportService.sendRequest(
                clusterService.state().nodes().get(nodeId),
                QueryCancelAction.NAME,
                new QueryCancelRequest(queryId, "query_failed"),
                TransportResponseHandler.empty()
            );
        }
    }
}

public enum QueryState {
    PLANNING,
    DISPATCHING,
    EXECUTING,
    COMPLETE,
    FAILED,
    CANCELLED
}
```

## 5. ExchangeService

```java
/**
 * Manages exchange buffers for all active distributed queries.
 * Each exchange (fragment boundary) has a buffer on the consumer node.
 */
public class ExchangeService {

    /** queryId → fragmentId → ExchangeBuffer */
    private final ConcurrentMap<String, ConcurrentMap<Integer, ExchangeBuffer>> buffers;

    /**
     * Create a consumer buffer for a specific exchange.
     * Called by DistributedExchangeOperator.open().
     */
    public ExchangeBuffer createConsumerBuffer(String queryId, int fragmentId,
                                                ExchangeSpec spec) {
        ExchangeBuffer buffer = new ExchangeBuffer(
            spec.getBufferSizeBytes(),
            spec.getExpectedSources()
        );
        buffers.computeIfAbsent(queryId, k -> new ConcurrentHashMap<>())
            .put(fragmentId, buffer);
        return buffer;
    }

    /**
     * Feed results from a shard execution into the appropriate exchange buffer.
     * Called by DistributedQueryContext.onShardComplete().
     */
    public void feedResults(String queryId, int fragmentId,
                            List<ExprValue> rows, boolean hasMore) {
        ExchangeBuffer buffer = getBuffer(queryId, fragmentId);
        if (buffer != null) {
            buffer.offer(rows);
            if (!hasMore) {
                buffer.markSourceComplete(fragmentId);
            }
        }
    }

    /**
     * Mark all sources complete for a query's exchanges.
     * Unblocks any waiting consumers.
     */
    public void markAllSourcesComplete(String queryId) {
        ConcurrentMap<Integer, ExchangeBuffer> queryBuffers = buffers.get(queryId);
        if (queryBuffers != null) {
            queryBuffers.values().forEach(ExchangeBuffer::markAllComplete);
        }
    }

    /**
     * Clean up all buffers for a completed/failed/cancelled query.
     */
    public void cleanup(String queryId) {
        ConcurrentMap<Integer, ExchangeBuffer> queryBuffers = buffers.remove(queryId);
        if (queryBuffers != null) {
            queryBuffers.values().forEach(ExchangeBuffer::close);
        }
    }
}
```

## 6. FragmentExecutor (Data Node Side)

```java
/**
 * Executes a plan fragment on a data node.
 * Invoked by TransportShardExecutionAction when a shard execution request arrives.
 */
public class FragmentExecutor {

    private final CircuitBreakerService circuitBreakerService;

    /**
     * Execute a fragment on the specified shard.
     * Called on the data node's transport thread pool.
     */
    public ShardExecutionResponse execute(ShardExecutionRequest request) {
        PhysicalPlan plan = deserializePlan(request.getSerializedPlan());
        ShardSplit split = new ShardSplit(
            request.getIndexName(), request.getShardId(),
            clusterService.localNode().getId(), true);

        // Bind the split to the plan (propagates to leaf operators)
        plan.add(split);

        DistributedOperatorContext context = new DistributedOperatorContext(
            request.getQueryId(),
            request.getFragmentId(),
            circuitBreakerService.getBreaker(CircuitBreaker.REQUEST),
            new AtomicBoolean(false),
            transportService,
            DistributedQuerySettings.from(request.getSettings())
        );

        List<ExprValue> results = new ArrayList<>();
        long rowsProcessed = 0;
        long startTime = System.nanoTime();

        try {
            plan.open();
            int batchLimit = context.getSettings().getShardBatchSize();
            while (plan.hasNext() && results.size() < batchLimit) {
                results.add(plan.next());
                rowsProcessed++;
            }

            boolean hasMore = plan.hasNext();
            if (!hasMore) {
                plan.close();
            }

            return new ShardExecutionResponse(
                request.getQueryId(),
                request.getFragmentId(),
                ShardExecutionResponse.Status.SUCCESS,
                results,
                hasMore,
                rowsProcessed,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime),
                null,
                clusterService.localNode().getId()
            );
        } catch (Exception e) {
            plan.close();
            return new ShardExecutionResponse(
                request.getQueryId(),
                request.getFragmentId(),
                ShardExecutionResponse.Status.FAILED,
                Collections.emptyList(),
                false,
                rowsProcessed,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime),
                e.getMessage(),
                clusterService.localNode().getId()
            );
        }
    }
}
```

## 7. PPLService Routing Logic

### 7.1 Decision Point

The routing decision happens in `PPLService` (or a new `QueryRouter`):

```java
public class QueryRouter {

    private final DistributedQuerySettings settings;
    private final OpenSearchExecutionEngine singleNodeEngine;
    private final DistributedExecutionEngine distributedEngine;
    private final FragmentPlanner fragmentPlanner;
    private final ShardSplitManager splitManager;

    public ExecutionEngine route(PhysicalPlan plan, RelNode relNode) {
        if (!settings.isDistributedEnabled()) {
            return singleNodeEngine;
        }

        // Check if query benefits from distribution
        if (!fragmentPlanner.requiresExchange(relNode)) {
            return singleNodeEngine;  // Pure filter/eval/project — single-node is fine
        }

        // Check index size
        String indexName = extractIndexName(relNode);
        long docCount = splitManager.getDocCount(indexName);
        if (docCount < settings.getThresholdDocs()) {
            return singleNodeEngine;  // Small index — overhead not worth it
        }

        // Check for unsupported commands
        if (containsUnsupportedCommands(relNode)) {
            return singleNodeEngine;  // Fallback
        }

        return distributedEngine;
    }
}
```

### 7.2 Integration with PPLService

```java
// In PPLService.execute():
// After: RelNode relNode = calciteRelNodeVisitor.visit(ast);
// Before: executionEngine.execute(plan, listener);

ExecutionEngine engine = queryRouter.route(plan, relNode);
if (engine instanceof DistributedExecutionEngine) {
    // Fragment plan and execute distributed
    Optional<Fragment> fragments = fragmentPlanner.plan(relNode, splitManager.getSplits(indexName));
    if (fragments.isPresent()) {
        DistributedPhysicalPlan distPlan = new DistributedPhysicalPlan(fragments.get());
        engine.execute(distPlan, listener);
    } else {
        // Fragment planner decided single-node is better
        singleNodeEngine.execute(plan, listener);
    }
} else {
    engine.execute(plan, listener);
}
```

## 8. Configuration

```yaml
# Master switch
plugins.ppl.distributed.enabled: false

# Routing
plugins.ppl.distributed.auto_route: true
plugins.ppl.distributed.threshold_docs: 100000

# Timeouts
plugins.ppl.distributed.query.timeout: 120s
plugins.ppl.distributed.shard.timeout: 60s
plugins.ppl.distributed.exchange.timeout: 30s

# Memory
plugins.ppl.distributed.join.memory_limit: 100mb
plugins.ppl.distributed.exchange.buffer_size: 10mb
plugins.ppl.distributed.agg.partial_memory_limit: 50mb
plugins.ppl.distributed.sort.memory_limit: 50mb

# Exchange
plugins.ppl.distributed.exchange.batch_size: 10000
plugins.ppl.distributed.exchange.backpressure_delay: 10ms

# Join optimization
plugins.ppl.distributed.join.broadcast_threshold: 10mb

# Shard execution
plugins.ppl.distributed.shard.batch_size: 50000
plugins.ppl.distributed.shard.max_concurrent: 50
```

## 9. Explain Support

Distributed queries should be explainable via the existing `_explain` API:

```json
POST /_plugins/_ppl/_explain
{
  "query": "source=idx | stats count() by status"
}

// Response when distributed:
{
  "execution_mode": "distributed",
  "fragments": [
    {
      "fragment_id": 0,
      "type": "SINGLE",
      "node": "coordinator",
      "operators": ["FinalAggregation(count, group_by=status)", "GatherExchange"]
    },
    {
      "fragment_id": 1,
      "type": "SOURCE",
      "nodes": ["node1(shards=0,1)", "node2(shards=2,3)", "node3(shard=4)"],
      "operators": ["PartialAggregation(count, group_by=status)", "IndexScan(idx)"]
    }
  ],
  "estimated_splits": 5,
  "routing_reason": "index_size=2000000 > threshold=100000, has_aggregation=true"
}
```

## 10. Metrics and Observability

```java
public class DistributedQueryMetrics {
    // Query-level
    Counter queriesTotal;           // Total distributed queries
    Counter queriesSuccess;
    Counter queriesFailed;
    Counter queriesCancelled;
    Gauge queriesActive;            // Currently executing
    Histogram queryLatency;         // End-to-end latency

    // Fragment-level
    Counter fragmentsDispatched;
    Counter fragmentsFailed;
    Histogram fragmentLatency;

    // Exchange-level
    Counter exchangeRowsTransferred;
    Counter exchangeBytesTransferred;
    Counter exchangeBackpressureEvents;

    // Routing
    Counter routedDistributed;      // Queries routed to distributed
    Counter routedSingleNode;       // Queries kept on single-node
    Counter routedFallback;         // Queries that fell back from distributed
}
```

## 11. Thread Pool Usage

```
Coordinator:
  - Fragment planning: caller thread (PPL transport handler)
  - Fragment dispatch: OpenSearch generic thread pool
  - Exchange buffer management: dedicated thread (or generic pool)
  - Root fragment execution: OpenSearch search thread pool

Data Nodes:
  - Fragment execution: OpenSearch search thread pool
  - Exchange data sending: OpenSearch generic thread pool
```

No new thread pools. Use existing OpenSearch pools with appropriate names
for thread context propagation.

## 12. Testing Strategy

### Unit Tests
- QueryRouter: correct routing decisions for various query patterns
- DistributedQueryContext: state transitions, cancellation, timeout
- ExchangeService: buffer creation, data feeding, cleanup
- FragmentExecutor: correct execution with mock plans

### Integration Tests
- End-to-end: PPL query → distributed execution → correct results
- Multi-shard: verify correct results across 5+ shards
- Cancellation: cancel mid-execution, verify cleanup
- Timeout: verify timeout triggers query failure
- Error handling: shard failure → query failure → cleanup

### Stress Tests
- 50 concurrent distributed queries
- Very large result sets (10M+ rows with exchange)
- Memory pressure: hash join near circuit breaker limit
