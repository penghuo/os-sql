# Transport Protocol Specification

**Status:** DRAFT
**Implements:** ADR Section 3 (Transport Protocol)

---

## 1. Overview

Distributed PPL uses OpenSearch's existing `TransportService` for node-to-node communication.
Three transport actions handle all distributed execution:

| Action | Wire Name | Purpose |
|--------|-----------|---------|
| `TransportShardExecutionAction` | `cluster:internal/ppl/shard/execute` | Execute plan fragment on data node |
| `TransportExchangeDataAction` | `cluster:internal/ppl/exchange/data` | Transfer exchange data between nodes |
| `TransportQueryCancelAction` | `cluster:internal/ppl/query/cancel` | Cancel distributed query on all nodes |

## 2. Action Registration

All actions registered in `SQLPlugin.getActions()`:

```java
// In SQLPlugin.java getActions() method:
actions.add(
    new ActionHandler<>(ShardExecutionAction.INSTANCE, TransportShardExecutionAction.class)
);
actions.add(
    new ActionHandler<>(ExchangeDataAction.INSTANCE, TransportExchangeDataAction.class)
);
actions.add(
    new ActionHandler<>(QueryCancelAction.INSTANCE, TransportQueryCancelAction.class)
);
```

## 3. Shard Execution Protocol

### 3.1 Purpose

The coordinator sends a plan fragment to a data node for execution on a specific shard.
The data node executes the fragment against its local shard and returns results.

### 3.2 Request

```java
public class ShardExecutionRequest extends TransportRequest {
    /**
     * Unique query ID (UUID). All fragments of the same query share this ID.
     * Used for cancellation, resource tracking, and correlation.
     */
    private String queryId;

    /**
     * Fragment ID within the query. Matches Fragment.fragmentId from planning.
     */
    private int fragmentId;

    /**
     * The physical plan to execute on this shard.
     * Serialized using SerializablePlan framework (StreamOutput/StreamInput).
     * Root operator is the fragment's root (e.g., PartialAggregation → Scan).
     */
    private byte[] serializedPlan;

    /**
     * Target index name (used for split resolution on the remote node).
     */
    private String indexName;

    /**
     * Target shard ID on this node.
     */
    private int shardId;

    /**
     * Query-level settings (timeout, memory limits, etc.)
     */
    private Map<String, String> settings;

    /**
     * Output schema: column names and types expected in the response.
     * Allows the receiver to validate output format.
     */
    private List<ColumnInfo> outputSchema;

    // StreamOutput/StreamInput serialization
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(queryId);
        out.writeVInt(fragmentId);
        out.writeByteArray(serializedPlan);
        out.writeString(indexName);
        out.writeVInt(shardId);
        out.writeMap(settings, StreamOutput::writeString, StreamOutput::writeString);
        out.writeList(outputSchema);
    }
}
```

### 3.3 Response

```java
public class ShardExecutionResponse extends TransportResponse {
    /**
     * Query and fragment IDs for correlation.
     */
    private String queryId;
    private int fragmentId;

    /**
     * Execution status.
     */
    private Status status;  // SUCCESS, PARTIAL, FAILED, CANCELLED

    /**
     * Result rows. For GATHER exchange, these are the final results.
     * For partial aggregation, these contain serialized partial states.
     */
    private List<ExprValue> results;

    /**
     * For streaming responses: whether more data is available.
     * If true, the coordinator should send another request to fetch more.
     */
    private boolean hasMore;

    /**
     * Total rows processed by this fragment (for metrics).
     */
    private long rowsProcessed;

    /**
     * Execution time on this shard (for performance monitoring).
     */
    private long executionTimeMs;

    /**
     * Error message if status is FAILED.
     */
    private String errorMessage;

    /**
     * Node ID that executed this fragment (for debugging).
     */
    private String executingNodeId;

    public enum Status {
        SUCCESS,    // Fragment completed, all results in this response
        PARTIAL,    // Fragment produced results but hasMore=true
        FAILED,     // Fragment failed, see errorMessage
        CANCELLED   // Fragment was cancelled
    }
}
```

### 3.4 Execution Flow

```
Coordinator                          Data Node (shard owner)
    |                                      |
    |  ShardExecutionRequest               |
    |  (queryId, fragmentId, plan, shard)  |
    |------------------------------------->|
    |                                      |  1. Deserialize plan fragment
    |                                      |  2. Bind shard split to plan
    |                                      |  3. Execute: open() → hasNext()/next() → close()
    |                                      |  4. Collect results into response
    |                                      |
    |  ShardExecutionResponse              |
    |  (results, status, hasMore)          |
    |<-------------------------------------|
    |                                      |
    |  [If hasMore=true, fetch more]       |
    |------------------------------------->|
    |<-------------------------------------|
```

### 3.5 Streaming Mode

For large result sets, the fragment executor uses streaming:

1. First response: up to `batch_size` rows (default 10,000) + `hasMore=true`
2. Coordinator sends follow-up request with same queryId/fragmentId
3. Data node continues from where it left off (cursor state kept in memory)
4. Repeat until `hasMore=false`

This prevents OOM on the data node for queries that produce many rows.

## 4. Exchange Data Protocol

### 4.1 Purpose

Transfers rows between fragments on different nodes. Used when data needs to be
repartitioned (HASH exchange) or gathered (GATHER exchange).

### 4.2 Request

```java
public class ExchangeDataRequest extends TransportRequest {
    /**
     * Query ID for correlation.
     */
    private String queryId;

    /**
     * Source fragment that produced the data.
     */
    private int sourceFragmentId;

    /**
     * Target fragment that will consume the data.
     */
    private int targetFragmentId;

    /**
     * For HASH exchange: which partition this data belongs to.
     * For GATHER: always 0.
     */
    private int partitionId;

    /**
     * The actual data rows being transferred.
     */
    private List<ExprValue> rows;

    /**
     * Whether this is the last batch from this source/partition.
     */
    private boolean isLastBatch;

    /**
     * Sequence number for ordering (in case of out-of-order delivery).
     */
    private long sequenceNumber;
}
```

### 4.3 Response

```java
public class ExchangeDataResponse extends TransportResponse {
    /**
     * Whether the target accepted the data.
     * False if the target's buffer is full (backpressure signal).
     */
    private boolean accepted;

    /**
     * Available buffer capacity on the target (bytes).
     * Producer can use this to adjust batch size.
     */
    private long availableBufferBytes;

    /**
     * Acknowledged sequence number.
     */
    private long acknowledgedSequence;
}
```

### 4.4 Exchange Flow: GATHER

```
Shard Node 1                  Coordinator
    |                              |
    | ExchangeDataRequest          |
    | (rows=[r1,r2,...], part=0)   |
    |---------------------------->|
    |                              | Buffer rows
    | ExchangeDataResponse         |
    | (accepted=true)              |
    |<----------------------------|
    |                              |
    | ExchangeDataRequest          |
    | (isLastBatch=true)           |
    |---------------------------->|
    |                              | Mark partition complete
    | ExchangeDataResponse         |
    |<----------------------------|

Shard Node 2
    | [same flow]
    |---------------------------->|
    |<----------------------------|
                                   All partitions complete → consumer unblocks
```

### 4.5 Exchange Flow: HASH

```
Shard Node 1                    Worker Node A (partition 0)
    |                                |
    | For each row:                  |
    |   partition = hash(key) % N    |
    |   route to target node         |
    |                                |
    | ExchangeDataRequest            |
    | (rows for partition 0)         |
    |------------------------------->|
    | ExchangeDataResponse           |
    |<-------------------------------|

                                Worker Node B (partition 1)
    | ExchangeDataRequest            |
    | (rows for partition 1)         |
    |------------------------------->|
    | ExchangeDataResponse           |
    |<-------------------------------|
```

### 4.6 Backpressure

When a consumer's buffer is full:

1. Producer sends `ExchangeDataRequest`
2. Consumer returns `accepted=false, availableBufferBytes=0`
3. Producer backs off (configurable delay, default 10ms)
4. Producer retries with exponential backoff (max 1s)
5. If consumer doesn't accept within timeout (default 30s), query fails

## 5. Query Cancel Protocol

### 5.1 Request

```java
public class QueryCancelRequest extends TransportRequest {
    private String queryId;
    private String reason;  // "user_cancelled", "timeout", "coordinator_error"
}
```

### 5.2 Response

```java
public class QueryCancelResponse extends TransportResponse {
    private boolean acknowledged;
    private int fragmentsCancelled;  // How many active fragments were stopped
}
```

### 5.3 Cancellation Flow

```
User cancels query (or timeout)
    |
Coordinator:
    1. Set query state to CANCELLED
    2. For each node with active fragments:
       Send QueryCancelRequest
    3. Close local exchange buffers
    4. Release circuit breaker memory

Each Data Node (on receiving cancel):
    1. Find active fragments for queryId
    2. Set cancellation flag on each fragment executor
    3. Fragment executor checks flag in operator loop:
       - hasNext() returns false immediately
       - close() called to free resources
    4. Drain and close exchange buffers for this query
    5. Return QueryCancelResponse(acknowledged=true, fragmentsCancelled=N)
```

## 6. Security

### 6.1 Action Permissions

All distributed PPL actions are `cluster:internal/*` — they are internal cluster actions
not directly invocable by users. They inherit the PPL query's security context.

### 6.2 Security Context Propagation

The coordinator propagates the original user's security context to shard execution:

```java
// In ShardExecutionRequest:
private ThreadContext.StoredContext securityContext;
```

This ensures index-level security, field-level security, and document-level security
are enforced on each shard execution, consistent with existing OpenSearch behavior.

### 6.3 Transport Security

When transport TLS is enabled (default in production), all distributed PPL actions
use the existing encrypted transport channel. No additional TLS configuration needed.

## 7. Timeouts

| Timeout | Default | Configuration | Behavior |
|---------|---------|---------------|----------|
| Shard execution | 60s | `plugins.ppl.distributed.shard.timeout` | Fragment execution cancelled, query fails |
| Exchange data | 30s | `plugins.ppl.distributed.exchange.timeout` | Backpressure timeout, query fails |
| Query overall | 120s | `plugins.ppl.distributed.query.timeout` | All fragments cancelled, query fails |
| Transport | 30s | OpenSearch default `transport.tcp.connect_timeout` | Node unreachable, query fails |

## 8. Metrics

Each transport action emits metrics for observability:

| Metric | Type | Description |
|--------|------|-------------|
| `ppl.distributed.shard_execution.count` | Counter | Total shard execution requests |
| `ppl.distributed.shard_execution.latency` | Histogram | Shard execution time (ms) |
| `ppl.distributed.shard_execution.failures` | Counter | Failed shard executions |
| `ppl.distributed.exchange.bytes` | Counter | Total bytes transferred via exchange |
| `ppl.distributed.exchange.rows` | Counter | Total rows transferred via exchange |
| `ppl.distributed.exchange.backpressure` | Counter | Backpressure events |
| `ppl.distributed.query.active` | Gauge | Currently active distributed queries |
| `ppl.distributed.query.cancelled` | Counter | Cancelled queries |

## 9. Wire Format Versioning

All request/response classes include a version field for forward compatibility:

```java
private static final int CURRENT_VERSION = 1;

@Override
public void writeTo(StreamOutput out) throws IOException {
    out.writeVInt(CURRENT_VERSION);
    // ... fields
}

public ShardExecutionRequest(StreamInput in) throws IOException {
    int version = in.readVInt();
    if (version > CURRENT_VERSION) {
        throw new IllegalStateException("Unknown version: " + version);
    }
    // ... read fields based on version
}
```

This allows rolling upgrades where some nodes run the old version and some run the new.
