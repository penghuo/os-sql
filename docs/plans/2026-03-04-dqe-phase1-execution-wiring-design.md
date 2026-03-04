# DQE Phase 1 Execution Wiring

Wire the existing operator, exchange, and metadata components into the orchestrator so queries return real data.

## Current State

All building blocks are implemented and tested individually:
- Operators: ShardScanOperator, FilterOperator, ProjectOperator, SortOperator, LimitOperator, TopNOperator
- Driver/Pipeline execution model
- PitManager, SearchRequestBuilder
- GatherExchangeSource/Sink, ExchangeBuffer, StageScheduler
- StageExecutionHandler with callback mechanism
- Transport actions (stage/execute, stage/cancel, exchange/push, exchange/close, exchange/abort)
- DqeMetadata (table, column, shard split resolution)
- SearchHitToPageConverter

The orchestrator currently stubs steps 4-6, returning `data=[]` and `rowsProcessed=0`.

## Execution Path

Full distributed exchange, even on single-node clusters.

### Coordinator (DqeQueryOrchestrator steps 4-6)

1. `PitManager.createPit(indexName, keepAlive)` → PitHandle
2. `DqeMetadata.getSplits(clusterState, table, localNodeId)` → List<DqeShardSplit>
3. Build PlanFragment (query text + column descriptors + pipeline decision metadata)
4. Serialize PlanFragment
5. Create ExchangeBuffer + GatherExchangeSource (expectedProducerCount = shard count)
6. Register ExchangePushHandler on coordinator to receive pages into ExchangeBuffer
7. `StageScheduler.scheduleStage()` → dispatch to data nodes
8. Build coordinator pipeline: ExchangeSourceOperator → Sort/TopN/Limit per PipelineDecision
9. `Driver.process()` loop → collect pages into List<List<Object>>
10. Build DqeQueryResponse with real schema + data + stats
11. Cleanup: release PIT, deregister query, release memory

### Data Node (ShardStageExecutor — StageExecutionCallback impl)

1. Receive DqeStageExecuteRequest
2. Deserialize PlanFragment
3. Re-parse + re-analyze the SQL query locally (avoids serializing Trino AST)
4. Per shard split assigned to this node:
   a. Create PIT for shard
   b. Build SearchRequestBuilder (required columns, pushdown query, shard ID)
   c. Build ShardScanOperator
   d. Wrap with FilterOperator (residual predicates, if any)
   e. Wrap with ProjectOperator (output expressions)
5. If multiple shards on this node: merge shard pipelines (concatenate output)
6. Wrap output in GatherExchangeSink → TransportChunkSender → coordinator
7. Drive pipeline via Driver, pushing pages through sink
8. Signal completion via exchange close

## New Classes

| Class | Module | Purpose |
|---|---|---|
| PlanFragment | dqe-execution | Serializable plan: query text, column descriptors, pipeline decision, keep-alive |
| ShardStageExecutor | dqe-plugin | StageExecutionCallback: builds shard pipelines, drives, sends via exchange |
| ExchangeSourceOperator | dqe-execution | Wraps GatherExchangeSource into Operator interface |
| TransportChunkSender | dqe-exchange | ChunkSender impl over TransportService |
| ExchangePushHandler | dqe-exchange | Receives exchange push requests, adds pages to ExchangeBuffer |
| CoordinatorPipelineBuilder | dqe-plugin | Builds coordinator pipeline from PipelineDecision + exchange source |

## Modified Classes

| Class | Change |
|---|---|
| DqeQueryOrchestrator | Replace step 4-6 TODOs with real execution |
| DqeEnginePlugin | Register transport handlers, create ShardStageExecutor, wire exchange push handler |

## PlanFragment Serialization

Send the SQL query text in the PlanFragment. Data nodes re-parse and re-analyze locally. This avoids serializing Trino AST expression trees. Re-parse overhead is negligible for Phase 1 workloads.

PlanFragment fields:
- `queryText` (String): original SQL
- `queryId` (String): for tracking
- `columnDescriptors` (List<ColumnDescriptor>): for SearchHitToPageConverter
- `pipelineStrategy` (PipelineDecision.PipelineStrategy): SCAN_ONLY, LIMIT_ONLY, SORT_ONLY, TOP_N
- `pitKeepAlive` (TimeValue): for PIT creation
- `batchSize` (int): scan batch size

## Coordinator Pipeline Strategies

Per PipelineDecision from analyzer:

| Strategy | Coordinator Pipeline |
|---|---|
| SCAN_ONLY | ExchangeSourceOperator only |
| LIMIT_ONLY | ExchangeSourceOperator → LimitOperator |
| SORT_ONLY | ExchangeSourceOperator → SortOperator |
| TOP_N | ExchangeSourceOperator → TopNOperator |

## Page-to-Row Conversion

Driver collects Pages from the coordinator pipeline. Each Page is columnar (Trino Block format). Convert to List<List<Object>> for DqeQueryResponse:
- Read each Block column-by-column, row-by-row
- Convert Trino types to JSON-serializable types (VARCHAR→String, BIGINT→Long, etc.)
- NULL handling: preserve nulls

## Error Handling

- PIT creation failure → DqeException(EXECUTION_ERROR)
- Shard not available → DqeException(SHARD_NOT_AVAILABLE)
- Stage scheduling failure (all nodes reject) → DqeException(EXECUTION_ERROR)
- Data node pipeline failure → abort exchange, propagate error to coordinator
- Coordinator pipeline failure → cancel query, release all resources
- Memory limit exceeded → circuit breaker trips, DqeException(MEMORY_LIMIT_EXCEEDED)

## Implementation Order

1. PlanFragment + serialization
2. ExchangeSourceOperator (adapter)
3. TransportChunkSender + ExchangePushHandler
4. ShardStageExecutor (data node pipeline builder + driver)
5. CoordinatorPipelineBuilder
6. DqeQueryOrchestrator steps 4-6
7. DqeEnginePlugin wiring (transport handlers, callbacks)
8. Page-to-row conversion in orchestrator
9. Integration test verification
