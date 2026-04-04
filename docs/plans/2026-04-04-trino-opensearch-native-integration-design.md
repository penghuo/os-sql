# Trino-OpenSearch Native Integration Design Spec

**Date:** 2026-04-04
**Status:** Draft
**Author:** Peng Huo

## 1. Overview

### Problem

Running analytical SQL queries against external datasets (S3 data lakes, Iceberg tables) from OpenSearch today requires a separate Trino cluster. This adds operational overhead, deployment complexity, and latency from inter-cluster communication.

### Solution

Natively embed Trino's distributed SQL query engine inside OpenSearch as a plugin. Every OpenSearch data node becomes both a Trino coordinator and worker. No separate Trino cluster is needed.

### Key Design Decision

**Layered Embedding (Approach C):** Keep Trino's core engine (parser, optimizer, planner, operators) as a mostly-unmodified "kernel." Build an OpenSearch-native "shell" layer that implements Trino's own interfaces, delegating infrastructure concerns (node discovery, communication, memory, threading) to OpenSearch primitives. The kernel doesn't know it's running inside OpenSearch.

### Motivation

- **Eliminate operational overhead** of running a separate Trino cluster
- **Leverage OpenSearch's cluster topology** — data nodes become Trino workers automatically
- **Deep integration** with OpenSearch's node discovery, task management, thread pools, and circuit breakers

### Non-Goals

- Querying OpenSearch indices via Trino (out of scope)
- Replacing the existing Calcite or v2 SQL engine
- Running Trino as a separate process with an OpenSearch connector

## 2. Architecture

### High-Level

```
+-----------------------------------------------------+
|                   OpenSearch Node                    |
|                                                     |
|  +-----------------------------------------------+  |
|  |          opensearch-trino-plugin               |  |
|  |                                               |  |
|  |  +-----------------------------------------+  |  |
|  |  |         OpenSearch Shell                 |  |  |
|  |  |                                         |  |  |
|  |  |  - REST Endpoint (/_plugins/_trino_sql) |  |  |
|  |  |  - OpenSearchNodeManager                |  |  |
|  |  |  - TransportRemoteTask                  |  |  |
|  |  |  - OpenSearchTaskExecutor               |  |  |
|  |  |  - CircuitBreakerMemoryPool             |  |  |
|  |  |  - OpenSearch Thread Pool registration  |  |  |
|  |  |  - Trino /v1/statement protocol         |  |  |
|  |  +------------------+----------------------+  |  |
|  |                     | implements Trino         |  |
|  |                     | interfaces               |  |
|  |  +------------------v----------------------+  |  |
|  |  |         Trino Kernel                    |  |  |
|  |  |                                         |  |  |
|  |  |  - SQL Parser (trino-parser)            |  |  |
|  |  |  - Analyzer + Type System               |  |  |
|  |  |  - Cost-Based Optimizer                 |  |  |
|  |  |  - Distributed Planner (stages/splits)  |  |  |
|  |  |  - Physical Operators (scan, agg, join) |  |  |
|  |  |  - Iceberg Connector (Parquet reader)   |  |  |
|  |  |  - Airlift Bootstrap + Guice DI         |  |  |
|  |  +-----------------------------------------+  |  |
|  +-----------------------------------------------+  |
|                                                     |
|  OpenSearch Core (cluster state, transport, pools)   |
+-----------------------------------------------------+
```

### Symmetric Node Design

Every data node runs the full stack — both coordinator and worker. Any node can accept a query via the REST endpoint and coordinate its execution, while also serving as a worker for queries coordinated by other nodes.

Future: a `plugins.trino.node.coordinator_only` setting can disable worker duties on designated nodes for production workloads.

## 3. OpenSearch Cluster Integration

### 3.1 Node Discovery & Topology

`OpenSearchNodeManager implements InternalNodeManager`:

- Subscribes to `ClusterStateListener.clusterChanged()`
- Converts `DiscoveryNode` to `InternalNode` on every cluster state change
- Every data node maps to an active worker + coordinator
- Fires `Consumer<AllNodes>` listeners on topology changes
- Event-driven (not polling) — better than upstream Trino's 5-second HTTP polling

**Mapping:**

| OpenSearch | Trino |
|---|---|
| `DiscoveryNode.getId()` | `InternalNode.nodeIdentifier` |
| `DiscoveryNode.getAddress()` → `trino://{host}:{transportPort}` | `InternalNode.internalUri` |
| Data node | `coordinator=true, worker=true` |
| Cluster state version | Stale topology detection |

### 3.2 Thread Pool Registration

The plugin registers dedicated thread pools via `Plugin.getExecutorBuilders()`:

| Pool | Type | Size | Purpose |
|---|---|---|---|
| `trino_query` | Fixed | `availableProcessors * 2` | Split processing (Driver.process()) |
| `trino_exchange` | Scaling | 1 to `availableProcessors` | TransportAction data exchange |
| `trino_scheduler` | Fixed | 4 | Coordinator scheduling work |

`OpenSearchTaskExecutor implements TaskExecutor`:

- `enqueueSplits()` wraps each `SplitRunner` in `AbstractRunnable`, submits to `trino_query` pool
- `SplitRunner.processFor(Duration)` is cooperative — re-submits if not finished
- Retains Trino's `MultilevelSplitQueue` as a local scheduling layer (priority queue, no thread dependency)
- Per-task concurrency limits via semaphores

Thread pools are visible via `GET /_cat/thread_pool?v` — full observability alongside OpenSearch workload.

### 3.3 Memory Management

```
OpenSearch Heap
|-- OpenSearch Circuit Breakers (request, fielddata, etc.)
+-- Trino Circuit Breaker (registered by plugin)
    +-- CircuitBreakerMemoryPool extends MemoryPool
        reserve() -> circuitBreaker.addEstimateBytesAndMaybeBreak()
        free()    -> circuitBreaker.addWithoutBreaking(-bytes)
```

**Configuration:**

```yaml
plugins.trino.memory.heap_percent: 30    # % of JVM heap for Trino queries
plugins.trino.memory.per_query_limit: 1g # Max memory per query
```

The plugin registers a parent circuit breaker with `HierarchyCircuitBreakerService`. `CircuitBreakerMemoryPool` subclasses Trino's `MemoryPool` (concrete but not final) and delegates `reserve()`/`free()` to the breaker. Trino's per-query tracking (`QueryContext`) sits on top unchanged.

When the breaker trips, Trino's `LowMemoryKiller` interface kicks in, implemented to consult circuit breaker stats and kill the most expensive query.

### 3.4 Task Management

Trino queries register as OpenSearch Tasks via `TaskManager`:

- Query submitted → parent `Task` on coordinator node
- Each stage on each worker → child `Task`
- `POST /_tasks/{taskId}/_cancel` triggers Trino query cancellation
- Task descriptions show the SQL query text
- Progress reporting via `Task.getStatus()` (rows processed, bytes read, splits completed)

```
GET /_tasks?actions=trino:*

{
  "nodes": {
    "node1": {
      "tasks": {
        "node1:12345": {
          "action": "trino:query/execute",
          "description": "SELECT region, COUNT(*) FROM hits GROUP BY region",
          "running_time_in_nanos": 1520000000,
          "cancellable": true
        }
      }
    }
  }
}
```

### 3.5 Transport Actions (Inter-Node Communication)

Replaces Trino's HTTP-based coordinator-worker communication with OpenSearch `TransportAction`:

**Control plane (JSON serialized):**

| Action | Purpose |
|---|---|
| `trino:task/update` | Dispatch plan fragment + splits to worker |
| `trino:task/status` | Poll task status |
| `trino:task/cancel` | Cancel a running task |
| `trino:task/fail` | Fail a task with error |
| `trino:task/dynamicfilters` | Get dynamic filter domains |

**Data plane (TRINO_PAGES binary, passed through as raw bytes):**

| Action | Purpose |
|---|---|
| `trino:task/results` | Pull page batch from worker OutputBuffer |
| `trino:task/results/ack` | Acknowledge received pages (frees buffer) |
| `trino:task/results/destroy` | Close buffer when done |

`TransportRemoteTask implements RemoteTask`:

- `start()` → sends `trino:task/update` with plan fragment
- `addSplits()` → batched `trino:task/update` with new splits
- Status monitoring → periodic `trino:task/status` via `trino_scheduler` pool
- All requests use `ActionListener` callbacks (non-blocking)
- Retry logic ported from Trino's `RequestErrorTracker` / `Backoff`

Data exchange keeps TRINO_PAGES binary format as-is — wrapped in `BytesReference` payloads, no re-serialization.

## 4. Distributed Query Execution

### Stage-Based Execution Model

Trino breaks queries into stages connected by exchanges:

```
Example: SELECT region, COUNT(*) FROM hits GROUP BY region

Stage 0 (Final -- runs on coordinator)
  +-- Aggregate (final merge)
       +-- Exchange (gather from all workers)

Stage 1 (Partial -- runs on N workers in parallel)
  +-- Aggregate (partial -- per-split)
       +-- TableScan (reads Parquet splits)
```

### Plan Dispatch

1. Coordinator node receives query via REST endpoint
2. Parser → Planner → Optimizer produces distributed plan with stages
3. `SqlQueryScheduler` assigns stages to worker nodes
4. For each (stage, worker) pair, creates `TransportRemoteTask`
5. `TransportRemoteTask.start()` sends `trino:task/update` with plan fragment + splits
6. Worker's `TransportUpdateTaskAction` → `SqlTaskManager.updateTask()` → creates Drivers → `TaskExecutor.enqueueSplits()`

### Data Flow (Partial → Final Reduce)

Workers write results into OutputBuffers. Coordinator pulls data:

1. Worker Drivers run operators, produce Pages (columnar batches)
2. Pages enqueued into OutputBuffer
3. Coordinator's ExchangeOperator uses `TransportPageBufferClient`
4. Sends `trino:task/results` with `(taskId, bufferId, token)` to worker
5. Receives TRINO_PAGES binary
6. Feeds into final aggregation operators
7. Produces result rows streamed to client

Token-based flow control: each response includes `nextToken`, coordinator uses it for next request, ensuring exactly-once delivery.

### Multi-Stage Shuffles (JOINs)

For queries with shuffles, workers partition output by hash key:

```
Stage 0 (Final -- coordinator)
  +-- Output
       +-- Exchange <- Stage 1

Stage 1 (runs on workers)
  +-- HashJoin
       +-- Exchange <- Stage 2 (build side, partitioned)
       +-- Exchange <- Stage 3 (probe side, partitioned)

Stage 2 (workers, partitioned by join key)
  +-- ScanFilter on table A

Stage 3 (workers, partitioned by join key)
  +-- ScanFilter on table B
```

Workers in Stage 2/3 partition output into multiple OutputBuffers (one per downstream worker). Workers in Stage 1 pull from the appropriate buffer. All via the same TransportAction mechanism.

## 5. REST Endpoint & Wire Protocol

### Trino Wire-Compatible Protocol

The primary endpoint implements Trino's `/v1/statement` protocol exactly, enabling:

- **Trino JDBC driver compatibility** — `jdbc:trino://localhost:9200/_plugins/_trino_sql`
- **Trino test suite reuse** — `AbstractTestQueries`, `AbstractTestEngineOnlyQueries`, etc. run unmodified
- **ClickBench result comparison** — same output format as standalone Trino

**Endpoints:**

```
POST   /_plugins/_trino_sql/v1/statement                 -- Submit query
GET    /_plugins/_trino_sql/v1/statement/{queryId}/{token} -- Fetch next page
DELETE /_plugins/_trino_sql/v1/statement/{queryId}         -- Cancel query
```

**Response format (Trino-native):**

```json
{
  "id": "20260404_120000_00001_abc12",
  "columns": [
    {"name": "region", "type": "varchar", "typeSignature": {"rawType": "varchar"}},
    {"name": "_col1", "type": "bigint", "typeSignature": {"rawType": "bigint"}}
  ],
  "data": [
    ["us-east", 42],
    ["eu-west", 17]
  ],
  "stats": {
    "state": "FINISHED",
    "queued": false,
    "scheduled": true,
    "nodes": 1,
    "totalSplits": 130,
    "completedSplits": 130,
    "cpuTimeMillis": 320,
    "wallTimeMillis": 1200,
    "processedRows": 99997497,
    "processedBytes": 7894332416
  },
  "nextUri": "/_plugins/_trino_sql/v1/statement/20260404_120000_00001_abc12/2"
}
```

Pagination via `nextUri` — client polls until `nextUri` is absent (query complete). `columns` only in first response. `stats` updated with each response for progress tracking.

### Test Reuse Strategy

By implementing the `/v1/statement` protocol exactly:

- **Trino JDBC driver** connects to our endpoint as-is
- **`trino-testing` framework** (`AbstractTestQueries` — hundreds of SQL correctness tests) runs against our endpoint
- **`trino-product-tests`** (end-to-end via JDBC) validates correctness
- **Connector-specific tests** (`trino-connector-iceberg` tests) validate Parquet/Iceberg reading
- Tests point at `jdbc:trino://localhost:9200/_plugins/_trino_sql` instead of a Trino server — same assertions

## 6. Gradle Module Structure

### Layout

```
os-sql/
+-- trino/                          # Parent module
|   +-- build.gradle                # Common dependency versions, shading config
|   |
|   +-- trino-kernel/               # Vendored Trino core (minimal modifications)
|   |   +-- build.gradle
|   |   +-- src/main/java/
|   |       +-- io/trino/...        # Trino source, kept close to upstream
|   |
|   +-- trino-opensearch/           # Shell -- all OpenSearch adapter code
|   |   +-- build.gradle
|   |   +-- src/main/java/
|   |       +-- org/opensearch/sql/trino/
|   |           +-- plugin/         # TrinoPlugin, REST handlers
|   |           +-- node/           # OpenSearchNodeManager
|   |           +-- execution/      # OpenSearchTaskExecutor
|   |           +-- memory/         # CircuitBreakerMemoryPool
|   |           +-- transport/      # TransportRemoteTask, TransportActions
|   |           +-- protocol/       # Trino /v1/statement protocol handler
|   |
|   +-- trino-connector-iceberg/    # Vendored Iceberg connector
|   |   +-- build.gradle
|   |   +-- src/main/java/
|   |       +-- io/trino/plugin/iceberg/...
|   |
|   +-- trino-integ-test/           # Integration tests
|       +-- build.gradle
|       +-- src/test/java/
|           +-- org/opensearch/sql/trino/
|               +-- clickbench/     # ClickBench 43 queries
|               +-- correctness/    # Reused Trino AbstractTestQueries
|               +-- cluster/        # Multi-node topology tests
|
+-- plugin/                         # Existing SQL plugin (add trino-opensearch dependency)
+-- settings.gradle                 # Add trino/* modules
```

### Dependency Graph

```
trino-integ-test
  +-- trino-opensearch
  |     +-- trino-kernel
  |     |     +-- io.airlift:* (bootstrap, configuration, json, log, slice)
  |     |     +-- io.trino:trino-parser (unmodified)
  |     |     +-- io.trino:trino-spi (unmodified)
  |     |     +-- io.trino:trino-main (vendored, ~10 files modified)
  |     |     +-- com.google.inject:guice:7.0.0
  |     +-- trino-connector-iceberg
  |     |     +-- trino-kernel
  |     |     +-- org.apache.iceberg:iceberg-core
  |     |     +-- org.apache.parquet:parquet-hadoop
  |     |     +-- org.apache.hadoop:hadoop-common (shaded)
  |     +-- opensearch (core OpenSearch APIs -- provided scope)
  +-- io.trino:trino-testing
  +-- io.trino:trino-jdbc
  +-- opensearch-test-framework
```

### Dependency Conflict Strategy

**No action needed (same or near-same versions):**

- Jackson 2.21.2 (identical)
- Netty 4.2.12 (identical)
- SLF4J 2.0.17 (identical)
- Guava 33.x (near-identical)

**Shade:**

| Dependency | Reason | Target |
|---|---|---|
| `com.google.inject:guice:7.0.0` | OpenSearch vendored Guice | `o.o.sql.trino.shaded.guice` |
| `org.apache.hadoop:hadoop-common` | Large transitive tree | `o.o.sql.trino.shaded.hadoop` |

**Plugin classloader isolation** handles the rest — Airlift classes load inside the plugin's `URLClassLoader` and don't conflict with OpenSearch core.

### Trino Version

Pin to latest stable (currently Trino 468). Upgrade deliberately.

## 7. Trino Kernel Modifications

### Principle

Every line changed in vendored Trino code is a merge cost on upgrades. Minimize modifications.

### Changes by Category

**Removed (unnecessary for embedded use):**

- `Server.java` HTTP server startup
- `ui/` package (web UI)
- `server/security/` (OpenSearch handles auth)
- HTTP-specific config classes

**Rebound (Guice bindings swapped):**

`OpenSearchServerMainModule` replaces `ServerMainModule` bindings:

| Trino Interface | Shell Implementation |
|---|---|
| `InternalNodeManager` | `OpenSearchNodeManager` |
| `TaskExecutor` | `OpenSearchTaskExecutor` |
| `RemoteTaskFactory` | `TransportRemoteTaskFactory` |
| `MemoryPool` | `CircuitBreakerMemoryPool` |
| `LocationFactory` | `OpenSearchLocationFactory` |
| `FailureDetector` | `OpenSearchFailureDetector` |

**Preserved (zero modifications):**

- SQL parser and grammar
- Query analyzer and type system
- Cost-based optimizer and all optimizer rules
- Distributed planner
- All physical operators (hash join, aggregation, scan, sort, TopN, etc.)
- Connector SPI framework
- Stage scheduler and split assignment
- OutputBuffer implementations
- Per-query memory tracking (`QueryContext`)
- Bytecode expression compiler

### Modification Count

~10 files modified out of hundreds in trino-main. The parser, planner, optimizer, operators, type system, and connectors are completely untouched.

### Bootstrap Flow

```
// Original Trino:
Server.doStart()
  -> Airlift Bootstrap
    -> HttpServer.start()        <- REMOVED
    -> DiscoveryClient.start()   <- REMOVED
    -> Announcer.start()         <- REMOVED

// Our version:
TrinoEngineBootstrap.start(opensearchComponents)
  -> Airlift Bootstrap (kept for Guice + config + lifecycle)
    -> OpenSearchServerMainModule bindings
    -> Inject OpenSearch adapters
    -> Load Iceberg connector
  -> Returns TrinoEngine facade
    -> .executeQuery(sql, session)
    -> .cancelQuery(queryId)
    -> .getQueryInfo(queryId)
```

Airlift `Bootstrap` is kept for Guice initialization and lifecycle management — it's too deeply integrated to replace. HTTP server startup is skipped.

## 8. Iceberg Connector

### Why Iceberg

- Metadata layer provides partition pruning, column stats, min/max filtering
- Matches production S3 data lake deployments
- Trino's Iceberg connector is actively maintained
- Enables reuse of Trino's Iceberg connector test suite

### Configuration

```
connector.name=iceberg
iceberg.catalog.type=hadoop
iceberg.hadoop.catalog.warehouse=file:///data/clickbench
```

### Query Syntax

```sql
SELECT RegionID, COUNT(*) FROM hits GROUP BY RegionID ORDER BY COUNT(*) DESC LIMIT 10
```

No `file://` scheme — `hits` is a registered Iceberg table in the configured catalog.

## 9. ClickBench PoC

### Goal

Run all 43 ClickBench queries against a local Iceberg table (converted from `hits.parquet`) on a single OpenSearch node with embedded Trino engine.

### Data Setup

```bash
# Download ClickBench Parquet (14GB, 99,997,497 rows, 105 columns)
wget https://datasets.clickhouse.com/hits_compatible/hits.parquet

# Convert to Iceberg using Trino standalone or setup script
CREATE SCHEMA iceberg.clickbench
  WITH (location = 'file:///data/clickbench');

CREATE TABLE iceberg.clickbench.hits
  WITH (format = 'PARQUET')
  AS SELECT * FROM hive.default.hits;
```

### Single Node Execution

Even on a single node, parallelism comes from multiple Iceberg data file splits running concurrently across the `trino_query` thread pool (default: `2 * cores` threads).

```
Query submitted
  -> TrinoEngine.executeQuery(sql, session)
  -> Parser -> Planner -> Optimizer -> Distributed plan
  -> Single node: both stages assigned locally
  -> Stage 1: Iceberg manifest -> data file splits
     -> N splits run in parallel in trino_query pool
     -> Each: scan + partial aggregation -> OutputBuffer
  -> Stage 0: Exchange reads from local OutputBuffers
     -> Final aggregation -> result pages
  -> REST handler streams results via /v1/statement protocol
```

### ClickBench Query Coverage

| Category | Queries | Operators |
|---|---|---|
| Simple scans + COUNT | Q0-Q3 | TableScan, Count aggregation |
| Filtered scans | Q4-Q9 | Filter, various predicates |
| GROUP BY + aggregation | Q10-Q22 | HashAggregation (partial + final) |
| ORDER BY + LIMIT | Q23-Q28 | TopN, Sort |
| COUNT DISTINCT | Q29-Q32 | Approximate/exact distinct aggregation |
| Complex filters + GROUP BY | Q33-Q38 | Filter + HashAgg + TopN |
| Multi-column GROUP BY | Q39-Q42 | Wide aggregation, multiple agg functions |

All standard Trino operators — no custom operators needed.

### Success Criteria

1. **All 43 queries return correct results** — validated against ClickBench expected output
2. **Results match Trino standalone** — same query, same data, same output (Trino test framework assertions)
3. **Performance within 2x of standalone Trino** — same hardware, single node
4. **Observability works:**
   - `GET /_cat/thread_pool/trino_query?v` shows active/queued splits
   - `GET /_tasks?actions=trino:*` shows running queries
   - Circuit breaker stats include Trino memory usage
5. **Trino JDBC driver connects** — `jdbc:trino://localhost:9200/_plugins/_trino_sql`
6. **Trino test suite passes** — `AbstractTestQueries` subset runs against embedded engine

### PoC Exclusions (Deferred)

- Multi-node distributed execution
- S3 data source
- Catalog management / DDL via REST
- Authentication / security integration
- Query result caching
- Fault tolerance / retry on node failures

## 10. Deployment

Single plugin JAR installed via `opensearch-plugin install`. Bundles all vendored Trino components, Iceberg connector, and shaded dependencies.

```bash
# Build
./gradlew :trino:trino-opensearch:bundlePlugin

# Install
opensearch-plugin install file:///path/to/opensearch-trino-plugin.zip

# Configure (opensearch.yml)
plugins.trino.enabled: true
plugins.trino.memory.heap_percent: 30
plugins.trino.memory.per_query_limit: 1g
```

## 11. Configuration Reference

| Setting | Default | Description |
|---|---|---|
| `plugins.trino.enabled` | `false` | Enable/disable Trino engine |
| `plugins.trino.memory.heap_percent` | `30` | % of JVM heap for Trino queries |
| `plugins.trino.memory.per_query_limit` | `1g` | Max memory per query |
| `plugins.trino.threadpool.query_size` | `cores * 2` | trino_query thread pool size |
| `plugins.trino.threadpool.exchange_max` | `cores` | trino_exchange max pool size |
| `plugins.trino.threadpool.scheduler_size` | `4` | trino_scheduler pool size |
| `plugins.trino.node.coordinator_only` | `false` | Disable worker duties on this node |
| `plugins.trino.catalog.iceberg.warehouse` | (required) | Iceberg warehouse location |

## 12. Feasibility Evidence

Investigation of Trino's source code confirms the layered embedding approach is viable:

| Integration Point | Trino Abstraction | Pluggable? | Evidence |
|---|---|---|---|
| Node Discovery | `InternalNodeManager` (interface) | Yes | 3 existing implementations, `NodeInventory` also pluggable |
| Task Execution | `TaskExecutor` (interface, 6 methods) | Yes | 2 existing implementations, Guice-bound |
| Remote Communication | `RemoteTask` (interface, ~20 methods) | Yes | `HttpRemoteTask` is sole impl, clean boundary |
| Memory Pool | `MemoryPool` (concrete, not final) | Subclassable | ~300 lines, `reserve()`/`free()` are the integration points |
| Memory Kill Policy | `LowMemoryKiller` (interface) | Yes | 4 existing implementations |
| Spill to Disk | `SpillerFactory` (interface) | Yes | Clean SPI |
| Dependency Conflicts | Jackson, Netty, Guava, SLF4J | Low risk | Versions nearly identical between Trino 468 and OpenSearch 3.x |

No prior art exists for embedding Trino in another JVM process. This is a first-of-its-kind integration.

## 13. Build Commands

```bash
# Build just the Trino plugin
./gradlew :trino:trino-opensearch:build

# Run ClickBench tests
./gradlew :trino:trino-integ-test:integTest --tests "*ClickBenchIT"

# Run Trino compatibility tests
./gradlew :trino:trino-integ-test:integTest --tests "*TrinoCompatibilityIT"

# Build the installable plugin ZIP
./gradlew :trino:trino-opensearch:bundlePlugin
```
