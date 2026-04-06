# Path B: Replace DistributedQueryRunner with TestingTrinoServer

**Date:** 2026-04-06
**Status:** Design
**Goal:** Eliminate `DistributedQueryRunner`, fix catalog sharing across nodes, run 43/43 ClickBench on a 3-node cluster with split-level distribution.

## Problem

`DistributedQueryRunner` is a Trino test utility (`io.trino.testing`) that simulates a multi-node Trino cluster inside a single JVM. We use it with `nodeCount=1` as a bootstrap mechanism, then patch it via reflection for cross-node distribution. This causes:

1. **Spill NPE** — `FileSingleStreamSpillerFactory` walks call stack via `StackCallerProtectionDomainChainExtractor`; `CodeSource.getLocation()` returns null in OpenSearch's plugin classloader.
2. **Catalog isolation** — each node creates its own `DistributedQueryRunner` with independent catalogs and temp directories. The Hive catalog's temp warehouse on node-0 doesn't exist on node-1/2.
3. **Reflection fragility** — `TransportDistributionPatcher` hacks `RemoteTaskFactory` and `NodeSelectorFactory` fields by name. Any Trino upgrade breaks this.
4. **Coordinator node ID mismatch** — `DistributedQueryRunner` assigns a UUID to the coordinator that differs from the OpenSearch `DiscoveryNode` ID, requiring an alias registration that gets wiped by `clusterChanged()`.
5. **Redundant HTTP servers** — each node runs an embedded Trino HTTP server (Airlift/Jetty) on a random port that's only needed for intra-engine data exchange.

## Solution

Replace `DistributedQueryRunner` with `TestingTrinoServer` directly. This eliminates the fake multi-node layer while keeping the same Trino engine internals.

### What Changes

| Component | Before (omni) | After (omni-v2) |
|-----------|---------------|-----------------|
| Bootstrap | `DistributedQueryRunner.builder().setNodeCount(1).build()` | `TestingTrinoServer.builder().setProperties(...).build()` |
| Query execution | `queryRunner.execute(session, sql)` | `new TestingTrinoClient(server, session).execute(session, sql).getResult()` |
| Session props | `queryRunner.getSessionPropertyManager()` | `server.getSessionPropertyManager()` |
| SqlTaskManager | `queryRunner.getCoordinator().getTaskManager()` | `server.getTaskManager()` |
| Guice access | `queryRunner.getCoordinator().getInstance(key)` | `server.getInstance(key)` |
| Coordinator URL | `queryRunner.getCoordinator().getBaseUrl()` | `server.getBaseUrl()` |
| Distribution patch | `TransportDistributionPatcher.patch(queryRunner, ...)` | `TransportDistributionPatcher.patch(server, ...)` |
| Hive warehouse | Temp dir per node (broken) | Configured path from `trino.iceberg.warehouse` system property |
| Spill | `spill-enabled=true` (crashes) | `spill-enabled=false` (classloader incompatible) |
| Close | `queryRunner.close()` | `client.close(); server.close()` |

### What Stays the Same

- Transport actions (task/update, task/results, etc.) — unchanged
- `TransportRemoteTask` — unchanged
- `TransportRemoteTaskFactory` — unchanged (signature change: `TestingTrinoServer` instead of `DistributedQueryRunner`)
- `OpenSearchSqlTaskManager` — unchanged
- `OpenSearchNodeManager` — unchanged (keep coordinator alias fix)
- `TrinoJsonCodec` — unchanged
- `RestTrinoQueryAction` — unchanged
- Shadow jar configuration — unchanged (still shades `trino-testing` for `TestingTrinoServer`)
- All transport action handlers — unchanged

### Catalog Sharing Fix

**Root cause:** Each node creates its own Hive catalog with `Files.createTempDirectory("trino-hive-warehouse")`. When the coordinator sends a plan fragment referencing the Hive catalog to a remote node, the remote node's Hive catalog points to a different temp directory.

**Fix:** Use the same configured path for Hive warehouse as for Iceberg warehouse:
```java
// Before: each node gets a unique temp dir
Path hiveWarehouse = Files.createTempDirectory("trino-hive-warehouse");

// After: all nodes share the same configured path
Path hiveWarehouse = warehouseDir; // same as Iceberg warehouse dir
```

This works because the Hive external table's `external_location` points to the actual data directory (e.g., `file:///path/to/data`), not the metastore catalog dir. The metastore dir just stores table metadata.

### Files Modified

| File | Change |
|------|--------|
| `TrinoEngine.java` | Replace `DistributedQueryRunner` with `TestingTrinoServer` + `TestingTrinoClient`. Fix Hive warehouse path. Fix memory config. |
| `TransportDistributionPatcher.java` | Change parameter type from `DistributedQueryRunner` to `TestingTrinoServer`. Remove `runner.getCoordinator()` indirection. |
| `TrinoServiceHolder.java` | Update `initializeWithEngine()` to use `server.getInstance()` instead of `queryRunner.getCoordinator().getInstance()`. Add `getServer()` method. |
| `TransportTrinoTaskUpdateAction.java` | Update session creation to use `server.getSessionPropertyManager()` instead of `queryRunner`. |

### Verification

1. Build plugin, install in 3-node cluster
2. `SELECT 1` passes on all nodes
3. `system.runtime.nodes` shows node count (may still show 1 — that's Trino-internal, not our topology)
4. Node stats: workers show `tasksReceived > 0` after GROUP BY query
5. Hive catalog accessible from all nodes (no "No catalog 'hive'" error)
6. ClickBench 43/43 on Hive Parquet table with split-level distribution
