# 3-Node ClickBench Iceberg Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pass all 43/43 ClickBench queries on a 3-node OpenSearch cluster with Trino split-level distribution.

**Architecture:** Each OpenSearch node runs an embedded Trino coordinator (nodeCount=1). The coordinator's NodeScheduler is patched via reflection to see all 3 nodes. TransportRemoteTaskFactory dispatches PlanFragments to remote nodes via OS transport. Data exchange between stages uses each node's embedded Trino HTTP server. The previous 3-node attempt failed 0/43 due to OOM at 10g/node. This plan increases heap, enables spill-to-disk, and tunes memory/concurrency.

**Tech Stack:** Java 21, Trino 440, OpenSearch 3.6, Iceberg, Parquet

**Current Results:**
| Config | Result |
|--------|--------|
| 1-node, 32g | 43/43, 283s |
| 3-node, 10g/node | 0/43 (OOM crash on Q1) |

---

## File Structure

| File | Responsibility | Tasks |
|------|---------------|-------|
| `trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/bootstrap/TrinoEngine.java` | Engine memory config, spill, concurrency, exchange compression | 1, 2, 3, 4 |
| `trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/transport/TransportRemoteTask.java` | Remote task dispatch, status polling thread pool | 5 |
| `trino/benchmark/run_clickbench_iceberg_multinode.sh` | Benchmark script: heap, JVM flags, spill dir | 6 |

---

### Task 1: Fix memory config for multi-node

**Why:** `query.max-memory` (cluster-wide limit) is set equal to `query.max-memory-per-node`. When the coordinator sees 3 nodes, Trino's memory accounting thinks the entire cluster budget equals one node's budget, starving remote nodes. Also, 70% of heap for queries leaves insufficient room for exchange buffers and Parquet readers in distributed mode.

**Files:**
- Modify: `trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/bootstrap/TrinoEngine.java:80-107`

- [x] **Step 1: Update memory allocation ratios and add cluster-wide max-memory**

Change lines 80-107 from:

```java
      // Configure Trino memory adaptively based on available heap.
      // Use 70% of heap for queries, 15% for headroom, rest for OS overhead.
      long maxHeap = Runtime.getRuntime().maxMemory();
      long queryMem = (long) (maxHeap * 0.70);
      long headroom = (long) (maxHeap * 0.15);
      String queryMemStr = (queryMem / (1024 * 1024)) + "MB";
      String headroomStr = (headroom / (1024 * 1024)) + "MB";
      LOG.info("Trino memory config: heap={}MB, queryMem={}, headroom={}",
          maxHeap / (1024 * 1024), queryMemStr, headroomStr);

      // Scale task concurrency to available processors for analytical workloads.
      int processors = Runtime.getRuntime().availableProcessors();
      int taskConcurrency = Math.max(4, processors);
      LOG.info("Trino parallelism config: processors={}, taskConcurrency={}",
          processors, taskConcurrency);

      // Use single coordinator node. Split-level distribution across OpenSearch cluster
      // nodes is handled by TransportRemoteTaskFactory + OpenSearchNodeManager, which are
      // patched into the coordinator via TransportDistributionPatcher after build.
      // Hash partitioning within the single node still provides parallelism via task_concurrency.
      int nodeCount = 1;
      LOG.info("Trino node count: {} (single coordinator, transport distribution)", nodeCount);
      DistributedQueryRunner runner =
          DistributedQueryRunner.builder(session)
              .setNodeCount(nodeCount)
              .addExtraProperty("query.max-memory-per-node", queryMemStr)
              .addExtraProperty("query.max-memory", queryMemStr)
              .addExtraProperty("memory.heap-headroom-per-node", headroomStr)
```

To:

```java
      // Configure Trino memory adaptively based on available heap.
      // Use 60% of heap for queries (reduced from 70% to leave room for exchange
      // buffers and Parquet readers in distributed mode), 15% for headroom.
      long maxHeap = Runtime.getRuntime().maxMemory();
      long queryMemPerNode = (long) (maxHeap * 0.60);
      long headroom = (long) (maxHeap * 0.15);
      // Cluster-wide limit: 10x per-node to avoid starving remote nodes.
      // The per-node limit is the real constraint; this just prevents the global
      // cap from blocking allocation when multiple nodes contribute to one query.
      long queryMemCluster = queryMemPerNode * 10;
      String perNodeMemStr = (queryMemPerNode / (1024 * 1024)) + "MB";
      String clusterMemStr = (queryMemCluster / (1024 * 1024)) + "MB";
      String headroomStr = (headroom / (1024 * 1024)) + "MB";
      LOG.info("Trino memory config: heap={}MB, perNodeQueryMem={}, clusterQueryMem={}, headroom={}",
          maxHeap / (1024 * 1024), perNodeMemStr, clusterMemStr, headroomStr);

      // Scale task concurrency: use half of available processors (max 16) to reduce
      // peak memory from concurrent hash partitions in distributed mode.
      int processors = Runtime.getRuntime().availableProcessors();
      int taskConcurrency = Math.min(16, Math.max(4, processors / 2));
      LOG.info("Trino parallelism config: processors={}, taskConcurrency={}",
          processors, taskConcurrency);

      // Use single coordinator node. Split-level distribution across OpenSearch cluster
      // nodes is handled by TransportRemoteTaskFactory + OpenSearchNodeManager, which are
      // patched into the coordinator via TransportDistributionPatcher after build.
      // Hash partitioning within the single node still provides parallelism via task_concurrency.
      int nodeCount = 1;
      LOG.info("Trino node count: {} (single coordinator, transport distribution)", nodeCount);
      DistributedQueryRunner runner =
          DistributedQueryRunner.builder(session)
              .setNodeCount(nodeCount)
              .addExtraProperty("query.max-memory-per-node", perNodeMemStr)
              .addExtraProperty("query.max-memory", clusterMemStr)
              .addExtraProperty("memory.heap-headroom-per-node", headroomStr)
```

- [ ] **Step 2: Commit**

```bash
git add trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/bootstrap/TrinoEngine.java
git commit -s -m "perf(trino): fix memory config for multi-node — 60% per-node, 10x cluster-wide"
```

---

### Task 2: Enable spill-to-disk

**Why:** ClickBench queries build large hash tables (UserID has ~67M distinct values). Without spill, exceeding per-node memory causes OOM. Spill converts OOM crashes into slower-but-successful executions.

**Files:**
- Modify: `trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/bootstrap/TrinoEngine.java:105-108` (the builder chain)

- [ ] **Step 1: Add spill properties to the DistributedQueryRunner builder**

After the `.addExtraProperty("memory.heap-headroom-per-node", headroomStr)` line, add:

```java
              .addExtraProperty("spill-enabled", "true")
              .addExtraProperty("spiller-spill-path", "/tmp/trino-spill")
              .addExtraProperty("spiller-max-used-space-threshold", "0.8")
              .addExtraProperty("spiller-threads", "4")
              .addExtraProperty("max-spill-per-node", "50GB")
              .addExtraProperty("query-max-spill-per-node", "10GB")
```

- [ ] **Step 2: Commit**

```bash
git add trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/bootstrap/TrinoEngine.java
git commit -s -m "perf(trino): enable spill-to-disk for large aggregations"
```

---

### Task 3: Enable exchange compression

**Why:** In distributed mode, stages shuffle data between nodes via HTTP. ClickBench has string-heavy columns (URL, SearchPhrase, Referer). LZ4 compression reduces exchange buffer memory by 2-5x with near-zero CPU cost.

**Files:**
- Modify: `trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/bootstrap/TrinoEngine.java` (the builder chain)

- [ ] **Step 1: Add exchange compression property to the builder**

After the spill properties from Task 2, add:

```java
              .addExtraProperty("exchange.compression-codec", "LZ4")
```

- [ ] **Step 2: Commit**

```bash
git add trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/bootstrap/TrinoEngine.java
git commit -s -m "perf(trino): enable LZ4 exchange compression for distributed mode"
```

---

### Task 4: Add query execution timeout and update session concurrency

**Why:** Prevents stuck queries from holding resources indefinitely. Also, the session-level `task_concurrency` must match the reduced builder-level value from Task 1.

**Files:**
- Modify: `trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/bootstrap/TrinoEngine.java` (builder chain + `createSessionWithCatalogSchema`)

- [ ] **Step 1: Add timeout to builder**

After the exchange compression property from Task 3, add:

```java
              .addExtraProperty("query.max-execution-time", "10m")
```

- [ ] **Step 2: Update session task concurrency to match builder config**

Change lines 217-227 from:

```java
    int taskConcurrency = Runtime.getRuntime().availableProcessors();
    Session.SessionBuilder builder =
        Session.builder(spm)
            .setIdentity(identity)
            .setOriginalIdentity(identity)
            .setSource("opensearch-sql-plugin")
            .setQueryId(QueryId.valueOf(UUID.randomUUID().toString().replace("-", "")))
            .setSystemProperty("task_concurrency", String.valueOf(taskConcurrency))
            .setSystemProperty("dictionary_aggregation", "true")
            .setSystemProperty("max_hash_partition_count", String.valueOf(taskConcurrency))
            .setSystemProperty("min_hash_partition_count", String.valueOf(taskConcurrency))
```

To:

```java
    int processors = Runtime.getRuntime().availableProcessors();
    int taskConcurrency = Math.min(16, Math.max(4, processors / 2));
    Session.SessionBuilder builder =
        Session.builder(spm)
            .setIdentity(identity)
            .setOriginalIdentity(identity)
            .setSource("opensearch-sql-plugin")
            .setQueryId(QueryId.valueOf(UUID.randomUUID().toString().replace("-", "")))
            .setSystemProperty("task_concurrency", String.valueOf(taskConcurrency))
            .setSystemProperty("dictionary_aggregation", "true")
            .setSystemProperty("max_hash_partition_count", String.valueOf(taskConcurrency))
            .setSystemProperty("min_hash_partition_count", String.valueOf(Math.min(4, taskConcurrency)))
```

- [ ] **Step 3: Commit**

```bash
git add trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/bootstrap/TrinoEngine.java
git commit -s -m "perf(trino): add query timeout, reduce session concurrency for distributed mode"
```

---

### Task 5: Increase STATUS_POLLER thread pool

**Why:** With 3 nodes and multi-stage ClickBench queries, the coordinator may have 6+ concurrent remote tasks. A 2-thread pool causes status polls and task updates to queue up, leading to stale task states and potential timeouts.

**Files:**
- Modify: `trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/transport/TransportRemoteTask.java:96-98`

- [ ] **Step 1: Increase thread pool from 2 to 8**

Change line 97 from:

```java
  private static final ScheduledExecutorService STATUS_POLLER =
      Executors.newScheduledThreadPool(2,
          r -> { Thread t = new Thread(r, "transport-task-status-poller"); t.setDaemon(true); return t; });
```

To:

```java
  private static final ScheduledExecutorService STATUS_POLLER =
      Executors.newScheduledThreadPool(8,
          r -> { Thread t = new Thread(r, "transport-task-status-poller"); t.setDaemon(true); return t; });
```

- [ ] **Step 2: Commit**

```bash
git add trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/transport/TransportRemoteTask.java
git commit -s -m "perf(trino): increase STATUS_POLLER threads from 2 to 8 for multi-node"
```

---

### Task 6: Update benchmark script — heap, GC, spill dir

**Why:** The benchmark script defaults to 10g/node which caused OOM. Need 24g/node (3x24g=72g, well within machine's 124GB RAM). Also need to create spill directory and tune GC for large heaps.

**Files:**
- Modify: `trino/benchmark/run_clickbench_iceberg_multinode.sh:27,98-99`

- [ ] **Step 1: Change default heap from 10g to 24g**

Change line 27 from:

```bash
HEAP="${HEAP_PER_NODE:-10g}"
```

To:

```bash
HEAP="${HEAP_PER_NODE:-24g}"
```

- [ ] **Step 2: Add spill directory creation before node startup loop**

After line 66 (the seed hosts loop), add:

```bash
# Create spill directory for Trino's spill-to-disk feature
mkdir -p /tmp/trino-spill
```

- [ ] **Step 3: Update JVM options with GC tuning**

Change line 99 from:

```bash
    export OPENSEARCH_JAVA_OPTS="-Xms${HEAP} -Xmx${HEAP} -Dtrino.iceberg.warehouse=${WAREHOUSE}"
```

To:

```bash
    export OPENSEARCH_JAVA_OPTS="-Xms${HEAP} -Xmx${HEAP} -Dtrino.iceberg.warehouse=${WAREHOUSE} -XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:+ExitOnOutOfMemoryError"
```

- [ ] **Step 4: Commit**

```bash
git add trino/benchmark/run_clickbench_iceberg_multinode.sh
git commit -s -m "perf(trino): 24g/node, G1GC tuning, spill dir for 3-node benchmark"
```

---

### Task 7: Build and run 3-node benchmark

**Files:**
- No code changes. Build + execute + verify.

- [ ] **Step 1: Build the plugin**

```bash
cd /local/home/penghuo/oss/os-sql
./gradlew :opensearch-sql-plugin:bundlePlugin -x test -x integTest
```

Expected: BUILD SUCCESSFUL

- [ ] **Step 2: Verify Iceberg data exists**

```bash
ls -lh /tmp/iceberg-clickbench-warehouse/clickbench/
```

Expected: Iceberg table metadata and data files present. If not, create the table first:

```bash
python3 trino/benchmark/create_iceberg_table.py
```

- [ ] **Step 3: Run the 3-node benchmark**

```bash
HEAP_PER_NODE=24g ./trino/benchmark/run_clickbench_iceberg_multinode.sh 2>&1 | tee /tmp/clickbench-3node-output.log
```

Expected: 43/43 queries pass. Output shows per-query times and node stats with `tasksReceived > 0` on worker nodes.

- [ ] **Step 4: Verify distributed execution via node stats**

Check the output log for Step 4 node stats. All 3 nodes should show:
- `trinoEnabled: true`
- Worker nodes (port 9211, 9212): `tasksReceived > 0`
- This proves splits were dispatched to remote nodes, not just local execution.

- [ ] **Step 5: Commit benchmark results**

```bash
cp /tmp/clickbench-multinode-results.json trino/benchmark/
git add trino/benchmark/clickbench-multinode-results.json
git commit -s -m "docs: 3-node ClickBench results — XX/43 pass, XXs total with split-level distribution"
```

(Replace XX with actual numbers from the run.)

---

## Execution Plan Diagram

```
Task 1 (memory config)
  → Task 2 (spill)
    → Task 3 (exchange compression)
      → Task 4 (timeout + session concurrency)
        → Task 5 (poller threads)
          → Task 6 (benchmark script)
            → Task 7 (build + run + verify)
```

All tasks are sequential — Tasks 1-4 modify the same file (TrinoEngine.java) and build on each other.

## Expected Outcome

| Config | Per-Node Query Budget | Spill | Result |
|--------|-----------------------|-------|--------|
| Before: 10g, 70% | 7g | No | 0/43 (OOM) |
| After: 24g, 60% | 14.4g | Yes | 43/43 expected |

Expected total time: ~150-200s (faster than single-node 283s due to 3-way parallelism).
