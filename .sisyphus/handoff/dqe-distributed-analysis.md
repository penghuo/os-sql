# DQE Distributed vs Single-Node Analysis

## Verdict: GENUINELY DISTRIBUTED with aggressive single-node optimizations

The DQE is architecturally distributed (multi-shard, multi-node) but has been heavily optimized for single-node deployments, which is the benchmark configuration.

---

## 1. TransportTrinoSqlAction.java — Coordinator Dispatch

**DISTRIBUTED: YES** — Two execution paths exist:

### Transport Path (multi-node):
- `PlanFragmenter.fragment()` creates per-shard `PlanFragment` records, each with a `nodeId` from cluster routing
- Coordinator serializes the shard plan once, then dispatches to each target node via `transportService.sendRequest(targetNode, ShardExecuteAction.NAME, ...)`
- Uses `GroupedActionListener` to collect async responses from all shards
- **File**: `TransportTrinoSqlAction.java:770-810` — full transport dispatch loop

### Local Fast Path (single-node optimization):
- Lines 500-510: Checks if ALL shard fragments are on the local node: `if (!localNodeId.equals(frag.nodeId())) { allLocal = false; break; }`
- If all local: bypasses serialization entirely, calls `shardAction.executeLocal(shardPlan, shardReq)` directly
- Uses `CountDownLatch` + thread pool for N-1 shards, runs last shard on coordinator thread
- **Comment at line 505**: "significant overhead reduction for single-node deployments"

### Key Evidence:
- `DiscoveryNode targetNode = clusterService.state().nodes().get(frag.nodeId())` — resolves actual cluster nodes
- `PlanFragment` record stores `nodeId` per shard — supports multi-node routing

---

## 2. PlanFragmenter.java — Per-Shard Plan Creation

**DISTRIBUTED: YES** — Creates one fragment per primary shard across all nodes.

- Lines 72-90: Iterates `IndexRoutingTable.shards()` from cluster state
- Each shard gets: `new PlanFragment(shardPlan, indexName, shardId, primaryShard.currentNodeId())`
- `primaryShard.currentNodeId()` returns the actual node hosting that shard — works across nodes
- No hardcoded shard count — uses whatever the index has
- Aggregation split: PARTIAL on shards → FINAL merge on coordinator (classic distributed agg pattern)
- COUNT(DISTINCT) decomposition: shards dedup locally, coordinator merges (reduces cross-node data)
- AVG decomposition: SUM + COUNT at shard level for correct weighted merge

### No Single-Node Assumptions:
- `buildShardPlanWithInflatedLimit()` uses `numShards` parameter for limit inflation heuristic
- `Math.max(numShards, 2)` at line 80 — ensures plan transformations fire even for single-shard indices

---

## 3. FusedScanAggregate & FusedGroupByAggregate — Shard-Level Execution

**TRULY DISTRIBUTED (partial agg on shards + final merge on coordinator)**

### FusedScanAggregate:
- Executes WITHIN a single shard — reads Lucene DocValues directly
- Returns partial aggregation results (SUM, COUNT, MIN, MAX per shard)
- Coordinator merges: `mergeScalarAggregation()` sums partial counts, takes min/max, computes weighted AVG

### FusedGroupByAggregate:
- Uses SortedSetDocValues ordinals for zero-allocation GROUP BY within a shard
- Intra-shard parallelism via `ForkJoinPool` for multi-segment scanning (line 123-124)
- `PARALLELISM_MODE` configurable via system property `dqe.parallelism` (default: "docrange")
- Returns per-group partial aggregates — coordinator runs `ResultMerger.mergeAggregation()` across shards

### Coordinator Merge (ResultMerger):
- `mergeAggregation()`: hash-based merge of partial results from all shards
- `mergeSorted()`: merge-sort across shard results for ORDER BY queries
- `mergeAggregationAndSort()`: fused merge+sort path
- `mergeAggregationCapped()`: capped merge for LIMIT without ORDER BY

---

## 4. Hardcoded Values / Single-Node Assumptions

### No Breaking Assumptions Found:
- No hardcoded shard counts in DQE code
- No hardcoded node counts
- `PlanFragmenter` dynamically reads cluster routing state

### Single-Node Optimizations (non-breaking):
- **Local fast path** in TransportTrinoSqlAction: bypasses transport when all shards are local — falls back to transport path automatically for multi-node
- **Query plan cache** (128 entries): keyed by SQL string + metadata version — works regardless of node count
- **Index metadata cache** in TransportShardExecuteAction: `ConcurrentHashMap<String, CachedIndexMeta>` — per-node cache, works correctly in multi-node
- **Lucene query cache**: `ConcurrentHashMap<String, Query>` — per-node, no cross-node issues
- **System.gc() hint** after large GROUP BY results (line ~1100 in TransportShardExecuteAction) — per-shard, harmless in multi-node

### Potential Multi-Node Concern:
- `ShardExecuteResponse` attachments (`distinctSets`, `varcharDistinctSets`, `scalarDistinctSet`) are transient Java objects — only available in the local fast path. The transport path falls back to Page-based merge (lines 600-610 in TransportTrinoSqlAction show `mergeCountDistinctValuesViaRawSets` vs `mergeCountDistinctValues`). This is correct but means multi-node COUNT(DISTINCT) is slower than single-node.

---

## 5. Benchmark Setup — SINGLE-NODE CLUSTER

**CONFIRMED: Single-node cluster for benchmarks**

### Evidence:
- `setup_opensearch.sh` line 37: `discovery.type: single-node`
- `setup_common.sh` line 37: `NUM_SHARDS_1M="${NUM_SHARDS_1M:-8}"` — 8 primary shards on 1 node
- Index mapping (`clickbench_index_mapping.json`): no `number_of_shards` specified — uses OpenSearch default (1 shard)
- The 1M dataset explicitly sets 8 shards: `jq --argjson shards "$NUM_SHARDS_1M" '.settings.index.number_of_shards = $shards'`
- Full dataset (100M rows): uses default shard count from mapping (no override visible)
- Instance type: `m5.8xlarge` (32 vCPUs, 128GB RAM) — single machine

### Implication:
- ALL benchmark numbers use the local fast path (no transport serialization)
- The `allLocal` optimization in TransportTrinoSqlAction always fires
- Intra-shard parallelism via ForkJoinPool compensates for single-node by using multiple CPU cores

---

## Summary Table

| Component | Distributed? | Single-Node Optimization? |
|-----------|-------------|--------------------------|
| PlanFragmenter | ✅ Creates per-shard fragments with real node IDs | None |
| TransportTrinoSqlAction | ✅ Transport dispatch to remote nodes | ✅ Local fast path bypasses serialization |
| TransportShardExecuteAction | ✅ Executes on target node | ✅ `executeLocal()` shortcut |
| FusedScanAggregate | ✅ Partial agg per shard | ✅ Intra-shard parallelism |
| FusedGroupByAggregate | ✅ Partial agg per shard | ✅ ForkJoinPool for segments |
| ResultMerger | ✅ Merges across N shards | None |
| HashSet attachments | ⚠️ Only in local path | Transport path uses Page-based fallback |
| Benchmark setup | N/A | ✅ Single-node, 8 shards |
