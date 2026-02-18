# PPL Distributed Query Engine — Design Document

## 1. Motivation

The PPL query engine currently translates Calcite RelNode plans into OpenSearch DSL
queries. This translation layer has three fundamental limitations:

1. **DSL cannot express all operators** — Window functions, joins, and complex
   expressions have no DSL equivalent. These must run entirely on the coordinator
   after pulling all data.
2. **Operators don't execute on shards** — Even simple expressions like `eval a=b+1`
   pull all rows to the coordinator instead of computing on data nodes.
3. **15 pushdown rules are brittle** — Each new PPL feature requires a new hand-coded
   pushdown rule in `OpenSearchIndexRules.java`, leading to combinatorial complexity
   and bugs.

**Root cause**: translating between two paradigms (Calcite RelNode and OpenSearch DSL).

**Solution**: Stop translating. Ship Calcite plans directly to shards. Use DSL only
for inverted index access at the scan level.

---

## 2. Architecture Overview

```
                            COORDINATOR
PPL → ANTLR → AST → Analyzer → CalciteRelNodeVisitor → RelNode
                                                          |
                                  HepPlanner + VolcanoPlanner (optimize)
                                                          |
                                                    PlanSplitter
                                                          |
                            ┌──── Exchange ───────────────┴──────────────┐
                            │                                            │
                  Shard Fragment                           Coordinator Fragment
                  (JSON-serialized)                       (merge operators)
                            │                                            │
             TransportCalciteShardAction                  • ConcatExchange
             TransportCalciteShuffleAction (Phase 2)      • MergeSortExchange
                            │                             • MergeAggregateExchange
                  DATA NODE (each shard)                  • HashExchange (Phase 2)
                  ShardCalciteRuntime
                  ├── Deserialize plan fragment
                  ├── CalciteLocalShardScan
                  │   └── NodeClient.search(preference=_shards:N|_local)
                  │       (query cache, DLS/FLS, concurrent segment search)
                  ├── Calcite Enumerable pipeline
                  │   ├── Filter, Project, PartialAggregate, Sort TopK
                  │   ├── Local HashJoin (Phase 2, post-shuffle)
                  │   └── Local Window (Phase 2, post-shuffle)
                  └── Return typed rows → coordinator
```

### Key Principles

- **One tree-walk replaces 15 rules** — `PlanSplitter` inserts Exchange nodes at the
  shard/coordinator boundary in a single post-order traversal.
- **Security preserved** — `CalciteLocalShardScan` routes through the standard
  OpenSearch search pipeline; DLS/FLS, RBAC, and audit logging all apply.
- **Same PPL interface** — The PPL → AST → RelNode input pipeline is unchanged.
  Only the execution backend changes.

---

## 3. Phase 1: Scatter-Gather

Phase 1 ships Calcite plan fragments to shards and merges results on the coordinator.

### Operator Placement

| Operator | Location | Exchange Type |
|----------|----------|---------------|
| Scan | Shard | — |
| Filter (simple `a=1`) | Shard (Lucene via PredicateAnalyzer) | — |
| Filter (complex `fn(a)>b`) | Shard (Calcite Filter) | — |
| Eval / Project | Shard | — |
| Partial Aggregate (COUNT, SUM, AVG, MIN, MAX) | Shard | MergeAggregateExchange |
| Sort + Limit (TopK) | Shard + Coordinator | MergeSortExchange |
| Sort (no limit) | Coordinator | ConcatExchange |
| Window | Coordinator | ConcatExchange |
| Join | Coordinator | ConcatExchange (both inputs) |
| Dedup | Coordinator | ConcatExchange |

### Exchange Node Hierarchy

```
Exchange (abstract base)
├── ConcatExchange         — unordered concatenation of shard results
├── MergeSortExchange      — priority-queue merge-sort (distributed TopK)
├── MergeAggregateExchange — merge partial aggregate states
├── HashExchange           — shuffle by hash key (Phase 2)
└── LocalJoinExchange      — concat post-shuffle join results (Phase 2)
```

### Partial Aggregation

`PartialAggregate` decomposes aggregate functions for shard-local execution:

| Function | Shard Partial State | Coordinator Merge |
|----------|-------------------|-------------------|
| COUNT | count (long) | SUM of counts |
| SUM | sum (double) | SUM of sums |
| AVG | {SUM, COUNT} (2 columns) | SUM(sums) / SUM(counts) |
| MIN | min (comparable) | MIN of mins |
| MAX | max (comparable) | MAX of maxes |

### Serialization

`RelNodeSerializer` converts RelNode trees to/from JSON for transport to data nodes:

- Scan nodes → `ShardScanPlaceholder` (rebound to `CalciteLocalShardScan` on shard)
- Supported operators: Filter, Project, Sort, LimitSort, TopK, Aggregate, Limit
- Uses Calcite's `RelJsonWriter` + custom `ExtendedRelJson` for PPL operators

### Transport

| Class | Purpose |
|-------|---------|
| `CalciteShardAction` | ActionType: `indices:data/read/opensearch/calcite/shard` |
| `CalciteShardRequest` | Carries: serializedPlan, indexName, shardId |
| `CalciteShardResponse` | Returns: rows (Object[][]), columnNames, binaryFields |
| `TransportCalciteShardAction` | HandledTransportAction with Guice DI |

### Execution Flow

```
source=access_logs | stats count() by status_code

  MergeAggregateExchange(merge counts)          ← COORDINATOR
    PartialAggregate(count, group=status_code)    ← SHARD
      CalciteLocalShardScan(access_logs)          ← SHARD (SearchService)
```

---

## 4. Phase 2: Shuffle

Phase 2 adds the ability to **repartition data by key across nodes**, enabling
distributed joins, distributed window functions, and advanced aggregation.

### 4.1 HashExchange

Partitions data by `hash(distributionKeys) % numPartitions`. Each row is routed to
the target partition node based on its key hash.

```java
HashExchange.create(input, List.of(joinKeyIndex), numPartitions)
```

### 4.2 Shuffle Transport

| Class | Purpose |
|-------|---------|
| `CalciteShuffleAction` | ActionType: `indices:data/read/opensearch/calcite/shuffle` |
| `CalciteShuffleRequest` | Carries: partitionId, serializedRows, columnNames, serializedPlan |
| `CalciteShuffleResponse` | Returns: rows, columnNames, partitionId, binaryFields |
| `TransportCalciteShuffleAction` | Node-to-node shuffle handler |

### 4.3 Distributed Joins

For equi-joins, both inputs are shuffled by their join keys so matching rows land on
the same partition. Each partition joins independently.

```
PPL: source=orders | join ON customer_id = id customers

  ConcatExchange                                    ← coordinator
    Join(customer_id = id)                          ← partition node
      HashExchange(orders, customer_id)             ← shuffle left
      HashExchange(customers, id)                   ← shuffle right
```

Non-equi-joins fall back to coordinator (Phase 1 behavior).

### 4.4 Distributed Window Functions

When a Window has PARTITION BY keys, data is shuffled by those keys so each partition
can compute the window function locally.

```
PPL: source=sales | eventstats avg(amount) by region

  ConcatExchange                                    ← coordinator
    Window(avg(amount) PARTITION BY region)          ← partition node
      HashExchange(sales, region)                   ← shuffle by region
```

Windows without PARTITION BY still gather to coordinator.

### 4.5 Advanced Aggregation

#### STDDEV/VAR — Welford's Online Algorithm

Each shard computes partial state `{count, sum, sumSquares}`. The coordinator
merges using Chan's parallel algorithm via `WelfordPartialState`:

```
M2 = sumOfSquares - sum * sum / count
STDDEV_POP = sqrt(M2 / count)
STDDEV_SAMP = sqrt(M2 / (count - 1))
```

#### HLL (DISTINCT_COUNT_APPROX) and t-digest (PERCENTILE_APPROX)

These UDAFs already use mergeable data structures internally:
- Shards: run UDAF normally (produces HLL/t-digest internally)
- Coordinator: merge binary states via `HyperLogLogPlusPlus.merge()` / `MergingDigest.merge()`

#### Updated Aggregation Table

| Function | Partial State | Merge Strategy | Phase |
|----------|--------------|----------------|-------|
| COUNT | long | SIMPLE (SUM) | 1 |
| SUM | double | SIMPLE (SUM) | 1 |
| AVG | {sum, count} | SIMPLE (SUM/SUM) | 1 |
| MIN | comparable | SIMPLE (MIN) | 1 |
| MAX | comparable | SIMPLE (MAX) | 1 |
| STDDEV_SAMP/POP | {count, sum, sumSquares} | WELFORD | 2 |
| VAR_SAMP/POP | {count, sum, sumSquares} | WELFORD | 2 |
| DISTINCT_COUNT_APPROX | HLL bytes | BINARY_STATE | 2 |
| PERCENTILE_APPROX | t-digest bytes | BINARY_STATE | 2 |

### 4.6 Multi-Phase Execution

```
Phase 1 (scatter):  coordinator → shards → partial results
Phase 2 (shuffle):  shards → shards → repartitioned data (NEW)
Phase 3 (gather):   shards → coordinator → final result
```

---

## 5. Components

### Source Files

| File | Purpose |
|------|---------|
| `opensearch/.../planner/PlanSplitter.java` | Exchange node insertion (single tree-walk) |
| `opensearch/.../planner/merge/Exchange.java` | Abstract exchange base class |
| `opensearch/.../planner/merge/ConcatExchange.java` | Unordered concatenation |
| `opensearch/.../planner/merge/MergeSortExchange.java` | Priority-queue merge-sort |
| `opensearch/.../planner/merge/MergeAggregateExchange.java` | Merge partial agg states + MergeStrategy |
| `opensearch/.../planner/merge/HashExchange.java` | Shuffle by hash key |
| `opensearch/.../planner/merge/LocalJoinExchange.java` | Concat post-shuffle join results |
| `opensearch/.../planner/merge/PartialAggregate.java` | Shard-side partial agg decomposition |
| `opensearch/.../planner/merge/WelfordPartialState.java` | STDDEV/VAR partial state (Chan's algorithm) |
| `opensearch/.../storage/serde/RelNodeSerializer.java` | RelNode ↔ JSON serialization |
| `opensearch/.../storage/scan/CalciteLocalShardScan.java` | Shard-local scan via SearchService |
| `opensearch/.../storage/scan/ShardCalciteRuntime.java` | Shard-side plan execution |
| `opensearch/.../executor/OpenSearchExecutionEngine.java` | Coordinator execution + explain |
| `plugin/.../transport/CalciteShardAction.java` | Shard scatter ActionType |
| `plugin/.../transport/CalciteShardRequest.java` | Shard scatter request |
| `plugin/.../transport/CalciteShardResponse.java` | Shard scatter response (+ binary fields) |
| `plugin/.../transport/TransportCalciteShardAction.java` | Shard scatter handler |
| `plugin/.../transport/CalciteShuffleAction.java` | Shuffle ActionType |
| `plugin/.../transport/CalciteShuffleRequest.java` | Shuffle request |
| `plugin/.../transport/CalciteShuffleResponse.java` | Shuffle response |
| `plugin/.../transport/TransportCalciteShuffleAction.java` | Shuffle handler |
| `plugin/.../SQLPlugin.java` | Action registration |

### Modified Files

| File | Change |
|------|--------|
| `common/.../setting/Settings.java` | `CALCITE_LEGACY_PUSHDOWN_ENABLED` key |
| `opensearch/.../setting/OpenSearchSettings.java` | Register legacy pushdown setting |
| `opensearch/.../planner/rules/OpenSearchIndexRules.java` | Deprecate pushdown rules |
| `opensearch/.../storage/scan/CalciteLogicalIndexScan.java` | Legacy pushdown gate |
| `core/.../executor/ExecutionEngine.java` | `distributed` field in ExplainResponseNodeV2 |

---

## 6. Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `plugins.calcite.enabled` | `true` | Enable Calcite engine |
| `plugins.calcite.legacy_pushdown.enabled` | `false` | Emergency rollback to old pushdown rules (deprecated) |

---

## 7. Verification

### Explain API

The PPL explain API returns a `distributed` field showing the plan with Exchange nodes:

```bash
POST /_plugins/_ppl/_explain
{"query": "source=orders | stats count() by region"}
```

Response includes:
- `logical` — Calcite logical plan
- `physical` — Calcite physical plan (after VolcanoPlanner)
- `distributed` — Plan with Exchange nodes from PlanSplitter

### Test Script

```bash
./scripts/dqe-phase2-test.sh setup     # Create test indices + sample data
./scripts/dqe-phase2-test.sh settings  # Configure DQE settings
./scripts/dqe-phase2-test.sh verify    # Run explain + execute queries
./scripts/dqe-phase2-test.sh teardown  # Cleanup
./scripts/dqe-phase2-test.sh all       # All of the above
```

### Unit Tests

```bash
./gradlew :opensearch:test -x spotlessCheck   # Exchange, Welford, PlanSplitter, Serializer tests
./gradlew :opensearch-sql-plugin:test          # Plugin transport tests
./gradlew :core:test                           # Core execution engine tests
```

---

## 8. Roadmap

### Phase 1 (Complete)
- Scatter-gather execution
- Partial aggregation (COUNT, SUM, AVG, MIN, MAX)
- Distributed TopK (merge-sort)
- Calcite Enumerable pipeline on shards
- 15 pushdown rules deleted

### Phase 2 (Complete)
- HashExchange for shuffle
- Distributed equi-joins
- Distributed partitioned window functions
- STDDEV/VAR via Welford's algorithm
- DISTINCT_COUNT_APPROX via HLL merge
- PERCENTILE_APPROX via t-digest merge
- Explain API `distributed` field

### Phase 3 (Planned)
- Arrow columnar batch transfer
- RangeExchange (sorted shuffles)
- Broadcast join optimization (small table broadcast)
- Non-equi join distribution
- Cost-based join strategy selection
- Off-heap memory management
