# Fragment Planner Design

**Status:** DRAFT
**Implements:** ADR Section 2 (Fragment-Based Execution Model)

---

## 1. Overview

The `FragmentPlanner` takes a Calcite `RelNode` tree (produced by `CalciteRelNodeVisitor`)
and decomposes it into a tree of `Fragment` objects connected by exchange operators.

```
Input:  RelNode tree (single logical plan)
Output: Fragment tree (distributed physical plan)
```

## 2. Data Structures

### 2.1 Fragment

```java
/**
 * A fragment is a subtree of the distributed plan that executes on a single
 * node (or set of nodes for SOURCE fragments, one per shard).
 */
public class Fragment {
    /** Unique ID within the query */
    private final int fragmentId;

    /** Where this fragment executes */
    private final FragmentType type;  // SOURCE, HASH, SINGLE

    /** Root operator of this fragment's physical plan */
    private final PhysicalPlan root;

    /** Child fragments that feed data into this fragment via exchanges */
    private final List<Fragment> children;

    /** For SOURCE fragments: which shards to execute on */
    private final List<ShardSplit> splits;

    /** For HASH fragments: partitioning specification */
    private final PartitionSpec partitionSpec;

    /** Estimated output cardinality (for optimization decisions) */
    private final long estimatedRows;
}
```

### 2.2 FragmentType

```java
public enum FragmentType {
    /**
     * Executes on data nodes that own the target shards.
     * One instance per shard. Reads data locally from Lucene.
     */
    SOURCE,

    /**
     * Executes on worker nodes with data hash-partitioned by key.
     * Used for hash joins and repartitioned aggregations.
     */
    HASH,

    /**
     * Executes on the coordinator node only.
     * Receives gathered results from child fragments.
     * Produces the final query output.
     */
    SINGLE
}
```

### 2.3 PartitionSpec

```java
/**
 * Describes how data is partitioned for HASH fragments.
 */
public class PartitionSpec {
    /** Columns to hash-partition by */
    private final List<String> partitionKeys;

    /** Number of partitions (typically = number of data nodes) */
    private final int numPartitions;

    /** Hash function for partition assignment */
    private final HashFunction hashFunction;  // MURMUR3 by default
}
```

### 2.4 ExchangeSpec

```java
/**
 * Describes data movement between two fragments.
 */
public class ExchangeSpec {
    /** How data is distributed */
    private final ExchangeType type;  // GATHER, HASH, BROADCAST, ROUND_ROBIN

    /** For ordered exchanges (merge sort) */
    private final List<SortField> orderBy;

    /** For HASH exchange: partition keys */
    private final List<String> partitionKeys;

    /** Buffer size in bytes */
    private final long bufferSizeBytes;
}

public enum ExchangeType {
    GATHER,       // All partitions → single coordinator
    HASH,         // Rows hashed by key → target partition
    BROADCAST,    // Full copy to every worker (small table join)
    ROUND_ROBIN   // Even distribution across workers
}
```

## 3. Fragment Planner Algorithm

### 3.1 Top-Down Walk

The planner walks the RelNode tree top-down, inserting exchange boundaries:

```
fragmentPlan(RelNode root):
    1. Create root SINGLE fragment (coordinator)
    2. Walk tree top-down:
       a. For each RelNode:
          - If it's an exchange boundary → create child fragment, insert exchange
          - Otherwise → keep in current fragment
    3. For each SOURCE fragment → resolve shard splits
    4. Return Fragment tree
```

### 3.2 Exchange Boundary Detection

```java
public class FragmentPlanner {

    public Fragment plan(RelNode relNode, List<ShardSplit> splits) {
        // Start with a SINGLE fragment on coordinator
        FragmentContext context = new FragmentContext(splits);
        return planFragment(relNode, FragmentType.SINGLE, context);
    }

    private Fragment planFragment(RelNode node, FragmentType parentType,
                                   FragmentContext context) {
        if (node instanceof LogicalAggregate) {
            return planAggregate((LogicalAggregate) node, context);
        } else if (node instanceof LogicalSort) {
            return planSort((LogicalSort) node, context);
        } else if (node instanceof LogicalJoin) {
            return planJoin((LogicalJoin) node, context);
        } else if (node instanceof LogicalTableScan) {
            return planTableScan((LogicalTableScan) node, context);
        } else {
            // Filter, Project, Calc, etc. — stay in current fragment
            return planPassthrough(node, context);
        }
    }
}
```

### 3.3 Aggregation Fragmentation

```
Input RelNode:
  LogicalAggregate(group=[status], agg=[count()])
    └─ LogicalTableScan(table=idx)

Output Fragments:
  Fragment 0 [SINGLE] — coordinator
    └─ FinalAggregation(group=[status], agg=[count()])
       └─ GatherExchange ← Fragment 1

  Fragment 1 [SOURCE] — per shard
    └─ PartialAggregation(group=[status], agg=[count()])
       └─ TableScan(shard=N)
```

```java
private Fragment planAggregate(LogicalAggregate agg, FragmentContext ctx) {
    // SOURCE fragment: partial aggregation + table scan
    Fragment source = new Fragment(
        ctx.nextFragmentId(),
        FragmentType.SOURCE,
        new PartialAggregationOperator(agg.getGroupSet(), agg.getAggCallList(),
            planChild(agg.getInput(), ctx)),
        ctx.getSplits()
    );

    // SINGLE fragment: final aggregation with GATHER exchange
    ExchangeSpec exchange = ExchangeSpec.gather();
    return new Fragment(
        ctx.nextFragmentId(),
        FragmentType.SINGLE,
        new FinalAggregationOperator(agg.getGroupSet(), agg.getAggCallList(),
            new DistributedExchangeOperator(exchange, source)),
        List.of(source)
    );
}
```

### 3.4 Sort + Limit Fragmentation

```
Input RelNode:
  LogicalSort(sort=[timestamp DESC], fetch=100)
    └─ LogicalTableScan(table=idx)

Output Fragments:
  Fragment 0 [SINGLE] — coordinator
    └─ MergeSortGather(order=[timestamp DESC], limit=100)
       └─ OrderedGatherExchange ← Fragment 1

  Fragment 1 [SOURCE] — per shard
    └─ LocalSort(order=[timestamp DESC], limit=100)
       └─ TableScan(shard=N)
```

Key optimization: Each shard only produces top-N rows, reducing data transfer from
`totalDocs` to `N × numShards`.

### 3.5 Join Fragmentation

```
Input RelNode:
  LogicalJoin(condition=[a.user_id = b.user_id], type=INNER)
    ├─ LogicalTableScan(table=left_idx)
    └─ LogicalTableScan(table=right_idx)

Output Fragments (hash-partitioned join):
  Fragment 0 [SINGLE] — coordinator
    └─ GatherExchange ← Fragment 1

  Fragment 1 [HASH by user_id] — distributed across N nodes
    └─ HashJoin(on=[user_id])
       ├─ HashExchange(by=user_id) ← Fragment 2
       └─ HashExchange(by=user_id) ← Fragment 3

  Fragment 2 [SOURCE] — per shard of left_idx
    └─ TableScan(left_idx, shard=N)

  Fragment 3 [SOURCE] — per shard of right_idx
    └─ TableScan(right_idx, shard=N)
```

Broadcast join optimization (small table):
```
If one side estimated < 10MB:
  Fragment 0 [SINGLE] — coordinator
    └─ GatherExchange ← Fragment 1

  Fragment 1 [SOURCE] — per shard of large table
    └─ HashJoin(on=[user_id])
       ├─ TableScan(large_idx, shard=N)     ← local data
       └─ BroadcastExchange ← Fragment 2    ← small table broadcast

  Fragment 2 [SOURCE] — all shards of small table
    └─ TableScan(small_idx, shard=N)
```

### 3.6 Passthrough Operations

Operations that don't require data movement stay within their parent fragment:

```java
// These RelNodes do NOT create fragment boundaries:
// - LogicalFilter  → stays in SOURCE fragment (filter pushdown to shard)
// - LogicalProject → stays in SOURCE fragment (projection at shard)
// - LogicalCalc    → stays in SOURCE fragment (eval at shard)
// - LogicalValues  → stays in SINGLE fragment (constant values)
```

This is critical for performance: `source=idx | where status='ok' | eval x=a+b | fields x,c`
executes entirely on each shard with NO exchange.

## 4. Optimization Rules

### 4.1 Predicate Pushdown Through Exchange

Filters above an exchange should be pushed below it when possible:

```
Before:
  FinalAgg
    └─ Exchange
       └─ PartialAgg
          └─ Scan

  Filter(status='ok')   ← above exchange

After:
  FinalAgg
    └─ Exchange
       └─ PartialAgg
          └─ Filter(status='ok')   ← pushed below exchange, into SOURCE
             └─ Scan
```

### 4.2 Limit Pushdown

Limits can be pushed to shard-level with local sort:

```
Before:
  Sort(ts DESC) + Limit(100)
    └─ Exchange
       └─ Scan

After:
  MergeSortGather(limit=100)
    └─ OrderedExchange
       └─ LocalSort(ts DESC) + Limit(100)  ← each shard produces only 100 rows
          └─ Scan
```

### 4.3 Broadcast vs Hash Decision

```java
private ExchangeType decideJoinExchangeType(RelNode left, RelNode right) {
    long leftRows = estimateRows(left);
    long rightRows = estimateRows(right);
    long broadcastThreshold = settings.getBroadcastJoinThreshold(); // default 10MB

    if (estimateSize(left) < broadcastThreshold) {
        return ExchangeType.BROADCAST;  // broadcast left, scan right locally
    } else if (estimateSize(right) < broadcastThreshold) {
        return ExchangeType.BROADCAST;  // broadcast right, scan left locally
    } else {
        return ExchangeType.HASH;  // hash-partition both sides
    }
}
```

### 4.4 Single-Node Bypass

If all operations are shard-local (no exchange needed), skip fragment planning entirely:

```java
public Optional<Fragment> plan(RelNode relNode, List<ShardSplit> splits) {
    if (!requiresExchange(relNode)) {
        return Optional.empty();  // Use single-node path
    }
    // ... fragment planning
}

private boolean requiresExchange(RelNode node) {
    // Walk tree: any Aggregate, Sort, or Join → needs exchange
    // Pure Filter/Project/Calc chain → no exchange needed
    return new ExchangeDetector().visit(node);
}
```

## 5. Row Count Estimation

The fragment planner uses simple cardinality estimates for optimization decisions
(broadcast vs hash, single-node bypass):

```java
public class CardinalityEstimator {
    /**
     * Estimate row count from RelNode metadata.
     * Uses Calcite's built-in RelMetadataQuery when available,
     * falls back to index stats (doc count from ClusterState).
     */
    public long estimateRows(RelNode node) {
        // 1. Try Calcite metadata
        RelMetadataQuery mq = node.getCluster().getMetadataQuery();
        Double rowCount = mq.getRowCount(node);
        if (rowCount != null) return rowCount.longValue();

        // 2. Fallback: index doc count from ClusterState
        if (node instanceof LogicalTableScan) {
            return getIndexDocCount(((LogicalTableScan) node).getTable());
        }

        // 3. Default conservative estimate
        return Long.MAX_VALUE;  // Assume large → use distributed path
    }
}
```

## 6. Error Handling in Fragment Planning

| Condition | Behavior |
|-----------|----------|
| Unsupported RelNode type | Return `Optional.empty()` → fall back to single-node |
| Cannot estimate cardinality | Assume large → use distributed path |
| Single shard index | Skip distributed → single-node path |
| Planning exception | Log warning, fall back to single-node |

The fragment planner must NEVER cause a query to fail. If it can't plan distributed
execution, it falls back to the existing single-node path.

## 7. Testing Strategy

### Unit Tests
- Fragment boundary detection for each RelNode pattern
- Correct fragment types assigned
- Exchange specs match expected partitioning
- Optimization rules (predicate pushdown, limit pushdown)
- Single-node bypass for filter-only queries
- Broadcast vs hash decision with mock cardinality

### Integration Tests
- Full pipeline: PPL → RelNode → Fragment → verify structure
- Round-trip: fragment plan produces same results as single-node
- Edge cases: empty tables, single shard, very wide schemas
