# Distributed Operator Interface Design

**Status:** DRAFT
**Implements:** ADR Section 4 (Operator Interface Design)

---

## 1. Design Principles

1. **Extends PhysicalPlan:** All distributed operators inherit from the existing base class.
2. **Iterator semantics:** open() → hasNext()/next() → close() — same as single-node operators.
3. **Serializable:** Each operator implements `SerializablePlan` for transport dispatch.
4. **Memory-bounded:** All operators integrate with OpenSearch's circuit breaker.
5. **Cancellable:** Operators check a cancellation token in their iteration loop.

## 2. Shared Infrastructure

### 2.1 DistributedOperatorContext

Every distributed operator receives a context with shared infrastructure:

```java
public class DistributedOperatorContext {
    /** Unique query identifier */
    private final String queryId;

    /** Fragment this operator belongs to */
    private final int fragmentId;

    /** Circuit breaker for memory accounting */
    private final CircuitBreaker circuitBreaker;

    /** Cancellation token — checked in operator loops */
    private final AtomicBoolean cancelled;

    /** Transport service for exchange communication */
    private final TransportService transportService;

    /** Query-level settings */
    private final DistributedQuerySettings settings;

    public void checkCancelled() {
        if (cancelled.get()) {
            throw new QueryCancelledException(queryId);
        }
    }

    public void reserveMemory(long bytes, String label) {
        circuitBreaker.addEstimateBytesAndMaybeBreak(bytes, label);
    }

    public void releaseMemory(long bytes) {
        circuitBreaker.addWithoutBreaking(-bytes);
    }
}
```

### 2.2 SerializablePlan Integration

```java
/**
 * Marker interface for operators that can be serialized for transport.
 * Already exists in the codebase — distributed operators implement this.
 */
public interface SerializablePlan {
    /** Serialize this plan (and children) to StreamOutput */
    void writeTo(StreamOutput out) throws IOException;

    /** Type ID for deserialization dispatch */
    int getTypeId();
}
```

## 3. PartialAggregationOperator

### Purpose
Executes partial aggregation on a single shard. Reads rows from child (table scan),
computes partial aggregate states per group key, emits (group_key, partial_state) tuples.

### Interface

```java
public class PartialAggregationOperator extends PhysicalPlan
    implements SerializablePlan {

    /** Child operator (typically a table scan or filter) */
    private final PhysicalPlan child;

    /** Group-by key indices in the input schema */
    private final List<Integer> groupByKeys;

    /** Aggregation functions to compute */
    private final List<AggregationSpec> aggregations;

    /** In-memory hash table: group_key → partial_state[] */
    private transient Map<ExprValue, PartialAggState[]> stateMap;

    /** Iterator over stateMap entries (after child is exhausted) */
    private transient Iterator<Map.Entry<ExprValue, PartialAggState[]>> resultIterator;

    // --- Lifecycle ---

    @Override
    public void open() {
        child.open();
        stateMap = new HashMap<>();
        // Consume all input rows, building partial states
        while (child.hasNext()) {
            context.checkCancelled();
            ExprValue row = child.next();
            ExprValue groupKey = extractGroupKey(row, groupByKeys);
            PartialAggState[] states = stateMap.computeIfAbsent(
                groupKey, k -> initStates(aggregations));
            for (int i = 0; i < aggregations.size(); i++) {
                states[i].accumulate(row);
            }
            trackMemory(stateMap);
        }
        resultIterator = stateMap.entrySet().iterator();
    }

    @Override
    public boolean hasNext() {
        return resultIterator.hasNext();
    }

    @Override
    public ExprValue next() {
        Map.Entry<ExprValue, PartialAggState[]> entry = resultIterator.next();
        // Emit: (group_key, serialized_partial_state_1, serialized_partial_state_2, ...)
        return buildPartialResult(entry.getKey(), entry.getValue());
    }

    @Override
    public void close() {
        child.close();
        if (stateMap != null) {
            releaseMemory();
            stateMap = null;
        }
    }
}
```

### PartialAggState Interface

```java
public interface PartialAggState {
    /** Accumulate one row into the partial state */
    void accumulate(ExprValue row);

    /** Serialize partial state for transport */
    void writeTo(StreamOutput out) throws IOException;

    /** Deserialize partial state */
    static PartialAggState readFrom(StreamInput in, AggregationType type);

    /** Estimated memory usage in bytes */
    long estimatedSizeBytes();
}
```

### Concrete States

| Agg Function | State Fields | Size (bytes) |
|-------------|--------------|-------------|
| COUNT | `long count` | 8 |
| SUM | `double sum` | 8 |
| AVG | `double sum, long count` | 16 |
| MIN | `ExprValue min` | ~32 |
| MAX | `ExprValue max` | ~32 |
| COUNT_DISTINCT | `HyperLogLog sketch` | ~16KB |

## 4. FinalAggregationOperator

### Purpose
Runs on coordinator. Reads partial states from exchange, merges states for the same
group key, emits final aggregation results.

### Interface

```java
public class FinalAggregationOperator extends PhysicalPlan
    implements SerializablePlan {

    /** Child operator (exchange that provides partial states) */
    private final PhysicalPlan child;

    /** Group-by key indices */
    private final List<Integer> groupByKeys;

    /** Aggregation functions (same as partial) */
    private final List<AggregationSpec> aggregations;

    /** Merged states: group_key → merged_partial_state[] */
    private transient Map<ExprValue, PartialAggState[]> mergedStates;

    private transient Iterator<Map.Entry<ExprValue, PartialAggState[]>> resultIterator;

    @Override
    public void open() {
        child.open();
        mergedStates = new HashMap<>();
        // Consume all partial states from exchange
        while (child.hasNext()) {
            context.checkCancelled();
            ExprValue partialResult = child.next();
            ExprValue groupKey = extractGroupKey(partialResult, groupByKeys);
            PartialAggState[] incoming = deserializeStates(partialResult);
            PartialAggState[] existing = mergedStates.computeIfAbsent(
                groupKey, k -> initStates(aggregations));
            for (int i = 0; i < aggregations.size(); i++) {
                existing[i].merge(incoming[i]);
            }
        }
        resultIterator = mergedStates.entrySet().iterator();
    }

    @Override
    public boolean hasNext() {
        return resultIterator.hasNext();
    }

    @Override
    public ExprValue next() {
        Map.Entry<ExprValue, PartialAggState[]> entry = resultIterator.next();
        // Emit: (group_key, final_result_1, final_result_2, ...)
        return buildFinalResult(entry.getKey(), entry.getValue());
    }

    @Override
    public void close() {
        child.close();
        if (mergedStates != null) {
            releaseMemory();
            mergedStates = null;
        }
    }
}
```

## 5. DistributedExchangeOperator

### Purpose
Moves data between fragments. On the consumer side, it pulls data from a transport
buffer. On the producer side (implicit), the fragment executor pushes data to remote
exchange buffers.

### Interface

```java
public class DistributedExchangeOperator extends PhysicalPlan
    implements SerializablePlan {

    /** Exchange specification (type, partition keys, ordering) */
    private final ExchangeSpec exchangeSpec;

    /** The child fragment that produces data for this exchange */
    private final int sourceFragmentId;

    /** Buffer that receives data from transport */
    private transient ExchangeBuffer buffer;

    /** For ordered exchanges: priority queue for merge sort */
    private transient PriorityQueue<BufferEntry> mergeQueue;

    @Override
    public void open() {
        // Register this exchange consumer with the ExchangeService
        buffer = exchangeService.createConsumerBuffer(
            context.getQueryId(), sourceFragmentId, exchangeSpec);
    }

    @Override
    public boolean hasNext() {
        context.checkCancelled();
        if (exchangeSpec.isOrdered()) {
            return mergeQueue != null && !mergeQueue.isEmpty();
        }
        return buffer.hasNext();
    }

    @Override
    public ExprValue next() {
        if (exchangeSpec.isOrdered()) {
            return mergeQueue.poll().getValue();
        }
        return buffer.next();
    }

    @Override
    public void close() {
        if (buffer != null) {
            buffer.close();
        }
    }
}
```

### ExchangeBuffer

```java
public class ExchangeBuffer implements AutoCloseable {
    /** Bounded blocking queue for received rows */
    private final BlockingQueue<ExprValue> queue;

    /** Track how many source partitions have completed */
    private final AtomicInteger completedSources;
    private final int totalSources;

    /** Capacity in bytes */
    private final long capacityBytes;
    private final AtomicLong usedBytes;

    public boolean hasNext() {
        // Block until data available or all sources complete
        return !(completedSources.get() >= totalSources && queue.isEmpty());
    }

    public ExprValue next() {
        return queue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    public boolean offer(List<ExprValue> rows) {
        long batchSize = estimateSize(rows);
        if (usedBytes.get() + batchSize > capacityBytes) {
            return false;  // Backpressure: buffer full
        }
        queue.addAll(rows);
        usedBytes.addAndGet(batchSize);
        return true;
    }

    public void markSourceComplete(int sourceId) {
        completedSources.incrementAndGet();
    }
}
```

## 6. DistributedHashJoinOperator

### Purpose
Hash-partitioned join. Both sides arrive via exchange (hash-partitioned by join key).
Build side accumulated into hash table, probe side streamed through.

### Interface

```java
public class DistributedHashJoinOperator extends PhysicalPlan
    implements SerializablePlan {

    /** Build side (smaller table, fully materialized) */
    private final PhysicalPlan buildChild;

    /** Probe side (larger table, streamed) */
    private final PhysicalPlan probeChild;

    /** Join condition: which columns to match */
    private final JoinCondition joinCondition;

    /** Join type: INNER, LEFT, RIGHT, FULL */
    private final JoinType joinType;

    /** Hash table: join_key → List<ExprValue> from build side */
    private transient Map<ExprValue, List<ExprValue>> hashTable;

    /** Current probe row and matching build rows iterator */
    private transient ExprValue currentProbeRow;
    private transient Iterator<ExprValue> currentMatches;

    /** Memory limit for hash table */
    private final long hashTableMemoryLimit;

    @Override
    public void open() {
        // Phase 1: Build
        buildChild.open();
        hashTable = new HashMap<>();
        long memoryUsed = 0;

        while (buildChild.hasNext()) {
            context.checkCancelled();
            ExprValue row = buildChild.next();
            ExprValue key = extractJoinKey(row, joinCondition.getBuildKeyIndices());
            hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(row);

            memoryUsed += estimateRowSize(row);
            if (memoryUsed > hashTableMemoryLimit) {
                throw new MemoryExceededException(
                    "Hash join build table exceeded " + hashTableMemoryLimit + " bytes");
            }
            context.reserveMemory(estimateRowSize(row), "hash_join_build");
        }
        buildChild.close();

        // Phase 2: Open probe side
        probeChild.open();
        advanceToNextMatch();
    }

    @Override
    public boolean hasNext() {
        return currentMatches != null && currentMatches.hasNext()
            || advanceToNextMatch();
    }

    @Override
    public ExprValue next() {
        ExprValue buildRow = currentMatches.next();
        return joinRows(currentProbeRow, buildRow);
    }

    private boolean advanceToNextMatch() {
        while (probeChild.hasNext()) {
            context.checkCancelled();
            currentProbeRow = probeChild.next();
            ExprValue key = extractJoinKey(currentProbeRow,
                joinCondition.getProbeKeyIndices());
            List<ExprValue> matches = hashTable.get(key);
            if (matches != null && !matches.isEmpty()) {
                currentMatches = matches.iterator();
                return true;
            } else if (joinType == JoinType.LEFT || joinType == JoinType.FULL) {
                // Emit probe row with nulls for build side
                currentMatches = Collections.singletonList(nullBuildRow()).iterator();
                return true;
            }
        }
        // Handle RIGHT/FULL: emit unmatched build rows
        if (joinType == JoinType.RIGHT || joinType == JoinType.FULL) {
            return emitUnmatchedBuildRows();
        }
        return false;
    }

    @Override
    public void close() {
        buildChild.close();
        probeChild.close();
        if (hashTable != null) {
            context.releaseMemory(estimateHashTableSize());
            hashTable = null;
        }
    }
}
```

## 7. DistributedSortOperator

### Purpose
K-way merge sort across sorted shard results. Each shard produces locally sorted
top-N rows. Coordinator merge-sorts them using a priority queue.

### Interface

```java
public class DistributedSortOperator extends PhysicalPlan
    implements SerializablePlan {

    /** Child operator (exchange providing sorted streams) */
    private final PhysicalPlan child;

    /** Sort specification */
    private final List<SortField> sortFields;

    /** Optional limit (for top-N optimization) */
    private final OptionalLong limit;

    /** Priority queue for k-way merge */
    private transient PriorityQueue<ExprValue> mergeQueue;

    /** Counter for limit enforcement */
    private transient long emitted;

    @Override
    public void open() {
        child.open();
        // For merge sort: child is an exchange operator that provides
        // sorted streams from each shard. We merge them.
        Comparator<ExprValue> comparator = buildComparator(sortFields);
        mergeQueue = new PriorityQueue<>(comparator);

        // Prime the queue with first row from each source
        // The exchange buffer provides rows in arrival order;
        // merge sort uses comparator to produce globally sorted output
        loadInitialRows();

        emitted = 0;
    }

    @Override
    public boolean hasNext() {
        if (limit.isPresent() && emitted >= limit.getAsLong()) {
            return false;
        }
        return !mergeQueue.isEmpty() || child.hasNext();
    }

    @Override
    public ExprValue next() {
        context.checkCancelled();
        emitted++;
        ExprValue result = mergeQueue.poll();
        // Refill from child if available
        if (child.hasNext()) {
            mergeQueue.offer(child.next());
        }
        return result;
    }

    @Override
    public void close() {
        child.close();
        mergeQueue = null;
    }
}
```

## 8. Operator Type Registry

For deserialization, each operator has a unique type ID:

```java
public enum DistributedOperatorType {
    PARTIAL_AGGREGATION(100),
    FINAL_AGGREGATION(101),
    DISTRIBUTED_EXCHANGE(102),
    DISTRIBUTED_HASH_JOIN(103),
    DISTRIBUTED_SORT(104);

    private final int typeId;

    public static PhysicalPlan deserialize(StreamInput in) {
        int typeId = in.readVInt();
        switch (typeId) {
            case 100: return new PartialAggregationOperator(in);
            case 101: return new FinalAggregationOperator(in);
            case 102: return new DistributedExchangeOperator(in);
            case 103: return new DistributedHashJoinOperator(in);
            case 104: return new DistributedSortOperator(in);
            default: throw new IllegalStateException("Unknown operator: " + typeId);
        }
    }
}
```

## 9. PhysicalPlanNodeVisitor Additions

```java
// Add to existing PhysicalPlanNodeVisitor<R, C>:

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

## 10. Testing Requirements

Each operator must have:

1. **Unit tests** with mock data:
   - Correct output for known input
   - Empty input handling
   - Single group / single row edge cases
   - Memory limit enforcement (hash join)
   - Cancellation mid-execution

2. **Serialization round-trip tests:**
   - Serialize → deserialize → same operator state
   - Child operators recursively serialized

3. **Integration tests with exchange:**
   - Operator produces correct results when fed by actual ExchangeBuffer
   - Backpressure behavior (producer faster than consumer)

4. **Performance micro-benchmarks:**
   - Throughput (rows/sec) for each operator
   - Memory usage at various cardinalities
