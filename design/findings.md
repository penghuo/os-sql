# Native Distributed Query Engine for OpenSearch - Design Findings

## Status: CONSENSUS REACHED

This document captures the consensus findings from 4 architect agents debating the design of a native distributed query engine embedded inside OpenSearch, inspired by Trino's architecture.

---

## 1. Design Overview

### Problem Statement
The current PPL execution path rewrites logical plans (Calcite RelNode) into OpenSearch DSL queries. This limits expressiveness (joins, window functions, complex aggregations) and prevents true distributed execution across OpenSearch nodes.

### Target Architecture
Replace the DSL rewrite path with a native distributed query engine that:
- Runs entirely inside OpenSearch nodes (no external process)
- Uses OpenSearch's transport layer, cluster topology, and thread pools
- Reads data directly from Lucene segments (not via DSL queries)
- Supports both Scatter-Gather and Fully Shuffle execution modes
- Implements Trino-inspired physical operators on columnar (Page/Block) data

---

## 2. Architect Hypotheses

### Hypothesis A: Full Trino Operator Port

**Advocate: Architect A**

#### Summary

Port Trino's physical execution model wholesale into OpenSearch: Page/Block columnar format, the Operator interface (`getOutput`/`addInput`/`isBlocked`), the Driver/Pipeline execution model, exchange operators adapted over OpenSearch TransportService, and query memory management with revocable memory tracking. Map OpenSearch shards to Trino Splits. This gives us a battle-tested, production-grade distributed execution engine with 10+ years of hardening.

#### Motivation: Why the Current Model Cannot Scale

The existing `PhysicalPlan` hierarchy (`core/.../planner/physical/PhysicalPlan.java`) implements `Iterator<ExprValue>` -- a row-at-a-time volcano model. Each operator (`FilterOperator`, `AggregationOperator`, `ProjectOperator`, etc.) pulls one `ExprValue` per `next()` call. This has fundamental limitations:

1. **No vectorized execution.** Each row traverses the full operator chain individually. CPU branch predictors cannot amortize, and L1/L2 cache lines are wasted on per-row overhead (virtual dispatch, `ExprValue` boxing, `BindingTuple` map lookups in `FilterOperator.prepareNext()` at line 65).

2. **No backpressure or memory control.** `AggregationOperator.open()` (line 76-82) eagerly consumes the entire input into a `Collector` before emitting any output. For large GROUP BY cardinalities, this is unbounded memory. There is no spill-to-disk, no revocable memory tracking, and no way to pause upstream producers.

3. **No distributed execution.** The `Split` interface (`core/.../storage/split/Split.java`) exists but is vestigial -- it has only `getSplitId()`. There are no exchange operators, no shuffle, no inter-node data transfer. The engine cannot perform distributed joins or distributed aggregations.

4. **No pipeline parallelism.** Everything runs in a single thread per query on the coordinator. The `OpenSearchExecutionEngine.execute()` method (line 94-122) iterates `plan.hasNext()`/`plan.next()` in a tight loop on a single `sql-worker` thread.

#### Core Design: Porting Trino's Execution Model

**1. Page/Block Columnar Data Format**

Replace `ExprValue` (a row-oriented tagged union) with a columnar `Page` containing `Block` arrays. Each `Block` is a typed column vector:

```
Page {
    Block[] blocks;        // one per column
    int positionCount;     // number of rows in this page
}

Block (abstract) {
    // Concrete types: LongArrayBlock, VariableWidthBlock, IntArrayBlock, etc.
    boolean isNull(int position);
    long getLong(int position);
    Slice getSlice(int position);
}
```

**Why this matters:**
- Columnar layout enables SIMD-friendly tight loops (e.g., filtering a LongArrayBlock is a sequential scan of a `long[]` array, perfect for CPU prefetching).
- A Page of 1024 rows amortizes virtual dispatch once per page instead of once per row.
- Memory accounting is precise: `Block.getRetainedSizeInBytes()` gives exact memory usage, enabling the MemoryPool to make spill/kill decisions.
- Trino's `Page` is serializable over the wire, enabling zero-copy exchange between nodes.

**Mapping from ExprValue:** `ExprTupleValue` today holds a `Map<String, ExprValue>`. Converting to Page/Block means each column becomes a contiguous typed array. For OpenSearch documents, this means reading doc_values column-by-column from Lucene segments (which are already stored in columnar `DocValues` format) rather than reconstructing full `_source` documents.

**2. Operator Interface**

Replace the `Iterator<ExprValue>` pattern with Trino's push/pull hybrid:

```java
interface Operator {
    ListenableFuture<?> isBlocked();   // backpressure signal
    boolean needsInput();               // can accept more data
    void addInput(Page page);           // push data in
    Page getOutput();                   // pull data out
    void finish();                      // signal no more input
    boolean isFinished();               // all output produced
}
```

**Why this is superior to Iterator:**
- `isBlocked()` enables non-blocking pipeline execution. When an exchange operator is waiting for remote data, it returns an unresolved future, and the Driver moves to process another pipeline. This is how Trino achieves high throughput on concurrent queries.
- The push/pull hybrid prevents the "pull all the way down" problem. `AggregationOperator` can accept pages via `addInput()` and only produce output after `finish()`, with precise memory tracking at every step.
- `addInput`/`getOutput` operating on `Page` batches amortizes operator overhead across 1024+ rows per call.

**3. Driver/Pipeline Model**

A Driver chains a sequence of operators and processes pages in a loop:

```
Driver loop:
  for each operator in chain:
    if operator.isBlocked() -> skip (or yield the thread)
    if operator has output -> move Page to next operator
    if operator needs input -> pull from previous
```

A Pipeline is a set of Drivers running the same operator chain in parallel (one Driver per Split/shard). Pipelines are connected by LocalExchange (intra-node) or RemoteExchange (inter-node) operators.

**Key adaptation for OpenSearch:**
- Each `sql-worker` thread runs one Driver at a time (cooperative multitasking, no preemption).
- The existing `sql-worker` thread pool (configured in `SQLPlugin.java` line 325-328) would run Drivers. The `sql_background_io` pool handles async I/O for exchange buffers.
- A `SqlTask` runs on each data node, managing pipelines for its assigned splits (shards).

**4. Exchange Mechanism**

This is the critical component that enables distributed joins and aggregations:

- **LocalExchange:** Repartitions data within a single node. Used for parallelizing operators that need to see all rows for a partition (e.g., partitioned aggregation). Implemented as in-memory page queues.
- **RemoteExchange via TransportService:** Inter-node data transfer. Instead of Trino's HTTP-based `DirectExchangeClient`, we use OpenSearch's `TransportService.sendRequest()` with a custom `TransportAction`. Pages are serialized to `BytesStreamOutput` and sent as transport messages.
- **OutputBuffer:** Each SqlTask has an OutputBuffer that partitions output pages by hash (for shuffle) or broadcasts (for replicated joins). Downstream tasks pull from OutputBuffers via TransportService requests.

**Concrete wiring:**
- Register a `TransportAction` named `internal:sql/exchange` in `SQLPlugin.getActions()`.
- The exchange protocol: downstream sends `GetResults(taskId, outputId, sequenceId)`, upstream responds with `PagesSerde`-serialized `Page` data.
- Backpressure: if the OutputBuffer is full, upstream operators block via `isBlocked()`.

**5. Memory Management**

Port Trino's `MemoryPool` with two types of memory:
- **User memory:** Permanent allocations (hash tables, aggregation states). If exceeded, query is killed.
- **Revocable memory:** Can be spilled to disk (e.g., sort buffers, hash join build side). `MemoryRevokingScheduler` triggers spill when pool is under pressure.

Each `Operator` declares its memory usage via `OperatorContext.setMemoryReservation()`. The `MemoryPool` is shared per-node, allowing the engine to make global decisions about which queries to spill or kill.

**Adaptation:** Use OpenSearch's circuit breaker framework as a complementary mechanism. Register a `"sql_query_memory"` circuit breaker that the MemoryPool checks. This prevents the query engine from starving indexing or search.

**6. Shard-to-Split Mapping**

Each OpenSearch shard becomes a `Split`:
```java
class OpenSearchShardSplit implements ConnectorSplit {
    ShardId shardId;
    String nodeId;       // which node owns this shard
    IndexMetadata index; // for schema resolution
}
```

The `SplitManager` queries `ClusterState` to enumerate shards and assign them to nodes. Each shard-split drives a `LeafReaderContext`-based scan operator that reads directly from Lucene segments.

#### Addressing Counter-Arguments

**"This is too much code to port"**

Scope is significant but bounded. The core abstractions are:
- `Page`, `Block` types (~30 classes, mostly data structures)
- `Operator` interface + 15-20 concrete operators (Filter, Project, HashAgg, HashJoin, Sort, TopN, Exchange, TableScan, Limit, etc.)
- `Driver` + `DriverContext` (~2 classes)
- `Pipeline` + `PipelineContext` (~2 classes)
- `SqlTask` + `TaskExecutor` (~4 classes)
- `OutputBuffer` + `ExchangeOperator` (~6 classes)
- `MemoryPool` + `MemoryContext` (~4 classes)
- `PagesSerde` for serialization (~3 classes)

Estimated ~70 core classes. This is less code than the existing `CalciteRelNodeVisitor` (which is already ~2000+ lines). Trino is Apache 2.0 licensed (same as OpenSearch), so we can port code directly with attribution. We are not porting the entire Trino codebase -- just the execution layer. Trino's catalog system, metadata, connector SPI, optimizer, and SQL parser are all excluded.

**"Trino's model is designed for separate workers, not OpenSearch nodes"**

The impedance mismatch is real but manageable:

| Trino Concept | OpenSearch Adaptation |
|---|---|
| Coordinator node | Any node receiving the query (or a designated coordinator role) |
| Worker node | Data nodes holding index shards |
| HTTP exchange | `TransportService` transport actions (same serialization, different transport) |
| Thread pool (per-task) | `sql-worker` ExecutorService (already exists) |
| Memory pool (global) | Per-node memory tracked via circuit breakers |
| Split assignment | Shard routing from ClusterState |
| Connector SPI | Direct Lucene segment access via `IndexShard.acquireSearcher()` |

The key insight is that Trino's execution model is *transport-agnostic*. The Driver/Pipeline model cares about `Operator.isBlocked()` and `Page` flow, not how pages get between nodes. Replacing HTTP with TransportService is a well-bounded change.

**"We lose Lucene optimizations (inverted index, doc values, BKD trees)"**

This is the hardest problem and I address it head-on.

The current Calcite-based pushdown path (`AbstractCalciteIndexScan` in `opensearch/.../scan/`) pushes filters, aggregations, sorts, and limits into OpenSearch DSL. This leverages:
- Inverted index for term/prefix/wildcard filters (orders of magnitude faster than scanning)
- BKD trees for range queries on numeric/date fields
- Doc values for sorted retrieval and aggregation
- Global ordinals for low-cardinality GROUP BY

A full Trino port must NOT throw these away. The solution is a **Lucene-aware TableScanOperator**:

```java
class LucenePageSourceOperator implements Operator {
    // Accepts pushed-down predicates as Lucene Query objects
    private final Query pushedFilter;       // inverted index evaluation
    private final Sort pushedSort;          // doc values sort
    private final List<String> projectedColumns; // columnar doc_values read

    Page getOutput() {
        // 1. Use pushedFilter with IndexSearcher.search() to get matching docIds
        // 2. Read only projected columns from DocValues (already columnar!)
        // 3. Pack into Block arrays -> Page
    }
}
```

The critical design decision: **predicate pushdown happens at the Split/scan level, not at the operator level.** Just as Trino's JDBC connector pushes predicates into SQL queries, our LucenePageSourceOperator pushes predicates into Lucene queries. The Calcite optimizer (which we keep) generates the pushed predicates; we just apply them differently.

For aggregations, we can implement a `LuceneAggregationOperator` that delegates to Lucene's built-in aggregation framework for simple cases (COUNT, SUM on indexed fields), while the Trino-style `HashAggregationOperator` handles complex cases (multi-stage aggregation, HAVING, window functions).

This is not "losing" Lucene optimizations -- it is **preserving them at the scan layer while gaining distributed execution at the operator layer.**

**Important acknowledgment to Architect B:** Hypothesis B's insight about Lucene-native leaf operators is valid and compatible with the Full Trino Port. The difference is framing: Hypothesis A treats the Lucene-aware scan as a *connector implementation detail* (analogous to Trino's JDBC connector pushing predicates into SQL), while Hypothesis B elevates it to a first-class architectural layer. The end result is similar -- Lucene powers the leaf, Page/Block powers everything above -- but the Full Port provides the coherent Driver/Pipeline/MemoryPool framework that B's Layer 2 and Layer 3 would need to reinvent.

**"The row-based ExprValue model already works for simple queries"**

It works for single-node, DSL-rewritten queries. But the design document states the goal is distributed joins, window functions, and complex aggregations. The ExprValue/Iterator model cannot support:
- Hash joins (require building a hash table from one side, probing with the other -- needs memory management)
- Distributed aggregation (requires shuffle/exchange)
- Window functions (require partitioned, sorted input with frame management)
- Pipeline parallelism (Iterator is single-threaded by design)

We can maintain backward compatibility by keeping the existing Calcite-to-DSL path for simple queries (it already works well). The Trino execution path activates for queries that need distributed execution. The `ExecutionEngine` interface already supports both paths (`execute(PhysicalPlan, ...)` and `execute(RelNode, CalcitePlanContext, ...)`).

#### Response to Architect D's Calcite Convention Approach

Architect D proposes using Calcite's convention/trait system for distributed planning. This is appealing in theory but has practical concerns:

1. **Calcite's Volcano optimizer is a logical/physical planner, not an execution engine.** Even Architect D acknowledges the need for a custom Page/Block execution runtime. So the question becomes: do we use Calcite to produce the physical plan and then translate it to execution, or do we use Trino's proven physical planning? Calcite's convention system can express distribution, but it has no built-in understanding of Driver scheduling, pipeline parallelism, or memory budgets. These are execution concerns that Trino's model handles holistically.

2. **Convention translation is a bug-prone boundary.** Converting Calcite physical RelNodes to executable operators requires a translation layer that must handle every operator, every distribution, every edge case. This is the same work as building the Trino-style execution engine, plus the additional work of maintaining the Calcite-side convention and keeping them in sync.

3. **Cost model complexity.** Calcite's cost model works well for single-node optimization (which operators to push down). For distributed optimization, the cost model must account for network transfer, data skew, buffer memory, and pipeline parallelism. Trino's `CostComparator` and `StatsCalculator` have been tuned over years for distributed query costs. Porting these insights into Calcite's `RelOptCostFactory` is possible but amounts to reimplementing Trino's cost model inside Calcite's framework.

The full Trino port is more honest about the scope: we need a distributed execution engine, so let us port one that is proven. Using Calcite as a planning frontend that feeds into the Trino execution backend is a valid hybrid (and is essentially what this hypothesis proposes), but pretending that Calcite conventions alone solve the distributed execution problem understates the work.

#### Risk Assessment

| Risk | Severity | Mitigation |
|---|---|---|
| Large initial implementation scope | High | Phase the port: Page/Block + Operators first, then Driver, then Exchange. Each phase is independently testable. |
| Performance regression vs DSL pushdown for simple queries | Medium | Keep DSL path as default for simple queries. Route to Trino engine only for unsupported patterns (joins, complex aggs). |
| Memory pressure from columnar buffers | Medium | MemoryPool with revocable memory + circuit breaker integration. Spill to local disk via OpenSearch's transient store. |
| TransportService exchange reliability | Medium | Implement retry with exponential backoff. Trino's exchange protocol already handles task failure and retries. |
| Thread pool contention with search/index | Low | Dedicated `sql-worker` pool already isolates SQL execution. Size based on available cores minus search threads. |

#### Conclusion

The Full Trino Operator Port provides the most complete, battle-tested execution foundation. It gives us vectorized processing, memory safety, distributed execution, and pipeline parallelism in a single coherent design. The scope is large but bounded (~70 core classes), the license is compatible (Apache 2.0), and the architecture maps cleanly onto OpenSearch's infrastructure. The critical Lucene optimization concern is addressed through a Lucene-aware scan operator that preserves index-level optimizations while enabling operator-level distribution.

The alternative approaches (Hybrid, Scatter-Gather, Calcite Physical) will each end up reinventing portions of what Trino already provides. Building on proven infrastructure reduces risk and accelerates time-to-production-quality.

#### Revised Position After Debate (Round 2)

After reviewing the convergent design proposed by Architect D (which incorporates elements from all four hypotheses), I revise my position as follows:

**Concessions:**

1. **Calcite convention/trait system for physical plan selection (to Architect D).** I concede that using Calcite's Volcano optimizer with an `OpenSearchDistributedConvention` for choosing between DSL-pushdown, scatter-gather, and full-shuffle is superior to porting Trino's physical planner. The existing codebase has 15+ Calcite optimizer rules that already work in this framework. The key evidence that persuaded me: Flink, Dremio, and Beam all use Calcite for planning with separate execution engines. This is the correct boundary. I retract my proposal for a separate Trino-style physical planner.

2. **Separate Lucene-native leaf operators rather than a monolithic scan (to Architect B).** Architect B's argument for distinct `LuceneFilterScan`, `LuceneAggScan`, `LuceneSortScan` operators is stronger than my single `LucenePageSourceOperator`. The reason: distinct operators allow the Calcite optimizer to reason about each pattern independently and compose them. A monolithic scan with pushed predicates, pushed sorts, and pushed aggregations is harder for the optimizer to cost-model because the interactions are opaque. Separate operators with clear cost signatures are more composable.

3. **Scatter-gather as Phase 1 delivery target (to Architect C).** Delivering scatter-gather end-to-end first is the right incremental strategy, provided we build it on the Driver/Pipeline/Page framework from day one (not on the Iterator model). This avoids the rewrite problem I warned about while giving early visible progress.

**What I insist on from Hypothesis A (non-negotiable for the consensus):**

1. **Driver/Pipeline execution model.** The B+D synthesis proposes `execute() -> Iterator<Page>` -- a pull-based page iterator. This lacks `isBlocked()` for non-blocking exchange, lacks cooperative scheduling, and lacks the ability to suspend/resume operators when memory is under pressure. Trino's push/pull hybrid with `addInput(Page)`/`getOutput()`/`isBlocked()` is essential for production distributed execution. Even Architect D conceded this in their refined position (line 863): "The `Iterator<Page>` execution model proposed in Phase 4 is insufficient for production distributed execution."

2. **MemoryPool with revocable memory.** Without per-operator memory tracking and the ability to spill revocable memory under pressure, the engine will OOM on production workloads with large hash joins or high-cardinality GROUP BY. OpenSearch's circuit breakers are necessary but insufficient -- they are binary (trip/don't-trip), while MemoryPool provides graduated responses (spill first, kill later). This is not optional for an engine that must coexist with search/indexing workloads.

3. **Page/Block columnar format as the universal data exchange format.** All operators above the Lucene leaf level -- exchange, join, aggregate, window, project, filter -- operate on `Page(Block[], positionCount)`. No `ExprValue`, no `Iterator<ExprValue>` anywhere in the new execution path. The Block types should match Lucene DocValues types (LongArrayBlock for numeric doc values, BytesRefBlock for sorted doc values) to minimize conversion overhead at the leaf.

4. **Cooperative multitasking via Driver loop.** The Driver processes operators in a tight loop, checking `isBlocked()` and yielding when operators cannot make progress (e.g., exchange waiting for remote data). This runs on the existing `sql-worker` thread pool. Without this, exchange operators will block threads and the engine cannot achieve pipeline parallelism.

**My assessment of the convergent design (D's refined architecture):**

The 4-layer convergent design is sound and I support it:

```
Layer 4: Calcite Logical Optimization (existing, unchanged)
Layer 3: Calcite Physical Plan Selection (convention/traits) -- from D
Layer 2: Rule-Based Lucene-Native Leaf Selection -- from B
Layer 1: Trino-Inspired Execution Runtime (Driver/Pipeline/MemoryPool/Page) -- from A
Phase 1 target: Scatter-gather end-to-end -- from C
```

The critical implementation detail is that Layer 1 must be built FIRST, because Layers 2 and 3 depend on having the Page/Block format and Operator interface defined. The phasing should be:

1. Page/Block data types + Operator interface
2. Driver + Pipeline + MemoryPool (can test with synthetic data)
3. LucenePageSource operators (reading DocValues into Blocks)
4. Exchange over TransportService (scatter-gather first)
5. Calcite convention + distribution traits + optimizer rules
6. Hash join, window functions, full shuffle exchange

#### Final Position After Debate (Round 3)

After the full debate cycle with Architects B, C, and D, I arrive at the following final position.

**Additional concession to Architect B (aggregation routing):**

Architect B's aggregation routing question was decisive. In a monolithic `LucenePageSourceOperator`, the optimizer cannot compare:
- `LuceneAggScan(predicate, GROUP BY host, SUM(bytes))` -- Collector-based leaf agg, O(N) with sorted DocValues
- `OpenSearchHashAggregate(PARTIAL) -> Exchange(HASH[host]) -> OpenSearchHashAggregate(FINAL)` -- distributed hash agg
- `CalciteEnumerableIndexScan` with AggregateIndexScanRule -- DSL pushdown

With separate Lucene-native physical operators under a Calcite convention, the Volcano optimizer compares all three and picks the lowest cost. This is clean, extensible, and correct. A monolithic PageSource hides the leaf strategy from the optimizer.

I now fully endorse the separate operator model. The leaf operators mirror Lucene's own API composition:
- `LuceneFilterScan(predicate)` -- filter via Weight/Scorer, produces Pages of matching docs
- `LuceneAggScan(predicate, groupBy, accumulators)` -- filter + aggregate in one pass via Collector
- `LuceneSortScan(predicate, sort, limit)` -- filter + sort + limit via TopFieldCollector
- `LuceneFullScan(columns)` -- baseline DocValues columnar read

**What remains from the original Hypothesis A in the consensus:**

1. **Page/Block columnar format** -- universal data exchange above the leaf. Block types match Lucene DocValues types (LongArrayBlock, BytesRefBlock). Accepted by all architects.
2. **Operator interface** -- `addInput(Page)`, `getOutput() -> Page`, `isBlocked() -> ListenableFuture<?>`, `needsInput()`, `finish()`, `isFinished()`. Accepted by Architect D after conceding `Iterator<Page>` is insufficient.
3. **Driver/Pipeline execution model** -- cooperative multitasking via `Driver.processFor(Duration)`. Runs on `sql-worker` thread pool. Accepted by all architects.
4. **MemoryPool with revocable memory** -- per-operator memory tracking, spill-before-kill, circuit breaker integration. Accepted by Architect D.
5. **Port ~40 Trino classes rather than reimplement** -- Driver, DriverContext, Pipeline, DriverFactory, TaskExecutor, MemoryPool, MemoryContext, MemoryRevokingScheduler, OutputBuffer, ExchangeClient, PagesSerde, Page, Block types. Apache 2.0 license allows direct porting with attribution. Lower risk than reimplementation.

**What I conceded:**

1. No separate Trino physical planner -- Calcite Volcano with `OpenSearchDistributedConvention` handles plan selection (to Architect D).
2. Separate Lucene-native leaf operators, not monolithic PageSource -- optimizer visibility and cost-based selection require distinct physical operators (to Architect B).
3. Scatter-gather as Phase 1 delivery target -- pragmatic incremental delivery, built on Driver/Pipeline from day one (to Architect C).
4. Calcite's existing cost infrastructure is the right foundation for DSL-vs-distributed routing (to Architect D).

**Phase 1 concrete class list (agreed with Architect C):**

Core data types (~15 classes): Page, Block (abstract), LongBlock, DoubleBlock, BytesRefBlock, BooleanBlock, VariableWidthBlock, BlockBuilder variants, PagesSerde.

Operator interface + Phase 1 operators (~10 classes): Operator interface, LuceneFilterScan, LuceneAggScan, PartialAggregationOperator, FinalAggregationOperator, ProjectOperator, GatherExchangeOperator.

Execution framework (~8 classes): Driver, DriverContext, Pipeline, PipelineContext, DriverFactory, TaskExecutor, SqlTask.

Memory (~4 classes): MemoryPool (basic -- track allocations, enforce limit via circuit breaker), MemoryContext. No MemoryRevokingScheduler or spill in Phase 1.

Transport (~4 classes): ShardQueryAction, ShardQueryRequest, ShardQueryResponse, GatherExchangeClient.

Phase 1 total: ~41 classes. Phase 2 adds: HashAggregationOperator, HashJoinOperator, LocalExchange, OutputBuffer with hash partitioning, MemoryRevokingScheduler, spill-to-disk, WindowOperator, BroadcastExchange.

**Final assessment:** The original "Full Trino Operator Port" hypothesis was too broad in scope (porting the planner) but correct on the critical execution primitives (Driver/Pipeline/MemoryPool/Page/Block). The debate refined it to its essential contribution: Trino's execution runtime is the right foundation for Layer 1, while Calcite handles planning (Layer 3-4) and Lucene-native operators handle leaf access (Layer 2). This convergent design is stronger than any individual hypothesis.

### Hypothesis B: Hybrid Approach - Lucene-Native Operators with Trino Exchange

**Core Thesis:** Build Lucene-native leaf operators that exploit Lucene's inverted index, doc values, and BKD trees directly, combined with a Trino-inspired exchange mechanism over OpenSearch's TransportService for inter-node data transfer. Generic operators (joins, window functions) run above the exchange on columnar Page/Block data.

This is a three-layer architecture:

```
Layer 3: Upper-Stage Generic Operators (HashJoin, Window, Complex Agg)
            |  operates on Page/Block data  |
Layer 2: Exchange (Trino-inspired, over TransportService)
            |  hash-partition / broadcast / gather  |
Layer 1: Leaf-Stage Lucene-Native Operators (per-segment, per-shard)
            |  IndexSearcher, DocValues, Weight/Scorer  |
```

#### Layer 1: Lucene-Native Leaf Operators

The fundamental insight is that Lucene is not a row store -- it is a collection of specialized data structures (inverted indexes, BKD trees, doc-value columns, stored fields) each optimized for a particular access pattern. A query engine that treats Lucene shards as opaque row sources (as a pure Trino port would) throws away the very performance characteristics that make OpenSearch fast.

**1. LuceneTableScanOperator**
- Uses `IndexSearcher` to open a Lucene `IndexReader` over the shard's segments.
- Iterates per `LeafReaderContext` (one per Lucene segment), using `DocValues.getSorted*()` / `DocValues.getNumeric()` for columnar access to field values.
- Produces output in Page/Block format: one Block per column, one Page per batch (e.g., 1024 rows).
- DocValues are already column-oriented and memory-mapped -- reading them into Blocks is a near-zero-copy operation, unlike reconstructing rows from `_source` JSON.
- **Existing precedent in the codebase:** `CalciteFilterScriptLeafFactory` (opensearch/src/main/java/org/opensearch/sql/opensearch/storage/script/filter/CalciteFilterScriptLeafFactory.java:21) already receives a `LeafReaderContext` per segment and produces per-segment executors. The scan operator generalizes this pattern.

**2. LuceneFilterOperator**
- For predicates on indexed fields, constructs a Lucene `Query` (TermQuery, RangeQuery, BooleanQuery, etc.) and obtains a `Weight` from `IndexSearcher.createWeight()`.
- For each `LeafReaderContext`, calls `weight.scorer(leafCtx)` to get a `Scorer` whose `DocIdSetIterator` skips directly to matching documents via the inverted index's skip-list.
- For a selective filter like `WHERE status = 'error'` on a 100M-row index where 0.1% match, the Scorer skips 99.9% of documents in O(log N) per skip -- row-by-row evaluation would examine all 100M rows.
- BKD tree queries (range predicates on numeric/geo fields) prune entire segments or ranges without touching individual documents.
- Non-indexable predicates (complex expressions, UDFs) fall back to per-row evaluation on the post-filter Page, exactly as `CalciteFilterScript.execute()` does today (opensearch/src/main/java/org/opensearch/sql/opensearch/storage/script/filter/CalciteFilterScript.java:46-51).

**3. LuceneAggOperator**
- Uses Lucene's `Collector` / `LeafCollector` API for aggregation directly over doc values.
- For `GROUP BY field`, when field has `SortedDocValues`, the aggregation can exploit the sorted order to compute groups without hashing.
- For simple aggregations (COUNT, SUM, MIN, MAX, AVG), accumulates directly from `NumericDocValues` or `SortedNumericDocValues` without materializing rows.
- For composite aggregations across multiple group-by keys, uses Lucene's `CompositeAggregation`-style iteration over sorted doc values.
- **Existing precedent:** `CalciteAggregationScriptLeafFactory` (opensearch/src/main/java/org/opensearch/sql/opensearch/storage/script/aggregation/CalciteAggregationScriptLeafFactory.java:39-62) already does per-segment aggregation expression evaluation via `LeafReaderContext`. The agg operator extends this from expression evaluation to full aggregation accumulation.

**4. LuceneSortOperator**
- Uses `IndexSearcher.search(query, numHits, Sort)` with Lucene's `SortField` for efficient top-K retrieval with early termination.
- Lucene's priority-queue-based top-K over sorted segments is dramatically faster than materializing all rows and sorting.
- For `ORDER BY field LIMIT K`, if the field has an index-sorted segment order, Lucene can return results without visiting all documents.
- **Existing precedent:** `CalciteNumberSortScript` and `CalciteStringSortScript` already generate per-leaf sort scripts (opensearch/src/main/java/org/opensearch/sql/opensearch/storage/script/sort/CalciteNumberSortScript.java). The native sort operator replaces the script indirection with direct SortField usage.

#### Layer 2: Trino-Inspired Exchange over TransportService

**5. Exchange Operators**
- `ExchangeOperator` / `OutputBuffer` / `ExchangeClient` adapted from Trino's design, but using OpenSearch's `TransportService.sendRequest()` / `TransportService.registerRequestHandler()` instead of Trino's HTTP exchange.
- Transport actions are registered as `TransportAction` (just as `TransportPPLQueryAction` is today in plugin/src/main/java/org/opensearch/sql/plugin/transport/TransportPPLQueryAction.java).
- **TransportService** already handles node-to-node communication, connection management, retry, backpressure, and integrates with OpenSearch's thread pool infrastructure -- no need to build an HTTP server.

**6. Partitioning Strategies**
- **Hash partitioning**: For distributed joins and GROUP BY -- hash partition key determines target node.
- **Broadcast**: For small-table broadcast joins -- send to all nodes.
- **Gather**: For final result collection at coordinator node (single-stream gather, or ordered merge for ORDER BY).
- **Round-robin**: For load-balanced distribution.

**7. Page Format for Exchange**
- Simplified Page: `Page(Block[] blocks, int positionCount)`.
- Block types: `LongBlock`, `DoubleBlock`, `VarcharBlock`, `BooleanBlock`, matching Lucene's DocValues types.
- Serialization via OpenSearch's `StreamOutput` / `StreamInput` (already used by all transport actions).
- Pages are the universal data format above the leaf layer -- Lucene-native operators produce Pages, exchange transfers Pages, generic operators consume Pages.

#### Layer 3: Upper-Stage Generic Operators

**8. Generic Page-Based Operators**
- `HashJoinOperator`: Builds hash table from build side Pages, probes with probe side Pages. Runs after exchange. No Lucene dependency.
- `WindowOperator`: Evaluates window functions (ROW_NUMBER, RANK, LAG/LEAD) over partitioned/sorted Page streams.
- `MergeAggOperator`: Merges partial aggregation results from leaf stages.
- `ProjectOperator`: Column projection/expression evaluation on Pages.
- `FilterOperator`: Post-exchange predicate evaluation on Pages for non-pushable predicates.

These are straightforward ports from Trino's operator model -- they operate purely on Page/Block data and have no Lucene dependency.

#### Why This Hybrid is Necessary (vs. Pure Trino Port)

The critical performance argument: **Lucene's data structures provide sub-linear access patterns that a row-scanning Trino port cannot replicate.**

| Operation | Pure Trino Port | Hybrid Lucene-Native |
|---|---|---|
| Selective filter (`WHERE status = 'error'`, 0.1% selectivity) | Scan all rows, evaluate predicate per row: O(N) | Inverted index skip-list jump: O(K log N) where K << N |
| Range filter (`WHERE timestamp > X`) | Scan all rows: O(N) | BKD tree prune + skip: O(K log N) |
| Top-K sort (`ORDER BY field LIMIT 100`) | Materialize all rows, sort: O(N log N) | IndexSearcher.search with SortField + early termination: O(N log K) with segment skipping |
| GROUP BY on sorted field | Hash all rows: O(N) | Sorted DocValues iteration: O(N) with no hash table |
| COUNT(*) with filter | Scan + count: O(N) | Weight.count() short-circuit: O(1) for some queries |
| Column access | Parse `_source` JSON per row | Memory-mapped DocValues: near-zero-copy columnar read |

A pure Trino port that reads rows from shards via some scan API is functionally equivalent to what the current DSL path does -- it just changes the execution model from push (DSL) to pull (iterator). The actual bottleneck (data access pattern) remains unchanged. The hybrid approach changes the data access pattern itself.

#### Addressing Counter-Arguments

**"This requires deep Lucene knowledge and is complex."**
- The codebase already demonstrates deep Lucene integration. The `CalciteFilterScriptLeafFactory`, `CalciteAggregationScriptLeafFactory`, `CalciteNumberSortScriptLeafFactory`, and `CalciteStringSortScriptLeafFactory` classes all operate at the `LeafReaderContext` level. The team already writes Lucene-segment-level code daily.
- The leaf operators wrap Lucene APIs behind the Page/Block interface -- once written, upper-stage developers never touch Lucene.
- Lucene's `IndexSearcher`, `Weight`, `Scorer`, `DocValues` APIs are the public, documented Lucene APIs used by OpenSearch itself. This is the same API surface OpenSearch core uses.

**"Lucene APIs are internal and may change."**
- The APIs we use (`IndexSearcher`, `Weight`, `Scorer`, `LeafReaderContext`, `DocValues`, `SortField`) are part of Lucene's public `org.apache.lucene.search` and `org.apache.lucene.index` packages -- they are not internal/experimental.
- OpenSearch already depends on these same APIs for its core search functionality. If they change, OpenSearch itself must adapt, and the query engine adapts at the same time.
- The leaf operator layer is a thin adapter (perhaps 4-6 classes). If a Lucene API changes, the blast radius is confined to these adapters, not the entire engine.

**"Not all operations can leverage Lucene (joins, window functions)."**
- Exactly right -- and that is precisely why Layer 3 exists. The hybrid design acknowledges that Lucene is optimal for scan, filter, sort, and partial aggregation at the leaf level. Operations that require cross-shard data (joins, window functions, final aggregation merge) naturally belong above the exchange.
- This is not a compromise -- it is the correct architectural separation. Even Elasticsearch's own aggregation framework uses a two-phase approach: leaf-level Collector accumulation, then cross-shard merge.

**"Two different operator models is confusing."**
- There is only ONE data model above the leaf layer: Page/Block. Lucene-native operators produce Pages. Exchange transfers Pages. Generic operators consume Pages.
- The "two models" are really "Lucene data access" (Layer 1) and "Page-based computation" (Layers 2+3). This is the same separation every database with a storage engine has: the storage layer reads data into an in-memory format, and the execution engine operates on that format.
- The `AbstractCalciteIndexScan` already demonstrates this pattern: it has a Lucene-aware scan that produces `Enumerable<Object>` (its output format). We are generalizing this to a richer Page/Block output format.

#### Execution Model

```
Coordinator Node:
  1. Parse PPL -> AST -> Calcite RelNode (logical plan)
  2. Calcite optimizer produces physical plan with Lucene-native leaf operators
  3. Fragment plan at exchange boundaries
  4. Distribute leaf fragments to data nodes (shard routing)
  5. Collect results via gather exchange

Data Node (per shard):
  1. Receive leaf fragment
  2. Open IndexSearcher for local shard
  3. Execute Lucene-native operators per LeafReaderContext (segment)
  4. Produce Pages
  5. Send Pages to exchange

Coordinator / Intermediate Node:
  1. Receive Pages from exchange
  2. Execute generic operators (join, window, merge-agg)
  3. Return final result
```

#### Key Implementation Milestones

1. **Page/Block data format** -- Define Block types matching Lucene DocValues types (LongBlock, DoubleBlock, BytesRefBlock).
2. **LuceneTableScanOperator** -- Read DocValues into Blocks, produce Pages per segment batch.
3. **LuceneFilterOperator** -- Query/Weight/Scorer integration, produce filtered DocIdSets, combine with scan.
4. **Exchange over TransportService** -- Register transport actions, serialize/deserialize Pages, implement gather exchange.
5. **Hash partitioned exchange** -- For distributed GROUP BY and joins.
6. **LuceneAggOperator** -- Collector-based partial aggregation at leaf.
7. **LuceneSortOperator** -- IndexSearcher.search with Sort for top-K.
8. **Generic upper-stage operators** -- HashJoin, Window, MergeAgg ported from Trino.
9. **Calcite physical planning integration** -- Custom rules to emit Lucene-native operators.

#### Why This Hypothesis is Best Aligned with Requirements

The design requirement states the engine "MUST leverage Lucene data structures for filter, sort and aggregations." This hypothesis is the only one that directly addresses this requirement at the API level:
- Filter: `IndexSearcher.createWeight()` -> `Weight.scorer(LeafReaderContext)` -> `Scorer.iterator()` for skip-list traversal
- Sort: `IndexSearcher.search(Query, int, Sort)` with `SortField` for early-termination top-K
- Aggregation: `Collector.getLeafCollector(LeafReaderContext)` with `DocValues` for accumulation

The other hypotheses either ignore Lucene data structures entirely (pure Trino port), defer them indefinitely (scatter-gather first), or add an unnecessary Calcite physical layer between the optimizer and the Lucene primitives.

#### Revised Position After Debate with Architect D

After substantive debate with Architect D, I revise my position on the planning layer. The core Lucene-native operator design is unchanged, but the orchestration is refined:

**Concessions to Hypothesis D (evolved through 4 rounds of debate):**

1. **Calcite Volcano with a custom convention for physical plan selection is superior to a separate custom planner.** The existing codebase already uses Volcano rules (`FilterIndexScanRule`, `AggregateIndexScanRule`, `SortIndexScanRule`) that call `call.transformTo(newRel)` to compete alternatives via cost. Extending this pattern to distributed operators via an `OpenSearchDistributedConvention` is a natural evolution, not a new architecture. I retract my proposal for a separate "custom physical planner."

2. **Representing physical operators as `AbstractRelNode` subclasses gives us `explain()`, cost profiling, and plan comparison for free.** Operators carry Calcite metadata (RelTraitSet, RelOptCost) but execute via custom `execute()` methods returning `Iterator<Page>`, not via Calcite's Enumerable/Janino code generation.

3. **Cost-based comparison is needed at the strategy boundary -- not at the leaf level.** D demonstrated with two decisive examples: (a) DSL composite aggregation vs. distributed partial-aggregate-then-merge depends on estimated group cardinality vs. `max_buckets`; (b) broadcast join vs. hash-partition join depends on table sizes and network costs. These are genuinely cost-dependent decisions that benefit from Volcano exploration.

**Agreed Hybrid Planning Approach (B + D final position):**

1. **Calcite Volcano** for the high-level strategy decision: DSL pushdown (EnumerableConvention) vs. distributed execution (OpenSearchDistributedConvention), and within distributed, broadcast join vs. hash-partition join. Cost-based comparison in the Volcano search space.

2. **Rule-based structural translation** as a post-optimization pass within the distributed convention: once Volcano selects the distributed path, pattern-match to select Lucene-native leaf operators vs. generic Page-based operators. The decision "this predicate can be expressed as a Lucene TermQuery, so use LuceneFilterOperator" is deterministic -- cost-based exploration adds nothing here.

This clean split means:
- Cost-based where the decision is genuinely cost-dependent (strategy selection)
- Structural where the decision is deterministic (Lucene operator selection)
- No Lucene-aware cost model in Calcite (Calcite only needs "distributed aggregate costs X" vs "DSL pushdown costs Y")
- Adding new Lucene optimizations is a rule change in the post-optimization pass, not a Calcite change
- Adding new execution strategies (sort-merge join, index-nested-loop) is a Calcite ConverterRule, which is appropriate because those are complex cost decisions

**What I maintain from Hypothesis B (non-negotiable for consensus):**

1. **Lucene-native leaf operators are required by the MUST specification.** The leaf operators must use `Weight/Scorer/DocIdSetIterator` for filtered access, `Collector/LeafCollector` for aggregation, and `IndexSearcher.search(Query, K, Sort)` for top-K sort. Treating shards as opaque row sources discards the O(K log N) vs O(N) performance advantage.

2. **Separate Lucene-native physical operators, not a monolithic scan.** Each Lucene access pattern is a distinct operator selected by structural pattern matching:
   - `LuceneFilterScan`: implements `Weight.scorer(LeafReaderContext)` -> `Scorer.iterator()`
   - `LuceneAggScan`: implements `Collector.getLeafCollector(LeafReaderContext)` with DocValues
   - `LuceneSortScan`: implements `IndexSearcher.search(query, K, Sort)` with early termination
   - `LuceneFullScan`: baseline DocValues columnar scan

3. **Per-segment iteration is first-class.** All leaf operators iterate per `LeafReaderContext`, following the existing `CalciteFilterScriptLeafFactory.newInstance(LeafReaderContext ctx)` pattern. For Phase 1, the operator handles segment iteration internally within a shard-level Split. Segment-level parallelism (multiple Drivers per shard) can be added later if profiling shows it's needed.

**Final Converged Architecture (B + D resolved):**

```
Layer 4: Calcite Logical Optimization (EXISTING - unchanged)
  CalciteRelNodeVisitor -> RelNode -> Volcano optimizer with pushdown rules

Layer 3: Calcite Physical Plan Selection (NEW - cost-based strategy)
  Volcano compares EnumerableConvention (DSL pushdown) vs.
  OpenSearchDistributedConvention (distributed execution)
  Cost-based selection: DSL pushdown | scatter-gather | full shuffle
  Cost-based join strategy: broadcast | hash-partition | lookup

Layer 2: Rule-Based Leaf Operator Selection (NEW - structural pattern matching)
  Post-optimization pass within distributed plan:
    indexed predicate -> LuceneFilterOperator (Weight/Scorer skip-list)
    DocValues field access -> LuceneColumnReader (near-zero-copy columnar)
    sort on indexed field + LIMIT -> LuceneSortOperator (SortField + early termination)
    simple agg on DocValues -> LuceneAggOperator (Collector/LeafCollector API)
    complex expression -> GenericPageOperator (evaluate on materialized Page)

Layer 1: Trino-Inspired Execution Runtime (NEW - from Hypothesis A)
  Driver: chains operators, processes Pages in cooperative-multitask loop
  Pipeline: parallel Drivers per shard (one Driver per Split)
  Exchange: TransportService-based Page transfer (hash/broadcast/gather)
  MemoryPool: revocable memory with circuit breaker integration
  Page/Block: columnar data format (LongBlock, DoubleBlock, BytesRefBlock)
```

Phase 1 delivery: scatter-gather with Lucene-native leaf operators (from Hypothesis C).

### Hypothesis C: Minimal Viable Engine - Scatter-Gather First

**Core thesis**: Start with the simplest distributed execution model that delivers real value -- shard-level scatter-gather -- and layer on shuffle/exchange only when concrete use cases demand it. Overengineering is the single biggest risk to this project.

#### Why Scatter-Gather First

The dominant PPL query pattern is `source=<index> | where ... | stats ... by ... | sort ... | head N`. This is a single-index, filter-aggregate-sort pipeline. Every one of these queries maps directly to the scatter-gather execution model that OpenSearch already uses for its native search:

1. **Coordinator** receives the query, resolves the index to its shards, identifies the data nodes hosting those shards.
2. **Shard-local execution**: Each data node receives a plan fragment and executes filter, project, and partial aggregation locally against its Lucene segments.
3. **Coordinator merge**: Partial results stream back to the coordinator, which performs final aggregation, merge-sort, and limit.

This is not a toy. It IS distributed execution. Every shard runs operators in parallel across the cluster. The only thing it lacks compared to a full shuffle engine is the ability to repartition intermediate data between arbitrary nodes -- a capability needed only for joins and multi-index operations that represent a small minority of real PPL workloads.

#### Architecture: Three New Components

**1. `ShardQueryAction` (TransportAction)**

A new `HandledTransportAction` registered in `SQLPlugin.getActions()`. This follows the exact same pattern as the existing `TransportPPLQueryAction` at `plugin/src/main/java/org/opensearch/sql/plugin/transport/TransportPPLQueryAction.java:52`. The coordinator serializes a plan fragment and sends it to each data node that owns a relevant shard. The transport layer already handles discovery, serialization, retries, and backpressure via `TransportService`.

```
Coordinator                     Data Node 1              Data Node 2
    |                               |                        |
    |--- ShardQueryRequest(plan) -->|                        |
    |--- ShardQueryRequest(plan) --------------------------->|
    |                               |                        |
    |<-- ShardQueryResponse(rows) --|                        |
    |<-- ShardQueryResponse(rows) ---------------------------|
    |                               |                        |
    | [final merge/agg/sort]        |                        |
    | [return to client]            |                        |
```

**2. `ShardOperatorExecutor`**

Runs on data nodes. Receives a serialized plan fragment and executes it against the local shard. This is where the real work happens -- it can read directly from Lucene segments (via `IndexReader`/`IndexSearcher`) rather than going through the OpenSearch DSL/Search API. Operators include:

- **LuceneScanOperator**: Reads documents from a shard's Lucene segments, applying pushed-down filters as Lucene queries for maximum performance.
- **FilterOperator**: Post-Lucene filtering for expressions Lucene cannot evaluate (complex scripts, cross-field predicates).
- **ProjectOperator**: Column selection/projection to minimize data sent over the wire.
- **PartialAggregationOperator**: Computes partial aggregates (partial SUM, COUNT, AVG numerator/denominator) at the shard level, reducing network transfer dramatically.

These operators can initially use the existing `ExprValue`/`Iterator` pattern from `core/src/main/java/org/opensearch/sql/planner/physical/PhysicalPlan.java` (line 16: `implements PlanNode<PhysicalPlan>, Iterator<ExprValue>, AutoCloseable`). The row-based model works fine for Phase 1. Columnar/Page-based execution is an optimization for Phase 2.

**3. `CoordinatorMerger`**

Runs on the coordinator node. Receives partial results from all shards and performs:

- **Final aggregation**: Combines partial aggregates (e.g., sums partial SUMs, computes AVG from sum/count pairs). This follows the same two-phase aggregation pattern used by the existing `AggregationOperator` at `core/src/main/java/org/opensearch/sql/planner/physical/AggregationOperator.java`, but split across coordinator and data nodes.
- **Merge-sort**: For queries with ORDER BY, performs a k-way merge of pre-sorted shard results.
- **Limit**: Applies final LIMIT/HEAD after merge.
- **Streaming output**: Results can be streamed back to the client as they arrive, rather than buffering everything.

#### Why This Covers 80%+ of PPL Workloads

Analyzing the query patterns that PPL supports today:

| Query Pattern | Scatter-Gather? | Notes |
|---|---|---|
| `source=idx \| where ...` | Yes | Filter pushdown to each shard |
| `source=idx \| stats ... by ...` | Yes | Partial agg at shard, final at coordinator |
| `source=idx \| sort ... \| head N` | Yes | Top-N at shard, merge-sort at coordinator |
| `source=idx \| dedup field` | Yes | Local dedup + coordinator dedup |
| `source=idx \| eval ...` | Yes | Expression evaluation at shard |
| `source=idx \| rare/top ...` | Yes | Partial frequency counts, merge at coordinator |
| `source=idx1 \| join source=idx2` | **No** | Requires shuffle -- Phase 2 |
| `source=idx \| subsearch ...` | **No** | Requires data exchange -- Phase 2 |

The join and subsearch cases are the only ones that require full shuffle, and these are a small fraction of production PPL queries. Most observability and security analytics queries are single-index filter-aggregate-sort pipelines.

#### Why This Maps to OpenSearch's Existing Topology

OpenSearch already has every piece of infrastructure we need:

- **Shard routing**: `ClusterService` knows which data nodes host which shards. The routing table is already maintained.
- **Transport layer**: `TransportService` handles node-to-node communication with serialization, compression, backpressure, and connection pooling. We register a new `TransportAction` -- the same mechanism used by every existing OpenSearch operation.
- **Thread pools**: The plugin already creates custom thread pools (see `SQLPlugin.getExecutorBuilders()` at `plugin/src/main/java/org/opensearch/sql/plugin/SQLPlugin.java:317`). We use the same pattern for shard-local execution.
- **Lucene access**: Data nodes already have `IndexShard` handles that give direct access to `IndexReader` and `IndexSearcher`. No new infrastructure needed.
- **Backpressure and circuit breakers**: OpenSearch's existing memory circuit breakers protect against OOM. We respect them in shard-local execution.

We are not reinventing scatter-gather. We are plugging new operator logic into a proven execution framework.

#### Partial Aggregation: A Well-Understood Pattern

The two-phase aggregation model (partial at shard, final at coordinator) is battle-tested:

- **OpenSearch search aggregations** already work this way. Each shard computes partial aggregation buckets, the coordinator merges them.
- **Trino** uses the same pattern: `PARTIAL` aggregation at worker, `FINAL` at coordinator (or intermediate node). See Trino's `AggregationNode.Step` enum.
- **Spark** has the same model: `partial_sum` at mapper, `merge` at reducer.

For the existing `AggregationOperator` (which collects all input then iterates results), we split it:

```java
// At shard level:
PartialAggregationOperator {
    // Computes partial aggregates for this shard's data
    // For SUM: emits partial sum
    // For COUNT: emits partial count
    // For AVG: emits (sum, count) pair
    // For MIN/MAX: emits local min/max
}

// At coordinator:
FinalAggregationOperator {
    // Merges partial aggregates from all shards
    // For SUM: sums the partial sums
    // For COUNT: sums the partial counts
    // For AVG: sum(sums) / sum(counts)
    // For MIN/MAX: min/max of local min/maxes
}
```

This is directly analogous to MapReduce combiners and is the most natural way to distribute aggregation.

#### Addressing the Coordinator Bottleneck

The coordinator does become a merge point, but this is manageable:

1. **Streaming**: Shard results are streamed to the coordinator as they become available, not buffered. The coordinator processes results incrementally using a priority queue for merge-sort.
2. **Partial aggregation reduces data volume**: If each shard has 1M rows but only 100 distinct group-by keys, each shard sends 100 partial aggregates, not 1M rows. The coordinator merges 100 * N_shards entries.
3. **Limit pushdown**: For `head N` queries, each shard only sends its top-N rows. The coordinator merges N * N_shards rows (at most) to produce the final top-N.
4. **OpenSearch already does this**: The native search coordinator merges results from all shards on every search request. The pattern scales to thousands of shards.

For truly large result sets that cannot be reduced at the shard level (e.g., `source=idx | sort field` with no limit), we can add flow control using OpenSearch's existing backpressure mechanisms in the transport layer.

#### Phase 2: Adding Shuffle When Needed

Scatter-gather is Phase 1. When joins and multi-index operations become priorities, Phase 2 adds:

1. **Exchange operators**: `ShuffleExchangeOperator` that repartitions data by hash key across nodes. This builds on the `ShardQueryAction` transport infrastructure from Phase 1.
2. **Hash join**: Build side is broadcast or shuffled, probe side is scanned. Uses the same shard-local execution framework.
3. **Multi-stage plans**: The coordinator splits the plan into stages connected by exchanges, similar to Trino's `StageExecutionPlan`.

Phase 1 infrastructure (transport actions, shard-local execution, serialization) is reused directly. We are not throwing anything away.

#### Why This Reduces Risk

**The full Trino port (Hypothesis A) is a multi-year effort.** Trino's codebase has 500K+ lines of query engine code, built over a decade. Porting the Page/Block data model, the exchange protocol, the memory management, the spill-to-disk mechanism, the dynamic filtering, the task lifecycle -- all of this for an embedded engine that must coexist with OpenSearch's own memory management and thread model. The risk of this failing or stalling is very high.

**The hybrid approach (Hypothesis B) has unclear boundaries.** It proposes building three layers simultaneously: Lucene-native leaf operators, a Trino-inspired exchange layer, AND generic Page-based operators. That is three complex systems built in parallel with uncertain interaction semantics. The interface between "Lucene data access" and "Page-based computation" sounds clean in a diagram but will be a source of impedance mismatch bugs in practice. Hypothesis B assumes we need the full exchange infrastructure from day one -- but we do not.

**Calcite-based execution (Hypothesis D) ties distribution planning to Calcite's optimizer.** While it preserves the existing 67K+ lines of Calcite code, it adds distribution traits, a new convention, and custom physical RelNodes to a system that was not designed for distributed execution. Calcite's Volcano optimizer exploring distributed plan spaces is computationally expensive and unpredictable. The team will spend months debugging optimizer behavior instead of building execution infrastructure. And critically, Hypothesis D still needs the same Page/Block runtime and TransportService exchange that every other approach needs -- it just adds Calcite distribution planning on top.

**Scatter-gather (Hypothesis C) is the lowest-risk path because:**
- It reuses proven OpenSearch infrastructure (transport, shard routing, thread pools).
- The scope is bounded: 3 new components, not a full engine rewrite.
- It delivers measurable value for the majority of queries immediately.
- It provides a foundation for Phase 2 without constraining future design choices.
- Each component can be tested independently against the existing query path.
- The existing Calcite logical planning + optimizer is preserved unchanged. We do not break the planner; we add a new execution backend.

#### Integration with the Existing Codebase

The current execution path is:

```
PPL text -> Parser -> AST -> CalciteRelNodeVisitor -> RelNode (logical plan)
    -> Calcite optimizer -> OpenSearch DSL (via pushdown rules) -> Search API
```

The new path adds a branch after the optimizer:

```
PPL text -> Parser -> AST -> CalciteRelNodeVisitor -> RelNode (logical plan)
    -> Calcite optimizer -> [decision point]
        -> IF scatter-gather capable:
            -> Split plan into shard-local + coordinator fragments
            -> ShardQueryAction (transport to data nodes)
            -> ShardOperatorExecutor (Lucene-direct execution)
            -> CoordinatorMerger (final merge)
        -> ELSE (join, subsearch):
            -> Fall back to existing DSL rewrite path (Phase 1)
            -> Use exchange/shuffle engine (Phase 2)
```

The existing Calcite integration, pushdown rules, and DSL rewrite path remain intact as a fallback. We do not break anything.

#### Concrete Deliverables (Phase 1)

1. `ShardQueryAction` / `ShardQueryResponse`: New transport action pair. ~500 lines.
2. `ShardOperatorExecutor`: Shard-local plan execution engine. ~800 lines.
3. `LuceneScanOperator`: Direct Lucene segment reader. ~600 lines.
4. `PartialAggregationOperator` / `FinalAggregationOperator`: Two-phase aggregation. ~400 lines.
5. `CoordinatorMerger`: Result merging with merge-sort and streaming. ~500 lines.
6. `PlanSplitter`: Analyzes a RelNode plan and splits it into shard-local and coordinator fragments. ~400 lines.
7. Integration with `SQLPlugin` registration and the PPL execution path. ~200 lines.

Total: ~3,400 lines of new code, building on thousands of lines of existing, proven OpenSearch infrastructure.

#### Real-World Precedent

- **OpenSearch/Elasticsearch search**: Uses scatter-gather at massive scale (thousands of shards, billions of documents). Our approach is isomorphic.
- **Google F1 Query**: Started with scatter-gather for simple queries, added shuffle for complex ones. Published at VLDB 2018.
- **Presto/Trino "single stage" optimization**: For simple queries, Trino optimizes down to a single stage that is effectively scatter-gather. We start there.
- **ClickHouse**: Initially scatter-gather only, added distributed joins later. Became one of the fastest OLAP engines.
- **Apache Druid**: Scatter-gather architecture for its core query path. Shuffle only for specific operations.

#### Refined Position After Debate

After reviewing the arguments from Architects A, B, and D, I refine my position while maintaining that scatter-gather-first delivery is non-negotiable for risk management.

**Concessions:**

1. **To Architect A (Driver/Pipeline/MemoryPool):** The `Iterator<Page>` or `Iterator<ExprValue>` execution model is insufficient for production distributed execution. Trino's Driver/Pipeline model with `isBlocked()` for cooperative multitasking and `MemoryPool` with revocable memory are genuinely valuable runtime primitives. I concede that the shard-local execution engine should adopt these from the start rather than inventing a simpler runtime that we later throw away. The key insight is that the Driver/Pipeline model is useful even in a scatter-gather-only world -- it provides backpressure between the LuceneScanOperator and the network send, and it enables concurrent pipeline processing on multi-core data nodes.

2. **To Architect B (Lucene-native leaf operators):** I originally proposed that shard-local operators "can initially use the existing ExprValue/Iterator pattern." This was wrong. Architect B's argument that Lucene's data structures provide sub-linear access patterns (inverted index skip-lists, BKD tree pruning, DocValues columnar reads) is correct, and these should be first-class leaf operators from the start -- not deferred. The performance difference between a LuceneFilterOperator using Weight/Scorer (O(K log N)) and a generic FilterOperator scanning all rows (O(N)) is too large to defer.

3. **To Architect D (Calcite convention for strategy selection):** I concede that using Calcite's convention/trait system for the high-level strategy decision (DSL pushdown vs. scatter-gather vs. full shuffle) is superior to hand-written routing logic. The existing 15+ optimizer rules already prove the team can work with Calcite conventions. The `OpenSearchDistributedConvention` with a `SHARD_LOCAL` distribution trait is a clean way to represent scatter-gather within Calcite's framework. This is better than my original proposal of a standalone `PlanSplitter`.

**What I maintain (and insist must survive into consensus):**

1. **Phase 1 MUST be scatter-gather end-to-end before any shuffle/exchange work begins.** The convergent design from Hypothesis D lists 4 layers. The risk is that all 4 are built in parallel and none works end-to-end for months. The delivery order must be: (a) scatter-gather with Lucene-native operators working end-to-end for single-index queries, (b) Page/Block data format and Driver/Pipeline runtime, (c) exchange/shuffle for joins. Each phase delivers testable, measurable value.

2. **The exchange layer (hash-partition, broadcast) is Phase 2, not Phase 1.** The convergent design includes "Exchange: TransportService-based Page transfer (hash/broadcast/gather)" in Layer 1. For Phase 1, only `GatherExchange` (coordinator collects from shards) is needed. Hash and broadcast exchanges add complexity that is only justified when joins are implemented.

3. **The decision between scatter-gather and full shuffle should be simple and deterministic in Phase 1.** Calcite's cost-based selection between conventions is the right long-term answer. But for Phase 1, the decision should be a simple structural check: if the query involves only one index and no joins, use scatter-gather; otherwise, fall back to DSL pushdown. Cost-based selection between DSL pushdown and scatter-gather can be added iteratively as the cost model matures. Don't block Phase 1 on getting the cost model right.

4. **The ~3,400 lines estimate for the scatter-gather core is still valid as a Phase 1 scope marker.** Even with Lucene-native operators and Driver/Pipeline, the core scatter-gather machinery (ShardQueryAction, ShardOperatorExecutor, CoordinatorMerger, GatherExchange) should remain bounded. If Phase 1 scope creeps beyond this, we are overengineering.

**Revised Phase Plan:**

```
Phase 1 (Scatter-Gather MVP):
  - OpenSearchDistributedConvention with SHARD_LOCAL distribution trait
  - ShardQueryAction over TransportService (GatherExchange only)
  - Lucene-native leaf operators: LuceneFilterScan, LuceneFullScan, LuceneAggScan, LuceneSortScan
  - Page/Block data format for shard-to-coordinator transfer
  - Driver/Pipeline for shard-local execution
  - CoordinatorMerger: final aggregation, merge-sort, limit
  - Simple structural routing: single-index -> scatter-gather, else -> DSL fallback
  - Target: single-index filter+aggregate+sort+limit queries work end-to-end

Phase 2 (Full Shuffle):
  - Hash-partitioned exchange over TransportService
  - Broadcast exchange for small-table joins
  - HashJoinOperator, WindowOperator
  - MemoryPool with revocable memory and spill-to-disk
  - Cost-based selection between scatter-gather and full shuffle
  - Target: multi-index joins and complex aggregations

Phase 3 (Optimization):
  - Vectorized expression evaluation on Page/Block
  - Dynamic filtering (bloom filters from build side to probe side)
  - Adaptive query execution (runtime plan changes based on statistics)
  - Full cost model tuning with cluster topology awareness
```

This revised position converges with the other hypotheses while preserving my core argument: **deliver scatter-gather end-to-end first, then layer on complexity incrementally.** The risk of a multi-layer, multi-phase project failing is directly proportional to how long it takes before anything works end-to-end.

### Hypothesis D: Calcite Physical Plan with Custom Execution

**Core Thesis:** Extend the existing Calcite integration -- already 67K+ lines of production code across 80+ files -- to handle distributed physical planning and optimization, rather than building a separate Trino-inspired planner from scratch. The massive existing investment in Calcite (CalciteRelNodeVisitor, CalciteRexNodeVisitor, type system, UDFs, pushdown rules, optimizer rules) provides a foundation that is too valuable to discard.

#### 1. Current Calcite Investment (Evidence from Codebase)

The codebase already has a deep, production-hardened Calcite integration:

- **CalciteRelNodeVisitor** (3,694 lines): PPL AST -> Calcite RelNode for every PPL command (search, stats, join, lookup, dedup, sort, eval, window, subquery, etc.)
- **CalciteRexNodeVisitor**: Expression tree builder for all PPL expressions -> Calcite RexNode
- **CalciteEnumerableIndexScan / CalciteLogicalIndexScan**: Physical and logical scan operators with full pushdown support
- **PushDownContext**: Sophisticated filter, project, aggregate, sort, limit, dedup, rare/top pushdown to OpenSearch DSL
- **PredicateAnalyzer / AggregateAnalyzer**: Translates Calcite RelNode predicates and aggregations into OpenSearch queries
- **OpenSearchIndexRules**: 15+ Calcite optimizer rules (FilterIndexScanRule, ProjectIndexScanRule, AggregateIndexScanRule, LimitIndexScanRule, SortIndexScanRule, DedupPushdownRule, RareTopPushdownRule, EnumerableTopKMergeRule, etc.)
- **Custom type system**: ExprIPType, ExprDateType, ExprBinaryType, ExprTimeStampType mapped into Calcite's RelDataType framework
- **UDF framework**: UserDefinedFunction, UserDefinedAggFunction, plus 10+ custom aggregate functions (FirstAggFunction, LastAggFunction, PercentileApproxFunction, etc.)
- **Plan profiling**: PlanProfileBuilder, ProfileEnumerableRel, ProfileScannableRel
- **100+ integration tests** under `integ-test/src/test/java/org/opensearch/sql/calcite/remote/`

This represents person-years of work that is already tested, debugged, and running in production.

#### 2. Proposed Architecture

Instead of building a parallel Trino-style planner, extend Calcite's existing convention/trait system:

```
PPL Query
  |
  v
CalciteRelNodeVisitor (EXISTING, unchanged)
  |
  v
Calcite Logical RelNode Tree (EXISTING)
  |
  v
Calcite Volcano Optimizer with Distribution Rules (NEW rules, existing framework)
  |
  v
Distributed Physical Plan (NEW convention: OpenSearchDistributedConvention)
  |
  v
Custom Execution Engine on Page/Block data (NEW, but simpler than full Trino port)
```

##### Phase 1: Distributed Convention and Traits

**New Convention: `OpenSearchDistributedConvention`**

Define a new Calcite convention that represents distributed physical operators. This sits alongside the existing `EnumerableConvention` -- queries that can be fully pushed down to DSL continue to use the existing path; only queries requiring distributed execution (joins, complex aggregations, window functions) enter the new path.

```java
public enum OpenSearchDistributedConvention implements Convention {
    INSTANCE;
    // Operators under this convention execute on the distributed Page/Block engine
}
```

**Distribution Trait: `OpenSearchDistribution`**

Implement Calcite's `RelDistribution` interface to represent data partitioning:

- `SINGLETON` -- data on coordinator only
- `HASH[fieldList]` -- hash-partitioned across nodes by given fields
- `BROADCAST` -- replicated to all participating nodes
- `SHARD_LOCAL` -- data stays on the shard where it lives (scatter-gather)
- `ANY` -- no distribution requirement

This trait participates in Calcite's Volcano optimizer, which automatically inserts Exchange operators where distribution requirements do not match.

##### Phase 2: Custom Physical RelNodes

| Operator | Purpose | Distribution Behavior |
|---|---|---|
| `OpenSearchShardScan` | Reads from Lucene segments on a specific shard | Produces SHARD_LOCAL distribution |
| `OpenSearchExchange` | Shuffles data between nodes via TransportService | Converts between distributions |
| `OpenSearchHashAggregate` | Two-phase aggregation on Page/Block data | Partial (SHARD_LOCAL) -> Exchange(HASH) -> Final (HASH) |
| `OpenSearchHashJoin` | Hash join operator | Build side: BROADCAST or HASH; Probe side: HASH |
| `OpenSearchMergeSort` | Distributed sort with merge | SHARD_LOCAL sorted -> Exchange(SINGLETON) -> merge |
| `OpenSearchFilter` | Filter evaluation on columnar data | Preserves input distribution |
| `OpenSearchProject` | Projection/expression eval | Preserves input distribution |

##### Phase 3: Optimizer Rules

New rules added to Calcite's Volcano planner (alongside existing rules):

- `ShardScanRule`: Converts `CalciteLogicalIndexScan` -> `OpenSearchShardScan` when distributed execution is needed
- `DistributedAggregateRule`: Splits `LogicalAggregate` into partial + exchange + final
- `DistributedJoinRule`: Converts `LogicalJoin` into `OpenSearchHashJoin` with appropriate exchanges for broadcast vs. hash-partition
- `DistributedSortRule`: Converts `LogicalSort` into local sort + exchange + merge sort
- `ExchangeRemovalRule`: Removes unnecessary exchanges when distributions already match
- `PushdownFallbackRule`: Falls back to existing DSL pushdown when the operator can be handled by the current path

The Volcano optimizer's cost model decides **which path to take**:
- Simple filter + aggregate? Existing DSL pushdown wins (lower cost, no shuffle overhead)
- Multi-table join with complex predicates? Distributed convention wins
- This is **exactly what Calcite's convention/trait system is designed for**

##### Phase 4: Execution Runtime

The execution runtime is a Page/Block engine (similar to Trino's data model), but driven by the Calcite physical plan rather than a separate planner:

- **Page**: A batch of rows in columnar format (array of Blocks)
- **Block**: A typed column vector (LongArrayBlock, VariableWidthBlock, etc.)
- Each physical RelNode implements an `execute()` method that returns an `Iterator<Page>`
- `OpenSearchExchange` uses OpenSearch's `TransportService` to ship Pages between nodes
- `OpenSearchShardScan` uses the shard's IndexReader directly (Lucene segment access)

#### 3. Why This Approach Is Superior

**A. Preserves 67K+ lines of existing investment.**
Every PPL command, function, type mapping, and pushdown rule continues to work. The CalciteRelNodeVisitor does not change. The CalciteRexNodeVisitor does not change. The UDF framework does not change. Zero regression risk on existing functionality.

**B. Calcite's optimizer makes distribution decisions automatically.**
With properly defined cost functions and distribution traits, the Volcano optimizer will choose between:
- DSL pushdown (existing path, zero shuffle)
- Scatter-gather (shard-local compute + merge at coordinator)
- Full shuffle (hash-partition for joins, repartitioned aggregation)

No hand-written planner logic needed for these decisions. The optimizer explores all valid physical plans and picks the lowest cost.

**C. Join reordering comes for free.**
Calcite already implements join reordering rules (JoinCommuteRule, JoinAssociateRule, MultiJoin). These work on logical RelNodes and produce reordered logical plans that the Volcano optimizer then converts to physical plans with optimal distribution. Building a Trino-style planner means reimplementing join reordering from scratch.

**D. Predicate pushdown, aggregate splitting, and projection pruning are already implemented.**
The existing 15+ optimizer rules (FilterIndexScanRule, AggregateIndexScanRule, ProjectIndexScanRule, etc.) continue to apply. The distributed rules are *additive* -- they provide a new alternative that the optimizer can choose when DSL pushdown is insufficient.

**E. Single optimizer, single plan representation.**
Instead of a Calcite logical planner -> custom physical planner (two separate systems with a translation layer), everything stays in Calcite's RelNode tree. This means:
- One debugging interface (RelNode.explain())
- One cost model
- One set of optimizer rules
- One plan representation throughout the entire pipeline

**F. Incremental migration path.**
Existing queries that work today continue to work via EnumerableConvention. New distributed operators are introduced alongside, not instead of, the existing path. Each new operator (distributed join, distributed aggregate, etc.) can be added one at a time, with the optimizer routing queries to the new path only when it produces a lower-cost plan.

**G. Compatible with Lucene-native optimizations.**
Nothing in this approach precludes the `OpenSearchShardScan` from using Lucene APIs directly (IndexSearcher, Weight, Scorer, DocValues). The Calcite convention decides WHAT to execute where; the operators themselves decide HOW to read data. An `OpenSearchShardScan` can absolutely use DocValues for columnar access, Weight/Scorer for selective filtering, and Lucene Sort for efficient top-K. The Hypothesis B insights about Lucene-native access are fully compatible -- they simply become the implementation detail inside Hypothesis D's physical operators, with Calcite orchestrating the overall distributed plan.

#### 4. Addressing Counter-Arguments

**"Calcite's physical planning is complex and hard to debug."**

The existing codebase already works with Calcite's planner. The team has written 15+ custom optimizer rules, custom conventions, custom cost functions. Adding distribution traits and a new convention is an incremental step, not a conceptual leap. For debugging, Calcite provides:
- `RelOptPlanner.setExecutionCount()` to trace rule firing
- `RelNode.explain(RelWriterImpl)` for plan dumps
- `CalcitePrepareImpl.THREAD_TRIM` for tracing optimization
- The existing PlanProfileBuilder already profiles Calcite execution

Building a separate Trino-style planner introduces a *second* system to debug, with a translation boundary between Calcite logical plans and Trino physical plans that is itself a bug-prone surface.

**"Calcite's Enumerable execution is slow compared to hand-written operators."**

Agreed -- and this proposal does NOT use Enumerable execution for the distributed path. The proposal defines `OpenSearchDistributedConvention` operators that execute on a custom Page/Block engine, identical in data model to what a Trino port would use. Calcite's role is limited to planning and optimization. Execution is hand-written, columnar, and vectorized. The key insight is: Calcite is an excellent planner/optimizer but we bypass its execution for the hot path.

Optionally, Calcite's code generation (Linq4j expressions) can be used for scalar expression evaluation as a starting point, then replaced with hand-written vectorized evaluation if profiling shows it as a bottleneck.

**"Calcite does not know about OpenSearch's cluster topology."**

Correct, and this is addressed by providing a custom `RelMetadataProvider` and `RelOptCostFactory`:
- `OpenSearchStatisticsProvider`: Provides shard count, shard sizes, document counts per index/shard (from ClusterState and IndexStats)
- `OpenSearchDistributionProvider`: Provides data distribution information (shard routing, segment distribution)
- `OpenSearchCostFactory`: Weights network transfer, memory, and CPU differently based on cluster topology

The existing `CalciteLogicalIndexScan` already accesses `OpenSearchIndex` metadata (field types, settings). The distributed extension adds cluster-level metadata (shard locations, node capacities) in the same pattern.

**"We still need a runtime execution engine even with Calcite physical plans."**

Yes. The Page/Block runtime, TransportService-based exchange, and memory management are needed regardless of which approach we choose. The difference is:
- **Hypothesis D**: Calcite plans -> custom execution (runtime is the only new system)
- **Other hypotheses**: Calcite plans -> translation -> custom physical planner -> custom execution (two new systems: a planner AND a runtime)

Hypothesis D requires building ONE new system (the execution runtime). The other hypotheses require building TWO new systems (a physical planner AND an execution runtime), while simultaneously maintaining the existing Calcite planner for backward compatibility.

#### 5. Risk Analysis

| Risk | Mitigation |
|---|---|
| Calcite convention/trait system has a learning curve | Team already has deep Calcite expertise (67K+ lines prove this) |
| Cost model tuning for distributed plans is hard | Start with simple heuristics (prefer pushdown, then scatter-gather, then shuffle); refine with benchmarks |
| Calcite Volcano optimizer can be slow for large search spaces | Use pruning (cost caps), limit convention exploration to queries that need distribution |
| Edge cases in distribution trait propagation | Comprehensive test suite for distribution inference; fail gracefully to existing path |

#### 6. Implementation Phases

1. **Phase 1**: Define `OpenSearchDistributedConvention`, distribution trait, and `OpenSearchShardScan`. Wire up the Volcano optimizer to produce distributed scan plans. Output: scatter-gather queries work through the new convention.
2. **Phase 2**: Implement `OpenSearchExchange` using TransportService. Add Page/Block data model. Output: data can be shuffled between nodes.
3. **Phase 3**: Implement `OpenSearchHashAggregate` (two-phase) and `DistributedAggregateRule`. Output: distributed aggregations work.
4. **Phase 4**: Implement `OpenSearchHashJoin` and `DistributedJoinRule`. Output: distributed joins work with broadcast and hash-partition strategies.
5. **Phase 5**: Implement `OpenSearchMergeSort`, window functions, and advanced cost model tuning.

#### 7. Key Differentiator: Calcite Convention as Decision Framework

The fundamental question every distributed query engine must answer is: "For this query, what physical strategy should I use?" The options are typically:
1. Push everything to the storage engine (DSL pushdown)
2. Scatter-gather (parallel execution, simple merge)
3. Full shuffle (repartitioned joins, complex aggregation)

In Hypothesis A/B/C, this decision must be made by hand-written logic in a custom planner. In Hypothesis D, Calcite's Volcano optimizer makes this decision **automatically** by exploring plans across multiple conventions and choosing the lowest-cost alternative. This is the same mechanism used by Apache Flink (FlinkPhysicalConvention), Apache Beam (BeamConvention), and Dremio (DistributionTrait) -- all production-proven distributed systems built on Calcite.

The team does not need to invent a planner. Calcite IS the planner. The team needs to describe the physical operators and their costs, and Calcite does the rest.

#### 8. Refined Position After Debate

Through debate with Architects A, B, and C, the following refinements emerged:

**Synthesis with Architect B (agreed after 3 rounds of debate):** Lucene-native leaf operators should be separate Calcite physical operators under `OpenSearchDistributedConvention`, NOT a monolithic scan or a post-optimization rule-based pass. Each Lucene-native operator extends `AbstractRelNode`, declares `computeSelfCost()` using Lucene-specific statistics, and implements `execute()` returning `Iterator<Page>` via direct Lucene API calls. The Volcano optimizer compares Lucene-native vs. generic strategies via cost:

| Operator | Lucene API | Calcite Physical Operator | Cost Model Input |
|---|---|---|---|
| Filtered scan | `Weight.scorer(ctx)` -> `Scorer.iterator()` | `LuceneFilterScan` | Estimated selectivity from term statistics |
| Aggregation | `Collector.getLeafCollector(ctx)` + DocValues | `LuceneAggScan` | Field cardinality, DocValues availability |
| Top-K sort | `IndexSearcher.search(query, K, Sort)` | `LuceneSortScan` | Index sort order, K value |
| Full columnar scan | `DocValues.getNumeric/getSorted*()` | `LuceneFullScan` | Segment count, document count |

All Lucene-native operators iterate per `LeafReaderContext` (segment), following the existing `CalciteFilterScriptLeafFactory.newInstance(LeafReaderContext ctx)` pattern.

**Concession to Architect A:** The `Iterator<Page>` execution model proposed in Phase 4 is insufficient for production distributed execution. Trino's Driver/Pipeline model with `isBlocked()` for non-blocking pipeline execution, and `MemoryPool` with revocable memory for memory management, are genuinely valuable runtime primitives. The refined position incorporates these into the execution runtime BELOW the Calcite physical plan. Calcite produces the physical plan (WHAT to execute). The Trino-inspired runtime executes it (HOW -- Driver scheduling, memory budgets, pipeline parallelism, backpressure). This follows the Flink/Dremio model: Calcite for planning, custom runtime for execution.

**Agreement with Architect C (after 3 rounds of debate):** Scatter-gather is the right starting point for Phase 1. The implementation should deliver scatter-gather working end-to-end before tackling full shuffle. Phase 1 uses a simple cost heuristic (DSL pushdown if all operators can push down; scatter-gather otherwise), not full Volcano cost exploration. Sophisticated cost models mature iteratively across phases.

**Clarification (prompted by Architect C's question):** Leaf operator selection (LuceneFilterScan vs. GenericFilter) happens INSIDE Volcano optimization, not as a post-optimization pass. Each Lucene-native operator is a ConverterRule producing an alternative physical plan with its own `computeSelfCost()`. The Volcano optimizer compares all alternatives (DSL pushdown, Lucene-native distributed, generic distributed) and picks the lowest cost. This means Layers 2 and 3 from the original description are unified inside the Volcano optimizer -- there is no separate "post-optimization pass."

**Refined Architecture (Convergent A+B+C+D Design):**

```
Calcite Planning (Layers unified inside Volcano optimizer):

  Calcite Logical Optimization (EXISTING - unchanged):
    CalciteRelNodeVisitor -> RelNode -> Volcano with existing pushdown rules

  Calcite Physical Plan Selection (NEW - convention/trait + ConverterRules):
    Volcano explores plans across conventions:
      EnumerableConvention (existing DSL pushdown) vs.
      OpenSearchDistributedConvention (distributed execution)
    Within distributed convention, ConverterRules produce alternatives:
      LuceneFilterScan(predicate) vs. LuceneFullScan + OpenSearchFilter(predicate)
      LuceneAggScan(GROUP BY) vs. LuceneFullScan + OpenSearchHashAggregate
      LuceneSortScan(ORDER BY LIMIT K) vs. LuceneFullScan + OpenSearchMergeSort
    Distribution traits inform exchange insertion:
      broadcast vs. hash-partition for joins
      scatter-gather vs. full shuffle for aggregations
    Each alternative has computeSelfCost() -- optimizer picks lowest cost

  Physical Operators (AbstractRelNode subclasses, output of Volcano):
    Lucene-native (SHARD_LOCAL distribution, per-segment iteration):
      LuceneFilterScan: Weight/Scorer/DocIdSetIterator skip-list traversal
      LuceneAggScan: Collector/LeafCollector with DocValues accumulation
      LuceneSortScan: IndexSearcher.search(query, K, Sort) with early termination
      LuceneFullScan: DocValues columnar read (near-zero-copy)
    Exchange (distribution conversion):
      HashExchange, BroadcastExchange, GatherExchange (over TransportService)
    Generic (Page/Block computation, no Lucene dependency):
      OpenSearchFilter, OpenSearchHashAggregate, OpenSearchMergeSort
      OpenSearchHashJoin, OpenSearchWindow, OpenSearchProject

Execution Runtime (separate from planning):

  Trino-Inspired Runtime (NEW - Driver/Pipeline/MemoryPool):
    Driver: chains operators, processes Pages in cooperative-multitask loop
    Pipeline: parallel Drivers per shard (one Driver per Split)
    MemoryPool: revocable memory with circuit breaker integration
    Page/Block: columnar data format for all operators above the scan
    Operator interface: isBlocked() / needsInput() / addInput(Page) / getOutput()
```

**Phased Delivery (agreed with Architects A and C):**

- **Phase 1 (scatter-gather on real Operator/Driver/Page infrastructure):**
  - `Operator` interface: `isBlocked()`, `needsInput()`, `addInput(Page)`, `getOutput()`, `isFinished()` (genuine Trino port, not simplified Iterator)
  - `Driver.processFor(Duration)` loop for cooperative multitasking
  - `Page`/`Block` columnar data format (LongArrayBlock, VariableWidthBlock, etc.)
  - Basic `MemoryPool` with reservation tracking (kill on OOM; spill deferred to Phase 3)
  - `OpenSearchDistributedConvention` + `ShardScanRule` ConverterRule
  - `LuceneFullScan` operator (DocValues columnar read, per-segment iteration)
  - `PartialAggOperator` + `FinalAggOperator` (two-phase aggregation on Pages)
  - `GatherExchange` over TransportService (collect Pages from shards at coordinator)
  - Simple cost heuristic for DSL-vs-scatter-gather routing
  - **Phase 1 acceptance test:** `source=index | where field > 100 | stats count() by region` runs end-to-end on Driver/Pipeline/Page infrastructure with scatter-gather execution
- **Phase 2 (Lucene-native leaf operators)**: Add LuceneFilterScan (Weight/Scorer), LuceneAggScan (Collector/DocValues), LuceneSortScan (IndexSearcher.search with Sort) as ConverterRules. Cost model uses term statistics and DocValues metadata for Lucene-native vs. generic comparison.
- **Phase 3 (distributed aggregation + memory management)**: Add DistributedAggregateRule (partial + HashExchange + final). HashExchange over TransportService. `MemoryRevokingScheduler` with spill-to-disk for hash tables under memory pressure.
- **Phase 4 (distributed joins)**: Add DistributedJoinRule (broadcast vs. hash-partition). BroadcastExchange. HashJoinOperator with build/probe phases. Cost model uses table size estimates.
- **Phase 5 (advanced)**: Window functions, sort-merge join, Lucene operator fusion rules, LocalExchange for intra-node repartitioning, advanced cost model tuning.

**Segment Iteration Contract (agreed with Architect B):**

A Split represents a shard (the unit of ownership in ClusterState). Segment iteration is internal to the leaf operator:

1. A leaf operator receives a shard reference (`IndexShard` / `IndexSearcher`).
2. It iterates `LeafReaderContext` instances internally via `searcher.getIndexReader().leaves()`, processing one segment at a time.
3. It emits `Page` instances incrementally (lazy iteration, not eager materialization of all segments).
4. Memory usage per operator is bounded by `batchSize * columnCount * typeWidth` (e.g., 1024 rows), not by shard size or segment count. Backpressure from full output buffers naturally throttles segment iteration.
5. For intra-shard parallelism, the Pipeline can instantiate multiple leaf operators with disjoint segment ranges from the same shard (analogous to Trino's LocalExchange for intra-node parallelism).

This follows the existing `CalciteFilterScriptLeafFactory.newInstance(LeafReaderContext ctx)` pattern and ensures the Driver/Pipeline model operates at shard granularity while Lucene-native operators exploit segment-level APIs.

**Convergent design contributions from each hypothesis:**
- **From Hypothesis A**: Driver/Pipeline execution model (genuine Trino port: `Driver.processFor()`, `Operator.isBlocked()`, `addInput(Page)`/`getOutput()`), MemoryPool with revocable memory and `MemoryRevokingScheduler`, Page/Block columnar format, insistence on real Operator/Driver infrastructure from Phase 1 (not deferred). Architect A accepted Calcite convention/trait system for physical planning (retracting separate Trino PlanFragmenter proposal).
- **From Hypothesis B**: Separate Lucene-native leaf operators as Calcite physical operators (Weight/Scorer, Collector/DocValues, IndexSearcher.search with SortField), per-segment iteration contract, cost-based comparison between Lucene-native and generic strategies within Volcano. Architect B accepted Calcite convention approach (retracting separate custom planner proposal).
- **From Hypothesis C**: Scatter-gather as Phase 1, sequential phased delivery, simple cost heuristic for Phase 1 (sophisticated cost model deferred), reuse of OpenSearch transport infrastructure (TransportService, shard routing, thread pools, circuit breakers). Architect C accepted Calcite convention for strategy selection (retracting PlanSplitter proposal).
- **From Hypothesis D**: Calcite's convention/trait system (`OpenSearchDistributedConvention` with distribution traits) for cost-based strategy selection across DSL pushdown / Lucene-native / generic, single unified optimizer (no separate physical planner), preservation of existing 67K+ line Calcite integration, leaf operator selection inside Volcano (not a post-pass).

**Remaining minor disagreement (A vs. B+D):** Architect A prefers a monolithic Lucene PageSource (Trino connector model with pushed predicates) over separate Lucene operators. Architects B and D prefer separate operators for composability. Proposed resolution: start with separate operators (Phase 2), add fusion rules (Phase 5) when profiling shows composition overhead matters.

---

## 3. Debate Summary

### Key Debates and Resolutions

**Debate 1: Separate Trino planner vs. Calcite convention/trait system**
- Architect A argued for porting Trino's physical planner wholesale
- Architect D argued for extending Calcite's existing convention/trait system
- **Resolution**: Calcite Volcano optimizer with `OpenSearchDistributedConvention` handles plan selection (D wins). Trino's planner is NOT ported. Calcite already has cost-based optimization, join reordering, predicate pushdown. Evidence: Flink, Dremio, Beam all use Calcite for planning with custom runtimes.

**Debate 2: Monolithic scan operator vs. separate Lucene-native leaf operators**
- Architect A proposed a single `LucenePageSourceOperator` with pushed predicates
- Architect B argued for separate `LuceneFilterScan`, `LuceneAggScan`, `LuceneSortScan` operators
- **Resolution**: Separate operators (B wins). Each is a Calcite physical operator with `computeSelfCost()`, allowing the Volcano optimizer to compare Lucene-native vs. generic strategies. A monolithic scan hides strategy choices from the optimizer.

**Debate 3: Full engine upfront vs. incremental scatter-gather first**
- Architects A, B, D proposed multi-layer architectures built in parallel
- Architect C argued for scatter-gather MVP first, then layering on complexity
- **Resolution**: Scatter-gather as Phase 1 delivery target (C wins on phasing). But built on Driver/Pipeline/Page infrastructure from day one (A wins on foundation), not on Iterator/ExprValue that would need rewriting.

**Debate 4: Iterator\<Page\> vs. Trino's push/pull Operator interface**
- Architect D initially proposed `execute() -> Iterator<Page>` for physical operators
- Architect A argued for Trino's `addInput(Page)/getOutput()/isBlocked()` push/pull hybrid
- **Resolution**: Trino's Operator interface (A wins). `isBlocked()` is essential for non-blocking exchange, cooperative multitasking, and memory pressure handling. `Iterator<Page>` cannot express backpressure.

**Debate 5: Row-based ExprValue for Phase 1 vs. Page/Block from day one**
- Architect C initially proposed using existing ExprValue/Iterator for Phase 1
- Architect B argued that Lucene DocValues are already columnar; converting to ExprValue then back is wasteful
- **Resolution**: Page/Block from day one (B wins). DocValues -> Block is near-zero-copy. DocValues -> ExprValue -> Block would be a double conversion.

**Debate 6: Where does Lucene-native operator selection happen?**
- Architect B proposed a post-optimization rule-based pass for leaf operator selection
- Architect D argued it should happen inside Volcano as ConverterRules with `computeSelfCost()`
- **Resolution**: Inside Volcano (D wins, B conceded after round 4). Leaf operators are Calcite physical operators that compete via cost. This allows the optimizer to compare DSL pushdown vs. Lucene-native distributed vs. generic distributed in a single search space.

### Convergence Points (All 4 Architects Agreed)

1. **Page/Block columnar format** as universal data model above the leaf layer
2. **Calcite for planning, Trino-inspired runtime for execution** (Flink/Dremio model)
3. **Lucene-native leaf operators** that use Weight/Scorer, Collector/DocValues, IndexSearcher.search
4. **TransportService-based exchange** (not HTTP) using OpenSearch's existing transport layer
5. **Scatter-gather as Phase 1**, full shuffle as Phase 2
6. **Driver/Pipeline/MemoryPool** from Trino for cooperative multitasking and memory safety
7. **Feature flag** to enable/disable distributed engine (default: enabled, fallback to DSL)
8. **Existing Calcite integration preserved unchanged** (CalciteRelNodeVisitor, rules, UDFs)

---

## 4. Consensus Design

### Recommended Architecture: Convergent 4-Layer Design

```
┌─────────────────────────────────────────────────────────────────┐
│ Layer 4: Calcite Logical Optimization (EXISTING - unchanged)    │
│   CalciteRelNodeVisitor -> RelNode -> Volcano pushdown rules    │
│   67K+ lines preserved: UDFs, type system, expression visitor   │
├─────────────────────────────────────────────────────────────────┤
│ Layer 3: Calcite Physical Plan Selection (NEW)                  │
│   OpenSearchDistributedConvention + distribution traits         │
│   Volcano compares: DSL pushdown vs scatter-gather vs shuffle   │
│   ConverterRules produce Lucene-native vs generic alternatives  │
│   Cost-based selection: broadcast join vs hash-partition join    │
├─────────────────────────────────────────────────────────────────┤
│ Layer 2: Physical Operators (NEW - AbstractRelNode subclasses)  │
│   Lucene-native (SHARD_LOCAL, per-segment):                     │
│     LuceneFilterScan, LuceneAggScan, LuceneSortScan,           │
│     LuceneFullScan                                              │
│   Exchange (distribution conversion):                           │
│     GatherExchange, HashExchange, BroadcastExchange             │
│   Generic (Page/Block, no Lucene dependency):                   │
│     OpenSearchHashAggregate, OpenSearchHashJoin,                │
│     OpenSearchWindow, OpenSearchMergeSort,                      │
│     OpenSearchFilter, OpenSearchProject                         │
├─────────────────────────────────────────────────────────────────┤
│ Layer 1: Trino-Inspired Execution Runtime (NEW)                 │
│   Driver: cooperative multitask loop processing Pages           │
│   Pipeline: parallel Drivers per shard (1 Driver per Split)     │
│   MemoryPool: revocable memory + circuit breaker integration    │
│   Page/Block: columnar data (LongBlock, DoubleBlock,            │
│               BytesRefBlock, BooleanBlock)                      │
│   Operator: isBlocked()/needsInput()/addInput()/getOutput()     │
│   Exchange: TransportService-based Page transfer                │
└─────────────────────────────────────────────────────────────────┘
```

### Contribution from Each Hypothesis

| Hypothesis | Key Contribution to Consensus |
|---|---|
| A: Full Trino Port | Driver/Pipeline execution model, MemoryPool with revocable memory, Page/Block format, Operator interface with `isBlocked()` |
| B: Hybrid Lucene-Native | Separate Lucene-native leaf operators (Weight/Scorer, Collector/DocValues, IndexSearcher.search), per-segment iteration contract |
| C: Scatter-Gather First | Scatter-gather as Phase 1, incremental delivery, reuse of OpenSearch transport/shard routing/thread pools |
| D: Calcite Physical Plan | Convention/trait system for cost-based strategy selection, single Volcano optimizer, preservation of existing Calcite integration |

### Pros and Cons of Recommended Solution

**Pros:**
- Preserves 67K+ lines of existing Calcite integration unchanged
- Leverages Lucene data structures (inverted index, BKD trees, DocValues) for O(K log N) filter/sort/agg vs O(N)
- Battle-tested execution primitives from Trino (Driver/Pipeline/MemoryPool/Page)
- Incremental delivery: scatter-gather works end-to-end in Phase 1
- Cost-based optimization automatically selects best strategy (DSL pushdown, scatter-gather, full shuffle)
- No external process required -- runs entirely inside OpenSearch nodes
- Apache 2.0 license compatibility for porting Trino code

**Cons:**
- Large implementation scope (~70+ core classes across all phases)
- Requires deep expertise in three domains simultaneously: Calcite internals, Lucene segment APIs, Trino execution model
- Two data models coexist during migration: ExprValue (legacy path) and Page/Block (new path)
- Cost model tuning for distributed plans requires iterative refinement with real workloads
- Memory management complexity: MemoryPool must coexist with OpenSearch's circuit breakers without conflicts
- Calcite Volcano optimizer overhead for large plan spaces may need pruning

### Alternatives Considered and Rejected

1. **Pure Trino Port (without Calcite planning)**: Rejected because it wastes the existing Calcite investment and requires reimplementing join reordering, predicate pushdown, aggregate splitting.
2. **Pure Calcite Execution (Enumerable/Janino)**: Rejected because Calcite's generated code is slower than hand-written operators and lacks cooperative scheduling, backpressure, and memory management.
3. **DSL-rewrite improvement (no new engine)**: Rejected because DSL fundamentally cannot express distributed joins, window functions, or multi-stage aggregations.
4. **External Trino process alongside OpenSearch**: Rejected by design requirement -- engine must run inside OpenSearch nodes.

---

## 5. Technology Challenges

### Challenge 1: Lucene Segment Access from Plugin Code
**Problem**: OpenSearch plugins access data through the Search API (NodeClient). Direct Lucene segment access (`IndexShard.acquireSearcher()`, `IndexReader`, `LeafReaderContext`) requires accessing OpenSearch internal APIs that may not be exposed to plugins.
**Mitigation**: The existing codebase already accesses Lucene internals through script factories (`CalciteFilterScriptLeafFactory` receives `LeafReaderContext`). The new operators follow the same pattern. May require additional extension points in OpenSearch core for direct segment enumeration.
**Risk**: Medium. May require OpenSearch core changes to expose shard-level `IndexSearcher` handles to the SQL plugin.

### Challenge 2: Page/Block Data Format Design
**Problem**: The Page/Block format must efficiently represent all OpenSearch field types (keyword, text, long, double, date, ip, geo_point, nested, object) while being serializable for exchange and compatible with Lucene DocValues types.
**Mitigation**: Start with core types (LongBlock, DoubleBlock, BytesRefBlock, BooleanBlock) that map directly to Lucene DocValues. Add complex types (nested, geo) incrementally. Use OpenSearch's `StreamOutput`/`StreamInput` for serialization.
**Risk**: Medium. Type mapping edge cases (multi-valued fields, nested objects) will require careful design.

### Challenge 3: Memory Management Coexistence
**Problem**: OpenSearch has circuit breakers for search, indexing, and request memory. The new MemoryPool must coexist without causing OOM or starving other operations.
**Mitigation**: Register a dedicated `"sql_query_memory"` circuit breaker. MemoryPool reserves memory from this breaker. Revocable memory spills to local transient storage when under pressure. Total memory budget configurable (default: 25% of heap).
**Risk**: High. Getting the memory split right between search, indexing, and SQL query execution requires careful tuning and production testing.

### Challenge 4: TransportService Exchange Reliability
**Problem**: Pages transferred between nodes via TransportService may be lost or delayed during node failures, network partitions, or cluster rebalancing.
**Mitigation**: Implement retry with exponential backoff. Use query-level timeout (existing `PPL_QUERY_TIMEOUT`). For long-running queries, implement checkpoint/restart at exchange boundaries. Shard routing changes during query execution detected via ClusterState listener.
**Risk**: Medium. The exchange protocol needs idempotent delivery semantics.

### Challenge 5: Calcite Convention Integration Complexity
**Problem**: Adding `OpenSearchDistributedConvention` with distribution traits to the Volcano optimizer increases the plan search space, potentially causing optimization timeouts for complex queries.
**Mitigation**: Use cost caps to prune expensive alternatives early. Limit convention exploration to queries that cannot be fully pushed down to DSL. Set Volcano iteration limits. Profile optimizer time and add short-circuit rules.
**Risk**: Medium. Requires iterative tuning of planner configuration.

### Challenge 6: Backward Compatibility
**Problem**: All existing Calcite integration tests, YAML REST tests, and doctests must continue passing with the new engine.
**Mitigation**: Feature flag (`plugins.sql.distributed_engine.enabled`, default: true). Fallback to DSL path if distributed execution fails. Run full test suite on every change. The existing EnumerableConvention path remains the default for queries that work today.
**Risk**: Low if feature flag is properly implemented. The existing path is unchanged.

---

## 6. Security Considerations

### 6.1 Index-Level Access Control
**Current state**: The DSL rewrite path executes queries through OpenSearch's Search API, which enforces index-level and document-level security (DLS/FLS) via the security plugin.
**Challenge**: Direct Lucene segment access bypasses the Search API security layer.
**Mitigation**: Before acquiring an `IndexSearcher` for a shard, validate permissions using the security plugin's `PrivilegesEvaluator`. Apply document-level security as a Lucene `Query` filter at the `LuceneFilterScan` level. Apply field-level security by restricting which columns the `LuceneFullScan` reads. This mirrors how OpenSearch's internal search operations apply DLS/FLS.

### 6.2 Transport-Level Security
**Current state**: OpenSearch TransportService already supports TLS/SSL for inter-node communication.
**Mitigation**: Exchange pages over the existing TransportService, which inherits TLS configuration. No new network endpoints are created. All exchange traffic uses the existing transport port with existing security settings.

### 6.3 Query Resource Limits
**Current state**: `PPL_QUERY_TIMEOUT` and `QUERY_SIZE_LIMIT` settings exist.
**Mitigation**: MemoryPool enforces per-query memory limits. Driver cooperative scheduling enforces CPU time limits. Circuit breaker integration prevents cluster-wide OOM. Admin-configurable limits for max concurrent distributed queries, max memory per query, max exchange buffer size.

### 6.4 Audit Logging
**Mitigation**: Log distributed query execution plans (stages, operators, shard assignments) in the existing slow query log. Include distributed execution metadata in `_explain` API responses.

### 6.5 Cross-Cluster Compatibility
**Mitigation**: The distributed engine is intra-cluster only (all nodes must run the same OpenSearch version with the SQL plugin). Cross-cluster queries fall back to the DSL rewrite path. No new external-facing endpoints are created.

---

## 7. Performance Analysis

### 7.1 Expected Performance Profile

| Query Type | Current (DSL Rewrite) | Phase 1 (Scatter-Gather) | Phase 2 (Full Shuffle) |
|---|---|---|---|
| Simple filter + project | Baseline (DSL pushdown) | Comparable or slightly better (Lucene-native filter avoids DSL overhead) | Same as Phase 1 |
| Filter + aggregate | DSL aggregation API | Partial agg at shard + merge at coordinator (comparable latency, better for high-cardinality groups) | Hash-partitioned agg (better for extreme cardinality) |
| Filter + sort + limit | DSL sort + size | Lucene IndexSearcher.search with SortField + merge-sort (comparable or better with early termination) | Same as Phase 1 |
| Multi-index join | NOT SUPPORTED (falls back to post-processing or fails) | Falls back to DSL path | Hash join or broadcast join (NEW capability) |
| Window functions | Limited (post-processing only) | Falls back to DSL path | Native window operator (NEW capability) |
| Complex nested aggregation | DSL nested aggs (limited) | Falls back to DSL path | Multi-stage aggregation (NEW capability) |

### 7.2 Latency Targets
- **Simple queries (filter+agg)**: No regression from DSL path (p50 and p99)
- **Complex queries (joins, windows)**: New capability -- no baseline to compare
- **Query planning overhead**: < 10ms additional for Calcite convention exploration
- **Exchange latency**: < 5ms per Page transfer between nodes (TransportService RPC)

### 7.3 Performance Risks and Mitigations
- **Risk**: Page/Block conversion overhead at leaf operators
  - **Mitigation**: DocValues are already columnar; Block construction is near-zero-copy
- **Risk**: Network overhead from exchange pages
  - **Mitigation**: Partial aggregation at leaf reduces data volume; Page serialization uses efficient binary format
- **Risk**: Memory overhead from Driver/Pipeline framework
  - **Mitigation**: Page batch size tunable (default 1024 rows); MemoryPool limits prevent unbounded growth
- **Risk**: Coordinator bottleneck in scatter-gather mode
  - **Mitigation**: Streaming merge (not buffered); partial aggregation reduces merge work; limit pushdown minimizes data transfer

---

## 8. Execution Plan

### Modules and Tasks

#### New Module: `distributed-engine/`
A new Gradle module containing the distributed query engine components.

**Phase 1: Scatter-Gather MVP**

| # | Task | Description | Dependencies |
|---|---|---|---|
| 1.1 | Page/Block data types | `Page`, `Block` (abstract), `LongBlock`, `DoubleBlock`, `BytesRefBlock`, `BooleanBlock`, `BlockBuilder` variants, `PagesSerde` (~15 classes) | None |
| 1.2 | Operator interface | `Operator` interface with `isBlocked()`/`needsInput()`/`addInput(Page)`/`getOutput()`/`finish()`/`isFinished()` | 1.1 |
| 1.3 | Driver/Pipeline framework | `Driver`, `DriverContext`, `Pipeline`, `PipelineContext`, `DriverFactory`, `TaskExecutor` (~8 classes) | 1.2 |
| 1.4 | MemoryPool (basic) | Memory tracking per operator, circuit breaker integration. No spill in Phase 1 (~4 classes) | 1.3 |
| 1.5 | LuceneFullScan operator | Read DocValues into Blocks per `LeafReaderContext`. Produces Pages | 1.1, 1.2 |
| 1.6 | LuceneFilterScan operator | `Weight`/`Scorer`/`DocIdSetIterator` for predicate evaluation. Produces filtered Pages | 1.5 |
| 1.7 | PartialAggregation operator | Collector-based partial aggregation at leaf. Emits partial aggregate Pages | 1.5, 1.6 |
| 1.8 | FinalAggregation operator | Merges partial aggregates at coordinator | 1.7 |
| 1.9 | MergeSort operator | k-way merge of pre-sorted shard results at coordinator | 1.1 |
| 1.10 | GatherExchange | TransportService-based: coordinator collects Pages from data nodes. `ShardQueryAction`, `ShardQueryRequest`, `ShardQueryResponse` (~4 classes) | 1.1, 1.3 |
| 1.11 | OpenSearchDistributedConvention | Calcite convention + `SHARD_LOCAL` distribution trait + `ShardScanRule` | 1.5, 1.6 |
| 1.12 | Execution mode router | Simple structural routing: single-index no-join -> scatter-gather, else -> DSL fallback | 1.11 |
| 1.13 | Feature flag + integration | `plugins.sql.distributed_engine.enabled` setting. Wire into `QueryService.executeWithCalcite()` | 1.12 |
| 1.14 | `_explain` API for distributed plans | Show stages, operators, shard assignments in explain output | 1.11 |

Phase 1 total: ~41 core classes, ~3,400-5,000 lines of new code.

**Phase 2: Full Shuffle + Joins**

| # | Task | Description | Dependencies |
|---|---|---|---|
| 2.1 | HashExchange | Hash-partitioned Page transfer over TransportService | 1.10 |
| 2.2 | BroadcastExchange | Broadcast Page transfer to all nodes | 1.10 |
| 2.3 | HashJoinOperator | Build hash table from build side, probe with probe side. On Page/Block data | 1.1, 1.2 |
| 2.4 | DistributedJoinRule | Calcite rule: LogicalJoin -> HashJoin with broadcast or hash-partition exchanges | 2.1, 2.2, 2.3 |
| 2.5 | DistributedAggregateRule | Calcite rule: LogicalAggregate -> partial + exchange + final | 2.1 |
| 2.6 | LuceneAggScan operator | Collector/LeafCollector with DocValues accumulation. Lucene-native aggregation | 1.5 |
| 2.7 | LuceneSortScan operator | IndexSearcher.search with SortField for early-termination top-K | 1.5 |
| 2.8 | MemoryPool revocable memory | Spill-to-disk for hash tables and sort buffers | 1.4 |
| 2.9 | Cost model (basic) | Table size estimates from IndexStats, network cost weights | 1.11 |

**Phase 3: Advanced Features**

| # | Task | Description | Dependencies |
|---|---|---|---|
| 3.1 | WindowOperator | ROW_NUMBER, RANK, LAG/LEAD over partitioned/sorted Page streams | 2.1 |
| 3.2 | Vectorized expression evaluation | SIMD-friendly evaluation on Block arrays | 1.1 |
| 3.3 | Dynamic filtering | Bloom filter from join build side to probe side scan | 2.3 |
| 3.4 | Advanced cost model | Term statistics, DocValues metadata, cluster topology awareness | 2.9 |
| 3.5 | Adaptive execution | Runtime plan changes based on actual vs estimated statistics | 3.4 |
| 3.6 | LocalExchange | Intra-node repartitioning for pipeline parallelism within a task | 1.3 |

### Test Plan

**Unit Tests (per module, JUnit 5)**
- Page/Block serialization round-trip tests
- Operator correctness tests with synthetic Pages
- Driver/Pipeline lifecycle tests
- MemoryPool reservation and release tests
- Exchange serialization/deserialization tests
- Calcite convention rule matching tests

**Integration Tests (extends existing framework)**
- ALL existing Calcite integration tests: `./gradlew :integ-test:integTest --tests '*Calcite*IT' -DignorePrometheus`
  - Must pass with distributed engine enabled (no regression)
  - Must pass with distributed engine disabled (DSL fallback)
- New distributed-specific integration tests:
  - Scatter-gather execution for filter+aggregate+sort+limit
  - Multi-shard execution correctness (compare against DSL path results)
  - Node failure during exchange (graceful degradation)
  - Memory pressure and circuit breaker interaction
  - Concurrent query execution

**YAML REST Tests**: `./gradlew yamlRestTest`
- All existing tests must pass unchanged
- New YAML tests for `_explain` with distributed plan output

**Documentation Tests**: `./gradlew doctest -DignorePrometheus`
- All existing doctests must pass unchanged

**Performance Tests**
- Benchmark suite comparing DSL path vs distributed path for representative queries
- Latency regression tests (p50/p99 must not degrade for simple queries)
- Memory consumption benchmarks under various workloads
- Scalability tests with increasing shard count and cluster size

### Team Size Plan

**Recommended team: 6-8 engineers**

| Role | Count | Responsibilities |
|---|---|---|
| **Tech Lead / Architect** | 1 | Overall design, cross-module integration, Calcite convention design, code reviews |
| **Execution Runtime Engineer** | 2 | Page/Block format, Operator interface, Driver/Pipeline, MemoryPool (from Trino) |
| **Lucene Integration Engineer** | 1-2 | LuceneFilterScan, LuceneAggScan, LuceneSortScan, LuceneFullScan, per-segment iteration, DocValues -> Block conversion |
| **Distributed Systems Engineer** | 1-2 | Exchange over TransportService (gather, hash, broadcast), ShardQueryAction, backpressure, fault tolerance |
| **Calcite Planning Engineer** | 1 | OpenSearchDistributedConvention, distribution traits, ConverterRules, cost model, optimizer integration |
| **Test / Integration Engineer** | 1 | Integration test framework, performance benchmarks, regression testing, CI/CD pipeline |

**Phase 1 (Scatter-Gather MVP)**: 4-5 engineers
**Phase 2 (Full Shuffle + Joins)**: Full team of 6-8 engineers
**Phase 3 (Advanced Features)**: 4-5 engineers (some rotation to other projects)

---

## 9. Skills and Tools Needed

### Required Skills
1. **Apache Calcite internals**: Convention/trait system, Volcano optimizer, ConverterRule, RelMetadataProvider, RelOptCostFactory. The team already has this expertise (67K+ lines of Calcite integration).
2. **Apache Lucene segment APIs**: IndexSearcher, Weight, Scorer, DocIdSetIterator, LeafReaderContext, DocValues, SortField, Collector/LeafCollector. The team already writes per-segment code (`CalciteFilterScriptLeafFactory`, `CalciteAggregationScriptLeafFactory`).
3. **Trino execution model**: Page/Block data format, Operator interface, Driver/Pipeline cooperative scheduling, MemoryPool with revocable memory. New skill -- requires studying Trino source code (Apache 2.0).
4. **OpenSearch plugin development**: TransportAction, TransportService, thread pool management, circuit breakers, ClusterState, Settings framework. Existing skill.
5. **Distributed systems fundamentals**: Hash partitioning, broadcast, shuffle, two-phase aggregation, merge-sort, backpressure, fault tolerance.

### Tools and Infrastructure
- **Build**: Gradle (existing). New `distributed-engine/` module added to `settings.gradle`.
- **Testing**: JUnit 5, OpenSearch test framework, existing integ-test infrastructure.
- **Performance**: JMH for micro-benchmarks, Rally or custom harness for end-to-end benchmarks.
- **Profiling**: async-profiler for CPU/memory analysis, Calcite plan tracing for optimizer debugging.
- **CI/CD**: Existing GitHub Actions workflows extended with distributed engine test targets.

### Reference Materials
- Trino source code: `github.com/trinodb/trino` (Apache 2.0) -- specifically `core/trino-main/src/main/java/io/trino/operator/`, `io/trino/execution/`, `io/trino/memory/`
- Apache Calcite documentation: convention/trait system, RelDistribution, Volcano optimizer
- Elasticsearch/OpenSearch internal search architecture: scatter-gather, transport framework
- Academic: "Volcano -- An Extensible and Parallel Query Evaluation System" (Graefe, 1994)
- Industry: "F1 Query: Declarative Querying at Scale" (VLDB 2018) -- scatter-gather architecture precedent

### Skills Directory
Path: `oss/treasuretoken/skills/trino-v2`

Recommended skill files to create:
- `trino-v2/page-block-format.md` -- Page/Block data type specification and coding guidelines
- `trino-v2/operator-interface.md` -- Operator contract (isBlocked, addInput, getOutput) with examples
- `trino-v2/driver-pipeline.md` -- Driver loop implementation guide
- `trino-v2/lucene-leaf-operators.md` -- How to write Lucene-native operators (Weight/Scorer, Collector, DocValues patterns)
- `trino-v2/exchange-protocol.md` -- TransportService-based exchange specification
- `trino-v2/calcite-convention.md` -- OpenSearchDistributedConvention design and rule authoring guide
- `trino-v2/memory-management.md` -- MemoryPool integration with circuit breakers
- `trino-v2/testing-guide.md` -- How to write and run distributed engine tests

---

## 10. Design Challenge: Reassessing the Consensus

### Architect B: The Case for Porting Trino's Operators Directly

The current consensus (Section 4) proposes building six custom "generic" operators above the Lucene leaf layer: `OpenSearchHashAggregate`, `OpenSearchHashJoin`, `OpenSearchWindow`, `OpenSearchMergeSort`, `OpenSearchFilter`, and `OpenSearchProject`. These are listed in the Layer 2 box of the consensus architecture diagram (lines 1194-1197) as "Generic (Page/Block, no Lucene dependency)" operators. I argue this is a fundamental mistake. These operators are data-source-agnostic by definition -- they operate purely on Page/Block columnar data with zero Lucene involvement. Trino already provides production-grade implementations of every one of them. Building custom versions is reinventing the wheel.

**The exact Trino operators we should port instead of building custom:**

| Consensus Custom Operator | Trino Equivalent | Trino Source Path | Approx LOC |
|---|---|---|---|
| `OpenSearchHashAggregate` | `HashAggregationOperator` + `SpillableHashAggregationBuilder` + `InMemoryHashAggregationBuilder` | `io.trino.operator.HashAggregationOperator` | ~800 (operator) + ~600 (builders) |
| `OpenSearchHashJoin` | `LookupJoinOperator` + `HashBuilderOperator` + `JoinProbe` + `LookupJoinPageBuilder` | `io.trino.operator.join.LookupJoinOperator` | ~700 (operator) + ~500 (builder) + ~400 (probe) |
| `OpenSearchWindow` | `WindowOperator` + `RegularWindowPartition` + `RegularWindowPartitioner` + `PagesIndex` | `io.trino.operator.WindowOperator` | ~600 (operator) + ~300 (partition) |
| `OpenSearchMergeSort` | `MergeSortedPages` + `MergeOperator` + `OrderByOperator` | `io.trino.operator.MergeSortedPages` | ~400 (merge) + ~300 (operator) |
| `OpenSearchFilter` + `OpenSearchProject` | `FilterAndProjectOperator` + `PageProcessor` | `io.trino.operator.project.FilterAndProjectOperator` | ~500 (combined) |

This is roughly 5,000-6,000 lines of battle-tested Trino code that already handles every edge case. Trino's `HashAggregationOperator` alone has dealt with hash table resizing, null-key handling, type-specific hash functions, partial and final aggregation modes, memory revocation callbacks, and spill-to-disk via `SpillableHashAggregationBuilder`. Its `LookupJoinOperator` manages the build/probe lifecycle, outer join null padding, semi-join and anti-join semantics, per-position output limiting to avoid memory blowup, and integration with dynamic filtering. The `WindowOperator` handles frame specification (ROWS, RANGE, GROUPS), partition boundary detection, peer group tracking, and spill for large partitions.

**The user's challenge is correct: this is reinventing the wheel.** Every problem that `OpenSearchHashJoin` will encounter -- how to size the hash table, when to spill, how to handle skewed partitions, how to manage memory for the build side, how to implement all seven join types (INNER, LEFT, RIGHT, FULL, SEMI, ANTI, CROSS) -- has already been solved in Trino's `LookupJoinOperator`. That operator has been hardened across 10+ years of production use at Facebook/Meta (as Presto), then Trino, across petabyte-scale workloads. The `HashAggregationOperator` has survived every conceivable GROUP BY edge case: high-cardinality groups that exhaust memory (triggering spill), low-cardinality groups that fit in cache, null group keys, multi-column group keys with mixed types, and partial aggregation that adaptively disables itself when the reduction ratio is poor (a subtlety that would take months to discover and implement from scratch).

**The only OpenSearch-specific operators needed are the Lucene leaf operators.** The consensus already identifies these correctly: `LuceneFilterScan`, `LuceneAggScan`, `LuceneSortScan`, `LuceneFullScan`. These read from Lucene segments using Weight/Scorer, Collector/DocValues, and IndexSearcher.search -- APIs that are unique to OpenSearch's storage layer. Everything above this layer is pure data computation on columnar pages. There is nothing OpenSearch-specific about hashing rows for a GROUP BY, building a hash table for a join probe, or evaluating window functions over sorted partitions. These are universal relational algebra operations on columnar data.

**Cost comparison -- port vs. custom build:**

Porting Trino's operators means adapting ~5,000-6,000 lines of proven code to our Page/Block format (which the consensus already aligns with Trino's). The adaptation work is primarily: (1) adjusting import paths, (2) replacing Trino's `TypeOperators` with our type system bridge, (3) wiring `OperatorContext` memory tracking to our MemoryPool. The operators' core algorithms -- hash table management, spill logic, join probe mechanics, window frame evaluation -- remain unchanged.

Building custom operators means writing ~8,000-12,000 lines of new code (the implementations will be simpler initially, but will grow as edge cases emerge), plus discovering and fixing every bug that Trino's community has already fixed over a decade. The initial implementation might look simpler, but production hardening doubles or triples the code. Trino's `LookupJoinOperator` started simple too -- its current complexity reflects real-world requirements discovered through years of production failures.

**Long-term maintenance cost is the decisive argument.** If we build `OpenSearchHashJoin`, we own every hash table bug, every memory leak, every OOM under skew, every null-handling edge case, every spill-to-disk corruption. If we port Trino's `LookupJoinOperator`, the Trino community has 500+ contributors who have already found and fixed these problems. We can periodically re-sync with upstream improvements. The `SpillableHashAggregationBuilder` alone has had dozens of bug fixes related to memory accounting accuracy, spill file cleanup, and concurrent access -- bugs we would need to independently discover and fix if we write our own.

**The "custom operator" naming in the consensus reveals a conceptual error.** Naming them `OpenSearchHashAggregate` and `OpenSearchHashJoin` implies they are OpenSearch-specific. They are not. They are generic relational operators on columnar data. The "OpenSearch" prefix creates a false sense that these operators need OpenSearch-specific behavior. They do not. A hash join is a hash join, whether the pages came from Lucene segments, Parquet files, or PostgreSQL. The only OpenSearch-specific adaptation is at the leaf (reading from Lucene) and at the transport (exchange over TransportService). Everything between those two boundaries is data-source-agnostic and should come from a proven implementation.

**Concrete proposal -- revised Layer 2:**

```
Layer 2: Physical Operators (revised)
  Lucene-native (SHARD_LOCAL, per-segment) -- KEEP AS-IS:
    LuceneFilterScan, LuceneAggScan, LuceneSortScan, LuceneFullScan
  Exchange (distribution conversion) -- KEEP AS-IS:
    GatherExchange, HashExchange, BroadcastExchange
  Generic operators -- PORT FROM TRINO, do NOT build custom:
    HashAggregationOperator (from io.trino.operator.HashAggregationOperator)
    LookupJoinOperator + HashBuilderOperator (from io.trino.operator.join.*)
    WindowOperator (from io.trino.operator.WindowOperator)
    MergeSortedPages + OrderByOperator (from io.trino.operator.MergeSortedPages)
    FilterAndProjectOperator (from io.trino.operator.project.*)
    TopNOperator (from io.trino.operator.TopNOperator)
```

The consensus already agrees to port Driver, Pipeline, MemoryPool, Page, and Block from Trino (~40 classes). I am arguing we extend this to include the operators themselves -- an additional ~15-20 classes. The total port grows from ~40 classes to ~55-60 classes, but we eliminate ~8,000-12,000 lines of custom operator code that would need to be written, tested, debugged, and maintained indefinitely. The license is Apache 2.0. The Page/Block format is already aligned. The Operator interface is already agreed upon. There is no technical barrier to porting the operators alongside the runtime.

**Addressing the likely counter-argument: "Trino operators have too many dependencies."** Trino's operators depend on Trino's Page/Block/Type system, OperatorContext for memory tracking, and DriverContext for lifecycle management. The consensus ALREADY ports all of these. The operators sit on top of exactly the infrastructure we are already committed to building. The dependency chain is: `Operator -> OperatorContext -> DriverContext -> Pipeline -> Driver -> MemoryPool` -- all of which are in the consensus Phase 1 class list. Adding the concrete operator implementations on top of this foundation is incremental, not architectural.

### Architect D: Quantifying the Trino Port — Dependency Analysis

The consensus design claims ~70 core classes for a "Trino-inspired" execution runtime. The user challenges this by arguing we should use battle-tested solutions rather than reinventing the wheel. Architect B argues we should port operators directly. This analysis demands specifics: what does porting Trino's execution layer actually entail, class by class, and what are the hidden costs?

#### 1. The Actual Size of Trino's Execution Layer

Trino's execution engine spans multiple packages in `core/trino-main/src/main/java/io/trino/`. Here are the measured file counts from the actual repository (trinodb/trino, commit 5f1e962):

| Package | Files | Subdirectories | Key Contents |
|---|---|---|---|
| `operator/` | 168 | 12 | All operators, Driver, DriverContext, OperatorContext |
| `operator/join/` | ~38 | 1 | LookupJoinOperator, HashBuilderOperator, JoinHash, spill support |
| `operator/exchange/` | (subdir) | -- | DirectExchangeClient, exchange buffer management |
| `operator/aggregation/` | (subdir) | -- | Aggregation accumulators, builders, partial agg |
| `operator/window/` | (subdir) | -- | Window operator and window function execution |
| `execution/` | 123 | 6 | SqlTask, SqlTaskExecution, TaskStateMachine, SplitRunner, MemoryRevokingScheduler |
| `execution/scheduler/` | ~45 | 2 | PipelinedQueryScheduler (79KB!), StageExecution, NodeScheduler |
| `execution/buffer/` | (subdir) | -- | OutputBuffer, PagesSerde, partitioned/broadcast buffers |
| `memory/` | 23 | 0 | MemoryPool, QueryContext, ClusterMemoryManager, LowMemoryKiller |
| `sql/planner/` | 75 | 6 | PlanFragmenter, LocalExecutionPlanner, PartitioningScheme |

**Total: 470+ Java files** across the execution engine alone, excluding the type system (`spi/`), connector interfaces, and the SQL parser/analyzer.

#### 2. The Context Hierarchy — The Real Dependency Bottleneck

The consensus hand-waves "Driver + DriverContext (~2 classes)" and "Pipeline + PipelineContext (~2 classes)." The actual dependency chain from the source code tells a different story.

**Driver.java** imports and depends on:
- `DriverContext` -> `PipelineContext` -> `TaskContext` -> `QueryContext` -> `MemoryPool`
- `OperatorContext` -> `MemoryTrackingContext` -> `AggregatedMemoryContext` -> `LocalMemoryContext`
- `SplitAssignment`, `ScheduledSplit`, `Split` (from metadata layer)
- `SourceOperator` interface
- `DriverYieldSignal` (cooperative scheduling)
- `OperationTimer` / `OperationTiming` (stats infrastructure)

**OperatorContext.java** (~500 lines) depends on:
- `DriverContext` (parent in the context hierarchy)
- `MemoryTrackingContext` with internal `InternalLocalMemoryContext` and `InternalAggregatedMemoryContext` wrappers
- `OperatorSpillContext` -> `DriverContext.reserveSpill()`
- `PlanNodeId` (from the planner layer -- Trino's PlanNode, not Calcite's RelNode)
- `Session` (Trino's session object, deeply threaded throughout)
- `QueryContextVisitor` (visitor for memory accounting tree)
- `OperatorStats`, `OperatorInfo`, `BlockedReason`, `Metrics`, `TDigestHistogram`

**The context hierarchy:**
```
QueryContext (1 per query, holds MemoryPool reference)
  -> TaskContext (1 per node-task)
    -> PipelineContext (1 per pipeline within a task)
      -> DriverContext (1 per driver)
        -> OperatorContext (1 per operator)
```

Each layer adds memory tracking, stats collection, and lifecycle management. You cannot port `Driver` without `DriverContext`, which requires `PipelineContext`, which requires `TaskContext`, which requires `QueryContext`. **That is 5 classes forming an inseparable chain** before you write a single operator. And each class is 300-500 lines, not trivial data holders.

#### 3. HashAggregationOperator — The 15-Class Iceberg

Architect B proposes porting `HashAggregationOperator` directly. Its dependency tree from the source:

```
HashAggregationOperator
  -> HashAggregationBuilder / SpillableHashAggregationBuilder
    -> GroupByHash (abstract)
      -> FlatGroupByHash / BigintGroupByHash / NoChannelGroupByHash
        -> FlatHash -> FlatHashStrategy -> FlatHashStrategyCompiler (bytecode generation!)
    -> Accumulator / AccumulatorFactory (aggregation function interface)
    -> PartialAggregationController (adaptive partial agg)
    -> OperatorContext.aggregateRevocableMemoryContext() (revocable memory for spill)
    -> SpillContext -> Spiller -> FileSingleStreamSpiller / GenericPartitioningSpiller
```

The `FlatHashStrategyCompiler` **generates bytecode at runtime** for type-specialized hash functions. This pulls in Trino's bytecode generation infrastructure (`MethodHandle`, `CallSiteBinder`, ASM classes). This is not a simple port.

**Minimum viable HashAggregation: ~15 classes** (operator, builders, group-by hash variants, accumulators, spill context). The bytecode compilation can be replaced with a simpler reflection-based approach, but then you lose the "battle-tested" performance characteristics.

#### 4. LookupJoinOperator — The 38-File Subsystem

The join package is the largest single subsystem:

- `LookupJoinOperator` -> `LookupJoinOperatorFactory` -> `JoinBridgeManager`
- `HashBuilderOperator` (~700 lines, builds hash table from build side)
- `JoinHash` -> `JoinHashSupplier` -> `PagesHash` (BigintPagesHash / DefaultPagesHash)
- `LookupSource` / `LookupSourceFactory` / `PartitionedLookupSourceFactory` (~21K bytes)
- `PositionLinks` / `ArrayPositionLinks` / `SortedPositionLinks`
- `PageJoiner` / `DefaultPageJoiner` (~20K bytes, core join logic)
- `JoinFilterFunction` / `InternalJoinFilterFunction`
- `SpillingJoinProcessor` / `SpilledLookupSourceHandle`
- `LookupOuterOperator` (for OUTER join rows)
- `NestedLoopJoinOperator` / `NestedLoopBuildOperator` (cross joins)

**The join package alone contains 38 files plus an `unspilled/` subdirectory.** Minimum viable LookupJoin without spill: ~20 classes. With spill support (needed for production): ~30 classes.

#### 5. The RelNode-to-PlanNode Conversion Problem

This is the elephant in the room that neither the consensus nor Architect B addresses. Trino's operators are built by `LocalExecutionPlanner`, which traverses a **PlanNode** tree (Trino's IR), not a RelNode tree (Calcite's IR). The two are structurally different:

| Aspect | Calcite RelNode | Trino PlanNode |
|---|---|---|
| Plan representation | Immutable RelNode graph with RelTraitSet | Immutable PlanNode tree with node-specific properties |
| Distribution | RelDistribution trait | PartitioningScheme (explicit) |
| Exchange | No first-class exchange nodes | ExchangeNode / RemoteSourceNode |
| Fragmentation | Not built-in | PlanFragmenter splits at exchange boundaries |
| Operator mapping | Convention-based (ConverterRule) | Visitor-based (LocalExecutionPlanner) |

If we port Trino's operators, we need an equivalent of `LocalExecutionPlanner` to translate Calcite physical RelNodes into operator chains. In Trino, this is a ~3,000+ line visitor. It handles: mapping each PlanNode type to an OperatorFactory, creating PipelineContexts with correct partitioning, setting up LocalExchange for intra-task repartitioning, wiring join bridges between build and probe pipelines, and configuring spill settings per operator.

The consensus says "Calcite convention rules produce physical operators." But those rules produce Calcite RelNode objects with `execute()` methods -- they do not produce Trino Operator instances. Someone must write the bridge. Whether this is a visitor, a factory, or inline in convention rules, it is a non-trivial translation layer that does not exist in either codebase today.

#### 6. Quantified Porting Cost — Honest Numbers

| Subsystem | Consensus Estimate | Actual Minimum (from source analysis) | Delta |
|---|---|---|---|
| Page/Block data types | ~15 classes | ~15 classes | Accurate |
| Driver/Pipeline/Context hierarchy | ~8 classes | ~18-20 classes (Driver, DriverContext, PipelineContext, TaskContext, QueryContext, DriverFactory, DriverYieldSignal, OperationTimer, TaskExecutor, SplitRunner, TaskStateMachine, SqlTask, SqlTaskExecution, etc.) | +10-12 |
| MemoryPool | ~4 classes | ~8-10 classes (MemoryPool, MemoryTrackingContext, AggregatedMemoryContext, LocalMemoryContext, QueryContext integration, MemoryRevokingScheduler) | +4-6 |
| Transport/Exchange | ~4 classes | ~10-12 classes (OutputBuffer types, ExchangeClient, PagesSerde, serialization, backpressure) | +6-8 |
| Phase 1 operators | ~10 classes | ~10 classes (simple operators are small) | Accurate |
| RelNode-to-Operator bridge | 0 (not mentioned) | ~5-8 classes | +5-8 |
| **Phase 1 total** | **~41 classes** | **~65-80 classes** | **+24-39** |
| Phase 2 (joins + shuffle) | "adds" (unquantified) | ~50-60 additional classes | -- |
| **Full engine total** | **~70 classes** | **~130-150 classes** | **~2x** |

#### 7. The Devil's Advocate Verdict

**The user's three points, re-evaluated against the data:**

**Point 1 ("67K lines preserved is not important"):** Correct. The 67K lines are Calcite integration (parser, analyzer, UDFs). These are upstream of the execution engine and unaffected by either approach. This is not a differentiator.

**Point 2 ("Focus on long-term goals"):** This cuts both ways. If the long-term goal is a production-grade distributed execution engine, then half-porting Trino (simplified contexts, no spill, no bytecode compilation) delivers a prototype, not a production system. The long-term goal either demands the full dependency tree or demands a clean-room design that fits OpenSearch natively.

**Point 3 ("Use battle-tested solutions"):** The strongest argument, but the data shows that "battle-tested" is not a binary property. You can port Page/Block faithfully (they are self-contained). You can port the Operator interface (it is trivial). But the Driver/Context hierarchy, the memory management, the operators -- these are battle-tested as a *system*, not as individual components. Cherry-picking classes from Trino and grafting them onto OpenSearch's infrastructure means the battle-testing applies to the algorithms (hash join logic, aggregation builders) but NOT to the integration (memory accounting, lifecycle management, error handling, stats collection).

**The honest framing:** The team has three real options:

1. **Faithful port (~150 classes):** Port the full dependency tree. Get genuine battle-testing. Pay the integration cost of adapting Trino's Session, Metadata SPI, and HTTP exchange to OpenSearch equivalents. This is the user's "battle-tested" approach taken seriously.

2. **Inspired-by-Trino (~70-80 custom classes):** Use Trino as a blueprint but write new code. Lose battle-testing. Gain clean OpenSearch integration. This is what the consensus actually proposes, despite calling it a "Trino port."

3. **Selective port (~100 classes):** Port Page/Block and operators faithfully. Write simplified context hierarchy and memory management that integrates with OpenSearch circuit breakers. This is a middle ground that gets the most battle-tested value (the algorithm-heavy operators) while accepting the integration risk on the framework.

Option 3 is what Architect B is arguing for, and the data supports it as the best tradeoff -- provided the team acknowledges that the context hierarchy and memory management will be custom code, not ported code.

### Architect C: The Calcite Boundary — Where Logical Meets Physical

The user's challenge raises three points: (1) "preserves 67K+ lines" is not a compelling argument, (2) we should focus on long-term goals, and (3) we should use battle-tested solutions. I partially agree. But the user's own argument about battle-tested solutions, when applied rigorously, draws a sharper line than the user may have intended. It supports keeping Calcite for logical optimization while removing it from physical planning -- and this distinction is exactly what Flink and Dremio learned through production experience.

**Where Calcite is genuinely battle-tested: logical optimization.**

Calcite's Volcano optimizer for join reordering, predicate pushdown, constant folding, projection pruning, subquery decorrelation, and aggregate splitting is used in production by Apache Flink (processing petabytes daily at Alibaba, ByteDance, Uber), Dremio (enterprise data lakehouse), Apache Beam (Google Cloud Dataflow), and Apache Drill. This is not "preserving investment" for its own sake -- it is recognizing that rewriting a cost-based join reorderer and predicate pushdown framework from scratch is the kind of wheel-reinvention the user warns against. Our existing `CalciteRelNodeVisitor` (`core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java`), the optimizer rules in `opensearch/src/main/java/org/opensearch/sql/opensearch/planner/rules/`, and the UDF/type system integration represent genuine logical optimization infrastructure. The user is correct that tests will be kept and pushdown rules are a small part. But the Calcite integration is more than pushdown rules -- it includes the entire logical plan representation, the expression type system, join reordering, aggregate decomposition, and constant folding. These are the parts worth keeping because they are what Calcite was designed for.

**Where the previous consensus overextended Calcite: physical plan selection.**

Here I agree with the user's challenge. The consensus design proposes `OpenSearchDistributedConvention`, distribution traits, and `ConverterRules` inside Calcite's Volcano optimizer for physical plan selection (Layer 3 in the 4-layer design). This pushes Calcite beyond its proven domain. Calcite's convention/trait system was designed for single-node plan optimization with abstract physical properties, not for distributed physical planning -- deciding hash partitioning vs. broadcast, pipeline fragmentation, exchange placement, or stage boundaries.

The evidence is in what Flink and Dremio actually do at this boundary:

**Flink's architecture proves the boundary.** Flink uses Calcite conventions (`FlinkLogicalRel`, `FlinkPhysicalRel`) inside the Volcano optimizer, but these produce an intermediate physical RelNode tree -- not an executable plan. The actual execution planning happens in a separate layer: each `FlinkPhysicalRel` implements `translateToExecNode()` which converts to Flink's own `ExecNode` graph. The `ExecNode` layer handles parallelism decisions, shuffle strategy, operator chaining, and resource configuration. The `ExecNodeGraphGenerator` builds an `ExecNodeGraph` compiled to a `StreamGraph`/`JobGraph` by Flink's runtime. Flink does NOT use Calcite conventions for distribution, scheduling, or execution. Calcite's role ends after physical operator selection.

**Dremio follows the same pattern.** Dremio uses Calcite for logical optimization, then converts to physical `Prel` nodes that extend Calcite `RelNode` but add distribution traits. However, actual distributed execution uses Dremio's own `PhysicalOperator` tree, its own `FragmentWrapper` for plan fragmentation, and its own Arrow-based execution engine. The Calcite physical plan is an intermediate representation that gets translated.

**The handoff boundary I propose:**

```
Phase 1 (Calcite - KEEP): PPL/SQL -> AST -> Calcite LogicalRelNode
Phase 2 (Calcite - KEEP): Volcano optimizer applies logical rules:
    - join reordering, predicate pushdown, aggregate splitting,
    - constant folding, projection pruning, subquery decorrelation
    - Output: optimized Calcite LogicalRelNode tree
Phase 3 (NEW - converter): RelNodeToPlanNodeConverter
    - Walks the optimized LogicalRelNode tree
    - Converts each RelNode to a Trino-style PlanNode
    - Inserts ExchangeNodes based on distribution requirements
    - Consults ClusterState for shard counts and data locality
    - Output: PlanNode tree with ExchangeNodes
Phase 4 (NEW - Trino-style): PlanFragmenter
    - Splits PlanNode tree at ExchangeNode boundaries
    - Assigns fragments to nodes via shard routing
    - Output: List<StageFragment> with node assignments
Phase 5 (NEW - Trino-style): Execution via Driver/Pipeline/Page
```

Concrete interface classes:

```java
// The handoff: Calcite's optimized logical plan -> Trino-style physical plan
class RelNodeToPlanNodeConverter {
    PlanNode convert(RelNode optimizedLogical, PlanningContext ctx);
    // Dispatches by RelNode type:
    //   LogicalFilter    -> FilterNode
    //   LogicalAggregate -> AggregationNode (partial/final split + exchange)
    //   LogicalJoin      -> JoinNode (broadcast vs hash-partition decision)
    //   LogicalTableScan -> LuceneTableScanNode
    //   LogicalSort      -> SortNode / TopNNode
    //   LogicalProject   -> ProjectNode
}

// Plan nodes in Trino's style (NOT Calcite RelNodes)
abstract class PlanNode {
    List<PlanNode> getSources();
    List<Symbol> getOutputSymbols();
}

class ExchangeNode extends PlanNode {
    enum Type { GATHER, REPARTITION, REPLICATE }
    PartitioningScheme partitioning;
}

class LuceneTableScanNode extends PlanNode {
    String index;
    Optional<Query> pushedPredicate;
    List<String> projectedColumns;
}

// Fragmentation at exchange boundaries
class StageFragment {
    int stageId;
    PlanNode root;
    PartitioningScheme outputPartitioning;
    List<ShardRouting> assignments;
}

class PlanFragmenter {
    List<StageFragment> fragment(PlanNode plan, ClusterState state);
}
```

**Why this boundary is correct:**

1. **Calcite's cost model fits logical decisions.** Predicate pushdown, join reordering, aggregate decomposition -- these reason about cardinality and selectivity, not network topology or memory budgets.

2. **Distribution decisions need runtime context Calcite lacks.** Shard counts, data locality, memory pressure, network bandwidth -- these require `ClusterState` and runtime statistics that do not fit Calcite's `RelMetadataProvider`. Trino's `PlanFragmenter` and `NodeScheduler` were designed for these decisions.

3. **Clean translation avoids dual representation.** Without this boundary, every operator must exist as both a Calcite `RelNode` subclass (for optimization) and a Trino `Operator` (for execution). The converter lets each operator exist once, with a thin translation between the two worlds.

4. **This matches Flink and Dremio's proven architecture.** Flink's `translateToExecNode()` is this exact boundary. We follow the pattern that has been validated at scale.

**Responding to Architect D's quantification:**

Architect D correctly identifies that the porting cost is higher than the consensus estimates (~130-150 classes vs. ~70). This supports my argument: the physical planning layer (Layer 3) should NOT add more Calcite complexity on top of an already large port. Removing the `OpenSearchDistributedConvention` and its associated `ConverterRules` from Calcite eliminates one of the most complex integration surfaces. The `RelNodeToPlanNodeConverter` is a simpler, more direct translation that Architect D's analysis shows is needed regardless (the "RelNode-to-PlanNode conversion problem" of Section 5).

**Responding to Architect B's operator porting argument:**

I agree with Architect B that generic operators should be ported from Trino rather than written from scratch. My argument is complementary: Architect B addresses what operators to use (Trino's), while I address how the plan reaches those operators (through an explicit Calcite-to-Trino converter, not through Calcite conventions). The two positions combine: Calcite optimizes the logical plan, the converter translates to PlanNodes, the fragmenter distributes across nodes, and ported Trino operators execute on Page/Block data.

**What this means for the consensus layers:**

- **Layer 4 (Calcite Logical): KEEP unchanged.** All existing rules, UDFs, type system.
- **Layer 3 (Physical Planning): REPLACE.** Remove `OpenSearchDistributedConvention` and traits. Add `RelNodeToPlanNodeConverter` + `PlanFragmenter`. Distribution driven by `ClusterState`, not Calcite traits.
- **Layer 2 (Physical Operators): Port from Trino** per Architect B. `PlanNode` subclasses, not `RelNode` subclasses.
- **Layer 1 (Runtime): KEEP.** Driver/Pipeline/MemoryPool/Page/Block from Trino.

### Architect A: The Case for Porting Trino's Physical Planner

The user raised three challenges to the consensus design. After examining the codebase line counts and Trino's source code in detail, I believe all three are well-founded. The consensus Layer 3 (Calcite Physical Plan Selection) proposes reinventing a distributed planner from scratch inside Calcite's convention/trait system. We should instead port Trino's proven physical planning pipeline.

**Point 1: The "67K+ lines preserved" claim is hollow.**

The consensus states "Preserves 67K+ lines of existing Calcite integration unchanged" as a top-level Pro. I counted what those lines actually contain.

The `core/` module has ~60,000 lines: `CalciteRelNodeVisitor` (3,694 lines -- AST-to-RelNode), `CalciteRexNodeVisitor` (782 lines -- expression translation), the expression system under `core/.../expression/` (~22,900 lines of UDFs, type system, function resolution), utility classes (`PlanUtils`, `OpenSearchTypeFactory`, `PPLOperandTypes`, span/binning handlers), and PPL-specific Calcite rules (`PPLAggregateConvertRule`, `PPLSimplifyDedupRule`). The `opensearch/` module adds ~24,000 lines: storage access, request building, script factories, and pushdown rules.

None of these change regardless of whether we use Calcite or Trino for physical planning. They produce and consume `RelNode`/`RexNode` at the logical level. Physical plan selection is entirely downstream.

What actually differs is the pushdown rules in `opensearch/.../planner/rules/`: `FilterIndexScanRule` (79 lines), `AggregateIndexScanRule` (250), `SortIndexScanRule` (71), `ProjectIndexScanRule` (126), `LimitIndexScanRule` (101), `SortExprIndexScanRule` (270), `DedupPushdownRule` (167), `RareTopPushdownRule` (107), `RelevanceFunctionPushdownRule` (122), plus `EnumerableIndexScanRule` (54), `EnumerableSystemIndexScanRule` (50), `EnumerableNestedAggregateRule` (65), and `OpenSearchIndexRules` (81). That is 18 files totaling **2,109 lines**.

The honest framing: "We are preserving 2.1K lines of pushdown rules at the cost of building a custom distributed planner from scratch." The other ~65K lines are preserved identically under both approaches.

**Point 2: Trino's physical planner is battle-tested; the consensus reinvents it.**

The consensus Layer 3 proposes building: `OpenSearchDistributedConvention`, distribution traits (`SHARD_LOCAL`, `HASH_PARTITIONED`, `BROADCAST`, `SINGLE`), `ConverterRules` that produce physical alternatives, cost functions (`computeSelfCost()`) for strategy comparison, and a Volcano search space for all combinations. The document acknowledges this as Challenge 5: "Adding OpenSearchDistributedConvention with distribution traits to the Volcano optimizer increases the plan search space, potentially causing optimization timeouts."

Trino solves this problem with four production-proven classes:

1. **`AddExchanges`** (~1,700 lines, `io.trino.sql.planner.optimizations.AddExchanges`): Walks the logical plan and inserts `ExchangeNode` boundaries based on operator distribution requirements. Decides hash-partition vs. broadcast vs. gather vs. replicate based on data properties and join strategies. For joins, evaluates repartitioning both sides, broadcasting the small side, or co-locating on existing partitioning. This is what the proposed `ConverterRules` would reimagine inside Volcano.

2. **`AddLocalExchanges`** (~1,100 lines, `io.trino.sql.planner.optimizations.AddLocalExchanges`): Inserts intra-fragment local exchanges for pipeline parallelism within a single node. Ensures data arriving at `HashAggregationOperator` is hash-partitioned on group keys and data at `WindowOperator` is partitioned and sorted correctly. The consensus does not address intra-fragment parallelism.

3. **`PlanFragmenter`** (~850 lines, `io.trino.sql.planner.PlanFragmenter`): Cuts the plan at `ExchangeNode` boundaries into `SubPlan`/`PlanFragment` objects. Creates `RemoteSourceNode` references, manages cross-fragment partitioning schemes, validates stage count limits. Does deterministically in one pass what the consensus assumes Volcano will produce via combinatorial convention/trait exploration.

4. **`PipelinedQueryScheduler`** (~1,400 lines, `io.trino.execution.scheduler.PipelinedQueryScheduler`): Maps fragments to stages, assigns splits (shards) to workers, manages stage lifecycle (PLANNED -> RUNNING -> FLUSHING -> FINISHED), handles failure recovery, coordinates dynamic filtering. Creates per-fragment scheduler types: `SourcePartitioned`, `FixedCount`, `FixedSourcePartitioned`, or `ScaledWriter`. The consensus does not address scheduling.

These four classes total ~5,050 lines of code running at petabyte scale for over a decade.

The document cites Flink and Dremio as precedent for "Calcite for planning with custom runtimes." But neither uses Calcite conventions for physical distribution decisions. Flink uses Calcite for logical optimization then has its own `StreamGraphGenerator`/`BatchExecNode` for distribution. Dremio uses Calcite logically but has its own `PhysicalPlanCreator`. No major production system uses Volcano convention exploration for exchange insertion and data partitioning decisions. They all have deterministic physical planners downstream of Calcite logical optimization -- exactly what porting Trino's planner provides.

**Point 3: Long-term cost favors porting over inventing.**

Building `OpenSearchDistributedConvention` with custom traits and cost functions means owning every line forever. Bugs in join strategy selection, exchange placement, or cost estimation require debugging novel code with no reference. New distribution strategies (range partitioning, co-located joins) must be designed from first principles.

Porting Trino's planner gives us an upstream reference. Trino's community continuously improves these classes: `EventDrivenFaultTolerantQueryScheduler` for fault-tolerant execution, improved join distribution based on statistics, adaptive partial aggregation. We can check upstream for bug fixes and study feature implementations.

**Responding to Architect D's dependency analysis.**

Architect D correctly identifies that `AddExchanges` depends on `PreferredProperties`, `ActualProperties`, `StreamPropertyDerivations`, and the `Metadata` SPI. The adaptation cost is real. But it is *bounded and well-understood work* -- adapting interfaces to OpenSearch equivalents (`Metadata.getTableProperties()` maps to index metadata from `ClusterState`, partition properties map to shard routing). Building a novel Calcite convention system is *open-ended research* with no production precedent at this scale.

Architect D's Option 3 (selective port of ~100 classes) and Architect C's `RelNodeToPlanNodeConverter` proposal align with my position: use Calcite for logical optimization, then hand off to Trino-derived code for physical distribution. The key disagreement is whether Layer 3 should be a Calcite convention (consensus) or a Trino-derived planner (my argument). The data points all one direction: Calcite conventions are unproven for distribution, Trino's planner is proven.

**The clean boundary:**

- **Layer 4 (Calcite Logical): KEEP.** All ~65K lines: visitors, UDFs, type system, PPL rules. Unchanged under both approaches.
- **Layer 3 (Physical Planning): PORT from Trino.** `AddExchanges` (adapted), `PlanFragmenter`, scheduler. Remove `OpenSearchDistributedConvention` and traits. ~5K lines of ported code replaces an unknown amount of novel convention/trait/ConverterRule code.
- **Layer 2 (Operators): PORT from Trino** per Architect B. `HashAggregationOperator`, `LookupJoinOperator`, `WindowOperator`, etc.
- **Layer 1 (Runtime): KEEP** consensus. Driver/Pipeline/MemoryPool/Page/Block.

**Conclusion.** The 67K number is misleading (~65K untouched by either approach). The consensus reinvents distribution planning solved by ~5K lines of hardened Trino code. Long-term cost favors porting. The user is right.

---

### Synthesis: Revised Consensus After Challenge Round

All 4 architects converge on a revised architecture that accepts the user's three challenges. Here is what changed and why.

#### User's Points — Verdict

**Point 1: "67K preservation is not important."** — **ACCEPTED unanimously.**
Architect A quantified this precisely: the 67K lines are CalciteRelNodeVisitor (3,694), expression system (~22,900), UDFs, type system, storage access, and request building. **None of these change regardless of approach.** The only lines affected by the physical planning choice are ~2,109 lines of pushdown rules (18 files in `opensearch/.../planner/rules/`). The "preserves 67K+ lines" framing was misleading. Both approaches preserve ~65K lines identically. This argument is withdrawn from the design rationale.

**Point 2: "Focus on long-term goals."** — **ACCEPTED with nuance.**
All architects agree that building custom Calcite convention/trait infrastructure (OpenSearchDistributedConvention, ConverterRules, distribution traits) for physical planning is short-term expedience that creates long-term maintenance burden. No production system (Flink, Dremio, Beam, Drill) uses Calcite Volcano for distributed physical planning. Architect D cautions that "long-term" also means accepting the full dependency cost of ported code, not just the algorithms. The revised consensus accepts higher upfront porting cost for lower long-term maintenance.

**Point 3: "Use battle-tested solutions."** — **ACCEPTED as the decisive argument.**
Architect B showed that Trino's operators (HashAggregationOperator, LookupJoinOperator, WindowOperator, etc.) have 10+ years of production hardening including spill-to-disk, memory revocation, null handling, all join types, adaptive partial aggregation. Architect A showed that Trino's physical planner (AddExchanges, PlanFragmenter, PipelinedQueryScheduler) totals ~5,050 LOC solving exchange insertion and distribution at petabyte scale. Architect D validated that the algorithms are genuinely portable even if the integration layer needs custom work. The consensus shifts from "Trino-inspired" to "Trino-ported" for operators and physical planning.

#### Revised Architecture: 4-Layer Design (v2)

```
┌─────────────────────────────────────────────────────────────────┐
│ Layer 4: Calcite Logical Optimization (UNCHANGED)               │
│   CalciteRelNodeVisitor -> RelNode -> Volcano logical rules     │
│   Join reordering, predicate pushdown, constant folding,        │
│   aggregate splitting, projection pruning, subquery decorrel    │
│   All existing UDFs, type system, expression visitors preserved │
├─────────────────────────────────────────────────────────────────┤
│ Layer 3: Physical Planning (REPLACED — port from Trino)         │
│   RelNodeToPlanNodeConverter: Calcite RelNode -> Trino PlanNode │
│   AddExchanges (adapted): inserts ExchangeNodes for distrib.   │
│   PlanFragmenter: splits at ExchangeNode boundaries -> stages  │
│   StageScheduler: assigns fragments to nodes via shard routing  │
│   Decision inputs: ClusterState, shard counts, data locality    │
│   NO Calcite conventions/traits for distribution decisions      │
├─────────────────────────────────────────────────────────────────┤
│ Layer 2: Physical Operators (PORTED from Trino)                 │
│   Lucene-native (SHARD_LOCAL, OpenSearch-specific):             │
│     LuceneFilterScan, LuceneAggScan, LuceneSortScan,           │
│     LuceneFullScan                                              │
│   Exchange (adapted for TransportService):                      │
│     GatherExchange, HashExchange, BroadcastExchange             │
│   Generic (PORTED from Trino, NOT custom-built):                │
│     HashAggregationOperator, LookupJoinOperator,                │
│     HashBuilderOperator, WindowOperator, OrderByOperator,       │
│     TopNOperator, FilterAndProjectOperator, MergeSortedPages    │
├─────────────────────────────────────────────────────────────────┤
│ Layer 1: Trino Execution Runtime (PORTED from Trino)            │
│   Driver/Pipeline cooperative scheduling                        │
│   Simplified context hierarchy:                                 │
│     QueryContext -> TaskContext -> PipelineContext ->            │
│     DriverContext -> OperatorContext                             │
│   MemoryPool: integrated with OpenSearch circuit breakers       │
│   Page/Block: columnar data format (ported faithfully)          │
│   Operator interface: isBlocked/needsInput/addInput/getOutput   │
└─────────────────────────────────────────────────────────────────┘
```

#### What Changed from v1 Consensus

| Component | v1 Consensus | v2 Revised | Reason |
|---|---|---|---|
| Layer 3 planning | OpenSearchDistributedConvention + Calcite traits + ConverterRules | RelNodeToPlanNodeConverter + ported AddExchanges + PlanFragmenter | No production system uses Calcite Volcano for distribution (A, C) |
| Layer 2 generic ops | Custom OpenSearchHashAggregate, OpenSearchHashJoin, etc. | Port Trino's HashAggregationOperator, LookupJoinOperator, etc. | Battle-tested with spill, memory revocation, all edge cases (B) |
| "67K preserved" rationale | Listed as top Pro | Withdrawn | Only ~2K lines differ between approaches (A) |
| Pushdown rules | Kept via Calcite convention coexistence | Kept via separate DSL fallback path (feature flag) | Same outcome, simpler mechanism |
| Estimated class count | ~70 core classes | ~100 core classes (Architect D's Option 3) | Honest accounting of context hierarchy and dependencies |

#### What Stays the Same

- **Layer 4**: All Calcite logical optimization, CalciteRelNodeVisitor, UDFs, type system
- **Layer 1**: Driver/Pipeline/MemoryPool/Page/Block from Trino
- **Lucene leaf operators**: LuceneFilterScan, LuceneAggScan, etc. (OpenSearch-specific)
- **Exchange transport**: TransportService-based, not HTTP
- **Feature flag**: `plugins.sql.distributed_engine.enabled`
- **Test requirements**: All Calcite integTests, yamlRestTests, doctests must pass
- **Phasing**: Scatter-gather Phase 1, full shuffle Phase 2

#### Porting Strategy (per Architect D's Option 3)

**Port faithfully (~60 classes):**
- Page, Block (LongBlock, DoubleBlock, BytesRefBlock, BooleanBlock, DictionaryBlock, RunLengthEncodedBlock), BlockBuilder variants, PagesSerde (~15 classes)
- Operator interface, SourceOperator, SinkOperator (~3 classes)
- HashAggregationOperator + builders + GroupByHash variants (~12 classes)
- LookupJoinOperator + HashBuilderOperator + JoinHash + PageJoiner (~15 classes, minimal spill initially)
- WindowOperator + partitioning (~5 classes)
- FilterAndProjectOperator + PageProcessor (~3 classes)
- OrderByOperator, TopNOperator, MergeSortedPages (~4 classes)
- AddExchanges (adapted), PlanFragmenter (~3 classes)

**Write custom for OpenSearch integration (~40 classes):**
- Simplified context hierarchy (QueryContext, TaskContext, PipelineContext, DriverContext, OperatorContext) — stripped of Trino Session, Metadata SPI, HTTP stats (~8 classes)
- Driver + DriverFactory — adapted for OpenSearch thread pools (~3 classes)
- MemoryPool — integrated with OpenSearch circuit breakers, not Trino's ClusterMemoryManager (~5 classes)
- RelNodeToPlanNodeConverter — the Calcite-to-Trino bridge (~3 classes)
- Exchange over TransportService (GatherExchange, HashExchange, BroadcastExchange, OutputBuffer) — replaces Trino's HTTP exchange (~8 classes)
- LuceneFilterScan, LuceneAggScan, LuceneSortScan, LuceneFullScan — OpenSearch-specific leaf operators (~6 classes)
- StageScheduler, ShardAssigner — uses ClusterState for shard routing (~4 classes)
- Feature flag, execution router, _explain integration (~3 classes)

**Total: ~100 classes** (vs. 70 in v1, vs. 150 for full faithful port)

#### The Calcite-to-Trino Handoff (per Architect C)

```
PPL Query
  -> PPLSyntaxParser -> AST
  -> CalciteRelNodeVisitor -> Calcite LogicalRelNode
  -> Volcano optimizer (logical rules only):
     join reorder, predicate pushdown, aggregate split, constant fold
  -> Optimized LogicalRelNode
  -> RelNodeToPlanNodeConverter:
     LogicalFilter    -> FilterNode
     LogicalAggregate -> AggregationNode (partial + exchange + final)
     LogicalJoin      -> JoinNode (broadcast vs hash decision)
     LogicalTableScan -> LuceneTableScanNode
     LogicalSort      -> SortNode / TopNNode
     LogicalProject   -> ProjectNode
  -> AddExchanges (adapted from Trino):
     inserts ExchangeNodes based on distribution requirements
  -> PlanFragmenter:
     splits at exchange boundaries -> List<StageFragment>
  -> StageScheduler:
     assigns fragments to nodes, assigns shards to leaf stages
  -> Execution via Driver/Pipeline/Operator/Page
```

#### Risks of Revised Approach

1. **RelNode-to-PlanNode conversion complexity.** This bridge does not exist in either codebase. Architect D estimates 5-8 classes. Must handle all RelNode types including PPL-specific ones (dedup, rare, trendline).
2. **Trino operator dependencies deeper than expected.** Architect D showed HashAggregationOperator depends on bytecode generation (FlatHashStrategyCompiler). Either port bytecode infra or replace with simpler reflection-based hashing at some performance cost.
3. **Context hierarchy simplification may lose guarantees.** Stripping Trino's Session, Metadata SPI, and HTTP stats from the context hierarchy means the "battle-tested" argument applies to algorithms but not to integration. Custom context code will need its own hardening.
4. **Join package is 38 files.** Minimal viable LookupJoin without spill is ~20 classes (per Architect D). Full spill support adds ~10 more. Phase 1 can defer spill; Phase 2 must add it.

These risks are manageable and bounded, unlike the open-ended risk of building a novel Calcite convention system with no production precedent.
