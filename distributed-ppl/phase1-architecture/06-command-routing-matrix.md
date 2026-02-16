# PPL Command Routing Matrix (Distributed Execution)

**Status:** DRAFT
**Implements:** ADR Section 6 (Command Routing Strategy)

---

## 1. Routing Decision Per Command

Each PPL command is classified by how it behaves in the distributed path.

### Legend
- **Shard-local:** Executes entirely within each shard's SOURCE fragment
- **Partial+Final:** Two-phase execution (partial on shards, final on coordinator)
- **Exchange required:** Needs data exchange between nodes
- **Fallback:** Not supported in distributed path; routes to single-node engine

## 2. Tier 1: Day-One Distributed Commands

These commands are enabled first. They represent the highest-value use cases.

| PPL Command | Distributed Strategy | Fragment Pattern | Data Transfer |
|-------------|---------------------|------------------|---------------|
| `source` | Shard-local (leaf scan) | SOURCE: IndexScan | None (leaf) |
| `where` | Shard-local (filter pushdown) | SOURCE: Filter → Scan | None |
| `eval` | Shard-local | SOURCE: Calc → ... | None |
| `fields` | Shard-local (projection) | SOURCE: Project → ... | None |
| `rename` | Shard-local | SOURCE: Rename → ... | None |
| `fillnull` | Shard-local | SOURCE: Calc(COALESCE) → ... | None |
| `head`/`limit` | Partial+Final | SOURCE: Limit(N), SINGLE: Limit(N) | N × numShards rows |
| `stats` | Partial+Final agg | SOURCE: PartialAgg, SINGLE: FinalAgg | Groups × numShards |
| `sort` + `head` | Partial+Final sort | SOURCE: Sort+Limit(N), SINGLE: MergeSort+Limit(N) | N × numShards rows |
| `top` | Partial+Final agg | SOURCE: PartialAgg(count, group_by), SINGLE: FinalAgg+Sort+Limit | Groups × numShards |
| `rare` | Partial+Final agg | SOURCE: PartialAgg(count, group_by), SINGLE: FinalAgg+Sort+Limit | Groups × numShards |
| `join` | Hash-partitioned | SOURCE×2: Scan, HASH: HashJoin, SINGLE: Gather | All rows (partitioned) |
| `lookup` | Broadcast (small) or Hash | SOURCE: Scan+BroadcastJoin or HASH: HashJoin | Small table broadcast or hash |

### Example Fragment Plans

**`source=idx | where status='ok' | eval latency_ms=latency*1000 | fields status, latency_ms`**
```
Fragment 0 [SOURCE] — per shard (NO EXCHANGE — pure shard-local)
  └─ Project(status, latency_ms)
     └─ Calc(latency_ms = latency * 1000)
        └─ Filter(status = 'ok')
           └─ IndexScan(idx)

→ Each shard produces filtered, projected rows
→ Coordinator just gathers results (trivial, no computation)
→ This query SHOULD stay single-node if index is small
```

**`source=idx | stats count(), avg(latency) by status`**
```
Fragment 0 [SINGLE] — coordinator
  └─ FinalAggregation(count=SUM(partial_count), avg=SUM(partial_sum)/SUM(partial_n), group_by=status)
     └─ GatherExchange ← Fragment 1

Fragment 1 [SOURCE] — per shard
  └─ PartialAggregation(count=COUNT(*), avg=(SUM(latency), COUNT(latency)), group_by=status)
     └─ IndexScan(idx)

→ Each shard produces partial agg states (~100 groups)
→ Coordinator merges ~500 partial states (5 shards × 100 groups)
→ Data transfer: ~500 rows (not 1M raw rows)
```

**`source=idx | sort latency DESC | head 100`**
```
Fragment 0 [SINGLE] — coordinator
  └─ DistributedSort(order=latency DESC, limit=100)
     └─ OrderedGatherExchange ← Fragment 1

Fragment 1 [SOURCE] — per shard
  └─ Sort(order=latency DESC)
     └─ Limit(100)
        └─ IndexScan(idx)

→ Each shard sorts locally, produces top 100
→ Coordinator merge-sorts 5 × 100 = 500 rows
→ Data transfer: 500 rows (not 10M raw rows)
```

**`source=orders | join left=customers on user_id | stats count() by region`**
```
Fragment 0 [SINGLE] — coordinator
  └─ FinalAggregation(count, group_by=region)
     └─ GatherExchange ← Fragment 1

Fragment 1 [HASH by user_id] — distributed join workers
  └─ PartialAggregation(count, group_by=region)
     └─ HashJoin(on=user_id)
        ├─ HashExchange(by=user_id) ← Fragment 2
        └─ HashExchange(by=user_id) ← Fragment 3

Fragment 2 [SOURCE] — per shard of orders
  └─ IndexScan(orders)

Fragment 3 [SOURCE] — per shard of customers
  └─ IndexScan(customers)

→ Both tables hash-partitioned by user_id
→ Same-key rows land on same worker → local hash join
→ Partial aggregation on join results → final merge
```

## 3. Tier 2: Window Function Commands

These commands require full data materialization and are more complex.
Enabled after Tier 1 is stable.

| PPL Command | Distributed Strategy | Fragment Pattern | Notes |
|-------------|---------------------|------------------|-------|
| `dedup` | Hash by dedup keys | SOURCE: Scan, HASH(dedup_keys): Dedup | Must see all rows for same key |
| `eventstats` | Hash by partition keys | SOURCE: Scan, HASH(part_keys): WindowAgg | Full window computation |
| `trendline` | Hash by partition keys | SOURCE: Scan, HASH(part_keys): Moving avg | Ordered window |
| `bin`/`span` | Shard-local (transform only) | SOURCE: Calc(bucket) → ... | If followed by stats, partial agg |
| `existsSubquery` | Depends on subquery | Subquery → exchange | May benefit from distribution |
| `inSubquery` | Depends on subquery | Subquery → exchange | May benefit from distribution |
| `scalarSubquery` | Depends on subquery | Subquery → exchange | May benefit from distribution |

### Dedup Example

**`source=idx | dedup user_id`**
```
Fragment 0 [SINGLE] — coordinator
  └─ GatherExchange ← Fragment 1

Fragment 1 [HASH by user_id]
  └─ DedupOperator(keys=user_id, keepFirst=true)
     └─ HashExchange(by=user_id) ← Fragment 2

Fragment 2 [SOURCE] — per shard
  └─ IndexScan(idx)

→ All rows for same user_id land on same HASH worker
→ Worker deduplicates locally
→ Coordinator gathers unique rows
```

## 4. Tier 3: Complex / Fallback Commands

These commands fall back to single-node execution in the initial release.

| PPL Command | Reason for Fallback | Future Opportunity |
|-------------|--------------------|--------------------|
| `expand` | UNNEST requires full row context | Shard-local possible |
| `flatten` | Nested structure unpacking | Shard-local possible |
| `parse`/`grok`/`rex` | Regex extraction | Shard-local (CPU-bound, not I/O) |
| `patterns` | Pattern extraction | Custom operator needed |
| `fieldsummary` | Multi-column multi-agg | Could use partial agg per column |
| `append` | UNION ALL of subqueries | Each subquery could be distributed separately |
| `appendcol` | Subsearch correlation | Complex coordination |
| `correlation` | Time-series correlation | Window function variant |
| `describe` | Metadata query | No data movement needed |
| `kmeans`/`AD`/`ML` | ML operations | Permanently out of scope |

## 5. Mixed-Command Queries

Most real queries combine multiple commands. The fragment planner handles
combinations by composing the patterns:

### Example: Filter → Aggregate
**`source=idx | where region='US' | stats avg(latency) by city`**
```
Fragment 0 [SINGLE]
  └─ FinalAgg(avg=sum/count, by=city)
     └─ GatherExchange ← Fragment 1

Fragment 1 [SOURCE] — per shard
  └─ PartialAgg(sum(latency), count(latency), by=city)
     └─ Filter(region='US')    ← pushed into SOURCE fragment
        └─ IndexScan(idx)

→ Filter runs on shard (reduces data before aggregation)
→ Partial aggregation on filtered data
→ Minimal data transfer
```

### Example: Filter → Sort → Limit
**`source=idx | where status='error' | sort timestamp DESC | head 50`**
```
Fragment 0 [SINGLE]
  └─ DistributedSort(order=timestamp DESC, limit=50)
     └─ OrderedGatherExchange ← Fragment 1

Fragment 1 [SOURCE] — per shard
  └─ Sort(order=timestamp DESC)
     └─ Limit(50)
        └─ Filter(status='error')   ← pushed into SOURCE
           └─ IndexScan(idx)

→ Each shard: filter → sort → top 50
→ Coordinator: merge sort 5×50 = 250 rows → take 50
```

### Example: Join → Filter → Aggregate
**`source=orders | join left=products on product_id | where category='electronics' | stats sum(price) by region`**
```
Fragment 0 [SINGLE]
  └─ FinalAgg(sum(price), by=region)
     └─ GatherExchange ← Fragment 1

Fragment 1 [HASH by product_id]
  └─ PartialAgg(sum(price), by=region)
     └─ Filter(category='electronics')   ← pushed below agg, above join
        └─ HashJoin(on=product_id)
           ├─ HashExchange(by=product_id) ← Fragment 2
           └─ HashExchange(by=product_id) ← Fragment 3

Fragment 2 [SOURCE] — orders shards
  └─ IndexScan(orders)

Fragment 3 [SOURCE] — products shards
  └─ IndexScan(products)
```

## 6. Unsupported Combinations

If a query contains ANY unsupported command, the ENTIRE query falls back to
single-node execution. Partial distribution (some commands distributed, some not)
is not supported in v1.

```java
// In QueryRouter:
private boolean containsUnsupportedCommands(RelNode relNode) {
    return new UnsupportedCommandDetector().visit(relNode);
}
```

This is conservative but safe. Future versions may implement partial distribution.

## 7. Per-Command Performance Budget

| Command | Single-Node Baseline | Distributed Target | Acceptable Regression |
|---------|---------------------|-------------------|----------------------|
| where (filter) | 20ms @ 1M docs | 25ms (parallel filter) | Up to 2x |
| stats (aggregation) | 50ms @ 1M docs | 35ms (partial agg) | None (should be faster) |
| sort + head 100 | 200ms @ 10M docs | 30ms (shard-local sort) | None (should be faster) |
| join (100K × 100K) | 5000ms | 500ms (hash partition) | None (should be 3-10x faster) |
| dedup | 100ms @ 100K docs | 80ms (hash partition) | Up to 2x |
| eval/fields/rename | 10ms | 10ms (shard-local, no overhead) | None |

## 8. Migration Checklist Per Command

Before a command is enabled in the distributed path:

- [ ] Fragment plan pattern documented
- [ ] Operator(s) implemented with unit tests
- [ ] Round-trip correctness test: distributed result == single-node result
- [ ] Benchmark at 1K, 100K, 1M docs
- [ ] Performance gate: not >2x slower than single-node at any scale
- [ ] Edge cases: NULL values, empty results, single shard
- [ ] Explain output includes distributed plan
- [ ] PR with benchmark results attached
