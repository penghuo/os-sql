# DQE Design v1 — Design Decisions and Review Notes

> Companion document to [dqe_design_v1.md](dqe_design_v1.md).
> Captures key decisions made during the design review process, including
> trade-offs debated and rationale for final choices.

---

### D1: Phasing -- Wiring in Phase 1, Not Phase 3

**Decision**: Move DistributedExecutor, ShardRoutingResolver, and execution
engine wiring into Phase 1. The original ideas doc placed these in Phase 3.

**Rationale**: The reviewer and team-lead both agreed that without end-to-end
wiring, nothing executes. Phase 1 must produce a working system that can run
real queries and pass all 107 integration tests. Deferring wiring to Phase 3
would mean Phases 1-2 are unit-testable only, with integration issues surfacing
late.

**Trade-off**: Phase 1 is larger, but every component can be validated with
real integration tests from day one.

---

### D2: Limit and Sort Pushdown to DSLScan

**Concern raised**: The reviewer identified a critical performance issue:
removing `LimitIndexScanRule` means `source=index | head 10` on a 100M-doc
index reads ALL documents per shard (DSLScan paginates through everything),
then Calcite's `Limit` operator discards all but 10. With the rule, OpenSearch
stops reading after 10 hits via `SearchSourceBuilder.size(N)`.

Similarly, removing `SortIndexScanRule` discards Lucene's `TopFieldCollector`
which leverages BKD trees for numeric sorts and early termination. Calcite's
`EnumerableSort` must buffer all rows in memory and sort them.

**Decision**: **Retain `LimitIndexScanRule` in DQE mode.** A bare `head N`
without sort goes through ConcatExchange, and without a per-shard limit,
every shard reads its entire dataset. This is not acceptable. DSLScan must
apply `SearchSourceBuilder.size(N)` when a Calcite `Limit` sits above it.
The simplest path is to keep the existing rule.

For `SortIndexScanRule`, the decision is deferred to benchmarking. Lucene
sort is likely faster for sort+limit (TopK) patterns, but for sort-without-
limit the entire dataset must be read anyway. Phase 1 retains the rule for
the TopK case; sort-without-limit goes to the coordinator via ConcatExchange.

**Updated retained rules in DQE mode**: Filter (inverted-index only, no
scripts), Project, Limit, Sort (TopK case), Relevance — 5 rules.
`EnumerableIndexScanRule` is not retained (see D14).

---

### D3: DSLScan Field Retrieval Strategy

**Concern raised**: The reviewer found that `docvalue_fields` is **not used
anywhere** in the current codebase. This means DSLScan's primary retrieval
mechanism is entirely new infrastructure with several known pitfalls:

- `text` fields have DocValues disabled by default
- `docvalue_fields` returns arrays for multi-valued fields (Calcite expects
  scalars unless the column type is ARRAY)
- Nested objects returned via `docvalue_fields` flatten the hierarchy
- `_source: false` breaks scripts and highlighting

**Decision**: DSLScan uses a hybrid approach based on field mapping inspection:

- **DocValues-enabled fields** (keyword, numeric, date, etc.): use
  `docvalue_fields` with `_source: false` for those fields
- **Text-only fields** (no DocValues, no keyword subfield): fall back to
  `_source` for those specific fields
- **Multi-valued fields**: returned as arrays; Calcite row conversion must
  handle the scalar-vs-array distinction based on field mapping
- **Nested objects**: use `_source` retrieval to preserve hierarchy

DSLScan inspects the index mapping via `OpenSearchIndex.getFieldTypes()` at
plan time to classify each projected field. This is the same metadata the
current `CalciteLogicalIndexScan` uses for its nested-path handling.

**Risk**: Since `docvalue_fields` is new to this codebase, this is a
significant implementation risk. Phase 1 can start with `_source`-only
retrieval (same as today) and add `docvalue_fields` optimization
incrementally. This avoids blocking Phase 1 on a new retrieval path.

---

### D4: Nested and Multi-Valued Fields

**Concern raised**: How do nested objects and multi-valued fields work with
DSLScan?

**Decision**: DSLScan routes through the standard search pipeline, so nested
objects and multi-valued fields are returned in the same format as today's
search results. The conversion from SearchHit to Calcite row handles these
the same way the current `CalciteEnumerableIndexScan` does. No architectural
change needed.

---

### D5: Partial Aggregation Correctness

**Concern raised**: AVG decomposed as {SUM, COUNT} -- what about floating
point overflow? NULL handling? The reviewer asked for explicit NULL semantics.

**Decision — NULL handling (SQL standard semantics)**:
- `COUNT(*)` counts all rows including NULLs.
- `COUNT(field)` skips NULLs.
- `SUM(field)` skips NULLs. SUM of all-NULL inputs returns NULL, not 0.
- If a shard has no matching rows for a group, it omits the group entirely
  (emits nothing). The coordinator only sees groups that have data.
- Merge logic: if one shard emits `{sum=NULL, count=0}` for a group
  (all-NULL values), and another emits `{sum=5, count=1}`, the merge
  produces `{sum=5, count=1}`, yielding AVG=5.

**Decision — Overflow**:
- Partial SUM uses widened types: `long` for integer inputs, `double` for
  floating-point. This matches Calcite's default behavior.
- For extreme values, `double` SUM can lose precision. Acceptable for Phase 1
  (same as single-node Calcite). Kahan summation or `BigDecimal` can be added
  later if needed.
- Overflow unit tests are included in SC-1.5.

---

### D6: Serialization Format

**Decision**: Use JSON (via Calcite's RelJsonWriter/RelJsonReader) for Phase 1.
Plan fragments are typically small (< 10 KB). JSON is debuggable and matches
the existing explain output format. Binary formats (Protobuf) are deferred
unless benchmarking shows serialization is a bottleneck.

---

### D7: Error Handling Policy

**Decision**: DQE uses fail-fast semantics at all phases. If any shard fails,
the entire query fails with a clear error message identifying the failed shard
and the root cause. There is no silent fallback to the single-node Calcite
path — when `dqe.enabled=true`, the DQE path is the only path. Silent
fallback would mask bugs and make DQE untestable.

Users who want the non-DQE path set `dqe.enabled=false`. This is an explicit
operator choice, not an automatic recovery mechanism.

---

### D8: Explain API

**Decision**: The `_explain` API should show the distributed plan in DQE mode,
annotating each operator with its execution location (SHARD vs COORDINATOR)
and showing Exchange nodes. This is deferred to Phase 2 since it's not
required for correctness.

---

### D9: Thread Pool for Shard-Side Execution

**Concern raised**: The reviewer noted that shard-side Calcite execution is
CPU-intensive (expression evaluation, in-memory sorting, hashing). Which
OpenSearch thread pool does `TransportCalciteShardAction` use? The `search`
thread pool is sized for I/O-heavy operations and may not be appropriate.

**Decision**: Phase 1 uses the `search` thread pool. This is the same pool
used by existing search requests and has reasonable sizing
(`(cores * 3 / 2) + 1`). A dedicated `dqe` thread pool is a Phase 2
consideration if benchmarking shows contention with regular search queries.

**Mitigation**: DSLScan includes a configurable per-shard row limit
(default 10,000 rows) as a safety valve. Queries that exceed this limit
fail with a clear error indicating the row limit was exceeded, prompting
the operator to increase the limit or optimize the query.

---

### D10: Memory Safety Valve (Phase 1)

**Concern raised**: Without Phase 2's circuit breaker, a node with many shards
running concurrent DQE queries could OOM. Each shard runs a Calcite Enumerable
pipeline that may buffer rows for sorting/aggregation.

**Decision**: Phase 1 includes a simple per-shard row limit
(`plugins.query.dqe.shard_row_limit`, default 10,000). If a shard-side scan
produces more rows than this limit and the plan requires buffering (sort,
aggregate), the query fails fast with a clear error. Operators can increase
the limit or disable DQE for that workload. This is a coarse safety net,
not a replacement for Phase 2's circuit breaker.

---

### D11: Exchange Nodes and Calcite's Exchange RelNode

**Concern raised**: Calcite has its own `org.apache.calcite.rel.core.Exchange`
for distribution. Are DQE Exchange nodes related?

**Decision**: DQE Exchange nodes do NOT extend Calcite's `Exchange` RelNode.
They are execution-time constructs created by `PlanSplitter` after Calcite
optimization is complete. The Calcite optimizer cannot reason about them,
which is intentional: PlanSplitter runs post-optimization and produces an
execution plan, not an optimizable logical plan. This is explicitly
documented to avoid confusion.

---

### D12: Explain API in Phase 1

**Concern raised**: The reviewer suggested explain support is essential for
debugging during development and should not wait until Phase 2.

**Decision**: Phase 1 includes basic explain support. When `_explain` is
called with DQE enabled, the output shows the distributed plan annotated
with `[SHARD]` and `[COORDINATOR]` labels and Exchange node types. This is
a simple text annotation on top of the existing Calcite explain, not a new
format. Full explain design (JSON format, shard routing details, cost
estimates) is Phase 2.

---

### D13: SystemIndexScan and EnumerableNestedAggregateRule

**Note**: `EnumerableSystemIndexScanRule` (in NON_PUSHDOWN_RULES) handles
system index queries (e.g., `SHOW TABLES`). These queries do not go through
DQE because they don't scan user data shards. They are unaffected by the
`dqe.enabled` setting.

`EnumerableNestedAggregateRule` (also in NON_PUSHDOWN_RULES) is listed as
"removed" in Section 4. Clarification: it is already in the non-pushdown
category and always applies. In DQE mode, nested aggregation is handled by
Calcite Enumerable on the shard, so this rule's behavior is subsumed. It
can be kept in the non-pushdown list without harm.

---

### D14: EnumerableIndexScanRule Not Needed in DQE Mode

**Question raised**: Does `EnumerableIndexScanRule` still apply when DSLScan
replaces `CalciteEnumerableIndexScan`?

**Decision**: No. `EnumerableIndexScanRule` converts `CalciteLogicalIndexScan`
→ `CalciteEnumerableIndexScan`. In DQE mode, the physical scan is `DSLScan`,
not `CalciteEnumerableIndexScan`. The logical-to-physical conversion is
handled by a new `DSLScanRule` in the `dqe/` package.

This rule is therefore removed from the DQE-mode rule set.

---

### D15: FilterIndexScanRule — No Script Pushdown in DQE Mode

**Question raised**: Does `FilterIndexScanRule` still push script-based
filters (e.g., `abs(a)=1`) in DQE mode?

**Analysis**: The current `pushDownFilter()` in `CalciteLogicalIndexScan`
calls `PredicateAnalyzer.analyzeExpression()`, which has three outcomes:

1. **Inverted-index query** (`getScriptCount()==0`): the predicate becomes a
   `TermQuery`, `RangeQuery`, `BoolQuery`, etc. This is fast (uses the
   inverted index) and should always be pushed.
2. **Script query** (`getScriptCount()>0`): the predicate becomes a
   `ScriptQueryBuilder` that evaluates a Painless script per document. This
   is slower than Calcite evaluation because Painless has interpreter
   overhead and runs inside Lucene's scorer loop.
3. **Partial**: a compound condition like `a=1 AND abs(b)>2` where `a=1` is
   pushed as inverted-index and `abs(b)>2` remains as a Calcite `Filter`.

**Decision**: In DQE mode, `FilterIndexScanRule` should **not** push script-
based filters. When `PredicateAnalyzer` returns a script query, the DQE-mode
rule leaves the predicate as a Calcite `Filter` on the shard. Calcite
Enumerable evaluation of `abs(a)=1` is at least as fast as Painless, and the
expression stays visible to the Calcite optimizer for potential reordering.

Inverted-index predicates and partial pushdown (push the Lucene part, keep
the complex part as Calcite Filter) continue to work as before.

---

### D16: Mixed-Version Clusters

**Concern raised**: What happens in a mixed-version cluster where some nodes
have DQE and others don't?

**Decision**: The coordinator is always the node that runs PlanSplitter and
dispatches to shards. If the coordinator has DQE code, it can dispatch to
any data node because the shard-side execution is self-contained (the
serialized plan + ShardCalciteRuntime are in the request/response). Data
nodes that don't have DQE code will reject the transport action with an
`ActionNotFoundTransportException`. The coordinator fails the query with a
clear error indicating the target node does not support DQE.

Recommendation: DQE should require all nodes to be on the same version
before enabling. This is enforced by documentation, not code.
