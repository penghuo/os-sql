Natively integrating Trino's distributed SQL query engine components into OpenSearch. "Native integration" means embedding Trino's SQL parser, AST, optimizer, physical operators, function library, scheduler, and shuffle mechanisms directly inside the OpenSearch process as a plugin -- not running Trino as a separate cluster with a connector.

## Problem Statement

The current Calcite-based query engine translates RelNode plans into OpenSearch
DSL for execution. This translation approach has three fundamental limitations:

1. **DSL cannot express all RelNodes.** Window functions, joins, and complex
   expressions have no DSL equivalent. These queries either fail or fall back to
   inefficient coordinator-side execution.

2. **Operators do not execute on shards.** A query like
   `source=index | eval a=b+1` pulls every row to the coordinator, computes the
   expression there, and then returns results. There is no mechanism to push
   Calcite operators to the shard level.

3. **Pushdown rules are complex and error-prone.** There are 15 hand-coded Hep
   and Volcano rules that translate between Calcite RelNodes and OpenSearch DSL
   constructs. Each new PPL feature requires writing a new rule, which is
   fragile and hard to test.

**Root cause**: the system translates between two paradigms (Calcite RelNode and
OpenSearch DSL). Each paradigm has its own semantics, optimizer, and execution
model. Bridging them requires an ever-growing set of translation rules.

**Solution**: stop translating. Ship Calcite plan fragments directly to shards.
Use DSL only at the leaf level for inverted-index access (filters, projections,
relevance functions). Everything above the leaf executes as native Calcite
Enumerable operators -- on the shard.

## Recommended Architecture

A new `/_plugins/_trino_sql` REST endpoint using a **distributed query execution (DQE)** model:

1. Parse SQL with Trino's ANTLR4 parser (shaded library).
2. Analyze and resolve against OpenSearch cluster metadata.
3. Optimize with Trino's rule-based + cost-based optimizer.
4. Fragment the optimized plan and **distribute plan fragments to each shard (split)**.
5. Each shard executes its plan fragment locally using the **local search API on that single shard** (preserving FLS/DLS security).
6. Intermediate results flow back via OpenSearch's transport layer for shuffle, join, and final aggregation.

**Key design principles**:
- The coordinator does NOT translate SQL to Query DSL and execute it. Instead, the coordinator distributes the query plan.
- Each shard is a first-class execution unit that runs physical operators against its local data via the search API.
- The local search API is always used (never direct Lucene segment access) to preserve Field-Level Security (FLS) and Document-Level Security (DLS).
- This is a completely new execution path -- it does NOT build on or reference the existing Calcite-based SQL/PPL implementation.

## Guideline

- investigate trino key compoent of build a distributed query engine
- investigate how trion integrate with opensearch using connector
- **native opensearch integration** means bring trino key commponents sql parse, ast, optimizer, physical operator, functions, scheduler, shuffle code into opensearch.
- do not consider any existing module in current opensearch sql plugin, let's define a new _sql rest api to support trino sql
- focus on technology challenge