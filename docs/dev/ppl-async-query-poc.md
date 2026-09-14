# PPL Asynchronous Query and Partial Results PoC

## Status and purpose

This document describes the Calcite PPL asynchronous-query proof of concept implemented on branch
`feat/ppl-partial-results`. It is both an implementation guide and a runbook for an engineer or AI
agent preparing, executing, validating, and documenting the demo.

The public API contract is defined separately in
[`ppl-async-query-api-proposal.md`](ppl-async-query-api-proposal.md). Measured REST responses from
the latest two-query demonstration are retained in
[`ppl-partial-results-poc-demo.md`](ppl-partial-results-poc-demo.md).

The PoC has three goals:

1. detach a long-running Calcite PPL query from the submit HTTP connection;
2. return semantically valid intermediate results when the execution plan permits them;
3. provide useful progress without reporting page-local or otherwise misleading counters.

It does not change the existing synchronous request behavior. It does not support legacy PPL/SQL,
analytics-engine queries, durable jobs across owner-node loss, or distributed result persistence.

## User-visible contract

The existing PPL endpoint gains an opt-in asynchronous path:

| Operation | Method and path |
|---|---|
| Submit | `POST /_plugins/_ppl` |
| Poll | `GET /_plugins/_ppl/jobs/{id}` |
| Cancel and release | `DELETE /_plugins/_ppl/jobs/{id}` |

A submit request enters the asynchronous path only when it contains
`wait_for_completion_timeout` or `keep_alive`. Omitting both fields preserves the original
synchronous response.

The job lifecycle is:

```text
submit
  ├─ completes before wait timeout ──> final response without id
  └─ still running ──> id + RUNNING snapshot
                         ├─ poll by id/sequence
                         ├─ cancel by id
                         └─ SUCCEEDED / FAILED / CANCELLED
```

Every asynchronous snapshot has a monotonically increasing `sequence`. A client may long-poll
with `wait_for_sequence`; the server returns when a newer snapshot exists, the query becomes
terminal, or the wait timeout expires.

Intermediate row semantics are fixed after plan classification:

| Mode | Meaning |
|---|---|
| `APPEND` | Returned rows are an immutable prefix. A later snapshot may add rows but cannot revise or remove an earlier row. |
| `REPLACE` | Returned rows are a complete provisional snapshot. A later sequence replaces the earlier snapshot. Queries without a supported snapshot source return progress only until completion. |

Final results are immutable and pageable with `offset` and `count` regardless of update mode.

## Architecture

```text
REST submit/poll/delete
        |
        v
TransportPPLQueryAction
        |
        +---- PPLAsyncQueryJobService <---- owner-node routing / auth / TTL / waiters
        |
        v
Calcite PPL service
        |
        v
OpenSearchExecutionEngine
        |
        +---- stable root rows ----------> APPEND snapshots
        |
        +---- ProgressiveQueryContext
                    |
                    v
            OpenSearchNodeClient
                    |
                    +---- SearchProgressListener ----> progress
                    +---- partial aggregation reduce -> REPLACE snapshots
```

### REST and transport layer

`RestPPLQueryAction` registers the submit, poll, and delete routes. Async requests do not use
`RestCancellableNodeClient`: closing the submit connection must not cancel a detached job.
Responses carry `Cache-Control: no-store`.

`TransportPPLQueryAction`:

- selects synchronous or asynchronous behavior without changing the synchronous path;
- creates the job before Calcite execution starts;
- converts execution callbacks into job snapshots;
- formats result windows and lifecycle metadata;
- routes a poll or delete received by another node to the owner node encoded in the job ID;
- rejects unsupported formats, explain/analyze/profile requests, non-Calcite execution, and
  analytics-engine routing.

### Owner-node job service

`PPLAsyncQueryJobService` is an in-memory owner-node store. It provides:

- user ownership checks;
- status, sequence, update mode, progress, rows, and failure state;
- long-poll waiter registration and timeout handling;
- `keep_alive` renewal and periodic expiration;
- cancellation of the root PPL task and active OpenSearch search tasks;
- final result paging;
- atomic publication of partial rows and the progress represented by those rows.

The opaque `PPLAsyncQueryJobId` contains a format version, owner node ID, and random context ID.
State is not replicated. If the owner node leaves, the job is unavailable.

### Execution callback bridge

`ProgressiveQueryResponseListener` extends the normal query response listener with:

- plan classification;
- progress publication;
- partial result publication;
- search-task registration for cancellation.

The normal listener contract remains unchanged. Only an async request installs the progressive
listener and `ProgressiveQueryContext`.

`ProgressiveQueryContext` is request-scoped. The background PIT scanner captures and restores it
when work moves to another executor thread, allowing ordinary `NodeClient` searches to publish
progress and register cancellation callbacks.

`TransportAwareOpenSearchDataSourceService` preserves the existing data-source registry and
authorization behavior while replacing the local OpenSearch storage engine with the
progress-aware `OpenSearchNodeClient`.

## Query execution behavior

### APPEND: stable coordinator rows

The execution engine classifies a query as `APPEND` only when both the logical and optimized
physical plans prove that emitted root rows cannot be revised:

- no PPL aggregation;
- no blocking sort;
- no window, join, set operation, or multi-input node;
- the physical leaf is a non-aggregation `CalciteEnumerableIndexScan`;
- no timewrap post-processing.

Examples include `rex`, `eval`, and `fields` over a scan.

Calcite/JDBC evaluation remains lazy. Repeated `ResultSet.next()` calls pull rows through the
physical plan. The engine collects finalized root rows and publishes cumulative snapshots:

- first size threshold: 200 rows;
- later size thresholds grow exponentially;
- a 500 ms time threshold prevents long silence when row production is slow.

This is not a synthetic fallback and does not expose rows below a blocking operator. The partial
rows are the same objects that would otherwise be collected for the final response.

For a row-preserving plan with an explicit user `head`/`limit`, result progress is:

```text
fraction_done = committed_root_rows / explicit_limit
```

Rows and this fraction are stored in one atomic job publication and therefore share one
`sequence`. A running fraction is capped below `1.0`; only the terminal snapshot reports `1.0`.

PIT pagination consists of multiple `_search` requests. Shard completion from one page is not
query completion, so `shards_total` and `shards_completed` are `-1`. If there is no safe explicit
result target, progress remains indeterminate while available APPEND rows can still be returned.

### REPLACE: fully pushed single-search aggregation

A PPL query containing aggregation remains `REPLACE` even when Calcite pushes the aggregation
entirely into OpenSearch DSL.

For a fully pushed, non-composite aggregation executed by one `_search` request:

1. `OpenSearchNodeClient` attaches `SearchProgressListener` to the real `SearchTask`;
2. the request uses a small `batched_reduce_size` to create useful coordinator reduce points;
3. `onPartialReduce` parses the current aggregation result through the normal request parser;
4. the engine publishes the parsed rows as a complete replacement snapshot;
5. completed search shards provide an exact progress denominator for that single request.

The final response still follows the normal Calcite result path. The demo verifies that the final
asynchronous row exactly equals a separately executed synchronous response.

### REPLACE: blocking, coordinator, composite, or multi-search plans

Queries containing a blocking operator are conservatively classified as `REPLACE`.

- A plan without a supported snapshot source returns no running rows.
- Composite aggregation pagination and PIT scans issue multiple searches, so current-request shard
  completion is suppressed and progress is indeterminate.
- The PoC does not replay or recompute a blocking Calcite subtree for every poll.
- The PoC does not manufacture intermediate rows to make a demo appear progressive.

These queries still benefit from detached execution, cancellation, TTL, long polling, and final
result paging.

## Correctness rules

Changes to the PoC must preserve these invariants:

1. The synchronous API response is unchanged when async fields are absent.
2. `update_mode` is determined by the plan and cannot change within a job.
3. APPEND result size never decreases.
4. A row published in APPEND mode must equal the row at the same offset in the final response.
5. A REPLACE snapshot is complete for its represented reduce state; clients discard older
   snapshots.
6. Partial rows and their result-derived progress use one sequence increment.
7. `fraction_done=1.0` is terminal only.
8. Page-local shard counters are never presented as query-level progress.
9. Final asynchronous schema and rows must equal the synchronous result.
10. Cancellation and expiration release active search-task callbacks and retained result state.

## Important implementation files

| Area | File |
|---|---|
| Progressive listener contract | `core/src/main/java/org/opensearch/sql/executor/ProgressiveQueryResponseListener.java` |
| Calcite classification and row publication | `opensearch/src/main/java/org/opensearch/sql/opensearch/executor/OpenSearchExecutionEngine.java` |
| Search progress and reduce snapshots | `opensearch/src/main/java/org/opensearch/sql/opensearch/client/OpenSearchNodeClient.java` |
| Cross-thread execution context | `opensearch/src/main/java/org/opensearch/sql/opensearch/executor/ProgressiveQueryContext.java` |
| Local storage-engine wiring | `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/TransportAwareOpenSearchDataSourceService.java` |
| REST routes | `plugin/src/main/java/org/opensearch/sql/plugin/rest/RestPPLQueryAction.java` |
| Submit/poll/delete transport handling | `plugin/src/main/java/org/opensearch/sql/plugin/transport/TransportPPLQueryAction.java` |
| Job state machine | `plugin/src/main/java/org/opensearch/sql/plugin/transport/PPLAsyncQueryJobService.java` |
| Owner-node job ID | `plugin/src/main/java/org/opensearch/sql/plugin/transport/PPLAsyncQueryJobId.java` |
| Large-data integration demo | `integ-test/src/test/java/org/opensearch/sql/calcite/remote/CalcitePPLAsyncLargeDataDemoIT.java` |
| Two-query REST runners | `scripts/ppl-rex-partial-demo.py`, `scripts/ppl-aggregation-partial-demo.py` |

## Verification

### Fast test suite

Run formatting and the focused tests:

```text
./gradlew spotlessApply \
  :opensearch:test \
    --tests org.opensearch.sql.opensearch.executor.OpenSearchExecutionEngineTest \
    --tests org.opensearch.sql.opensearch.client.OpenSearchNodeClientTest \
  :opensearch-sql-plugin:test \
    --tests org.opensearch.sql.plugin.transport.PPLAsyncQueryJobServiceTest \
    --console=plain
```

Run documentation tests:

```text
./gradlew doctest -DignorePrometheus --console=plain
```

The wrapped PoC baseline passed all focused tests and 86/86 doctests.

### Self-contained large-data integration demo

The canonical reproducible data-preparation path is:

```text
./gradlew :integ-test:pplAsyncLargeDataDemo --console=plain
```

This task provisions three OpenSearch nodes with at least 16 GiB heap per node and a 16 GiB test
JVM. The test:

- creates `ppl_async_large_data_demo`;
- indexes 1,000,000 deterministic documents into 12 primary shards;
- runs REX, EVAL, blocking sort, STATS, and EVENTSTATS patterns;
- compares every final asynchronous result with a synchronous result;
- checks stable-prefix equality for APPEND patterns;
- checks that blocking patterns do not expose running rows;
- pages and reassembles the complete final result;
- writes raw responses under
  `integ-test/build/reports/ppl-async-large-data-demo/`.

Those report files are generated artifacts and must not be committed.

## Reproducing the retained two-query demo report

The retained report uses a larger dataset to make latency differences visible:

| Query | Required data |
|---|---|
| REX | `ppl_async_agg_demo_00`, 5,000,000 documents, 12 primary shards |
| STATS | `ppl_async_agg_demo_00` through `ppl_async_agg_demo_23`, 120,000,000 total documents, 288 primary shards |

Each index has 5,000,000 deterministic documents and no replicas:

```json
{
  "event_id": 0,
  "group_id": 0,
  "email": "user0000000@example000.com"
}
```

For document number `n` within each index:

```text
event_id = n
group_id = n % 10000
email = "user%07d@example%03d.com" % (n, n % 1000)
```

Use this mapping:

```json
{
  "settings": {
    "number_of_shards": 12,
    "number_of_replicas": 0,
    "refresh_interval": "-1"
  },
  "mappings": {
    "properties": {
      "event_id": {"type": "integer"},
      "group_id": {"type": "integer"},
      "email": {"type": "keyword"}
    }
  }
}
```

An AI preparing the dataset should bulk-index deterministic batches, verify every bulk response has
`errors=false`, refresh each index once after ingestion, and verify the counts before running the
demo. Do not add sleeps, delayed scripts, or other artificial work to improve the measured ratio.

Set the query size limit to at least 5,000,000:

```http
PUT /_cluster/settings
{
  "persistent": {
    "plugins.query.size_limit": "5000000"
  }
}
```

Start or restart the local plugin cluster with the new code and a 16 GiB heap:

```text
./gradlew :opensearch-sql-plugin:run \
  -Dtests.heap.size=16g \
  --data-dir /local/home/penghuo/oss/os-sql/build/ppl-demo-cluster \
  --preserve-data \
  --console=plain
```

Before measuring, verify:

```text
curl -sS 'http://localhost:9200/_cluster/health?wait_for_status=green'
curl -sS 'http://localhost:9200/ppl_async_agg_demo_00/_count'
curl -sS 'http://localhost:9200/ppl_async_agg_demo_*/_count'
curl -sS 'http://localhost:9200/_nodes/stats/jvm'
```

Run the two REST collectors:

```text
python3 scripts/ppl-rex-partial-demo.py
python3 scripts/ppl-aggregation-partial-demo.py
```

They clear the request cache, execute real REST requests, retain full raw responses, and fail when
correctness or first-byte requirements are not met. Generated files are written to:

```text
build/reports/ppl-rex-partial-demo/
build/reports/ppl-aggregation-partial-demo/
```

The exact timing varies by hardware and cluster load. The required evidence is:

- a non-empty running response arrives before 80% of final latency;
- REX running rows are correct and equal the final prefix;
- REX progress equals committed rows divided by 4,000,000 and is monotonic;
- REX PIT shard counters remain `-1/-1`;
- STATS emits non-empty replacement snapshots;
- STATS final schema and rows exactly equal the synchronous response.

## Updating the demo report

An engineer or AI updating `ppl-partial-results-poc-demo.md` must:

1. build and launch the current code;
2. verify the dataset counts and 16 GiB heap from REST responses;
3. run both collectors without guessing values;
4. copy timings from each generated `summary.json`;
5. copy complete 5-second poll payloads and final payloads from the generated JSON files;
6. keep only REX and STATS in the retained report;
7. state that timings are observations, not deterministic test expectations;
8. run focused tests, doctest, and `git diff --check`;
9. remove generated report directories before committing.

Never hand-edit raw response values, infer responses from code, or claim a demo was run when only
unit tests were executed.

## PoC limitations and production follow-up

- Job rows are retained in owner-node heap; production needs explicit memory accounting and
  backpressure or an external result store.
- Job metadata and results are lost with the owner node.
- Result accumulation currently copies cumulative APPEND snapshots.
- Aggregate snapshots initially cover fully pushed, single-request, non-composite aggregations.
- Composite pagination and coordinator blocking operators need operator-specific progress or
  snapshot support.
- The exact APPEND fraction requires a safe explicit result target; otherwise progress is
  indeterminate.
- Metrics, admission control, rolling-upgrade compatibility, and a dedicated multi-node REST suite
  require further production hardening.
