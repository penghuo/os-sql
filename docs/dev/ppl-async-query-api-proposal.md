# PPL Asynchronous Query API — Proposal

## Endpoint layout

| Purpose | Method | Path |
|---|---|---|
| Submit / long-poll for completion | `POST` | `/_plugins/_ppl` |
| Poll status + results | `GET`  | `/_plugins/_ppl/jobs/{id}` |
| Cancel and release | `DELETE` | `/_plugins/_ppl/jobs/{id}` |

## Submit

### Request

```http
POST /_plugins/_ppl
Content-Type: application/json
```

```json
{
  "query": "source=account | stats count() by age",
  "wait_for_completion_timeout": "1s",
  "keep_alive": "5m"
}
```

| Field | Type | Required | Default | Description |
|---|---|---:|---|---|
| `query` | string | yes | — | PPL query. |
| `wait_for_completion_timeout` | duration | no | `1s` on the async-capable path | Server blocks up to this long waiting for completion. `≤ 30s`. If execution finishes within it, the response carries the final result and no `id`. |
| `keep_alive` | duration | no | `5m` on the async-capable path | Job lease interval. Each accepted, authenticated poll renews expiration using this interval before waiting for a newer snapshot. `> 0`, `≤ 24h`. |

Omitting `wait_for_completion_timeout` **and** `keep_alive` keeps the existing synchronous request
behavior for backward compatibility. Presence of either field enables the async-capable path. The
async defaults above are applied only after that path has been selected.

### Fast-path response (completed within timeout)

The existing synchronous PPL response, with lifecycle fields added. Field origin marked:
**[existing]** = returned by the current synchronous PPL response today, unchanged;
**[new]** = added by this proposal.

```json
{
  "status": "SUCCEEDED",              // [new]
  "took": 214,                        // [new]  (OpenSearch convention: millis, implicit)
  "start_time_in_millis": 1789142700000, // [new]
  "schema": [                         // [existing]
    {"name": "count()", "type": "long"},
    {"name": "age",     "type": "integer"}
  ],
  "datarows": [[521, 20], [442, 30], [398, 40]],  // [existing]
  "total": 3,                         // [existing]
  "size": 3                           // [existing]
}
```

No `id`. The client has nothing further to poll. `total == size` on the fast path (single-shot,
whole result in one response).

### Async-path response (not completed within timeout)

```json
{
  "id": "<opaque-id>",                        // [new]
  "status": "RUNNING",                        // [new]
  "sequence": 0,                              // [new]
  "start_time_in_millis": 1789142700000,      // [new]
  "expiration_time_in_millis": 1789143000000, // [new]
  "progress": {                               // [new]
    "fraction_done": 0.15,
    "shards_total": 5,
    "shards_completed": 1
  },
  "schema": [],                               // [existing]
  "datarows": [],                             // [existing]
  "total": 0,                                 // [existing name, extended] accumulator size
  "size": 0                                   // [existing]                 rows in this response
}
```

## Poll

### Request

```http
GET /_plugins/_ppl/jobs/{id}
```

Query parameters:

| Param | Type | Default | Description |
|---|---|---|---|
| `wait_for_sequence` | integer | `-1` | Last sequence observed by the client. If the current sequence is greater, return immediately. Otherwise wait for a newer sequence, terminal status, or `wait_for_completion_timeout`. `-1` returns the current snapshot immediately. |
| `wait_for_completion_timeout` | duration | `0` | Maximum long-poll duration. `≤ 30s`. Effective when `wait_for_sequence ≥ 0`; `0` always returns immediately. |
| `keep_alive` | duration | current job lease | Optionally changes the lease interval. Every accepted, authenticated poll renews `expiration_time_in_millis` before entering the long-poll wait. `> 0`, `≤ 24h`. |
| `offset` | integer | `0` | 0-based row window start over the current accumulator. |
| `count` | integer | `1000` | Max rows to return in this response. Must be between `1` and `plugins.ppl.async.max_page_size` (default `10000`). |

For example, after receiving `sequence=4`, the next long-poll request is:

```http
GET /_plugins/_ppl/jobs/{id}?wait_for_sequence=4&wait_for_completion_timeout=1s
```

If sequence 5 already exists, the server returns it immediately. Otherwise the request waits until
sequence 5 is published, the job becomes terminal, or one second elapses.

### Response

Same envelope as the async-path submit response. Example mid-run
(field origin: **[existing]** = today's sync PPL response; **[new]** = added by this proposal;
**[extended]** = existing name, meaning extended in the async path):

```json
{
  "id": "<opaque-id>",                        // [new]
  "status": "RUNNING",                        // [new]
  "sequence": 4,                              // [new]
  "start_time_in_millis": 1789142700000,      // [new]
  "expiration_time_in_millis": 1789143000000, // [new]
  "progress": {                               // [new]
    "fraction_done": 0.62,
    "shards_total": 5,
    "shards_completed": 3
  },
  "update_mode": "APPEND",                    // [new]
  "schema": [                                 // [existing]
    {"name": "event_id", "type": "long"},
    {"name": "email",    "type": "string"}
  ],
  "window": {"offset": 0, "count": 1000},     // [new]     served window
  "datarows": [ /* … 1000 rows … */ ],        // [existing] rows in this window
  "size": 1000,                               // [existing] datarows.length
  "total": 208114                             // [extended] accumulator size (Splunk's resultCount role)
}
```

Terminal (successful, last page) example:

```json
{
  "id": "<opaque-id>",                        // [new]
  "status": "SUCCEEDED",                      // [new]
  "sequence": 8,                              // [new]
  "took": 715,                                // [new]
  "start_time_in_millis": 1789142700000,      // [new]
  "expiration_time_in_millis": 1789143000000, // [new]
  "progress": { /* fraction_done=1.0, all counters final */ },  // [new]
  "update_mode": "APPEND",                    // [new]
  "schema": [ /* … */ ],                      // [existing]
  "window": {"offset": 273088, "count": 1000},// [new]
  "datarows": [ /* 1000 rows */ ],            // [existing]
  "size": 1000,                               // [existing]
  "total": 274088                             // [extended]  offset+size == total → last page
}
```

## Cancel and release

### Request

```http
DELETE /_plugins/_ppl/jobs/{id}
```

### Response

`HTTP 200 OK` with lifecycle metadata only:

```json
{
  "id": "<opaque-id>",
  "status": "CANCELLED"
}
```

If the job was already terminal, the response contains its existing terminal status. The operation
cancels a running job and releases all retained result data. It does not return result rows because
there may be additional pages that cannot be fetched after release. Subsequent requests for the
same `id` return `HTTP 404`.

## Submit and poll response envelope

Field summary for submit and poll responses. `DELETE` intentionally returns only the lifecycle
metadata documented in §Cancel and release. **Origin** tells you whether the field is already
emitted by today's synchronous PPL response (`existing`) or introduced by this proposal (`new`).

| Field | Origin | Type | When | Description |
|---|---|---|---|---|
| `schema` | existing | array | always | Column definitions. Empty until known. |
| `datarows` | existing | array | when rows are being returned | Rows in the returned window. |
| `size` | existing | integer | always | Rows in **this response** (`datarows.length`), including `0` when no rows are returned. |
| `total` | extended | integer | always | Row count represented by the current result state. For `RUNNING + APPEND`, this is the number of committed stable rows currently available. For `RUNNING + REPLACE`, this is the current aggregation snapshot size and may change with `sequence`. For `SUCCEEDED`, this is the exact immutable final result size. On the fast path, `total == size`. |
| `status` | new | enum | always | `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`. Canonical lifecycle state; carries `is_partial`/`is_running` information (see §Status). |
| `id` | new | string | async path only | Opaque handle. Absent on fast-path success. Present on any async-path response and on `DELETE`/`GET` responses. |
| `sequence` | new | integer | async only | Monotonic result/progress snapshot version. The same `(id, sequence, offset, count)` returns the same logical schema, rows, and progress. Transport bytes and lease metadata such as `expiration_time_in_millis` need not be identical. |
| `start_time_in_millis` | new | integer | submit and poll | Submission time, Unix epoch millis. |
| `expiration_time_in_millis` | new | integer | async only | Current job expiration, Unix epoch millis. An accepted, authenticated poll renews it before waiting, using the current `keep_alive` lease interval. |
| `took` | new | integer | terminal only | Total execution time in millis (OpenSearch convention). |
| `progress` | new | object | always on async path | Counters (§Progress). Progress counters do not change without a `sequence` bump. |
| `update_mode` | new | enum | async after plan classification | `APPEND` = rows returned are stable relative to the request's `offset`, and nothing already delivered will be revised; the client advances `offset += size` while running. `REPLACE` = rows, when present, are the complete current snapshot and a later `sequence` replaces them. Determined by the query plan and fixed within a job. |
| `window` | new | object | when rows are being returned | `{offset, count}` echoing the served window. |

Fields dropped from earlier drafts and their equivalents:

- **`is_running`** — was `status == RUNNING`; use `status` directly.
- **`is_partial`** — was `status != SUCCEEDED`; use `status` directly.
- **`has_more`** — for immutable final paging or `RUNNING + APPEND`, compute it as `offset + size < total`.
- **`progress.rows_in_result`** — redundant with the top-level `total`.
- **`took_millis`** — renamed to `took` per OpenSearch convention (implicit millis).

### Progress object

| Field | Type | Description |
|---|---|---|
| `fraction_done` | number | `0.0`–`1.0`. `1.0` only on terminal snapshots. |
| `shards_total` | integer | Total shards involved. |
| `shards_completed` | integer | Shards that have finished. |

Accumulator size is the top-level `total`, not a progress field. Any counter may be `-1` when the
engine cannot supply it for a given plan; clients treat `-1` as "unknown."

Progress is query-level, not the state of the most recent underlying `_search` request:

- For `RUNNING + APPEND` plans with row-preserving operators and an explicit user `head`/`limit`,
  `fraction_done = total / limit`. The result rows and this fraction are published in the same
  `sequence`. PIT pagination spans multiple `_search` requests, so `shards_total` and
  `shards_completed` are `-1`.
- For a fully pushed, single-request aggregation, `fraction_done` may use completed search shards,
  and the shard counters are exact for that request.
- If neither denominator is exact, all progress fields remain `-1`; the UI shows indeterminate
  progress and may still render available rows.

While `status=RUNNING`, `fraction_done` is capped below `1.0`. Only a terminal snapshot reports
`1.0`.

### Status enum

| Status | Terminal | Meaning | Rows are authoritative? |
|---|---:|---|---|
| `RUNNING` | no | Execution active. | `APPEND` rows are an authoritative stable prefix. `REPLACE` rows are a provisional snapshot and may change. |
| `SUCCEEDED` | yes | Final authoritative result. | **yes** |
| `FAILED` | yes | Execution failed; response contains `error`. | no (partial rows if any) |
| `CANCELLED` | yes | Cancelled while running. | no (partial rows if any) |

`status` alone is sufficient for lifecycle decisions:

- **"Still running?"** — `status == RUNNING`.
- **"Is the result immutable?"** — `status == SUCCEEDED`.
- **"Has this client fetched the complete result?"** — `status == SUCCEEDED` and the client has
  consumed `total` rows across final-result pages.
- **"Any terminal (stop polling)?"** — `status ∈ {SUCCEEDED, FAILED, CANCELLED}`.

Expiration is `HTTP 404` (no pollable `EXPIRED` snapshot). Terminal status does not imply success;
clients must check specifically for `SUCCEEDED`.

## Update-mode contract

Per response on the async path:

| `update_mode` | Client semantics | When produced |
|---|---|---|
| `APPEND` | `datarows` are stable rows at the requested `offset`; nothing already delivered will be revised. Client advances `offset += size` while the job is running. | Plan is append-safe: no downstream blocking operator can revise emitted rows. |
| `REPLACE` | `RUNNING` may return a complete current aggregation snapshot. When `sequence` changes, the client discards the previous snapshot and renders the new one. Queries without a supported snapshot source continue to return progress only until `SUCCEEDED`. | The PPL query contains aggregation or a blocking operator whose output can still change. |

`update_mode` is determined by the PPL logical and physical plans and fixed within a job. A PPL
aggregation remains `REPLACE` even when the aggregation is fully pushed down into OpenSearch. The
initial supported snapshot source is a fully pushed, single-request, non-bucket metric or
count aggregation. Its incremental reduce results are exposed as replacement snapshots. Bucketed
or multi-request aggregations (including composite pagination) and coordinator-only aggregations
require their own snapshot implementations.

After `status=SUCCEEDED`, the result is immutable and can be paginated with `offset` and `count`
regardless of `update_mode`.

The PoC implementation and reproduction guide are documented in
[`ppl-async-query-poc.md`](ppl-async-query-poc.md).

## Failure response

An execution failure discovered after successful submission is a lifecycle response (HTTP 200):

```json
{
  "id": "<opaque-id>",
  "status": "FAILED",
  "sequence": 1,
  "start_time_in_millis": 1789142700000,
  "expiration_time_in_millis": 1789143000000,
  "progress": {
    "fraction_done": 0.0,
    "shards_total": 5,
    "shards_completed": 0
  },
  "schema": [],
  "datarows": [],
  "size": 0,
  "total": 0,
  "error": {
    "type": "IllegalStateException",
    "reason": "query execution failed"
  }
}
```

## HTTP errors

| HTTP | Condition |
|---:|---|
| `400 Bad Request` | Invalid JSON, invalid `id`, invalid `keep_alive`/`wait_for_sequence`/`wait_for_completion_timeout`, invalid `offset`/`count`, unsupported format or execution mode. |
| `403 Forbidden` | Authenticated user does not own the job. |
| `404 Not Found` | Unknown or expired `id`, or owner node no longer available. |
| `500 Internal Server Error` | Submission, routing, or execution setup failure. |

Error bodies use the existing PPL error envelope:

```json
{
  "status": 400,
  "error": {
    "type": "IllegalArgumentException",
    "reason": "Invalid Query",
    "details": "offset must be >= 0"
  }
}
```

## Client polling algorithm

Iteration:

```text
POST /_plugins/_ppl {query, wait_for_completion_timeout: "1s", keep_alive: "5m"}
if response has no id: render final; exit

last_sequence = response.sequence
offset = 0

while response.status == RUNNING:
  request_offset = offset if response.update_mode == APPEND else 0
  response = GET /_plugins/_ppl/jobs/{id}
    ?offset=<request_offset>
    &count=1000
    &wait_for_sequence=<last_sequence>
    &wait_for_completion_timeout=1s

  if response.sequence == last_sequence:
    continue

  last_sequence = response.sequence
  if response.status == RUNNING && response.update_mode == APPEND:
    render_append(datarows)
    offset += size
  else if response.status == RUNNING && response.update_mode == REPLACE:
    render_progress(response.progress)

if response.status == SUCCEEDED:
  if response.update_mode == REPLACE:
    offset = 0

  # The final result is immutable. Page it regardless of update_mode.
  while offset < response.total:
    page = GET /_plugins/_ppl/jobs/{id}?offset=<offset>&count=1000
    render_append(page.datarows)
    offset += page.size

if response.status == FAILED or response.status == CANCELLED:
  stop polling
```

Termination rules:

- `RUNNING` means execution is active; continue long-polling.
- `SUCCEEDED` means the result is immutable; page until `offset == total`.
- `FAILED` or `CANCELLED` means stop polling. Any preview already rendered remains
  non-authoritative unless it came from `APPEND`.

Clients should ignore a response whose `sequence` is older than the last one rendered. Two polls
with the same `(sequence, offset, count)` return the same logical result window, so the client may
short-circuit result processing.

## Restrictions

- Calcite PPL execution path only.
- JDBC JSON response format only. CSV, raw, and visualization formats are unsupported for
  async-capable requests.
- `_explain`, `analyze`, and `profile` do not create jobs; they retain synchronous behavior.
- A running job is not durable across owner-node failure; requests return `HTTP 404` if the owner
  leaves the cluster.
- Job responses use `Cache-Control: no-store`.

## Multi-node behavior

The `id` identifies the owner node. `GET` and `DELETE` can be sent to any node; the receiving node
routes internally to the owner. The job is bound to the submitting user; the owner validates the
same user on every request. Owner-node state is not replicated.

## Resource limits

- Default `keep_alive`: `5m`. Maximum: `24h`. Each accepted, authenticated poll renews the job
  lease before entering any long-poll wait.
- Default `wait_for_completion_timeout`: `1s`. Maximum: `30s`.
- Maximum retained jobs per owner node: `10,000`. Terminal jobs count toward the limit until they
  expire.
- `count` must be between `1` and `plugins.ppl.async.max_page_size` (default `10,000`).
