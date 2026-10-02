## Version 3.10.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.10.0

### Features

* Add opt-in asynchronous PPL submission using `wait_for_completion_timeout` and `keep_alive`, with result retrieval through `GET /_plugins/_async_query/{id}`.
* Report `progress.fraction_done` on every asynchronous PPL response. The value is bounded to `[0.0, 1.0]`, never decreases, stays at or below `0.8` while `RUNNING`, reports exactly `1.0` on `SUCCEEDED`, and retains the last running value on failure or cancellation. Progress is derived from per-shard search-phase completion, page counts, Composite bucket counts, a bounded index-size lookup, and coordinator limits reporting their own output quota, so it requires neither `track_total_hits` nor partial results and covers single-request search, point-in-time paged search, aggregation, Composite aggregation, coordinator-limited sources, and multi-source plans.

### Bug Fixes

* Release jobs that return rows, explain output, or failures inline without creating retention timers. Retain terminal results only for submissions that return a polling ID.

### Compatibility Notes

* `QueryJobService.submit` accepts a submission wait and retention duration and returns `CompletionStage<QueryResult>`. Embedding callers receive an inline result or a `RUNNING` result containing the polling ID.
* Fetching PPL async results requires `cluster:admin/opensearch/ql/async_query/result` in addition to the PPL submission permission. Updating the default security-plugin PPL role is a separate follow-up.
* PPL async cancellation and dispatching synchronous runner preparation outside the submit thread remain follow-ups.
* `OpenSearchClient` gains a defaulted `documentCountEstimate` used only for progress weighting; implementations that do not override it fall back to equal-weight progress. `QueryJobStatus` and `QueryResult.Running` gain a `progress` component, and `QueryRunner` gains a defaulted `progress()`. Synchronous responses, and the Spark async-query responses, are unchanged: they carry no `progress` field.
* Asynchronous searches now wrap their `SearchRequest` to attach a progress listener. The wrapper preserves the parent task, so cancellation and resource accounting are unaffected.
* Asynchronous plans substitute a progress-reporting `EnumerableLimit` at the physical-plan boundary, through a new core-neutral `PhysicalPlanHook` invoked by `CalciteToolsHelper` once the planner has chosen a plan. The replacement carries the original node's input, offset, fetch, traits, and cost model and delegates row production to Calcite, so plan choice and results are unchanged. Synchronous plans are not substituted and their generated code, costs, and explain output are byte-identical.
