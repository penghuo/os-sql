## Version 3.10.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.10.0

### Features

* Add opt-in asynchronous PPL submission using `wait_for_completion_timeout` and `keep_alive`, with result retrieval through `GET /_plugins/_async_query/{id}` and deletion through `DELETE /_plugins/_async_query/{id}`. Deleting a running query cancels it; deleting any job removes its retained state.

### Bug Fixes

* Release jobs that return rows, explain output, or failures inline without creating retention timers. Retain terminal results only for submissions that return a polling ID.
* Stop a cancelled PPL query instead of restarting it on the legacy engine when `plugins.calcite.fallback.allowed` is enabled, and stop legacy-engine index scans when their query task is cancelled.

### Compatibility Notes

* `QueryJobService.submit` accepts a submission wait and retention duration and returns `CompletionStage<QueryResult>`. Embedding callers receive an inline result or a `RUNNING` result containing the polling ID.
* `QueryJobService.delete` removes a job, cancelling it first if it is still running, and returns its final snapshot. `QueryJobService.cancel` keeps its cancel-only, idempotent contract.
* Fetching PPL async results requires `cluster:admin/opensearch/ql/async_query/result`, and deleting requires `cluster:admin/opensearch/ql/async_query/delete`, in addition to the PPL submission permission. Updating the default security-plugin PPL role is a separate follow-up.
* Dispatching synchronous runner preparation outside the submit thread remains a follow-up.
