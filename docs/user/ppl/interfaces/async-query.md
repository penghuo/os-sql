# Asynchronous PPL queries

The existing `POST /_plugins/_ppl` endpoint remains synchronous unless the request includes `wait_for_completion_timeout` or `keep_alive`.

An asynchronous submit waits up to `wait_for_completion_timeout`. If the query finishes in that interval, the response contains the final result and no job ID. Otherwise, the response contains an opaque `id` and a `RUNNING` status.

`wait_for_completion_timeout` defaults to `5s`, and `keep_alive` defaults to `5m`. The configured maximums default to `60s` and `24h`.

```bash ignore
curl -sS -H 'Content-Type: application/json' \
  -X POST localhost:9200/_plugins/_ppl \
  -d '{
        "query": "source=accounts | sort account_number",
        "wait_for_completion_timeout": "1s",
        "keep_alive": "5m"
      }'
```

Poll the complete current snapshot with:

```bash ignore
curl -sS \
  -X GET 'localhost:9200/_plugins/_ppl/jobs/<job-id>?keep_alive=5m'
```

An authorized GET renews the lease. If `keep_alive` is omitted, the current lease interval is reused.

Cancel a running query, or delete a retained terminal result, with:

```bash ignore
curl -sS \
  -X DELETE 'localhost:9200/_plugins/_ppl/jobs/<job-id>'
```

Responses use these states:

- `RUNNING`: the query is still executing. `schema`, `datarows`, and `total` are empty.
- `SUCCEEDED`: the response contains the complete schema and rows, with the same defined ordering as the synchronous query.
- `FAILED`: the response contains an error and no partial rows.
- `CANCELLED`: returned by DELETE when a running query is cancelled. The job is no longer available afterward.

Expired jobs and jobs whose owner node has left the cluster return `404`.

The interface returns a complete snapshot on every submit or poll response. It does not provide partial results, progress, paging, or delta delivery. Results remain subject to `plugins.query.size_limit`, which is 10,000 rows by default.

Asynchronous requests support the Calcite PPL JSON execution path. Explain, analyze, profile, CSV, raw, and visualization modes are not supported.
