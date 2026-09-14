# PPL Partial Results — Two-Query REST Demo

Date: 2026-09-14

## Summary

These results were collected from real REST requests against a local OpenSearch cluster with a
16 GiB JVM. The request cache was cleared before each measured query. No artificial delay was
added.

| Query | Update mode | Input | First-byte latency | Final-byte latency | First / final | Validation |
|---|---|---:|---:|---:|---:|---|
| REX | `APPEND` | 5,000,000 documents, 12 shards | **61 ms** | **13,586 ms** | **0.45%** | 4,000,000 final rows; every REX field validated; first rows equal final prefix |
| STATS | `REPLACE` | 120,000,000 documents, 288 shards | **1,124 ms** | **24,918 ms** | **4.51%** | Fully pushed aggregation; asynchronous final response exactly equals synchronous response |

`First-byte latency` is submission start to the first `RUNNING` response containing non-empty
`datarows`. `Final-byte latency` is submission start to the complete terminal response.

## Result interpretation

### REX

The REX query has row-preserving coordinator processing and an explicit `head 4000000`. It returns
stable rows with `update_mode=APPEND`.

- The first 200 committed rows were available at 61 ms.
- At 5 seconds, 1,515,089 rows were committed and
  `fraction_done = 1,515,089 / 4,000,000 = 0.37877225`.
- At 10 seconds, 2,980,182 rows were committed and
  `fraction_done = 2,980,182 / 4,000,000 = 0.7450455`.
- The final 4,000,000 rows were available at 13.586 seconds.
- PIT pagination uses multiple `_search` requests, so page-local shard counters are not exposed:
  `shards_total=-1` and `shards_completed=-1`.
- The runner checked every running response for exact result-row progress, monotonic progress,
  correct REX output, and stable-prefix equality.

### STATS

The STATS query is a fully pushed, single-request metric aggregation and returns mutable reduce
snapshots with `update_mode=REPLACE`.

- The first non-empty reduce snapshot was available at 1.124 seconds after 7/288 shards.
- `fraction_done` is exact shard completion for this one underlying search request.
- Aggregation values changed as additional shard results were reduced.
- The final result was available at 24.918 seconds and exactly matched a separately executed
  synchronous query.

## Raw artifacts

```text
build/reports/ppl-rex-partial-demo/
build/reports/ppl-aggregation-partial-demo/
```

The appendix payloads are copied from those artifacts without removing response fields. Polling
used `count=5` for REX and `count=100` for the one-row STATS result.

## Appendix A — REX

### A.1 Query

```text
source=ppl_async_agg_demo_00
| rex field=email "(?<user>[^@]+)@(?<domain>.+)"
| fields event_id, email, user, domain
| head 4000000
```

### A.2 Response at 5 seconds

Observed at 5,053.147 ms:

```json
{
  "schema": [
    {
      "name": "event_id",
      "type": "int"
    },
    {
      "name": "email",
      "type": "string"
    },
    {
      "name": "user",
      "type": "string"
    },
    {
      "name": "domain",
      "type": "string"
    }
  ],
  "sequence": 20,
  "total": 1515089,
  "datarows": [
    [
      0,
      "user0000000@example000.com",
      "user0000000",
      "example000.com"
    ],
    [
      4,
      "user0000004@example004.com",
      "user0000004",
      "example004.com"
    ],
    [
      28,
      "user0000028@example028.com",
      "user0000028",
      "example028.com"
    ],
    [
      1,
      "user0000001@example001.com",
      "user0000001",
      "example001.com"
    ],
    [
      5,
      "user0000005@example005.com",
      "user0000005",
      "example005.com"
    ]
  ],
  "size": 5,
  "expiration_time_in_millis": 1789408349922,
  "update_mode": "APPEND",
  "progress": {
    "fraction_done": 0.37877225,
    "shards_total": -1,
    "shards_completed": -1
  },
  "start_time_in_millis": 1789408045072,
  "window": {
    "offset": 0,
    "count": 5
  },
  "id": "AAAAAQAAABZMSm9ZQzJuVlNHeUtyck5qSERlSWpRAAAAJGU0MmFkZjJmLTU0YzktNDY2NC1hMjY4LWFkYjU4ZTdiYTBmZA",
  "status": "RUNNING"
}
```

### A.3 Response at 10 seconds

Observed at 10,064.797 ms:

```json
{
  "schema": [
    {
      "name": "event_id",
      "type": "int"
    },
    {
      "name": "email",
      "type": "string"
    },
    {
      "name": "user",
      "type": "string"
    },
    {
      "name": "domain",
      "type": "string"
    }
  ],
  "sequence": 30,
  "total": 2980182,
  "datarows": [
    [
      0,
      "user0000000@example000.com",
      "user0000000",
      "example000.com"
    ],
    [
      4,
      "user0000004@example004.com",
      "user0000004",
      "example004.com"
    ],
    [
      28,
      "user0000028@example028.com",
      "user0000028",
      "example028.com"
    ],
    [
      1,
      "user0000001@example001.com",
      "user0000001",
      "example001.com"
    ],
    [
      5,
      "user0000005@example005.com",
      "user0000005",
      "example005.com"
    ]
  ],
  "size": 5,
  "expiration_time_in_millis": 1789408354934,
  "update_mode": "APPEND",
  "progress": {
    "fraction_done": 0.7450455,
    "shards_total": -1,
    "shards_completed": -1
  },
  "start_time_in_millis": 1789408045072,
  "window": {
    "offset": 0,
    "count": 5
  },
  "id": "AAAAAQAAABZMSm9ZQzJuVlNHeUtyck5qSERlSWpRAAAAJGU0MmFkZjJmLTU0YzktNDY2NC1hMjY4LWFkYjU4ZTdiYTBmZA",
  "status": "RUNNING"
}
```

### A.4 Final response

Observed at 13,585.546 ms:

```json
{
  "schema": [
    {
      "name": "event_id",
      "type": "int"
    },
    {
      "name": "email",
      "type": "string"
    },
    {
      "name": "user",
      "type": "string"
    },
    {
      "name": "domain",
      "type": "string"
    }
  ],
  "took": 13583,
  "sequence": 38,
  "total": 4000000,
  "datarows": [
    [
      0,
      "user0000000@example000.com",
      "user0000000",
      "example000.com"
    ],
    [
      4,
      "user0000004@example004.com",
      "user0000004",
      "example004.com"
    ],
    [
      28,
      "user0000028@example028.com",
      "user0000028",
      "example028.com"
    ],
    [
      1,
      "user0000001@example001.com",
      "user0000001",
      "example001.com"
    ],
    [
      5,
      "user0000005@example005.com",
      "user0000005",
      "example005.com"
    ]
  ],
  "size": 5,
  "expiration_time_in_millis": 1789408358544,
  "update_mode": "APPEND",
  "progress": {
    "fraction_done": 1,
    "shards_total": -1,
    "shards_completed": -1
  },
  "start_time_in_millis": 1789408045072,
  "window": {
    "offset": 0,
    "count": 5
  },
  "id": "AAAAAQAAABZMSm9ZQzJuVlNHeUtyck5qSERlSWpRAAAAJGU0MmFkZjJmLTU0YzktNDY2NC1hMjY4LWFkYjU4ZTdiYTBmZA",
  "status": "SUCCEEDED"
}
```

## Appendix B — STATS

### B.1 Query

```text
source=ppl_async_agg_demo_*
| stats sum(event_id % 1000) as sum_mod_1000,
        avg(event_id % 997) as avg_mod_997,
        max(event_id % 991) as max_mod_991,
        min(event_id % 983) as min_mod_983
```

### B.2 Response at 5 seconds

Observed at 5,043.646 ms:

```json
{
  "schema": [
    {
      "name": "sum_mod_1000",
      "type": "bigint"
    },
    {
      "name": "avg_mod_997",
      "type": "double"
    },
    {
      "name": "max_mod_991",
      "type": "int"
    },
    {
      "name": "min_mod_983",
      "type": "int"
    }
  ],
  "sequence": 62,
  "total": 1,
  "datarows": [
    [
      10172124083,
      498.16730579284115,
      990,
      0
    ]
  ],
  "size": 1,
  "expiration_time_in_millis": 1789408276935,
  "update_mode": "REPLACE",
  "progress": {
    "fraction_done": 0.1875,
    "shards_total": 288,
    "shards_completed": 54
  },
  "start_time_in_millis": 1789407971948,
  "window": {
    "offset": 0,
    "count": 100
  },
  "id": "AAAAAQAAABZMSm9ZQzJuVlNHeUtyck5qSERlSWpRAAAAJDYyMWMxODg4LTJkMjQtNGE3Ni1iOTZlLTVjM2Q4NGFkMGZhMw",
  "status": "RUNNING"
}
```

### B.3 Response at 10 seconds

Observed at 10,038.857 ms:

```json
{
  "schema": [
    {
      "name": "sum_mod_1000",
      "type": "bigint"
    },
    {
      "name": "avg_mod_997",
      "type": "double"
    },
    {
      "name": "max_mod_991",
      "type": "int"
    },
    {
      "name": "min_mod_983",
      "type": "int"
    }
  ],
  "sequence": 126,
  "total": 1,
  "datarows": [
    [
      22680356030,
      497.8617516771681,
      990,
      0
    ]
  ],
  "size": 1,
  "expiration_time_in_millis": 1789408281790,
  "update_mode": "REPLACE",
  "progress": {
    "fraction_done": 0.3854166666666667,
    "shards_total": 288,
    "shards_completed": 111
  },
  "start_time_in_millis": 1789407971948,
  "window": {
    "offset": 0,
    "count": 100
  },
  "id": "AAAAAQAAABZMSm9ZQzJuVlNHeUtyck5qSERlSWpRAAAAJDYyMWMxODg4LTJkMjQtNGE3Ni1iOTZlLTVjM2Q4NGFkMGZhMw",
  "status": "RUNNING"
}
```

### B.4 Response at 15 seconds

Observed at 15,069.332 ms:

```json
{
  "schema": [
    {
      "name": "sum_mod_1000",
      "type": "bigint"
    },
    {
      "name": "avg_mod_997",
      "type": "double"
    },
    {
      "name": "max_mod_991",
      "type": "int"
    },
    {
      "name": "min_mod_983",
      "type": "int"
    }
  ],
  "sequence": 192,
  "total": 1,
  "datarows": [
    [
      34340407332,
      497.84151129991307,
      990,
      0
    ]
  ],
  "size": 1,
  "expiration_time_in_millis": 1789408286924,
  "update_mode": "REPLACE",
  "progress": {
    "fraction_done": 0.5902777777777778,
    "shards_total": 288,
    "shards_completed": 170
  },
  "start_time_in_millis": 1789407971948,
  "window": {
    "offset": 0,
    "count": 100
  },
  "id": "AAAAAQAAABZMSm9ZQzJuVlNHeUtyck5qSERlSWpRAAAAJDYyMWMxODg4LTJkMjQtNGE3Ni1iOTZlLTVjM2Q4NGFkMGZhMw",
  "status": "RUNNING"
}
```

### B.5 Response at 20 seconds

Observed at 20,024.860 ms:

```json
{
  "schema": [
    {
      "name": "sum_mod_1000",
      "type": "bigint"
    },
    {
      "name": "avg_mod_997",
      "type": "double"
    },
    {
      "name": "max_mod_991",
      "type": "int"
    },
    {
      "name": "min_mod_983",
      "type": "int"
    }
  ],
  "sequence": 258,
  "total": 1,
  "datarows": [
    [
      45990396625,
      497.9500058742298,
      990,
      0
    ]
  ],
  "size": 1,
  "expiration_time_in_millis": 1789408291942,
  "update_mode": "REPLACE",
  "progress": {
    "fraction_done": 0.7951388888888888,
    "shards_total": 288,
    "shards_completed": 229
  },
  "start_time_in_millis": 1789407971948,
  "window": {
    "offset": 0,
    "count": 100
  },
  "id": "AAAAAQAAABZMSm9ZQzJuVlNHeUtyck5qSERlSWpRAAAAJDYyMWMxODg4LTJkMjQtNGE3Ni1iOTZlLTVjM2Q4NGFkMGZhMw",
  "status": "RUNNING"
}
```

### B.6 Final response

Observed at 24,917.701 ms:

```json
{
  "schema": [
    {
      "name": "sum_mod_1000",
      "type": "bigint"
    },
    {
      "name": "avg_mod_997",
      "type": "double"
    },
    {
      "name": "max_mod_991",
      "type": "int"
    },
    {
      "name": "min_mod_983",
      "type": "int"
    }
  ],
  "took": 24915,
  "sequence": 326,
  "total": 1,
  "datarows": [
    [
      59940000000,
      497.995716,
      990,
      0
    ]
  ],
  "size": 1,
  "expiration_time_in_millis": 1789408296861,
  "update_mode": "REPLACE",
  "progress": {
    "fraction_done": 1,
    "shards_total": 288,
    "shards_completed": 288
  },
  "start_time_in_millis": 1789407971948,
  "window": {
    "offset": 0,
    "count": 100
  },
  "id": "AAAAAQAAABZMSm9ZQzJuVlNHeUtyck5qSERlSWpRAAAAJDYyMWMxODg4LTJkMjQtNGE3Ni1iOTZlLTVjM2Q4NGFkMGZhMw",
  "status": "SUCCEEDED"
}
```

## Reproduce

Start the preserved demo cluster with a 16 GiB JVM:

```text
./gradlew :opensearch-sql-plugin:run \
  -Dtests.heap.size=16g \
  --data-dir /local/home/penghuo/oss/os-sql/build/ppl-demo-cluster \
  --preserve-data \
  --console=plain
```

Then run:

```text
python3 scripts/ppl-rex-partial-demo.py
python3 scripts/ppl-aggregation-partial-demo.py
```

The REX runner validates the plan, every returned REX field, expected final row count, stable-prefix
property, exact result-row progress, unknown PIT shard counters, and monotonicity. The STATS runner
validates full aggregation pushdown, non-empty running snapshots, first-byte latency below 80% of
final-byte latency, and exact asynchronous/synchronous final equality.
