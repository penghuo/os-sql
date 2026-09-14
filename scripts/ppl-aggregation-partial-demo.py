#!/usr/bin/env python3
"""Collect raw REST evidence for PPL DSL-aggregation replacement snapshots."""

import json
import time
import urllib.request
from pathlib import Path


BASE_URL = "http://localhost:9200"
INDEX_PATTERN = "ppl_async_agg_demo_*"
QUERY = (
    f"source={INDEX_PATTERN}"
    " | stats sum(event_id % 1000) as sum_mod_1000,"
    " avg(event_id % 997) as avg_mod_997,"
    " max(event_id % 991) as max_mod_991,"
    " min(event_id % 983) as min_mod_983"
)
REPORT_DIR = Path("build/reports/ppl-aggregation-partial-demo")


def request(method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        BASE_URL + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=180) as response:
        return json.load(response)


def write_json(name, value):
    (REPORT_DIR / name).write_text(json.dumps(value, indent=2) + "\n")


def clear_request_cache():
    request("POST", f"/{INDEX_PATTERN}/_cache/clear?request=true")


def main():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    for old_artifact in REPORT_DIR.glob("*.json"):
        old_artifact.unlink()

    cluster = request("GET", "/_cluster/health")
    count = request("GET", f"/{INDEX_PATTERN}/_count")
    shards = request(
        "GET",
        f"/_cat/shards/{INDEX_PATTERN}?format=json&h=index,shard,prirep,state",
    )
    nodes = request("GET", "/_nodes/stats/jvm")
    heap_max_bytes = [
        node["jvm"]["mem"]["heap_max_in_bytes"] for node in nodes["nodes"].values()
    ]
    explain = request(
        "POST",
        "/_plugins/_ppl/_explain?mode=standard",
        {"query": QUERY},
    )
    write_json("explain.json", explain)
    physical_plan = explain["calcite"]["physical"]
    fully_pushed = (
        "CalciteEnumerableIndexScan" in physical_plan
        and "EnumerableAggregate" not in physical_plan
    )

    clear_request_cache()
    started = time.perf_counter()
    current = request(
        "POST",
        "/_plugins/_ppl",
        {
            "query": QUERY,
            "wait_for_completion_timeout": "1ms",
            "keep_alive": "5m",
        },
    )
    submit_ms = round((time.perf_counter() - started) * 1000, 3)
    write_json("submit.json", current)

    if "id" not in current:
        raise AssertionError("Query completed on submit; no asynchronous job id was returned")

    job_id = current["id"]
    sequence = current["sequence"]
    first_non_empty = None
    first_non_empty_ms = None
    timeline = []
    next_sample_second = 1

    while current["status"] == "RUNNING":
        current = request(
            "GET",
            (
                f"/_plugins/_ppl/jobs/{job_id}"
                f"?wait_for_sequence={sequence}"
                "&wait_for_completion_timeout=200ms"
                "&count=100"
            ),
        )
        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
        sequence = current["sequence"]

        if (
            first_non_empty is None
            and current["status"] == "RUNNING"
            and current.get("datarows")
        ):
            first_non_empty = current
            first_non_empty_ms = elapsed_ms
            write_json("first-non-empty.json", current)

        elapsed_second = int(elapsed_ms // 1000)
        while next_sample_second <= elapsed_second:
            sample_name = f"poll-{next_sample_second:03d}s.json"
            write_json(sample_name, current)
            timeline.append(
                {
                    "sample_second": next_sample_second,
                    "elapsed_ms": elapsed_ms,
                    "file": sample_name,
                    "sequence": current["sequence"],
                    "status": current["status"],
                    "size": current["size"],
                }
            )
            next_sample_second += 1

    final_ms = round((time.perf_counter() - started) * 1000, 3)
    write_json("final.json", current)

    clear_request_cache()
    sync_started = time.perf_counter()
    synchronous = request("POST", "/_plugins/_ppl", {"query": QUERY})
    synchronous_ms = round((time.perf_counter() - sync_started) * 1000, 3)
    write_json("synchronous.json", synchronous)

    final_projection = {
        key: current[key] for key in ("schema", "datarows", "total", "size")
    }
    synchronous_projection = {
        key: synchronous[key] for key in ("schema", "datarows", "total", "size")
    }
    final_matches_synchronous = final_projection == synchronous_projection
    ratio = None if first_non_empty_ms is None else first_non_empty_ms / final_ms

    summary = {
        "query": QUERY,
        "dataset": {
            "index_pattern": INDEX_PATTERN,
            "documents": count["count"],
            "primary_shards": sum(1 for shard in shards if shard["prirep"] == "p"),
            "cluster_status": cluster["status"],
            "heap_max_bytes_per_node": heap_max_bytes,
        },
        "timing": {
            "submit_ms": submit_ms,
            "first_non_empty_ms": first_non_empty_ms,
            "final_ms": final_ms,
            "first_non_empty_over_final": ratio,
            "synchronous_ms": synchronous_ms,
        },
        "gate": {
            "aggregation_is_fully_pushed": fully_pushed,
            "running_result_is_non_empty": first_non_empty is not None,
            "first_non_empty_under_80_percent": ratio is not None and ratio < 0.8,
            "final_matches_synchronous": final_matches_synchronous,
        },
        "first_non_empty": first_non_empty,
        "final": current,
        "synchronous": synchronous,
        "timeline": timeline,
    }
    write_json("summary.json", summary)

    if not all(summary["gate"].values()):
        raise AssertionError(json.dumps(summary["gate"], indent=2))

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
