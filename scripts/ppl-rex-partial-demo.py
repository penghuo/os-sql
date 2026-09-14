#!/usr/bin/env python3
"""Collect raw REST evidence for PPL REX stable-prefix results."""

import json
import re
import time
import urllib.request
from pathlib import Path


BASE_URL = "http://localhost:9200"
INDEX = "ppl_async_agg_demo_00"
RESULT_COUNT = 4_000_000
RESPONSE_COUNT = 5
QUERY = (
    f"source={INDEX}"
    ' | rex field=email "(?<user>[^@]+)@(?<domain>.+)"'
    " | fields event_id, email, user, domain"
    f" | head {RESULT_COUNT}"
)
REPORT_DIR = Path("build/reports/ppl-rex-partial-demo")
EMAIL_PATTERN = re.compile(r"(?P<user>[^@]+)@(?P<domain>.+)")


def request(method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        BASE_URL + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=300) as response:
        return json.load(response)


def write_json(name, value):
    (REPORT_DIR / name).write_text(json.dumps(value, indent=2) + "\n")


def clear_request_cache():
    request("POST", f"/{INDEX}/_cache/clear?request=true")


def validate_rows(response):
    for row in response.get("datarows", []):
        event_id, email, user, domain = row
        match = EMAIL_PATTERN.fullmatch(email)
        if match is None:
            raise AssertionError(f"Email did not match REX pattern: {email}")
        if user != match.group("user") or domain != match.group("domain"):
            raise AssertionError(f"Incorrect REX output for event_id={event_id}: {row}")


def validate_running_progress(response):
    if response["status"] != "RUNNING" or response["total"] == 0:
        return
    progress = response["progress"]
    expected = response["total"] / RESULT_COUNT
    if abs(progress["fraction_done"] - expected) > 1e-12:
        raise AssertionError(
            "Running progress does not match committed rows: "
            f"expected={expected}, actual={progress['fraction_done']}"
        )
    if progress["shards_total"] != -1 or progress["shards_completed"] != -1:
        raise AssertionError(f"PIT page-local shard counters were exposed: {progress}")
    if progress["fraction_done"] >= 1:
        raise AssertionError(f"Running response claimed completion: {progress}")


def main():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    for old_artifact in REPORT_DIR.glob("*.json"):
        old_artifact.unlink()

    cluster = request("GET", "/_cluster/health")
    count = request("GET", f"/{INDEX}/_count")
    shards = request(
        "GET",
        f"/_cat/shards/{INDEX}?format=json&h=index,shard,prirep,state",
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
    append_safe_rex = (
        "EnumerableCalc" in physical_plan
        and "REX_EXTRACT" in physical_plan
        and "EnumerableAggregate" not in physical_plan
        and "EnumerableSort" not in physical_plan
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
    samples = []
    next_sample_second = 5
    previous_fraction = -1

    while current["status"] == "RUNNING":
        current = request(
            "GET",
            (
                f"/_plugins/_ppl/jobs/{job_id}"
                f"?wait_for_sequence={sequence}"
                "&wait_for_completion_timeout=200ms"
                f"&count={RESPONSE_COUNT}"
            ),
        )
        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
        sequence = current["sequence"]
        validate_rows(current)
        validate_running_progress(current)
        fraction = current["progress"]["fraction_done"]
        if fraction >= 0:
            if fraction < previous_fraction:
                raise AssertionError(
                    f"Result progress decreased: {previous_fraction} -> {fraction}"
                )
            previous_fraction = fraction

        if (
            first_non_empty is None
            and current["status"] == "RUNNING"
            and current.get("datarows")
        ):
            first_non_empty = current
            first_non_empty_ms = elapsed_ms
            write_json("first-non-empty.json", current)

        while next_sample_second * 1000 <= elapsed_ms and current["status"] == "RUNNING":
            sample_name = f"poll-{next_sample_second:03d}s.json"
            write_json(sample_name, current)
            samples.append(
                {
                    "sample_second": next_sample_second,
                    "elapsed_ms": elapsed_ms,
                    "file": sample_name,
                    "sequence": current["sequence"],
                    "status": current["status"],
                    "total": current["total"],
                    "size": current["size"],
                }
            )
            next_sample_second += 5

    final_ms = round((time.perf_counter() - started) * 1000, 3)
    validate_rows(current)
    final_progress_is_complete = (
        current["progress"]["fraction_done"] == 1
        and current["progress"]["shards_total"] == -1
        and current["progress"]["shards_completed"] == -1
    )
    write_json("final.json", current)

    final_first_window = request(
        "GET",
        f"/_plugins/_ppl/jobs/{job_id}?offset=0&count={RESPONSE_COUNT}",
    )
    write_json("final-first-window.json", final_first_window)
    first_rows_are_final_prefix = (
        first_non_empty is not None
        and first_non_empty["schema"] == final_first_window["schema"]
        and first_non_empty["datarows"] == final_first_window["datarows"]
    )
    ratio = None if first_non_empty_ms is None else first_non_empty_ms / final_ms

    summary = {
        "query": QUERY,
        "dataset": {
            "index": INDEX,
            "documents": count["count"],
            "primary_shards": sum(1 for shard in shards if shard["prirep"] == "p"),
            "cluster_status": cluster["status"],
            "heap_max_bytes_per_node": heap_max_bytes,
        },
        "timing": {
            "submit_ms": submit_ms,
            "first_byte_ms": first_non_empty_ms,
            "final_byte_ms": final_ms,
            "first_over_final": ratio,
        },
        "gate": {
            "plan_is_append_safe_rex": append_safe_rex,
            "running_result_is_non_empty": first_non_empty is not None,
            "first_non_empty_under_80_percent": ratio is not None and ratio < 0.8,
            "final_total_is_expected": current["total"] == RESULT_COUNT,
            "first_rows_are_final_prefix": first_rows_are_final_prefix,
            "running_progress_matches_committed_rows": previous_fraction >= 0,
            "final_progress_is_complete": final_progress_is_complete,
        },
        "first_non_empty": first_non_empty,
        "final": current,
        "samples": samples,
    }
    write_json("summary.json", summary)

    if not all(summary["gate"].values()):
        raise AssertionError(json.dumps(summary["gate"], indent=2))

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
