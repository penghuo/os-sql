#!/usr/bin/env python3
"""Validate whether PPL partial responses are meaningful, not merely non-empty."""

import argparse
import json
import math
import os
import shutil
import time
import urllib.request
from pathlib import Path


BASE_URL = os.environ.get("PPL_BASE_URL", "http://localhost:9200")
REPORT_DIR = Path("build/reports/ppl-partial-results-semantic-validation")
INDEX = "ppl_async_agg_demo_00"
INDEX_PATTERN = "ppl_async_agg_demo_*"
MIN_HEAP_BYTES = 16 * 1024 * 1024 * 1024

CASES = {
    "non_blocking_rex": {
        "semantic_model": "FINAL_PREFIX",
        "query": (
            f"source={INDEX}"
            ' | rex field=email "(?<user>[^@]+)@(?<domain>.+)"'
            " | fields event_id, email, user, domain"
            " | head 4000000"
        ),
        "poll_count": 3,
        "final_count": 100,
        "expected_total": 4_000_000,
        "expected_mode": "APPEND",
    },
    "fully_pushed_aggregation": {
        "semantic_model": "PROVISIONAL_WITH_COVERAGE",
        "query": (
            f"source={INDEX_PATTERN}"
            " | stats sum(event_id % 1000) as sum_mod_1000,"
            " avg(event_id % 997) as avg_mod_997,"
            " max(event_id % 991) as max_mod_991,"
            " min(event_id % 983) as min_mod_983"
        ),
        "poll_count": 10,
        "final_count": 10,
        "expected_total": 1,
        "expected_mode": "REPLACE",
        "compare_sync": True,
    },
    "composite_aggregation": {
        "semantic_model": "FINAL_PAGE_PREFIX",
        "query": (
            f"source={INDEX_PATTERN}"
            " | stats count() as event_count by group_id"
        ),
        "poll_count": 10_000,
        "final_count": 10_000,
        "expected_total": 10_000,
        "expected_mode": "REPLACE",
        "compare_sync": True,
    },
    "incremental_eventstats": {
        "semantic_model": "PROVISIONAL_WITH_COVERAGE",
        "query": (
            f"source={INDEX}"
            " | sort event_id"
            " | eventstats count() as group_count by group_id"
            " | head 10000"
        ),
        "poll_count": 3,
        "final_count": 10_000,
        "expected_total": 10_000,
        "expected_mode": "REPLACE",
        "compare_sync": True,
    },
    "unsupported_finite_window": {
        "semantic_model": "PROGRESS_ONLY",
        "query": (
            f"source={INDEX}"
            " | sort event_id"
            " | streamstats window=100 avg(event_id) as moving_avg"
            " | head 10000"
            " | fields event_id, moving_avg"
        ),
        "poll_count": 3,
        "final_count": 10_000,
        "expected_total": 10_000,
        "expected_mode": "REPLACE",
        "compare_sync": True,
    },
}


def request(method, path, body=None, timeout=600):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        BASE_URL + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.load(response)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")


def projection(response):
    return {
        key: response[key]
        for key in ("schema", "datarows", "total", "size")
        if key in response
    }


def clear_request_cache(index):
    request("POST", f"/{index}/_cache/clear?request=true")


def validate_rex_rows(rows):
    for event_id, email, user, domain in rows:
        expected_user, expected_domain = email.split("@", 1)
        if user != expected_user or domain != expected_domain:
            raise AssertionError(
                f"invalid REX row event_id={event_id}: {email}, {user}, {domain}"
            )


def finite_number(value):
    return isinstance(value, (int, float)) and math.isfinite(value)


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Run the five-query PPL partial-result semantic validation matrix "
            "against a prepared demo cluster."
        )
    )
    parser.add_argument(
        "--base-url",
        default=BASE_URL,
        help=f"OpenSearch endpoint (default: {BASE_URL})",
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=REPORT_DIR,
        help=f"Directory for raw responses and summary.json (default: {REPORT_DIR})",
    )
    return parser.parse_args()


def first_elapsed(snapshots, predicate):
    for snapshot in snapshots:
        if predicate(snapshot["response"]):
            return snapshot["elapsed_ms"]
    return None


def collect(name, case):
    case_dir = REPORT_DIR / name
    explain = request(
        "POST",
        "/_plugins/_ppl/_explain?mode=standard",
        {"query": case["query"]},
    )
    write_json(case_dir / "explain.json", explain)

    clear_request_cache(INDEX_PATTERN if INDEX_PATTERN in case["query"] else INDEX)
    started = time.perf_counter()
    current = request(
        "POST",
        "/_plugins/_ppl",
        {
            "query": case["query"],
            "wait_for_completion_timeout": "1ms",
            "keep_alive": "10m",
        },
    )
    submit_ms = round((time.perf_counter() - started) * 1000, 3)
    write_json(case_dir / "submit.json", current)
    if "id" not in current:
        raise AssertionError(f"{name} completed during submit")

    job_id = current["id"]
    sequence = current["sequence"]
    snapshots = []
    rex_samples = []

    while current["status"] == "RUNNING":
        current = request(
            "GET",
            (
                f"/_plugins/_ppl/jobs/{job_id}"
                f"?wait_for_sequence={sequence}"
                "&wait_for_completion_timeout=200ms"
                f"&count={case['poll_count']}"
            ),
        )
        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
        if current["sequence"] == sequence:
            continue
        sequence = current["sequence"]
        snapshot_file = f"sequence-{sequence:04d}.json"
        write_json(case_dir / snapshot_file, current)
        snapshots.append(
            {
                "elapsed_ms": elapsed_ms,
                "file": snapshot_file,
                "response": current,
            }
        )

        if name == "non_blocking_rex" and current["status"] == "RUNNING":
            total = current["total"]
            if total > 0:
                offset = max(0, total - 3)
                sample = request(
                    "GET",
                    f"/_plugins/_ppl/jobs/{job_id}?offset={offset}&count=3",
                )
                validate_rex_rows(sample["datarows"])
                sample_file = f"sequence-{sequence:04d}-tail.json"
                write_json(case_dir / sample_file, sample)
                rex_samples.append(
                    {
                        "sequence": sequence,
                        "elapsed_ms": elapsed_ms,
                        "offset": offset,
                        "rows": sample["datarows"],
                    }
                )

    final_ms = round((time.perf_counter() - started) * 1000, 3)
    final_response = request(
        "GET",
        f"/_plugins/_ppl/jobs/{job_id}?offset=0&count={case['final_count']}",
    )
    write_json(case_dir / "final.json", final_response)

    synchronous = None
    synchronous_ms = None
    if case.get("compare_sync"):
        clear_request_cache(INDEX_PATTERN if INDEX_PATTERN in case["query"] else INDEX)
        sync_started = time.perf_counter()
        synchronous = request("POST", "/_plugins/_ppl", {"query": case["query"]})
        synchronous_ms = round((time.perf_counter() - sync_started) * 1000, 3)
        write_json(case_dir / "synchronous.json", synchronous)

    evidence = {
        "submit_ms": submit_ms,
        "final_ms": final_ms,
        "synchronous_ms": synchronous_ms,
        "snapshots": snapshots,
        "final": final_response,
        "synchronous": synchronous,
        "rex_samples": rex_samples,
        "job_id": job_id,
    }
    return validate(name, case, evidence)


def validate(name, case, evidence):
    snapshots = evidence["snapshots"]
    running = [
        item for item in snapshots if item["response"]["status"] == "RUNNING"
    ]
    non_empty = [item for item in running if item["response"].get("datarows")]
    final_response = evidence["final"]
    synchronous = evidence["synchronous"]

    integrity = {
        "update_mode_matches": final_response["update_mode"] == case["expected_mode"],
        "final_total_matches": final_response["total"] == case["expected_total"],
        "final_progress_is_complete": final_response["progress"]["fraction_done"] == 1,
    }
    if synchronous is not None:
        integrity["final_matches_synchronous_exactly"] = (
            projection(final_response) == projection(synchronous)
        )

    meaningful_ms = None
    semantic_evidence = {}

    if name == "non_blocking_rex":
        validate_rex_rows(final_response["datarows"])
        stable_samples = True
        for sample in evidence["rex_samples"]:
            final_window = request(
                "GET",
                (
                    f"/_plugins/_ppl/jobs/{evidence['job_id']}"
                    f"?offset={sample['offset']}&count=3"
                ),
            )
            if sample["rows"] != final_window["datarows"]:
                stable_samples = False
                break
        integrity["all_sampled_partial_rows_are_final"] = stable_samples
        integrity["observed_rows_have_correct_rex_values"] = bool(evidence["rex_samples"])
        meaningful_ms = first_elapsed(
            running, lambda response: bool(response.get("datarows"))
        )
        semantic_evidence = {
            "sampled_append_windows": len(evidence["rex_samples"]),
            "all_sampled_windows_match_final": stable_samples,
        }

    elif name == "fully_pushed_aggregation":
        fractions = [
            item["response"]["progress"]["fraction_done"]
            for item in running
            if item["response"]["progress"]["fraction_done"] >= 0
        ]
        values = [
            item["response"]["datarows"][0]
            for item in non_empty
            if item["response"]["datarows"]
        ]
        valid_values = all(
            len(row) == 4
            and all(finite_number(value) for value in row)
            and row[0] >= 0
            and 0 <= row[1] <= 996
            and 0 <= row[2] <= 990
            and 0 <= row[3] <= 982
            for row in values
        )
        monotonic_progress = all(
            earlier <= later for earlier, later in zip(fractions, fractions[1:])
        )
        coverage_snapshots = [
            item
            for item in non_empty
            if 0 < item["response"]["progress"]["fraction_done"] < 1
        ]
        integrity["partial_aggregation_values_are_valid"] = valid_values
        integrity["running_progress_is_monotonic"] = monotonic_progress
        integrity["multiple_distinct_partial_states"] = len({tuple(v) for v in values}) > 1
        meaningful_ms = (
            None if not coverage_snapshots else coverage_snapshots[0]["elapsed_ms"]
        )
        semantic_evidence = {
            "first_covered_fraction": (
                None
                if not coverage_snapshots
                else coverage_snapshots[0]["response"]["progress"]["fraction_done"]
            ),
            "distinct_partial_states": len({tuple(v) for v in values}),
            "progress_samples": fractions,
        }

    elif name == "composite_aggregation":
        final_rows = final_response["datarows"]
        exact_pages = True
        prefix_pages = True
        totals = []
        for item in non_empty:
            response = item["response"]
            total = response["total"]
            rows = response["datarows"]
            totals.append(total)
            if total % 1000 != 0 or len(rows) != total:
                exact_pages = False
            if rows != final_rows[: len(rows)]:
                prefix_pages = False
            for event_count, group_id in rows:
                if event_count != 12000 or group_id < 0 or group_id >= 10000:
                    exact_pages = False
        integrity["each_snapshot_is_a_complete_page_prefix"] = exact_pages
        integrity["each_snapshot_matches_final_prefix"] = prefix_pages
        integrity["observed_all_ten_pages"] = totals == list(range(1000, 10001, 1000))
        meaningful_ms = None if not non_empty else non_empty[0]["elapsed_ms"]
        semantic_evidence = {
            "page_totals": totals,
            "partial_rows_are_final": exact_pages and prefix_pages,
        }

    elif name == "incremental_eventstats":
        first_values = [
            item["response"]["datarows"][0][3]
            for item in non_empty
            if item["response"]["datarows"]
        ]
        expected_values = [1, 2, 4, 8, 16, 32, 64, 128, 256]
        integrity["checkpoint_values_follow_expected_prefix_state"] = (
            first_values == expected_values
        )
        coverage_snapshots = [
            item
            for item in non_empty
            if 0 < item["response"]["progress"]["fraction_done"] < 1
        ]
        meaningful_ms = (
            None if not coverage_snapshots else coverage_snapshots[0]["elapsed_ms"]
        )
        semantic_evidence = {
            "first_non_empty_ms": None if not non_empty else non_empty[0]["elapsed_ms"],
            "first_row_checkpoint_values": first_values,
            "running_progress_values": [
                item["response"]["progress"]["fraction_done"] for item in non_empty
            ],
            "coverage_is_exposed": bool(coverage_snapshots),
        }

    elif name == "unsupported_finite_window":
        running_rows_empty = all(
            not item["response"].get("datarows") for item in running
        )
        progress_updates = [
            item
            for item in running
            if 0 < item["response"]["progress"]["fraction_done"] < 1
        ]
        integrity["running_rows_are_empty"] = running_rows_empty
        meaningful_ms = None if not progress_updates else progress_updates[0]["elapsed_ms"]
        semantic_evidence = {
            "running_progress_values": [
                item["response"]["progress"]["fraction_done"] for item in running
            ],
            "meaningful_progress_is_exposed": bool(progress_updates),
        }

    ratio = None if meaningful_ms is None else meaningful_ms / evidence["final_ms"]
    meaningful = meaningful_ms is not None and ratio < 0.8
    result = {
        "semantic_model": case["semantic_model"],
        "query": case["query"],
        "timing": {
            "first_non_empty_ms": (
                None if not non_empty else non_empty[0]["elapsed_ms"]
            ),
            "first_meaningful_ms": meaningful_ms,
            "final_ms": evidence["final_ms"],
            "first_meaningful_over_final": ratio,
            "synchronous_ms": evidence["synchronous_ms"],
        },
        "semantic_evidence": semantic_evidence,
        "integrity_gate": integrity,
        "meaningful_partial_result": meaningful,
        "verdict": "PASS" if meaningful else "FAIL",
    }
    if not all(integrity.values()):
        raise AssertionError(
            f"{name} integrity failure:\n{json.dumps(integrity, indent=2)}"
        )
    write_json(REPORT_DIR / name / "validation.json", result)
    return result


def main():
    global BASE_URL, REPORT_DIR
    args = parse_args()
    BASE_URL = args.base_url.rstrip("/")
    REPORT_DIR = args.report_dir

    if REPORT_DIR.exists():
        shutil.rmtree(REPORT_DIR)
    REPORT_DIR.mkdir(parents=True)

    request(
        "PUT",
        "/_cluster/settings",
        {"transient": {"plugins.query.buckets": 1000}},
    )
    cluster = request("GET", "/_cluster/health")
    single_count = request("GET", f"/{INDEX}/_count")["count"]
    all_count = request("GET", f"/{INDEX_PATTERN}/_count")["count"]
    nodes = request("GET", "/_nodes/stats/jvm")
    heap_max_bytes = [
        node["jvm"]["mem"]["heap_max_in_bytes"] for node in nodes["nodes"].values()
    ]
    environment_gate = {
        "cluster_is_green": cluster["status"] == "green",
        "single_index_has_5m_documents": single_count == 5_000_000,
        "pattern_has_120m_documents": all_count == 120_000_000,
        "every_node_has_at_least_16_gib_heap": (
            bool(heap_max_bytes) and min(heap_max_bytes) >= MIN_HEAP_BYTES
        ),
    }
    if not all(environment_gate.values()):
        raise AssertionError(json.dumps(environment_gate, indent=2))

    results = {}
    for name, case in CASES.items():
        print(f"running {name}", flush=True)
        results[name] = collect(name, case)
        print(json.dumps(results[name], indent=2), flush=True)

    report = {
        "environment": {
            "single_index_documents": single_count,
            "all_indices_documents": all_count,
            "heap_max_bytes_per_node": heap_max_bytes,
            "gate": environment_gate,
        },
        "results": results,
    }
    write_json(REPORT_DIR / "summary.json", report)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
