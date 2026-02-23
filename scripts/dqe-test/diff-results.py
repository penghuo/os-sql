#!/usr/bin/env python3
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

"""
Compares DQE query results against standalone Trino query results for
differential testing. Handles type mapping differences between engines
and supports configurable float/timestamp tolerances.

Usage:
    python3 diff-results.py <case_file> <dqe_response_json> <trino_response_json>

    The case file is a JSON file with at minimum:
        {"id": "D001", "query": "...", "differential": true}
    Optional tolerance overrides:
        {"tolerance": {"float_epsilon": 1e-10, "timestamp_precision_ms": true}}

Exit code: 0 = match, 1 = mismatch, 2 = execution error.
Output: JSON report to stdout.
"""

import json
import math
import sys
from typing import Any, Optional


# --- Type mapping: Trino types -> DQE common types ---
# DQE and Trino may report slightly different type names for the same data.
# This mapping normalizes both to a common representation for comparison.
TRINO_TYPE_MAP = {
    "varchar": "VARCHAR",
    "bigint": "BIGINT",
    "integer": "INTEGER",
    "smallint": "SMALLINT",
    "tinyint": "TINYINT",
    "double": "DOUBLE",
    "real": "FLOAT",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "timestamp with time zone": "TIMESTAMP",
    "timestamp(3)": "TIMESTAMP",
    "timestamp(3) with time zone": "TIMESTAMP",
    "varbinary": "BINARY",
    "ipaddress": "IP",
    "row": "OBJECT",
    "array": "ARRAY",
    "map": "OBJECT",
    "json": "VARCHAR",
}

DQE_TYPE_MAP = {
    "VARCHAR": "VARCHAR",
    "TEXT": "VARCHAR",
    "KEYWORD": "VARCHAR",
    "BIGINT": "BIGINT",
    "LONG": "BIGINT",
    "INTEGER": "INTEGER",
    "INT": "INTEGER",
    "SMALLINT": "SMALLINT",
    "SHORT": "SMALLINT",
    "TINYINT": "TINYINT",
    "BYTE": "TINYINT",
    "DOUBLE": "DOUBLE",
    "FLOAT": "FLOAT",
    "HALF_FLOAT": "FLOAT",
    "SCALED_FLOAT": "DOUBLE",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "DATE_NANOS": "TIMESTAMP",
    "TIMESTAMP": "TIMESTAMP",
    "BINARY": "BINARY",
    "IP": "IP",
    "GEO_POINT": "VARCHAR",
    "NESTED": "OBJECT",
    "OBJECT": "OBJECT",
    "FLATTENED": "VARCHAR",
    "UNSIGNED_LONG": "BIGINT",
}


def normalize_type(type_name: str, engine: str) -> str:
    """Normalize a type name to the common type system."""
    if type_name is None:
        return "UNKNOWN"
    upper = type_name.upper().strip()
    lower = type_name.lower().strip()
    if engine == "trino":
        # Handle parameterized types like varchar(255)
        base = lower.split("(")[0].strip() if "(" in lower and "with" not in lower else lower
        return TRINO_TYPE_MAP.get(lower, TRINO_TYPE_MAP.get(base, upper))
    else:
        return DQE_TYPE_MAP.get(upper, upper)


def extract_dqe_results(response: dict) -> tuple:
    """
    Extract (columns, rows) from a DQE REST API response.
    DQE format: {"schema": [{"name":..., "type":...}], "datarows": [[...]]}
    """
    schema = response.get("schema", [])
    columns = [(col.get("name", ""), col.get("type", "")) for col in schema]
    rows = response.get("datarows", response.get("data", []))
    return columns, rows


def extract_trino_results(response: dict) -> tuple:
    """
    Extract (columns, rows) from a Trino final response.
    Trino format: {"columns": [{"name":..., "type":...}], "data": [[...]]}
    """
    columns_raw = response.get("columns", [])
    columns = [(col.get("name", ""), col.get("type", "")) for col in columns_raw]
    rows = response.get("data", [])
    return columns, rows


def values_match(
    dqe_val: Any,
    trino_val: Any,
    float_epsilon: float,
    timestamp_precision_ms: bool,
) -> bool:
    """Compare two cell values with tolerance."""
    # Both NULL
    if dqe_val is None and trino_val is None:
        return True
    # One NULL
    if dqe_val is None or trino_val is None:
        return False

    # Numeric comparison with epsilon
    if isinstance(dqe_val, (int, float)) and isinstance(trino_val, (int, float)):
        if math.isnan(float(dqe_val)) and math.isnan(float(trino_val)):
            return True
        if math.isinf(float(dqe_val)) and math.isinf(float(trino_val)):
            return float(dqe_val) == float(trino_val)  # same sign
        diff = abs(float(dqe_val) - float(trino_val))
        magnitude = max(abs(float(dqe_val)), abs(float(trino_val)), 1.0)
        return diff <= float_epsilon * magnitude

    # Boolean comparison (Trino may return bool, DQE may return string)
    if isinstance(dqe_val, bool) or isinstance(trino_val, bool):
        return str(dqe_val).lower() == str(trino_val).lower()

    # Timestamp comparison: truncate to ms if configured
    if timestamp_precision_ms:
        dqe_str = str(dqe_val)
        trino_str = str(trino_val)
        # If both look like timestamps, compare truncated to ms (23 chars: YYYY-MM-DDTHH:MM:SS.mmm)
        if len(dqe_str) >= 19 and len(trino_str) >= 19:
            if dqe_str[4] == '-' and trino_str[4] == '-':
                # Normalize separators
                dqe_norm = dqe_str.replace("T", " ").replace("Z", "")
                trino_norm = trino_str.replace("T", " ").replace("Z", "")
                if dqe_norm[:23] == trino_norm[:23]:
                    return True

    # String comparison as fallback
    return str(dqe_val) == str(trino_val)


def compare_results(
    dqe_columns: list,
    dqe_rows: list,
    trino_columns: list,
    trino_rows: list,
    has_order_by: bool,
    float_epsilon: float,
    timestamp_precision_ms: bool,
) -> dict:
    """
    Compare DQE and Trino results. Returns a report dict with:
    {"match": bool, "errors": [...], "column_diffs": [...], "row_diffs": [...]}
    """
    report = {"match": True, "errors": [], "column_diffs": [], "row_diffs": []}

    # Compare column count
    if len(dqe_columns) != len(trino_columns):
        report["match"] = False
        report["errors"].append(
            f"Column count mismatch: DQE={len(dqe_columns)}, Trino={len(trino_columns)}"
        )
        return report

    # Compare column names and types
    for i, ((dqe_name, dqe_type), (trino_name, trino_type)) in enumerate(
        zip(dqe_columns, trino_columns)
    ):
        name_match = dqe_name.lower() == trino_name.lower()
        dqe_norm = normalize_type(dqe_type, "dqe")
        trino_norm = normalize_type(trino_type, "trino")
        type_match = dqe_norm == trino_norm

        if not name_match:
            report["column_diffs"].append(
                {
                    "column": i,
                    "field": "name",
                    "dqe": dqe_name,
                    "trino": trino_name,
                }
            )
        if not type_match:
            report["column_diffs"].append(
                {
                    "column": i,
                    "field": "type",
                    "dqe": f"{dqe_type} -> {dqe_norm}",
                    "trino": f"{trino_type} -> {trino_norm}",
                }
            )

    if report["column_diffs"]:
        report["match"] = False

    # Compare row count
    if len(dqe_rows) != len(trino_rows):
        report["match"] = False
        report["errors"].append(
            f"Row count mismatch: DQE={len(dqe_rows)}, Trino={len(trino_rows)}"
        )
        return report

    # Sort if no ORDER BY (both by all columns, stringified)
    if not has_order_by:
        dqe_sorted = sorted(dqe_rows, key=lambda r: json.dumps(r, default=str))
        trino_sorted = sorted(trino_rows, key=lambda r: json.dumps(r, default=str))
    else:
        dqe_sorted = dqe_rows
        trino_sorted = trino_rows

    # Compare row by row
    for row_idx, (dqe_row, trino_row) in enumerate(zip(dqe_sorted, trino_sorted)):
        if len(dqe_row) != len(trino_row):
            report["match"] = False
            report["row_diffs"].append(
                {
                    "row": row_idx,
                    "error": f"Column count in row differs: DQE={len(dqe_row)}, Trino={len(trino_row)}",
                }
            )
            continue

        for col_idx, (dqe_val, trino_val) in enumerate(zip(dqe_row, trino_row)):
            if not values_match(dqe_val, trino_val, float_epsilon, timestamp_precision_ms):
                report["match"] = False
                report["row_diffs"].append(
                    {
                        "row": row_idx,
                        "column": col_idx,
                        "dqe_value": dqe_val,
                        "trino_value": trino_val,
                    }
                )

    return report


def has_explicit_order_by(query: str) -> bool:
    """Check if a SQL query contains an explicit ORDER BY clause."""
    # Simple heuristic: look for ORDER BY outside of subqueries
    upper = query.upper()
    # Remove content inside parentheses (subqueries) for a rough check
    depth = 0
    filtered = []
    for ch in upper:
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth = max(0, depth - 1)
        elif depth == 0:
            filtered.append(ch)
    return "ORDER BY" in "".join(filtered)


def main():
    if len(sys.argv) != 4:
        print(
            "Usage: diff-results.py <case_file.json> <dqe_response_json> <trino_response_json>",
            file=sys.stderr,
        )
        sys.exit(2)

    case_file = sys.argv[1]
    dqe_json_str = sys.argv[2]
    trino_json_str = sys.argv[3]

    # Load test case
    try:
        with open(case_file, "r") as f:
            test_case = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(json.dumps({"match": False, "errors": [f"Failed to load case file: {e}"]}))
        sys.exit(2)

    # Parse responses
    try:
        dqe_response = json.loads(dqe_json_str)
    except json.JSONDecodeError as e:
        print(json.dumps({"match": False, "errors": [f"Failed to parse DQE response: {e}"]}))
        sys.exit(2)

    try:
        trino_response = json.loads(trino_json_str)
    except json.JSONDecodeError as e:
        print(json.dumps({"match": False, "errors": [f"Failed to parse Trino response: {e}"]}))
        sys.exit(2)

    # Check for error responses
    if "error" in dqe_response:
        err = dqe_response["error"]
        msg = err.get("reason", err.get("message", str(err)))
        print(json.dumps({"match": False, "errors": [f"DQE returned error: {msg}"]}))
        sys.exit(1)

    if "error" in trino_response:
        err = trino_response["error"]
        msg = err.get("message", str(err))
        print(json.dumps({"match": False, "errors": [f"Trino returned error: {msg}"]}))
        sys.exit(1)

    # Extract tolerance settings
    tolerance = test_case.get("tolerance", {})
    float_epsilon = tolerance.get("float_epsilon", 1e-10)
    timestamp_precision_ms = tolerance.get("timestamp_precision_ms", True)

    # Extract results
    dqe_columns, dqe_rows = extract_dqe_results(dqe_response)
    trino_columns, trino_rows = extract_trino_results(trino_response)

    # Determine if query has ORDER BY
    query = test_case.get("query", "")
    order_by = has_explicit_order_by(query)

    # Compare
    report = compare_results(
        dqe_columns,
        dqe_rows,
        trino_columns,
        trino_rows,
        has_order_by=order_by,
        float_epsilon=float_epsilon,
        timestamp_precision_ms=timestamp_precision_ms,
    )

    report["case_id"] = test_case.get("id", "unknown")
    report["query"] = query

    print(json.dumps(report, indent=2, default=str))
    sys.exit(0 if report["match"] else 1)


if __name__ == "__main__":
    main()
