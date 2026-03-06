#!/usr/bin/env python3
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

"""
Validates DQE REST API responses against expected results defined in
test case JSON files.

Test case format (JSON files under cases/):
{
    "name": "test_name",
    "description": "Optional description",
    "query": "SELECT ...",
    "expected": {
        "schema": [
            {"name": "col_name", "type": "VARCHAR"}
        ],
        "data": [
            ["value1", "value2"],
            ["value3", "value4"]
        ]
    },
    "error": {                  // Optional: expect an error instead of results
        "error_code": "PARSING_ERROR",
        "message_contains": "syntax error"
    },
    "ignore_order": false,      // Optional: ignore row order in comparison
    "float_tolerance": 0.001    // Optional: tolerance for floating-point comparison
}

Usage:
    python3 validate.py --url http://localhost:9200 --cases cases/phase1/
    python3 validate.py --url http://localhost:9200 --case cases/phase1/test_select_star.json
"""

import argparse
import json
import math
import os
import sys
from typing import Any

import urllib.request
import urllib.error

# Type normalization maps — both Trino and DQE types are mapped to a
# common uppercase representation for comparison. This allows test files
# to store Trino-native types while validating against DQE responses.
_TRINO_TYPE_MAP = {
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

_DQE_TYPE_MAP = {
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


def normalize_type(type_name):
    """Normalize a type name to the common type system.

    Handles both Trino-style (lowercase, e.g. 'varchar', 'bigint') and
    DQE-style (uppercase, e.g. 'VARCHAR', 'LONG') type names.
    """
    if type_name is None:
        return "UNKNOWN"
    stripped = type_name.strip()
    lower = stripped.lower()
    upper = stripped.upper()
    # Try Trino map first (lowercase keys)
    base = lower.split("(")[0].strip() if "(" in lower and "with" not in lower else lower
    if lower in _TRINO_TYPE_MAP:
        return _TRINO_TYPE_MAP[lower]
    if base in _TRINO_TYPE_MAP:
        return _TRINO_TYPE_MAP[base]
    # Try DQE map (uppercase keys)
    if upper in _DQE_TYPE_MAP:
        return _DQE_TYPE_MAP[upper]
    return upper


def execute_query(url: str, query: str) -> dict:
    """Execute a SQL query via the DQE REST API."""
    endpoint = f"{url}/_plugins/_trino_sql"
    body = {"query": query}
    payload = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        endpoint,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {"error": {"reason": body, "status": e.code}}


def values_equal(expected: Any, actual: Any, float_tolerance: float = 0.001) -> bool:
    """Compare two values with optional float tolerance."""
    if expected is None and actual is None:
        return True
    if expected is None or actual is None:
        return False
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        if math.isnan(expected) and math.isnan(actual):
            return True
        if abs(expected) < 1e-10 and abs(actual) < 1e-10:
            return True
        return abs(expected - actual) <= float_tolerance * max(abs(expected), abs(actual), 1)
    return str(expected) == str(actual)


def compare_schema(expected_schema, actual_schema):
    """Compare expected and actual schema. Returns list of error messages."""
    errors = []
    if len(expected_schema) != len(actual_schema):
        errors.append(
            f"Schema length mismatch: expected {len(expected_schema)}, got {len(actual_schema)}"
        )
        return errors

    for i, (exp, act) in enumerate(zip(expected_schema, actual_schema)):
        if exp.get("name") != act.get("name"):
            errors.append(
                f"Column {i} name mismatch: expected '{exp.get('name')}', got '{act.get('name')}'"
            )
        if "type" in exp:
            exp_norm = normalize_type(exp.get("type"))
            act_norm = normalize_type(act.get("type"))
            if exp_norm != act_norm:
                errors.append(
                    f"Column {i} type mismatch: expected '{exp.get('type')}' ({exp_norm}), "
                    f"got '{act.get('type')}' ({act_norm})"
                )
    return errors


def compare_data(
    expected_data: list, actual_data: list, ignore_order: bool, float_tolerance: float
) -> list:
    """Compare expected and actual data rows. Returns list of error messages."""
    errors = []
    if len(expected_data) != len(actual_data):
        errors.append(
            f"Row count mismatch: expected {len(expected_data)}, got {len(actual_data)}"
        )
        return errors

    if ignore_order:
        expected_sorted = sorted(expected_data, key=lambda r: json.dumps(r, default=str))
        actual_sorted = sorted(actual_data, key=lambda r: json.dumps(r, default=str))
    else:
        expected_sorted = expected_data
        actual_sorted = actual_data

    for row_idx, (exp_row, act_row) in enumerate(zip(expected_sorted, actual_sorted)):
        if len(exp_row) != len(act_row):
            errors.append(
                f"Row {row_idx}: column count mismatch: expected {len(exp_row)}, got {len(act_row)}"
            )
            continue
        for col_idx, (exp_val, act_val) in enumerate(zip(exp_row, act_row)):
            if not values_equal(exp_val, act_val, float_tolerance):
                errors.append(
                    f"Row {row_idx}, Col {col_idx}: expected {exp_val!r}, got {act_val!r}"
                )
    return errors


def validate_test_case(url: str, test_case: dict) -> tuple:
    """
    Validate a single test case. Returns (passed: bool, errors: list[str]).
    """
    name = test_case.get("name", "unnamed")
    query = test_case.get("query", "")
    ignore_order = test_case.get("ignore_order", False)
    float_tolerance = test_case.get("float_tolerance", 0.001)

    result = execute_query(url, query)

    # Check if we expect an error
    expected_error = test_case.get("error")
    if expected_error:
        if "error" not in result:
            return False, [f"Expected error but got success response"]
        error_resp = result["error"]
        errs = []
        if "error_code" in expected_error:
            actual_code = error_resp.get("type", error_resp.get("error_code", ""))
            if expected_error["error_code"] not in str(actual_code):
                errs.append(
                    f"Error code mismatch: expected '{expected_error['error_code']}', got '{actual_code}'"
                )
        if "message_contains" in expected_error:
            # Check both 'reason' and 'details' fields for the expected substring,
            # since the error message may appear in either field depending on the formatter
            actual_reason = str(error_resp.get("reason", ""))
            actual_details = str(error_resp.get("details", ""))
            expected_substr = expected_error["message_contains"]
            if expected_substr not in actual_reason and expected_substr not in actual_details:
                errs.append(
                    f"Error message does not contain '{expected_substr}': "
                    f"reason='{actual_reason}', details='{actual_details}'"
                )
        return len(errs) == 0, errs

    # Compare successful results
    expected = test_case.get("expected", {})
    errors = []

    if "schema" in expected and "schema" in result:
        errors.extend(compare_schema(expected["schema"], result["schema"]))

    if "data" in expected:
        actual_data = result.get("datarows", result.get("data", []))
        errors.extend(
            compare_data(expected["data"], actual_data, ignore_order, float_tolerance)
        )

    # Check expected_row_count (lightweight assertion that real data is returned)
    if "expected_row_count" in expected:
        actual_data = result.get("datarows", result.get("data", []))
        expected_count = expected["expected_row_count"]
        actual_count = len(actual_data)
        if actual_count != expected_count:
            errors.append(
                f"Row count mismatch: expected {expected_count}, got {actual_count}"
            )

    # Check expected_min_row_count (at least N rows returned)
    if "expected_min_row_count" in expected:
        actual_data = result.get("datarows", result.get("data", []))
        min_count = expected["expected_min_row_count"]
        actual_count = len(actual_data)
        if actual_count < min_count:
            errors.append(
                f"Row count too low: expected at least {min_count}, got {actual_count}"
            )

    return len(errors) == 0, errors


def load_test_cases(path: str) -> list:
    """Load test cases from a file or directory (recursive)."""
    cases = []
    if os.path.isfile(path):
        with open(path, "r") as f:
            data = json.load(f)
            if isinstance(data, list):
                cases.extend(data)
            else:
                cases.append(data)
    elif os.path.isdir(path):
        for dirpath, _dirnames, filenames in os.walk(path):
            for filename in sorted(filenames):
                if filename.endswith(".json"):
                    filepath = os.path.join(dirpath, filename)
                    with open(filepath, "r") as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            cases.extend(data)
                        else:
                            cases.append(data)
    return cases


def main():
    parser = argparse.ArgumentParser(description="Validate DQE REST API responses")
    parser.add_argument("--url", default="http://localhost:9200", help="OpenSearch URL")
    parser.add_argument("--cases", help="Path to test case directory")
    parser.add_argument("--case", help="Path to a single test case file")
    parser.add_argument("--verbose", action="store_true", help="Print details for passing tests")
    args = parser.parse_args()

    path = args.case or args.cases
    if not path:
        print("ERROR: Specify --cases <dir> or --case <file>", file=sys.stderr)
        sys.exit(1)

    test_cases = load_test_cases(path)
    if not test_cases:
        print(f"No test cases found at: {path}", file=sys.stderr)
        sys.exit(1)

    passed = 0
    failed = 0
    total = len(test_cases)

    print(f"Running {total} test case(s) against {args.url}")
    print("=" * 60)

    for tc in test_cases:
        name = tc.get("name", "unnamed")
        ok, errors = validate_test_case(args.url, tc)
        if ok:
            passed += 1
            if args.verbose:
                print(f"  PASS: {name}")
        else:
            failed += 1
            print(f"  FAIL: {name}")
            for err in errors:
                print(f"        {err}")

    print("=" * 60)
    print(f"Results: {passed}/{total} passed, {failed} failed")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
