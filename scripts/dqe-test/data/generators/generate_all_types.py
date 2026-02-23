#!/usr/bin/env python3
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

"""
Generates dqe_test_all_types bulk data for DQE Phase 1 testing.
Outputs ndjson bulk format to stdout.

Usage:
    python3 generate_all_types.py --count 100 > ../bulk/dqe_test_all_types.ndjson
    python3 generate_all_types.py --count 10000 > ../bulk/dqe_test_all_types.ndjson
"""

import argparse
import base64
import json
import random
import sys

STATUSES = ["active", "pending", "closed", "archived"]
NAMES = [
    "test_name_1", "test_name_2", "test_name_3", "alpha_widget", "beta_gadget",
    "gamma_device", "delta_sensor", "epsilon_node", "zeta_relay", "eta_switch",
    "exact match", "test_prefix_abc", "test_prefix_xyz", "middle_content_here",
    "some_other_name", "long_name_for_testing_purposes", "short", "A", "Z",
    "test_name_special_chars"
]
DESCRIPTIONS = [
    "A quick brown fox", "Lazy dog sleeps", "Numbers 123 and symbols",
    "Short text value", "Longer descriptive text for testing fielddata access",
    "Empty-ish", "Technical description with details about implementation",
    "Another description", "Final description variant", ""
]
SOURCES = ["api", "web", "batch", "manual", "import"]


def generate_doc(doc_id, count, null_prob=0.1):
    """Generate a single dqe_test_all_types document."""
    rng = random  # uses module-level seeded random
    is_null = lambda: rng.random() < null_prob

    doc = {}
    doc["id"] = f"id_{doc_id:05d}"
    doc["name"] = rng.choice(NAMES) if not is_null() else None
    doc["description"] = rng.choice(DESCRIPTIONS) if not is_null() else None
    doc["status"] = rng.choice(STATUSES) if not is_null() else None
    doc["count_long"] = rng.randint(-1000000, 1000000) if not is_null() else None
    doc["count_int"] = rng.randint(-500, 500) if not is_null() else None
    doc["count_short"] = rng.randint(-32768, 32767) if not is_null() else None
    doc["count_byte"] = rng.randint(-128, 127) if not is_null() else None
    doc["price_double"] = round(rng.uniform(-1000.0, 10000.0), 6) if not is_null() else None
    doc["price_float"] = round(rng.uniform(-100.0, 1000.0), 3) if not is_null() else None
    doc["price_half"] = round(rng.uniform(-10.0, 100.0), 1) if not is_null() else None
    doc["price_scaled"] = round(rng.uniform(0.0, 999.99), 2) if not is_null() else None
    doc["is_active"] = rng.choice([True, False]) if not is_null() else None

    # Timestamps: created_at uses epoch_millis
    if not is_null():
        # epoch_millis from 2020-01-01 to 2025-12-31
        doc["created_at"] = rng.randint(1577836800000, 1767225600000)
    else:
        doc["created_at"] = None

    # updated_at uses strict_date_optional_time
    if not is_null():
        year = rng.randint(2020, 2025)
        month = rng.randint(1, 12)
        day = rng.randint(1, 28)
        hour = rng.randint(0, 23)
        minute = rng.randint(0, 59)
        second = rng.randint(0, 59)
        doc["updated_at"] = f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}.000Z"
    else:
        doc["updated_at"] = None

    # custom_date uses yyyy/MM/dd HH:mm:ss
    if not is_null():
        year = rng.randint(2020, 2025)
        month = rng.randint(1, 12)
        day = rng.randint(1, 28)
        hour = rng.randint(0, 23)
        minute = rng.randint(0, 59)
        second = rng.randint(0, 59)
        doc["custom_date"] = f"{year:04d}/{month:02d}/{day:02d} {hour:02d}:{minute:02d}:{second:02d}"
    else:
        doc["custom_date"] = None

    # precise_time uses date_nanos
    if not is_null():
        year = rng.randint(2020, 2025)
        month = rng.randint(1, 12)
        day = rng.randint(1, 28)
        nanos = rng.randint(0, 999999999)
        doc["precise_time"] = f"{year:04d}-{month:02d}-{day:02d}T12:00:00.{nanos:09d}Z"
    else:
        doc["precise_time"] = None

    # IP address
    if not is_null():
        ips = [
            f"192.168.{rng.randint(0,255)}.{rng.randint(1,254)}",
            f"10.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(1,254)}",
            "192.168.1.1",
            "::1",
            "255.255.255.255"
        ]
        doc["ip_address"] = rng.choice(ips)
    else:
        doc["ip_address"] = None

    # geo_point
    if not is_null():
        lats = [40.7128, -33.8688, 0.0, 90.0, -90.0, rng.uniform(-90, 90)]
        lons = [-74.006, 151.2093, 0.0, 180.0, -180.0, rng.uniform(-180, 180)]
        idx = rng.randint(0, len(lats) - 1)
        doc["location"] = {"lat": round(lats[idx], 4), "lon": round(lons[idx], 4)}
    else:
        doc["location"] = None

    # geo_shape
    if not is_null():
        doc["boundary"] = {
            "type": "Point",
            "coordinates": [round(rng.uniform(-180, 180), 4), round(rng.uniform(-90, 90), 4)]
        }
    else:
        doc["boundary"] = None

    # tags (multi-valued keyword, 0-5 values)
    tag_count = rng.randint(0, 5)
    all_tags = ["java", "python", "go", "rust", "ml", "data", "web", "api", "cloud", "infra"]
    doc["tags"] = rng.sample(all_tags, min(tag_count, len(all_tags)))

    # nested_items
    if not is_null():
        n = rng.randint(1, 3)
        doc["nested_items"] = [
            {"item_name": f"item_{rng.randint(1,100)}", "item_value": rng.randint(1, 1000)}
            for _ in range(n)
        ]
    else:
        doc["nested_items"] = None

    # metadata (object)
    if not is_null():
        doc["metadata"] = {"source": rng.choice(SOURCES), "version": rng.randint(1, 10)}
    else:
        doc["metadata"] = None

    # flattened attributes
    if not is_null():
        doc["attributes"] = {
            f"attr_{rng.randint(1,20)}": f"val_{rng.randint(1,100)}",
            f"attr_{rng.randint(21,40)}": f"val_{rng.randint(100,200)}"
        }
    else:
        doc["attributes"] = None

    # dense_vector
    doc["vector_field"] = [round(rng.random(), 4) for _ in range(3)]

    # binary_data
    if not is_null():
        raw = bytes([rng.randint(0, 255) for _ in range(rng.randint(4, 20))])
        doc["binary_data"] = base64.b64encode(raw).decode("ascii")
    else:
        doc["binary_data"] = None

    # unsigned_long (include some values > 2^63)
    if not is_null():
        if rng.random() < 0.2:
            # Exceeds BIGINT range
            doc["big_number"] = rng.randint(9223372036854775808, 18446744073709551615)
        else:
            doc["big_number"] = rng.randint(0, 9223372036854775807)
    else:
        doc["big_number"] = None

    # Remove None values from doc (OpenSearch treats missing fields as null)
    return {k: v for k, v in doc.items() if v is not None}


def main():
    parser = argparse.ArgumentParser(description="Generate dqe_test_all_types bulk data")
    parser.add_argument("--count", type=int, default=100, help="Number of documents")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--null-prob", type=float, default=0.1, help="Null probability per field")
    args = parser.parse_args()

    random.seed(args.seed)

    # Ensure specific docs exist for deterministic test assertions
    # Doc 1: all fields populated, known values
    special_doc1 = {
        "id": "id_00001",
        "name": "test_name_1",
        "description": "A quick brown fox",
        "status": "active",
        "count_long": 42,
        "count_int": 42,
        "count_short": 42,
        "count_byte": 42,
        "price_double": 99.99,
        "price_float": 10.5,
        "price_half": 5.0,
        "price_scaled": 100.50,
        "is_active": True,
        "created_at": 1718451000000,
        "updated_at": "2024-06-15T10:30:00.000Z",
        "custom_date": "2024/06/15 10:30:00",
        "precise_time": "2024-06-15T10:30:00.123456789Z",
        "ip_address": "192.168.1.1",
        "location": {"lat": 40.7128, "lon": -74.006},
        "boundary": {"type": "Point", "coordinates": [-74.006, 40.7128]},
        "tags": ["java", "python"],
        "nested_items": [{"item_name": "widget", "item_value": 100}],
        "metadata": {"source": "api", "version": 1},
        "attributes": {"env": "production", "tier": "gold"},
        "vector_field": [0.5, 0.25, 0.75],
        "binary_data": "SGVsbG8gV29ybGQ=",
        "big_number": 18446744073709551000
    }

    # Write special doc 1
    sys.stdout.write(json.dumps({"index": {"_id": "1"}}) + "\n")
    sys.stdout.write(json.dumps(special_doc1) + "\n")

    # Doc 2: known values for name.keyword exact match test
    special_doc2 = {
        "id": "id_00002",
        "name": "exact match",
        "description": "Document for exact keyword match test",
        "status": "pending",
        "count_long": 100,
        "count_int": 0,
        "count_short": 0,
        "count_byte": 0,
        "price_double": 0.0,
        "price_float": 0.0,
        "price_half": 0.0,
        "price_scaled": 0.0,
        "is_active": False,
        "created_at": 1577836800000,
        "updated_at": "2024-01-01T00:00:00.000Z",
        "custom_date": "2024/01/01 00:00:00",
        "precise_time": "2024-01-01T00:00:00.000000001Z",
        "ip_address": "10.0.0.1",
        "location": {"lat": 0.0, "lon": 0.0},
        "boundary": {"type": "Point", "coordinates": [0.0, 0.0]},
        "tags": [],
        "nested_items": [{"item_name": "basic", "item_value": 0}],
        "metadata": {"source": "batch", "version": 2},
        "attributes": {"env": "staging"},
        "vector_field": [0.0, 0.0, 0.0],
        "binary_data": "AQID",
        "big_number": 0
    }
    sys.stdout.write(json.dumps({"index": {"_id": "2"}}) + "\n")
    sys.stdout.write(json.dumps(special_doc2) + "\n")

    # Generate remaining docs
    for i in range(3, args.count + 1):
        doc = generate_doc(i, args.count, args.null_prob)
        sys.stdout.write(json.dumps({"index": {"_id": str(i)}}) + "\n")
        sys.stdout.write(json.dumps(doc) + "\n")


if __name__ == "__main__":
    main()
