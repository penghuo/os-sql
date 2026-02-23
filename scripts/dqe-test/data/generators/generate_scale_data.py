#!/usr/bin/env python3
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

"""
Generates scale test data for DQE performance and stress testing.
Outputs ndjson bulk format to stdout.

Usage:
    python3 generate_scale_data.py --index dqe_scale_test --count 10000 > data/bulk/dqe_scale_test.ndjson
"""

import argparse
import json
import random
import sys
from datetime import datetime, timedelta

DEPARTMENTS = ["Engineering", "Marketing", "Sales", "Finance", "HR", "Legal", "Support"]
CITIES = ["Seattle", "Portland", "San Francisco", "Austin", "New York", "Chicago", "Denver"]
STATES = ["WA", "OR", "CA", "TX", "NY", "IL", "CO"]
FIRST_NAMES = ["Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Hank", "Iris", "Jack"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor", "Anderson"]


def generate_record(record_id: int, null_probability: float = 0.1) -> dict:
    """Generate a single employee-like record with configurable null probability."""
    record = {"id": record_id}

    if random.random() > null_probability:
        record["name"] = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
    else:
        record["name"] = None

    if random.random() > null_probability:
        record["department"] = random.choice(DEPARTMENTS)

    if random.random() > null_probability:
        record["salary"] = round(random.uniform(40000, 200000), 2)

    if random.random() > null_probability:
        record["age"] = random.randint(22, 65)

    if random.random() > null_probability:
        record["active"] = random.choice([True, False])

    if random.random() > null_probability:
        base_date = datetime(2015, 1, 1)
        delta = timedelta(days=random.randint(0, 3650))
        record["hire_date"] = (base_date + delta).strftime("%Y-%m-%d")

    if random.random() > null_probability:
        idx = random.randint(0, len(CITIES) - 1)
        record["address"] = {
            "city": CITIES[idx],
            "state": STATES[idx],
        }

    return record


def main():
    parser = argparse.ArgumentParser(description="Generate scale test data for DQE")
    parser.add_argument("--index", default="dqe_scale_test", help="Index name for bulk metadata")
    parser.add_argument("--count", type=int, default=10000, help="Number of records to generate")
    parser.add_argument("--null-probability", type=float, default=0.1,
                        help="Probability of null values (0.0-1.0)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    args = parser.parse_args()

    random.seed(args.seed)

    for i in range(1, args.count + 1):
        action = json.dumps({"index": {"_id": str(i)}})
        record = json.dumps(generate_record(i, args.null_probability))
        sys.stdout.write(action + "\n")
        sys.stdout.write(record + "\n")


if __name__ == "__main__":
    main()
