#!/usr/bin/env python3
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

"""
Generates dqe_test_nulls bulk data with systematic NULL patterns.
Outputs ndjson bulk format to stdout.

Usage:
    python3 generate_nulls.py --count 50 > ../bulk/dqe_test_nulls.ndjson
    python3 generate_nulls.py --count 1000 > ../bulk/dqe_test_nulls.ndjson
"""

import argparse
import json
import random
import sys


def main():
    parser = argparse.ArgumentParser(description="Generate dqe_test_nulls bulk data")
    parser.add_argument("--count", type=int, default=50, help="Number of documents")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()

    random.seed(args.seed)
    nullable_fields = ["int_val", "str_val", "bool_val", "date_val", "dbl_val"]

    for i in range(1, args.count + 1):
        doc = {"id": f"null_{i:04d}"}

        if i == 1:
            # All-NULL row
            pass
        elif i == 2:
            # No-NULL row
            doc["int_val"] = 100
            doc["str_val"] = "present"
            doc["bool_val"] = True
            doc["date_val"] = "2024-06-15T00:00:00Z"
            doc["dbl_val"] = 3.14
        elif i <= 7:
            # Single-NULL rows: each field is null once
            all_vals = {
                "int_val": (i - 2) * 10,
                "str_val": f"value_{i}",
                "bool_val": i % 2 == 0,
                "date_val": f"2024-0{i}-01T00:00:00Z",
                "dbl_val": round(i * 1.1, 2)
            }
            skip_field = nullable_fields[(i - 3) % len(nullable_fields)]
            for f, v in all_vals.items():
                if f != skip_field:
                    doc[f] = v
        else:
            # Random ~30% null per field
            if random.random() > 0.3:
                doc["int_val"] = random.randint(-1000, 1000)
            if random.random() > 0.3:
                doc["str_val"] = random.choice(["alpha", "beta", "gamma", "delta", "present", "value_x"])
            if random.random() > 0.3:
                doc["bool_val"] = random.choice([True, False])
            if random.random() > 0.3:
                month = random.randint(1, 12)
                day = random.randint(1, 28)
                doc["date_val"] = f"2024-{month:02d}-{day:02d}T00:00:00Z"
            if random.random() > 0.3:
                doc["dbl_val"] = round(random.uniform(-100.0, 100.0), 4)

        sys.stdout.write(json.dumps({"index": {"_id": str(i)}}) + "\n")
        sys.stdout.write(json.dumps(doc) + "\n")


if __name__ == "__main__":
    main()
