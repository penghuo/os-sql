#!/usr/bin/env python3
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

"""
Generates dqe_test_sharded bulk data with adversarial amount distribution.
Amount is inversely correlated with id order: id_00001 has amount=1.0,
id_50000 has amount=99999.99. This ensures ORDER BY amount DESC LIMIT N
must scan all shards.

Outputs ndjson bulk format to stdout.

Usage:
    python3 generate_sharded.py --count 500 > ../bulk/dqe_test_sharded.ndjson
    python3 generate_sharded.py --count 50000 > ../bulk/dqe_test_sharded.ndjson
"""

import argparse
import json
import sys


def main():
    parser = argparse.ArgumentParser(description="Generate dqe_test_sharded bulk data")
    parser.add_argument("--count", type=int, default=500, help="Number of documents")
    parser.add_argument("--categories", type=int, default=100, help="Number of categories")
    args = parser.parse_args()

    count = args.count
    num_cats = args.categories

    for i in range(1, count + 1):
        doc_id = f"id_{i:05d}"
        # Adversarial: amount increases with id, so top-N by DESC is in last shard
        # amount ranges from ~1.0 to ~(count * 2 - 1).0 with sub-cent precision
        amount = round(i * 2.0 - 0.01, 2)
        # Category assignment: cat_01 through cat_NN
        cat_num = ((i - 1) % num_cats) + 1
        category = f"cat_{cat_num:02d}"

        doc = {
            "id": doc_id,
            "category": category,
            "amount": amount
        }

        sys.stdout.write(json.dumps({"index": {"_id": str(i)}}) + "\n")
        sys.stdout.write(json.dumps(doc) + "\n")


if __name__ == "__main__":
    main()
