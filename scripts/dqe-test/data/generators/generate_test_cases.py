#!/usr/bin/env python3
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

"""
Generates all DQE Phase 1 test case JSON files.

Usage:
    python3 generate_test_cases.py --output-dir ../../cases
"""

import json
import os
import sys


def write_case(base_dir, subdir, filename, case):
    """Write a test case JSON file."""
    dirpath = os.path.join(base_dir, subdir)
    os.makedirs(dirpath, exist_ok=True)
    filepath = os.path.join(dirpath, filename)
    with open(filepath, "w") as f:
        json.dump(case, f, indent=2)
        f.write("\n")


def basic_select_cases():
    """Q001-Q015: Basic SELECT tests (section 4.1)."""
    return [
        {
            "id": "Q001", "name": "basic_column_selection",
            "query": "SELECT id, name FROM dqe_test_all_types LIMIT 10",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "name", "type": "VARCHAR"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q002", "name": "star_expansion",
            "query": "SELECT * FROM dqe_test_all_types LIMIT 5",
            "expected": {
                "schema": []
            },
            "ignore_order": True,
            "_note": "Schema validated by presence of all columns; exact schema depends on mapping"
        },
        {
            "id": "Q003", "name": "column_aliases",
            "query": "SELECT id AS identifier, name AS label FROM dqe_test_all_types LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "identifier", "type": "VARCHAR"},
                    {"name": "label", "type": "VARCHAR"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q004", "name": "multi_column_selection",
            "query": "SELECT id, status, count_int FROM dqe_test_all_types LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "status", "type": "VARCHAR"},
                    {"name": "count_int", "type": "INTEGER"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q005", "name": "arithmetic_expression",
            "query": "SELECT count_int + 1 AS incremented FROM dqe_test_all_types LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "incremented", "type": "INTEGER"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q006", "name": "cross_type_arithmetic",
            "query": "SELECT count_long * price_double FROM dqe_test_all_types LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "_col0", "type": "DOUBLE"}
                ]
            },
            "ignore_order": True,
            "float_tolerance": 0.001
        },
        {
            "id": "Q007", "name": "explicit_cast_bigint",
            "query": "SELECT CAST(count_int AS BIGINT) FROM dqe_test_all_types LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "_col0", "type": "BIGINT"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q008", "name": "cast_to_string",
            "query": "SELECT CAST(count_int AS VARCHAR) FROM dqe_test_all_types LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "_col0", "type": "VARCHAR"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q009", "name": "object_field_dot_notation",
            "query": "SELECT id, metadata.source, metadata.version FROM dqe_test_all_types LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "source", "type": "VARCHAR"},
                    {"name": "version", "type": "INTEGER"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q010", "name": "geo_point_row_access",
            "query": "SELECT location.lat, location.lon FROM dqe_test_all_types WHERE location IS NOT NULL LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "lat", "type": "DOUBLE"},
                    {"name": "lon", "type": "DOUBLE"}
                ]
            },
            "ignore_order": True,
            "float_tolerance": 0.0001
        },
        {
            "id": "Q011", "name": "limit_zero",
            "query": "SELECT id FROM dqe_test_all_types LIMIT 0",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"}
                ],
                "data": []
            }
        },
        {
            "id": "Q012", "name": "offset",
            "query": "SELECT id FROM dqe_test_all_types LIMIT 5 OFFSET 3",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q013", "name": "multiple_cast_targets",
            "query": "SELECT id, CAST(count_int AS DOUBLE), CAST(price_double AS VARCHAR) FROM dqe_test_all_types LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "_col1", "type": "DOUBLE"},
                    {"name": "_col2", "type": "VARCHAR"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q014", "name": "coalesce",
            "query": "SELECT id, COALESCE(str_val, 'default') FROM dqe_test_nulls LIMIT 10",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "_col1", "type": "VARCHAR"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q015", "name": "nullif",
            "query": "SELECT id, NULLIF(int_val, 0) FROM dqe_test_nulls LIMIT 10",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "_col1", "type": "INTEGER"}
                ]
            },
            "ignore_order": True
        },
    ]


def where_predicate_cases():
    """Q016-Q040: WHERE predicate tests (section 4.2)."""
    return [
        {
            "id": "Q016", "name": "equality_term_pushdown",
            "query": "SELECT id FROM dqe_test_all_types WHERE status = 'active'",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q017", "name": "inequality",
            "query": "SELECT id FROM dqe_test_all_types WHERE status != 'active'",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q018", "name": "greater_than_range",
            "query": "SELECT id FROM dqe_test_all_types WHERE count_int > 100",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q019", "name": "greater_equal",
            "query": "SELECT id FROM dqe_test_all_types WHERE count_int >= 100",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q020", "name": "less_than",
            "query": "SELECT id FROM dqe_test_all_types WHERE count_int < 50",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q021", "name": "less_equal",
            "query": "SELECT id FROM dqe_test_all_types WHERE count_int <= 50",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q022", "name": "and_bool_must",
            "query": "SELECT id FROM dqe_test_all_types WHERE status = 'active' AND count_int > 10",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q023", "name": "or_bool_should",
            "query": "SELECT id FROM dqe_test_all_types WHERE status = 'active' OR count_int > 100",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q024", "name": "not_bool_must_not",
            "query": "SELECT id FROM dqe_test_all_types WHERE NOT is_active",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q025", "name": "in_terms_pushdown",
            "query": "SELECT id FROM dqe_test_all_types WHERE status IN ('active', 'pending', 'closed')",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q026", "name": "between_range",
            "query": "SELECT id FROM dqe_test_all_types WHERE count_int BETWEEN 10 AND 100",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q027", "name": "like_prefix",
            "query": "SELECT id FROM dqe_test_all_types WHERE name LIKE 'test%'",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q028", "name": "like_contains",
            "query": "SELECT id FROM dqe_test_all_types WHERE name LIKE '%middle%'",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q029", "name": "is_null",
            "query": "SELECT id FROM dqe_test_all_types WHERE count_int IS NULL",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q030", "name": "is_not_null",
            "query": "SELECT id FROM dqe_test_all_types WHERE count_int IS NOT NULL",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q031", "name": "nested_boolean",
            "query": "SELECT id FROM dqe_test_all_types WHERE status = 'active' AND (count_int > 10 OR price_double < 5.0)",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q032", "name": "cast_in_where",
            "query": "SELECT id FROM dqe_test_all_types WHERE CAST(count_int AS DOUBLE) > 10.5",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q033", "name": "cross_column_expression",
            "query": "SELECT id FROM dqe_test_all_types WHERE count_int + count_long > 200",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q034", "name": "boolean_equality",
            "query": "SELECT id FROM dqe_test_all_types WHERE is_active = true",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q035", "name": "timestamp_comparison",
            "query": "SELECT id FROM dqe_test_all_types WHERE created_at > TIMESTAMP '2024-01-01 00:00:00'",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q036", "name": "ip_field_equality",
            "query": "SELECT id FROM dqe_test_all_types WHERE ip_address = '192.168.1.1'",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q037", "name": "multiple_null_checks",
            "query": "SELECT id FROM dqe_test_nulls WHERE int_val IS NULL AND str_val IS NOT NULL",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q038", "name": "scaled_float_comparison",
            "query": "SELECT id FROM dqe_test_all_types WHERE price_scaled > 99.99",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q039", "name": "unsigned_long_exceeding_bigint",
            "query": "SELECT id FROM dqe_test_all_types WHERE big_number > 9223372036854775807",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q040", "name": "always_true_predicate",
            "query": "SELECT id FROM dqe_test_all_types WHERE 1 = 1",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
    ]


def type_specific_cases():
    """Q041-Q060: Type-specific queries (section 4.3)."""
    return [
        {
            "id": "Q041", "name": "long_bigint",
            "query": "SELECT count_long FROM dqe_test_all_types WHERE count_long = 42 LIMIT 5",
            "expected": {"schema": [{"name": "count_long", "type": "BIGINT"}]},
            "ignore_order": True
        },
        {
            "id": "Q042", "name": "integer_int",
            "query": "SELECT count_int FROM dqe_test_all_types WHERE count_int = 42 LIMIT 5",
            "expected": {"schema": [{"name": "count_int", "type": "INTEGER"}]},
            "ignore_order": True
        },
        {
            "id": "Q043", "name": "short_smallint",
            "query": "SELECT count_short FROM dqe_test_all_types WHERE count_short = 42 LIMIT 5",
            "expected": {"schema": [{"name": "count_short", "type": "SMALLINT"}]},
            "ignore_order": True
        },
        {
            "id": "Q044", "name": "byte_tinyint",
            "query": "SELECT count_byte FROM dqe_test_all_types WHERE count_byte = 42 LIMIT 5",
            "expected": {"schema": [{"name": "count_byte", "type": "TINYINT"}]},
            "ignore_order": True
        },
        {
            "id": "Q045", "name": "double_double",
            "query": "SELECT price_double FROM dqe_test_all_types WHERE price_double > 0.0 LIMIT 5",
            "expected": {"schema": [{"name": "price_double", "type": "DOUBLE"}]},
            "ignore_order": True, "float_tolerance": 0.001
        },
        {
            "id": "Q046", "name": "float_real",
            "query": "SELECT price_float FROM dqe_test_all_types WHERE price_float > 0.0 LIMIT 5",
            "expected": {"schema": [{"name": "price_float", "type": "REAL"}]},
            "ignore_order": True, "float_tolerance": 0.001
        },
        {
            "id": "Q047", "name": "half_float_real",
            "query": "SELECT price_half FROM dqe_test_all_types WHERE price_half > 0.0 LIMIT 5",
            "expected": {"schema": [{"name": "price_half", "type": "REAL"}]},
            "ignore_order": True, "float_tolerance": 0.01
        },
        {
            "id": "Q048", "name": "scaled_float_decimal",
            "query": "SELECT price_scaled FROM dqe_test_all_types WHERE price_scaled > 0 LIMIT 5",
            "expected": {"schema": [{"name": "price_scaled", "type": "DECIMAL"}]},
            "ignore_order": True, "float_tolerance": 0.01
        },
        {
            "id": "Q049", "name": "boolean_boolean",
            "query": "SELECT is_active FROM dqe_test_all_types WHERE is_active = true LIMIT 5",
            "expected": {"schema": [{"name": "is_active", "type": "BOOLEAN"}]},
            "ignore_order": True
        },
        {
            "id": "Q050", "name": "date_epoch_millis_timestamp3",
            "query": "SELECT created_at FROM dqe_test_all_types WHERE created_at IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "created_at", "type": "TIMESTAMP"}]},
            "ignore_order": True
        },
        {
            "id": "Q051", "name": "date_strict_timestamp3",
            "query": "SELECT updated_at FROM dqe_test_all_types WHERE updated_at IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "updated_at", "type": "TIMESTAMP"}]},
            "ignore_order": True
        },
        {
            "id": "Q052", "name": "date_custom_format_timestamp3",
            "query": "SELECT custom_date FROM dqe_test_all_types WHERE custom_date IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "custom_date", "type": "TIMESTAMP"}]},
            "ignore_order": True
        },
        {
            "id": "Q053", "name": "date_nanos_timestamp9",
            "query": "SELECT precise_time FROM dqe_test_all_types WHERE precise_time IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "precise_time", "type": "TIMESTAMP"}]},
            "ignore_order": True
        },
        {
            "id": "Q054", "name": "ip_varchar",
            "query": "SELECT ip_address FROM dqe_test_all_types WHERE ip_address IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "ip_address", "type": "VARCHAR"}]},
            "ignore_order": True
        },
        {
            "id": "Q055", "name": "geo_point_row",
            "query": "SELECT location FROM dqe_test_all_types WHERE location IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "location", "type": "STRUCT"}]},
            "ignore_order": True
        },
        {
            "id": "Q056", "name": "geo_shape_varchar",
            "query": "SELECT boundary FROM dqe_test_all_types WHERE boundary IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "boundary", "type": "VARCHAR"}]},
            "ignore_order": True
        },
        {
            "id": "Q057", "name": "flattened_map",
            "query": "SELECT attributes FROM dqe_test_all_types WHERE attributes IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "attributes", "type": "STRUCT"}]},
            "ignore_order": True
        },
        {
            "id": "Q058", "name": "binary_varbinary",
            "query": "SELECT binary_data FROM dqe_test_all_types WHERE binary_data IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "binary_data", "type": "VARBINARY"}]},
            "ignore_order": True
        },
        {
            "id": "Q059", "name": "unsigned_long_decimal",
            "query": "SELECT big_number FROM dqe_test_all_types WHERE big_number IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "big_number", "type": "DECIMAL"}]},
            "ignore_order": True
        },
        {
            "id": "Q060", "name": "text_multifield_keyword",
            "query": "SELECT id, name FROM dqe_test_all_types WHERE name.keyword = 'exact match' LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "name", "type": "VARCHAR"}
                ]
            },
            "ignore_order": True
        },
    ]


def order_by_limit_cases():
    """Q061-Q075, Q114-Q118: ORDER BY and LIMIT tests (section 4.4)."""
    cases = [
        {
            "id": "Q061", "name": "single_column_asc",
            "query": "SELECT id, count_int FROM dqe_test_all_types ORDER BY count_int ASC LIMIT 10",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "count_int", "type": "INTEGER"}
                ]
            }
        },
        {
            "id": "Q062", "name": "single_column_desc",
            "query": "SELECT id, count_int FROM dqe_test_all_types ORDER BY count_int DESC LIMIT 10",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "count_int", "type": "INTEGER"}
                ]
            }
        },
        {
            "id": "Q063", "name": "multi_column_sort",
            "query": "SELECT id, status, count_int FROM dqe_test_all_types ORDER BY status ASC, count_int DESC LIMIT 10",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "status", "type": "VARCHAR"},
                    {"name": "count_int", "type": "INTEGER"}
                ]
            }
        },
        {
            "id": "Q064", "name": "nulls_first",
            "query": "SELECT id, int_val FROM dqe_test_nulls ORDER BY int_val ASC NULLS FIRST LIMIT 10",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "int_val", "type": "INTEGER"}
                ]
            }
        },
        {
            "id": "Q065", "name": "nulls_last",
            "query": "SELECT id, int_val FROM dqe_test_nulls ORDER BY int_val ASC NULLS LAST LIMIT 10",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "int_val", "type": "INTEGER"}
                ]
            }
        },
        {
            "id": "Q066", "name": "limit_with_offset",
            "query": "SELECT id FROM dqe_test_all_types ORDER BY id LIMIT 5 OFFSET 10",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            }
        },
        {
            "id": "Q067", "name": "sort_by_timestamp",
            "query": "SELECT id FROM dqe_test_all_types ORDER BY created_at DESC LIMIT 10",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            }
        },
        {
            "id": "Q068", "name": "sort_by_double",
            "query": "SELECT id FROM dqe_test_all_types ORDER BY price_double ASC LIMIT 10",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            }
        },
        {
            "id": "Q069", "name": "filter_sort_limit",
            "query": "SELECT id, status FROM dqe_test_all_types WHERE status = 'active' ORDER BY count_int DESC LIMIT 10",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "status", "type": "VARCHAR"}
                ]
            }
        },
        {
            "id": "Q070", "name": "large_limit",
            "query": "SELECT id FROM dqe_test_all_types ORDER BY id LIMIT 10000",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            }
        },
        {
            "id": "Q071", "name": "limit_one_topn",
            "query": "SELECT id FROM dqe_test_all_types ORDER BY id LIMIT 1",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}],
                "data": [["id_00001"]]
            }
        },
        {
            "id": "Q072", "name": "sort_text_fielddata",
            "query": "SELECT id FROM dqe_test_all_types ORDER BY description ASC LIMIT 5",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            }
        },
        {
            "id": "Q073", "name": "sort_scaled_float",
            "query": "SELECT id, price_scaled FROM dqe_test_all_types ORDER BY price_scaled DESC LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "price_scaled", "type": "DECIMAL"}
                ]
            },
            "float_tolerance": 0.01
        },
        {
            "id": "Q074", "name": "sort_unsigned_long",
            "query": "SELECT id, big_number FROM dqe_test_all_types ORDER BY big_number ASC LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "big_number", "type": "DECIMAL"}
                ]
            }
        },
        {
            "id": "Q075", "name": "full_sort_sharded_no_limit",
            "query": "SELECT id FROM dqe_test_sharded ORDER BY id ASC",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            }
        },
    ]

    # Adversarial TopN cases Q114-Q118
    # With 500-doc sharded dataset: amount = i*2 - 0.01
    # id_00500 has amount 999.99, id_00499 has 997.99, etc.
    cases.extend([
        {
            "id": "Q114", "name": "adversarial_topn_desc",
            "query": "SELECT id, amount FROM dqe_test_sharded ORDER BY amount DESC LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "amount", "type": "DOUBLE"}
                ],
                "data": [
                    ["id_00500", 999.99],
                    ["id_00499", 997.99],
                    ["id_00498", 995.99],
                    ["id_00497", 993.99],
                    ["id_00496", 991.99]
                ]
            },
            "float_tolerance": 0.01
        },
        {
            "id": "Q115", "name": "adversarial_topn_asc",
            "query": "SELECT id, amount FROM dqe_test_sharded ORDER BY amount ASC LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "amount", "type": "DOUBLE"}
                ],
                "data": [
                    ["id_00001", 1.99],
                    ["id_00002", 3.99],
                    ["id_00003", 5.99],
                    ["id_00004", 7.99],
                    ["id_00005", 9.99]
                ]
            },
            "float_tolerance": 0.01
        },
        {
            "id": "Q116", "name": "adversarial_topn_offset",
            "query": "SELECT id, amount FROM dqe_test_sharded ORDER BY amount DESC LIMIT 5 OFFSET 10",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "amount", "type": "DOUBLE"}
                ]
            },
            "float_tolerance": 0.01
        },
        {
            "id": "Q117", "name": "adversarial_topn_single_row",
            "query": "SELECT id, amount FROM dqe_test_sharded ORDER BY amount DESC LIMIT 1",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "amount", "type": "DOUBLE"}
                ],
                "data": [
                    ["id_00500", 999.99]
                ]
            },
            "float_tolerance": 0.01
        },
        {
            "id": "Q118", "name": "adversarial_topn_filter",
            "query": "SELECT id FROM dqe_test_sharded WHERE category = 'cat_50' ORDER BY id DESC LIMIT 10",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            }
        },
    ])
    return cases


def multi_shard_cases():
    """Q076-Q085, Q111-Q113: Multi-shard correctness tests (section 4.5)."""
    return [
        {
            "id": "Q076", "name": "range_scan_across_shards",
            "query": "SELECT id FROM dqe_test_sharded WHERE id BETWEEN 'id_00001' AND 'id_00100' ORDER BY id ASC",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            }
        },
        {
            "id": "Q077", "name": "full_scan_5_shards",
            "query": "SELECT id FROM dqe_test_sharded ORDER BY id ASC LIMIT 500",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            }
        },
        {
            "id": "Q078", "name": "filter_sort_across_shards",
            "query": "SELECT id FROM dqe_test_sharded WHERE id >= 'id_00250' ORDER BY id ASC",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            }
        },
        {
            "id": "Q079", "name": "reverse_sort_across_shards",
            "query": "SELECT id FROM dqe_test_sharded ORDER BY id DESC LIMIT 100",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            }
        },
        {
            "id": "Q080", "name": "point_lookup_single_shard",
            "query": "SELECT * FROM dqe_test_sharded WHERE id = 'id_00001'",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "category", "type": "VARCHAR"},
                    {"name": "amount", "type": "DOUBLE"}
                ],
                "data": [["id_00001", "cat_01", 1.99]]
            },
            "float_tolerance": 0.01
        },
        {
            "id": "Q081", "name": "limit_arbitrary_shard_order",
            "query": "SELECT id FROM dqe_test_sharded LIMIT 100",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q082", "name": "late_offset_across_shards",
            "query": "SELECT id FROM dqe_test_sharded LIMIT 100 OFFSET 400",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "ignore_order": True
        },
        {
            "id": "Q083", "name": "conflict_index_a",
            "query": "SELECT * FROM dqe_test_conflict_a LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "value", "type": "BIGINT"},
                    {"name": "label", "type": "VARCHAR"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q084", "name": "conflict_index_b",
            "query": "SELECT * FROM dqe_test_conflict_b LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "value", "type": "INTEGER"},
                    {"name": "label", "type": "VARCHAR"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q085", "name": "index_pattern_type_widening",
            "query": "SELECT id, value FROM dqe_test_conflict_* ORDER BY id LIMIT 10",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "value", "type": "BIGINT"}
                ]
            }
        },
        {
            "id": "Q111", "name": "full_scan_uniqueness_assertion",
            "query": "SELECT id FROM dqe_test_sharded ORDER BY id ASC",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "_note": "Validator must check: row_count=500, all id values unique"
        },
        {
            "id": "Q112", "name": "uniqueness_completeness_multi_col",
            "query": "SELECT id, category, amount FROM dqe_test_sharded ORDER BY id ASC",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "category", "type": "VARCHAR"},
                    {"name": "amount", "type": "DOUBLE"}
                ]
            },
            "float_tolerance": 0.01,
            "_note": "Validator must check: row_count=500, all id values unique"
        },
        {
            "id": "Q113", "name": "filtered_uniqueness",
            "query": "SELECT id FROM dqe_test_sharded WHERE category = 'cat_01' ORDER BY id ASC",
            "expected": {
                "schema": [{"name": "id", "type": "VARCHAR"}]
            },
            "_note": "Validator must check: all returned id values unique"
        },
    ]


def expression_cases():
    """Q086-Q095: Expression evaluation tests (section 4.6)."""
    return [
        {
            "id": "Q086", "name": "arithmetic_chain",
            "query": "SELECT id, count_int * 2 + 1 AS computed FROM dqe_test_all_types LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "computed", "type": "INTEGER"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q087", "name": "unary_negation",
            "query": "SELECT id, -count_int AS negated FROM dqe_test_all_types LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "negated", "type": "INTEGER"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q088", "name": "modulo_operator",
            "query": "SELECT id, count_int % 3 AS modulo FROM dqe_test_all_types LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "modulo", "type": "INTEGER"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q089", "name": "searched_case_expression",
            "query": "SELECT id, CASE WHEN count_int > 100 THEN 'high' WHEN count_int > 50 THEN 'medium' ELSE 'low' END AS level FROM dqe_test_all_types LIMIT 10",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "level", "type": "VARCHAR"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q090", "name": "simple_case_expression",
            "query": "SELECT id, CASE status WHEN 'active' THEN 1 WHEN 'pending' THEN 2 ELSE 0 END AS status_code FROM dqe_test_all_types LIMIT 10",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "status_code", "type": "INTEGER"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q091", "name": "case_as_conditional",
            "query": "SELECT id, CASE WHEN is_active THEN 'yes' ELSE 'no' END AS active_label FROM dqe_test_all_types LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "active_label", "type": "VARCHAR"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q092", "name": "try_cast_null_on_failure",
            "query": "SELECT id, TRY_CAST(status AS INTEGER) FROM dqe_test_all_types LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "_col1", "type": "INTEGER"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q093", "name": "nullif_coalesce_combined",
            "query": "SELECT id, NULLIF(int_val, 0) AS non_zero, COALESCE(str_val, 'fallback') FROM dqe_test_nulls LIMIT 10",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "non_zero", "type": "INTEGER"},
                    {"name": "_col2", "type": "VARCHAR"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q094", "name": "coalesce_with_null",
            "query": "SELECT id, COALESCE(int_val, -1) AS val FROM dqe_test_nulls LIMIT 10",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "val", "type": "INTEGER"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "Q095", "name": "combined_predicates",
            "query": "SELECT id, count_int FROM dqe_test_all_types WHERE count_int BETWEEN 10 AND 100 AND status IN ('active', 'pending') ORDER BY count_int DESC LIMIT 20",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "count_int", "type": "INTEGER"}
                ]
            }
        },
    ]


def error_cases():
    """Q096-Q110: Error cases (section 4.7)."""
    return [
        {
            "id": "Q096", "name": "missing_index",
            "query": "SELECT * FROM nonexistent_index",
            "error": {"message_contains": "not found"}
        },
        {
            "id": "Q097", "name": "missing_column",
            "query": "SELECT nonexistent_column FROM dqe_test_all_types",
            "error": {"message_contains": "not found"}
        },
        {
            "id": "Q098", "name": "unsupported_group_by",
            "query": "SELECT id FROM dqe_test_all_types GROUP BY id",
            "error": {"message_contains": "GROUP BY"}
        },
        {
            "id": "Q099", "name": "unsupported_join",
            "query": "SELECT id FROM dqe_test_all_types a JOIN dqe_test_nulls b ON a.id = b.id",
            "error": {"message_contains": "JOIN"}
        },
        {
            "id": "Q100", "name": "unsupported_window_function",
            "query": "SELECT ROW_NUMBER() OVER () FROM dqe_test_all_types",
            "error": {"message_contains": "window"}
        },
        {
            "id": "Q101", "name": "cast_failure",
            "query": "SELECT CAST('abc' AS INTEGER) FROM dqe_test_all_types LIMIT 1",
            "error": {"message_contains": "CAST"}
        },
        {
            "id": "Q102", "name": "syntax_error",
            "query": "THIS IS NOT SQL",
            "error": {"message_contains": "Syntax error"}
        },
        {
            "id": "Q103", "name": "unsortable_geo_point",
            "query": "SELECT id FROM dqe_test_all_types ORDER BY location",
            "error": {"message_contains": "sort"}
        },
        {
            "id": "Q104", "name": "unsupported_aggregate",
            "query": "SELECT COUNT(*) FROM dqe_test_all_types",
            "error": {"message_contains": "aggregate"}
        },
        {
            "id": "Q105", "name": "type_mismatch_predicate",
            "query": "SELECT id FROM dqe_test_all_types WHERE count_int = 'not_a_number'",
            "error": {"message_contains": "type"}
        },
        {
            "id": "Q106", "name": "unsupported_distinct",
            "query": "SELECT DISTINCT status FROM dqe_test_all_types",
            "error": {"message_contains": "DISTINCT"}
        },
        {
            "id": "Q107", "name": "unsupported_no_from",
            "query": "SELECT 1 + 2 AS constant",
            "error": {"message_contains": "FROM"}
        },
        {
            "id": "Q108", "name": "unsupported_function_concat",
            "query": "SELECT id, CONCAT(status, '_suffix') FROM dqe_test_all_types LIMIT 5",
            "error": {"message_contains": "CONCAT"}
        },
        {
            "id": "Q109", "name": "unsupported_function_length",
            "query": "SELECT id, LENGTH(status) FROM dqe_test_all_types LIMIT 5",
            "error": {"message_contains": "LENGTH"}
        },
        {
            "id": "Q110", "name": "unsupported_function_if",
            "query": "SELECT id, IF(is_active, 'yes', 'no') FROM dqe_test_all_types LIMIT 5",
            "error": {"message_contains": "IF"}
        },
    ]


def null_conformance_cases():
    """N01-N55: NULL conformance tests (section 5)."""
    cases = []

    # 5.1 NULL in Arithmetic (N01-N10)
    null_arith = [
        ("N01", "null_plus_one", "SELECT NULL + 1 AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N02", "one_plus_null", "SELECT 1 + NULL AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N03", "null_times_zero", "SELECT NULL * 0 AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N04", "null_minus_null", "SELECT NULL - NULL AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N05", "null_div_one", "SELECT NULL / 1 AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N06", "null_mod_two", "SELECT NULL % 2 AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N07", "column_null_plus_one", "SELECT id, int_val + 1 AS result FROM dqe_test_nulls WHERE id = 'null_0001' LIMIT 1", None),
        ("N08", "column_null_multiply", "SELECT id, int_val * dbl_val AS result FROM dqe_test_nulls WHERE id = 'null_0001' LIMIT 1", None),
        ("N09", "negate_null", "SELECT -CAST(NULL AS INTEGER) AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N10", "cast_null_plus_one", "SELECT CAST(NULL AS INTEGER) + 1 AS result FROM dqe_test_nulls LIMIT 1", None),
    ]
    for nid, name, query, _expected_val in null_arith:
        cases.append({
            "id": nid, "name": name, "query": query,
            "expected": {"schema": [{"name": "result", "type": "INTEGER"}]},
            "_note": "Result must be NULL for all NULL arithmetic"
        })
    # Fix multi-column cases
    cases[6] = {
        "id": "N07", "name": "column_null_plus_one",
        "query": "SELECT id, int_val + 1 AS result FROM dqe_test_nulls WHERE id = 'null_0001' LIMIT 1",
        "expected": {
            "schema": [
                {"name": "id", "type": "VARCHAR"},
                {"name": "result", "type": "INTEGER"}
            ],
            "data": [["null_0001", None]]
        }
    }
    cases[7] = {
        "id": "N08", "name": "column_null_multiply",
        "query": "SELECT id, int_val * dbl_val AS result FROM dqe_test_nulls WHERE id = 'null_0001' LIMIT 1",
        "expected": {
            "schema": [
                {"name": "id", "type": "VARCHAR"},
                {"name": "result", "type": "DOUBLE"}
            ],
            "data": [["null_0001", None]]
        },
        "float_tolerance": 0.001
    }

    # 5.2 NULL in Comparison (N11-N20)
    null_cmp = [
        ("N11", "null_eq_null", "SELECT (NULL = NULL) AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N12", "null_neq_null", "SELECT (NULL != NULL) AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N13", "null_eq_one", "SELECT (NULL = 1) AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N14", "null_neq_one", "SELECT (NULL != 1) AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N15", "null_lt_one", "SELECT (NULL < 1) AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N16", "null_gt_one", "SELECT (NULL > 1) AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N17", "null_le_one", "SELECT (NULL <= 1) AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N18", "null_ge_one", "SELECT (NULL >= 1) AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N19", "one_eq_null", "SELECT (1 = NULL) AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N20", "null_is_null", "SELECT (NULL IS NULL) AS result FROM dqe_test_nulls LIMIT 1", True),
    ]
    for nid, name, query, expected_val in null_cmp:
        c = {"id": nid, "name": name, "query": query}
        if expected_val is True:
            c["expected"] = {
                "schema": [{"name": "result", "type": "BOOLEAN"}],
                "data": [[True]]
            }
        else:
            c["expected"] = {"schema": [{"name": "result", "type": "BOOLEAN"}]}
            c["_note"] = "Result must be NULL (three-valued logic)"
        cases.append(c)

    # 5.3 NULL in Logical Operators (N21-N30)
    null_logic = [
        ("N21", "null_and_true", "SELECT (NULL AND TRUE) AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N22", "null_and_false", "SELECT (NULL AND FALSE) AS result FROM dqe_test_nulls LIMIT 1", False),
        ("N23", "null_and_null", "SELECT (NULL AND NULL) AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N24", "null_or_true", "SELECT (NULL OR TRUE) AS result FROM dqe_test_nulls LIMIT 1", True),
        ("N25", "null_or_false", "SELECT (NULL OR FALSE) AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N26", "null_or_null", "SELECT (NULL OR NULL) AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N27", "not_null", "SELECT (NOT CAST(NULL AS BOOLEAN)) AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N28", "true_and_null", "SELECT (TRUE AND NULL) AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N29", "false_or_null", "SELECT (FALSE OR NULL) AS result FROM dqe_test_nulls LIMIT 1", None),
        ("N30", "not_null_and_true", "SELECT (NOT (NULL AND TRUE)) AS result FROM dqe_test_nulls LIMIT 1", None),
    ]
    for nid, name, query, expected_val in null_logic:
        c = {"id": nid, "name": name, "query": query}
        if expected_val is not None:
            c["expected"] = {
                "schema": [{"name": "result", "type": "BOOLEAN"}],
                "data": [[expected_val]]
            }
        else:
            c["expected"] = {"schema": [{"name": "result", "type": "BOOLEAN"}]}
            c["_note"] = "Result must be NULL (three-valued logic)"
        cases.append(c)

    # 5.4 NULL in String/Predicate (N31-N32)
    cases.append({
        "id": "N31", "name": "cast_null_varchar",
        "query": "SELECT CAST(NULL AS VARCHAR) AS result FROM dqe_test_nulls LIMIT 1",
        "expected": {
            "schema": [{"name": "result", "type": "VARCHAR"}],
            "data": [[None]]
        }
    })
    cases.append({
        "id": "N32", "name": "null_like",
        "query": "SELECT (NULL LIKE '%test%') AS result FROM dqe_test_nulls LIMIT 1",
        "expected": {"schema": [{"name": "result", "type": "BOOLEAN"}]},
        "_note": "Result must be NULL"
    })

    # 5.5 NULL in CAST (N36-N40)
    cast_types = [
        ("N36", "cast_null_integer", "INTEGER"),
        ("N37", "cast_null_varchar_2", "VARCHAR"),
        ("N38", "cast_null_boolean", "BOOLEAN"),
        ("N39", "cast_null_double", "DOUBLE"),
        ("N40", "cast_null_timestamp", "TIMESTAMP"),
    ]
    for nid, name, target_type in cast_types:
        cases.append({
            "id": nid, "name": name,
            "query": f"SELECT CAST(NULL AS {target_type}) AS result FROM dqe_test_nulls LIMIT 1",
            "expected": {
                "schema": [{"name": "result", "type": target_type}],
                "data": [[None]]
            }
        })

    # 5.6 NULL in CASE (N41-N45)
    cases.append({
        "id": "N41", "name": "case_when_null_condition",
        "query": "SELECT CASE WHEN NULL THEN 'a' ELSE 'b' END AS result FROM dqe_test_nulls LIMIT 1",
        "expected": {
            "schema": [{"name": "result", "type": "VARCHAR"}],
            "data": [["b"]]
        }
    })
    cases.append({
        "id": "N42", "name": "case_then_null",
        "query": "SELECT CASE WHEN TRUE THEN NULL ELSE 'b' END AS result FROM dqe_test_nulls LIMIT 1",
        "expected": {
            "schema": [{"name": "result", "type": "VARCHAR"}],
            "data": [[None]]
        }
    })
    cases.append({
        "id": "N43", "name": "case_no_else",
        "query": "SELECT CASE WHEN FALSE THEN 'a' END AS result FROM dqe_test_nulls LIMIT 1",
        "expected": {
            "schema": [{"name": "result", "type": "VARCHAR"}],
            "data": [[None]]
        }
    })
    cases.append({
        "id": "N44", "name": "simple_case_null_null",
        "query": "SELECT CASE NULL WHEN NULL THEN 'match' ELSE 'no match' END AS result FROM dqe_test_nulls LIMIT 1",
        "expected": {
            "schema": [{"name": "result", "type": "VARCHAR"}],
            "data": [["no match"]]
        }
    })
    cases.append({
        "id": "N45", "name": "case_is_null_per_row",
        "query": "SELECT id, CASE WHEN int_val IS NULL THEN 'null' ELSE 'not null' END AS result FROM dqe_test_nulls ORDER BY id LIMIT 5",
        "expected": {
            "schema": [
                {"name": "id", "type": "VARCHAR"},
                {"name": "result", "type": "VARCHAR"}
            ]
        }
    })

    # 5.7 NULL in Set Operations (N46-N50)
    cases.append({
        "id": "N46", "name": "null_in_list",
        "query": "SELECT (NULL IN (1, 2, 3)) AS result FROM dqe_test_nulls LIMIT 1",
        "expected": {"schema": [{"name": "result", "type": "BOOLEAN"}]},
        "_note": "Result must be NULL"
    })
    cases.append({
        "id": "N47", "name": "value_in_list_with_null",
        "query": "SELECT (1 IN (NULL, 2, 3)) AS result FROM dqe_test_nulls LIMIT 1",
        "expected": {"schema": [{"name": "result", "type": "BOOLEAN"}]},
        "_note": "Result must be NULL (1 not in {2,3}, NULL makes it unknown)"
    })
    cases.append({
        "id": "N48", "name": "value_found_in_list_with_null",
        "query": "SELECT (1 IN (NULL, 1, 3)) AS result FROM dqe_test_nulls LIMIT 1",
        "expected": {
            "schema": [{"name": "result", "type": "BOOLEAN"}],
            "data": [[True]]
        }
    })
    cases.append({
        "id": "N49", "name": "null_between",
        "query": "SELECT (NULL BETWEEN 1 AND 10) AS result FROM dqe_test_nulls LIMIT 1",
        "expected": {"schema": [{"name": "result", "type": "BOOLEAN"}]},
        "_note": "Result must be NULL"
    })
    cases.append({
        "id": "N50", "name": "between_null_bound",
        "query": "SELECT (5 BETWEEN NULL AND 10) AS result FROM dqe_test_nulls LIMIT 1",
        "expected": {"schema": [{"name": "result", "type": "BOOLEAN"}]},
        "_note": "Result must be NULL"
    })

    # 5.8 NULL in ORDER BY and Functions (N51-N55)
    cases.append({
        "id": "N51", "name": "order_by_nulls_first",
        "query": "SELECT id, int_val FROM dqe_test_nulls ORDER BY int_val ASC NULLS FIRST LIMIT 10",
        "expected": {
            "schema": [
                {"name": "id", "type": "VARCHAR"},
                {"name": "int_val", "type": "INTEGER"}
            ]
        },
        "_note": "First rows must have int_val=NULL"
    })
    cases.append({
        "id": "N52", "name": "order_by_nulls_last",
        "query": "SELECT id, int_val FROM dqe_test_nulls ORDER BY int_val ASC NULLS LAST LIMIT 10",
        "expected": {
            "schema": [
                {"name": "id", "type": "VARCHAR"},
                {"name": "int_val", "type": "INTEGER"}
            ]
        },
        "_note": "First rows must have non-NULL int_val"
    })
    cases.append({
        "id": "N53", "name": "coalesce_all_null_except_last",
        "query": "SELECT COALESCE(NULL, NULL, 'c') AS result FROM dqe_test_nulls LIMIT 1",
        "expected": {
            "schema": [{"name": "result", "type": "VARCHAR"}],
            "data": [["c"]]
        }
    })
    cases.append({
        "id": "N54", "name": "coalesce_second_non_null",
        "query": "SELECT COALESCE(NULL, 'b', 'c') AS result FROM dqe_test_nulls LIMIT 1",
        "expected": {
            "schema": [{"name": "result", "type": "VARCHAR"}],
            "data": [["b"]]
        }
    })
    cases.append({
        "id": "N55", "name": "nullif_equal_values",
        "query": "SELECT NULLIF(1, 1) AS result FROM dqe_test_nulls LIMIT 1",
        "expected": {
            "schema": [{"name": "result", "type": "INTEGER"}],
            "data": [[None]]
        }
    })

    return cases


def type_coercion_cases():
    """C01-C30: Type coercion tests (section 6)."""
    cases = []

    # 6.1 Implicit Widening (C01-C12)
    cases.extend([
        {
            "id": "C01", "name": "byte_plus_short",
            "query": "SELECT count_byte + count_short AS result FROM dqe_test_all_types WHERE count_byte IS NOT NULL AND count_short IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "result", "type": "SMALLINT"}]},
            "ignore_order": True
        },
        {
            "id": "C02", "name": "short_plus_int",
            "query": "SELECT count_short + count_int AS result FROM dqe_test_all_types WHERE count_short IS NOT NULL AND count_int IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "result", "type": "INTEGER"}]},
            "ignore_order": True
        },
        {
            "id": "C03", "name": "int_plus_long",
            "query": "SELECT count_int + count_long AS result FROM dqe_test_all_types WHERE count_int IS NOT NULL AND count_long IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "result", "type": "BIGINT"}]},
            "ignore_order": True
        },
        {
            "id": "C04", "name": "float_plus_double",
            "query": "SELECT price_float + price_double AS result FROM dqe_test_all_types WHERE price_float IS NOT NULL AND price_double IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "result", "type": "DOUBLE"}]},
            "ignore_order": True, "float_tolerance": 0.001
        },
        {
            "id": "C05", "name": "int_plus_double",
            "query": "SELECT count_int + price_double AS result FROM dqe_test_all_types WHERE count_int IS NOT NULL AND price_double IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "result", "type": "DOUBLE"}]},
            "ignore_order": True, "float_tolerance": 0.001
        },
        {
            "id": "C06", "name": "int_times_decimal_literal",
            "query": "SELECT count_int * 1.5 AS result FROM dqe_test_all_types WHERE count_int IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "result", "type": "DECIMAL"}]},
            "ignore_order": True, "float_tolerance": 0.01
        },
        {
            "id": "C07", "name": "long_plus_int_literal",
            "query": "SELECT count_long + 1 AS result FROM dqe_test_all_types WHERE count_long IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "result", "type": "BIGINT"}]},
            "ignore_order": True
        },
        {
            "id": "C08", "name": "float_plus_double_literal",
            "query": "SELECT price_float + 0.0 AS result FROM dqe_test_all_types WHERE price_float IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "result", "type": "DOUBLE"}]},
            "ignore_order": True, "float_tolerance": 0.001
        },
        {
            "id": "C09", "name": "int_eq_long_comparison",
            "query": "SELECT id FROM dqe_test_all_types WHERE count_int = count_long LIMIT 5",
            "expected": {"schema": [{"name": "id", "type": "VARCHAR"}]},
            "ignore_order": True
        },
        {
            "id": "C10", "name": "int_gt_double_comparison",
            "query": "SELECT id FROM dqe_test_all_types WHERE count_int > price_double LIMIT 5",
            "expected": {"schema": [{"name": "id", "type": "VARCHAR"}]},
            "ignore_order": True
        },
        {
            "id": "C11", "name": "coalesce_int_long",
            "query": "SELECT COALESCE(count_int, count_long) AS result FROM dqe_test_all_types LIMIT 5",
            "expected": {"schema": [{"name": "result", "type": "BIGINT"}]},
            "ignore_order": True
        },
        {
            "id": "C12", "name": "case_int_long",
            "query": "SELECT CASE WHEN true THEN count_int ELSE count_long END AS result FROM dqe_test_all_types LIMIT 5",
            "expected": {"schema": [{"name": "result", "type": "BIGINT"}]},
            "ignore_order": True
        },
    ])

    # 6.2 Explicit CAST (C13-C22)
    cases.extend([
        {
            "id": "C13", "name": "cast_int_to_bigint",
            "query": "SELECT CAST(count_int AS BIGINT) AS result FROM dqe_test_all_types WHERE count_int IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "result", "type": "BIGINT"}]},
            "ignore_order": True
        },
        {
            "id": "C14", "name": "cast_long_to_int",
            "query": "SELECT CAST(count_long AS INTEGER) AS result FROM dqe_test_all_types WHERE count_long IS NOT NULL AND count_long BETWEEN -2147483648 AND 2147483647 LIMIT 5",
            "expected": {"schema": [{"name": "result", "type": "INTEGER"}]},
            "ignore_order": True
        },
        {
            "id": "C15", "name": "cast_int_to_varchar",
            "query": "SELECT CAST(count_int AS VARCHAR) AS result FROM dqe_test_all_types WHERE count_int IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "result", "type": "VARCHAR"}]},
            "ignore_order": True
        },
        {
            "id": "C16", "name": "cast_string_to_int",
            "query": "SELECT CAST('123' AS INTEGER) AS result FROM dqe_test_all_types LIMIT 1",
            "expected": {
                "schema": [{"name": "result", "type": "INTEGER"}],
                "data": [[123]]
            }
        },
        {
            "id": "C17", "name": "cast_double_to_int",
            "query": "SELECT CAST(price_double AS INTEGER) AS result FROM dqe_test_all_types WHERE price_double IS NOT NULL AND price_double BETWEEN -2147483648 AND 2147483647 LIMIT 5",
            "expected": {"schema": [{"name": "result", "type": "INTEGER"}]},
            "ignore_order": True
        },
        {
            "id": "C18", "name": "cast_int_to_double",
            "query": "SELECT CAST(count_int AS DOUBLE) AS result FROM dqe_test_all_types WHERE count_int IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "result", "type": "DOUBLE"}]},
            "ignore_order": True, "float_tolerance": 0.001
        },
        {
            "id": "C19", "name": "cast_bool_to_varchar",
            "query": "SELECT CAST(is_active AS VARCHAR) AS result FROM dqe_test_all_types WHERE is_active IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "result", "type": "VARCHAR"}]},
            "ignore_order": True
        },
        {
            "id": "C20", "name": "cast_string_to_bool",
            "query": "SELECT CAST('true' AS BOOLEAN) AS result FROM dqe_test_all_types LIMIT 1",
            "expected": {
                "schema": [{"name": "result", "type": "BOOLEAN"}],
                "data": [[True]]
            }
        },
        {
            "id": "C21", "name": "cast_timestamp_to_varchar",
            "query": "SELECT CAST(created_at AS VARCHAR) AS result FROM dqe_test_all_types WHERE created_at IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "result", "type": "VARCHAR"}]},
            "ignore_order": True
        },
        {
            "id": "C22", "name": "cast_scaled_to_double",
            "query": "SELECT CAST(price_scaled AS DOUBLE) AS result FROM dqe_test_all_types WHERE price_scaled IS NOT NULL LIMIT 5",
            "expected": {"schema": [{"name": "result", "type": "DOUBLE"}]},
            "ignore_order": True, "float_tolerance": 0.01
        },
    ])

    # 6.3 CAST Failures (C23-C30)
    cases.extend([
        {
            "id": "C23", "name": "cast_abc_to_int",
            "query": "SELECT CAST('abc' AS INTEGER) AS result FROM dqe_test_all_types LIMIT 1",
            "error": {"message_contains": "CAST"}
        },
        {
            "id": "C24", "name": "cast_abc_to_double",
            "query": "SELECT CAST('abc' AS DOUBLE) AS result FROM dqe_test_all_types LIMIT 1",
            "error": {"message_contains": "CAST"}
        },
        {
            "id": "C25", "name": "cast_abc_to_bool",
            "query": "SELECT CAST('abc' AS BOOLEAN) AS result FROM dqe_test_all_types LIMIT 1",
            "error": {"message_contains": "CAST"}
        },
        {
            "id": "C26", "name": "cast_overflow_int",
            "query": "SELECT CAST(99999999999999999999 AS INTEGER) AS result FROM dqe_test_all_types LIMIT 1",
            "error": {"message_contains": "overflow"}
        },
        {
            "id": "C27", "name": "cast_invalid_target_type",
            "query": "SELECT CAST(count_int AS geo_point) AS result FROM dqe_test_all_types LIMIT 1",
            "error": {"message_contains": "type"}
        },
        {
            "id": "C28", "name": "try_cast_abc_to_int",
            "query": "SELECT TRY_CAST('abc' AS INTEGER) AS result FROM dqe_test_all_types LIMIT 1",
            "expected": {
                "schema": [{"name": "result", "type": "INTEGER"}],
                "data": [[None]]
            }
        },
        {
            "id": "C29", "name": "try_cast_abc_to_double",
            "query": "SELECT TRY_CAST('abc' AS DOUBLE) AS result FROM dqe_test_all_types LIMIT 1",
            "expected": {
                "schema": [{"name": "result", "type": "DOUBLE"}],
                "data": [[None]]
            }
        },
        {
            "id": "C30", "name": "cast_empty_string_to_int",
            "query": "SELECT CAST('' AS INTEGER) AS result FROM dqe_test_all_types LIMIT 1",
            "error": {"message_contains": "CAST"}
        },
    ])

    return cases


def timezone_cases():
    """TZ01-TZ12: Timezone conformance tests (section 7)."""
    return [
        {
            "id": "TZ01", "name": "epoch_millis_precision",
            "query": "SELECT created_at FROM dqe_test_all_types WHERE created_at IS NOT NULL LIMIT 1",
            "expected": {"schema": [{"name": "created_at", "type": "TIMESTAMP"}]},
            "_note": "TIMESTAMP(3) with millisecond precision"
        },
        {
            "id": "TZ02", "name": "date_nanos_precision",
            "query": "SELECT precise_time FROM dqe_test_all_types WHERE precise_time IS NOT NULL LIMIT 1",
            "expected": {"schema": [{"name": "precise_time", "type": "TIMESTAMP"}]},
            "_note": "TIMESTAMP(9) with nanosecond precision"
        },
        {
            "id": "TZ03", "name": "custom_date_format",
            "query": "SELECT custom_date FROM dqe_test_all_types WHERE custom_date IS NOT NULL LIMIT 1",
            "expected": {"schema": [{"name": "custom_date", "type": "TIMESTAMP"}]},
            "_note": "Custom format yyyy/MM/dd HH:mm:ss parsed correctly"
        },
        {
            "id": "TZ04", "name": "strict_date_comparison",
            "query": "SELECT updated_at FROM dqe_test_all_types WHERE updated_at = TIMESTAMP '2024-06-15 10:30:00'",
            "expected": {"schema": [{"name": "updated_at", "type": "TIMESTAMP"}]}
        },
        {
            "id": "TZ05", "name": "timestamp_range_filter",
            "query": "SELECT created_at FROM dqe_test_all_types WHERE created_at > TIMESTAMP '2024-01-01 00:00:00' AND created_at < TIMESTAMP '2024-12-31 23:59:59'",
            "expected": {"schema": [{"name": "created_at", "type": "TIMESTAMP"}]},
            "ignore_order": True
        },
        {
            "id": "TZ06", "name": "ordering_through_dst",
            "query": "SELECT id, created_at FROM dqe_test_all_types ORDER BY created_at ASC LIMIT 20",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "created_at", "type": "TIMESTAMP"}
                ]
            }
        },
        {
            "id": "TZ07", "name": "range_spanning_dst_gap",
            "query": "SELECT id FROM dqe_test_all_types WHERE created_at BETWEEN TIMESTAMP '2024-03-10 00:00:00' AND TIMESTAMP '2024-03-10 04:00:00'",
            "expected": {"schema": [{"name": "id", "type": "VARCHAR"}]},
            "ignore_order": True
        },
        {
            "id": "TZ08", "name": "nanosecond_precision_comparison",
            "query": "SELECT id FROM dqe_test_all_types WHERE precise_time > TIMESTAMP '2024-01-01 00:00:00.123456789'",
            "expected": {"schema": [{"name": "id", "type": "VARCHAR"}]},
            "ignore_order": True
        },
        {
            "id": "TZ09", "name": "timestamp_to_string_cast",
            "query": "SELECT id, CAST(created_at AS VARCHAR) FROM dqe_test_all_types WHERE created_at IS NOT NULL LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "_col1", "type": "VARCHAR"}
                ]
            },
            "ignore_order": True
        },
        {
            "id": "TZ10", "name": "string_to_timestamp_cast",
            "query": "SELECT id, CAST('2024-03-10T02:30:00Z' AS TIMESTAMP) AS parsed FROM dqe_test_all_types LIMIT 1",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "parsed", "type": "TIMESTAMP"}
                ]
            }
        },
        {
            "id": "TZ11", "name": "multiple_date_formats_same_query",
            "query": "SELECT id, created_at, custom_date FROM dqe_test_all_types WHERE created_at IS NOT NULL AND custom_date IS NOT NULL ORDER BY created_at LIMIT 5",
            "expected": {
                "schema": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "created_at", "type": "TIMESTAMP"},
                    {"name": "custom_date", "type": "TIMESTAMP"}
                ]
            }
        },
        {
            "id": "TZ12", "name": "null_timestamps_filter_sort",
            "query": "SELECT id FROM dqe_test_all_types WHERE created_at IS NULL ORDER BY id LIMIT 10",
            "expected": {"schema": [{"name": "id", "type": "VARCHAR"}]}
        },
    ]


def main():
    base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "cases")
    if len(sys.argv) > 1:
        base_dir = sys.argv[1]

    count = 0

    # Phase 1 cases
    for case in basic_select_cases():
        write_case(base_dir, "phase1/basic_select", f"{case['id']}_{case['name']}.json", case)
        count += 1

    for case in where_predicate_cases():
        write_case(base_dir, "phase1/where_predicates", f"{case['id']}_{case['name']}.json", case)
        count += 1

    for case in type_specific_cases():
        write_case(base_dir, "phase1/type_specific", f"{case['id']}_{case['name']}.json", case)
        count += 1

    for case in order_by_limit_cases():
        write_case(base_dir, "phase1/order_by_limit", f"{case['id']}_{case['name']}.json", case)
        count += 1

    for case in multi_shard_cases():
        write_case(base_dir, "phase1/multi_shard", f"{case['id']}_{case['name']}.json", case)
        count += 1

    for case in expression_cases():
        write_case(base_dir, "phase1/expressions", f"{case['id']}_{case['name']}.json", case)
        count += 1

    for case in error_cases():
        write_case(base_dir, "phase1/error_cases", f"{case['id']}_{case['name']}.json", case)
        count += 1

    # NULL conformance
    for case in null_conformance_cases():
        write_case(base_dir, "null_conformance", f"{case['id']}_{case['name']}.json", case)
        count += 1

    # Type coercion
    for case in type_coercion_cases():
        write_case(base_dir, "type_coercion", f"{case['id']}_{case['name']}.json", case)
        count += 1

    # Timezone
    for case in timezone_cases():
        write_case(base_dir, "timezone", f"{case['id']}_{case['name']}.json", case)
        count += 1

    print(f"Generated {count} test case files under {base_dir}")


if __name__ == "__main__":
    main()
