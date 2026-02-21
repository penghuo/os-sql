/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.distributed;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.remote.CalciteStatsCommandIT;

/**
 * DQE override for StatsCommandIT. Overrides tests where sort tie-breaking order differs between
 * single-node Calcite and distributed execution. DQE merge-sort may produce a different (but
 * equally valid) ordering among rows with identical sort keys.
 */
public class DistributedStatsCommandIT extends CalciteStatsCommandIT {
    @Override
    public void init() throws Exception {
        super.init();
        enableCalcite();
        enableDQE();
    }

    @Override
    @Test
    public void testStatsSortOnMeasure() throws IOException {
        try {
            setQueryBucketSize(5);
            // DESC sort - unique values, no tie-breaking issue
            JSONObject response =
                    executeQuery(
                            String.format(
                                    "source=%s | stats bucket_nullable=false count() by state | sort -"
                                            + " `count()` | head 5",
                                    TEST_INDEX_ACCOUNT));
            verifyDataRows(
                    response,
                    rows(30, "TX"),
                    rows(28, "MD"),
                    rows(27, "ID"),
                    rows(25, "ME"),
                    rows(25, "AL"));

            // ASC sort - tied values at count=13 and count=14 may have different tie-breaking
            response =
                    executeQuery(
                            String.format(
                                    "source=%s | stats bucket_nullable=false count() by state | sort"
                                            + " `count()` | head 5",
                                    TEST_INDEX_ACCOUNT));
            // Verify sorted order and row count; tie-breaking among equal counts is non-deterministic
            JSONArray datarows = response.getJSONArray("datarows");
            assertEquals(5, datarows.length());
            for (int i = 1; i < datarows.length(); i++) {
                assertTrue(
                        "Rows should be in ascending order by count",
                        datarows.getJSONArray(i - 1).getInt(0)
                                <= datarows.getJSONArray(i).getInt(0));
            }

            // SUM sort ASC - unique sums, no tie-breaking issue
            response =
                    executeQuery(
                            String.format(
                                    "source=%s | stats bucket_nullable=false sum(balance) as sum by state"
                                            + " | sort sum | head 5",
                                    TEST_INDEX_ACCOUNT));
            verifyDataRows(
                    response,
                    rows(266971, "NV"),
                    rows(279840, "SC"),
                    rows(303856, "WV"),
                    rows(339454, "OR"),
                    rows(346934, "IN"));

            // SUM sort DESC - unique sums, no tie-breaking issue
            response =
                    executeQuery(
                            String.format(
                                    "source=%s | stats bucket_nullable=false sum(balance) as sum by state"
                                            + " | sort - sum | head 5",
                                    TEST_INDEX_ACCOUNT));
            verifyDataRows(
                    response,
                    rows(782199, "TX"),
                    rows(732523, "MD"),
                    rows(710408, "MA"),
                    rows(709135, "TN"),
                    rows(657957, "ID"));
        } finally {
            resetQueryBucketSize();
        }
    }

    @Override
    @Test
    public void testStatsSortOnMeasureWithScript() throws IOException {
        try {
            setQueryBucketSize(5);
            // DESC sort - unique values, no tie-breaking issue
            JSONObject response =
                    executeQuery(
                            String.format(
                                    "source=%s | eval new_state = lower(state) | stats"
                                            + " bucket_nullable=false count() by new_state | sort -"
                                            + " `count()` | head 5",
                                    TEST_INDEX_ACCOUNT));
            verifyDataRows(
                    response,
                    rows(30, "tx"),
                    rows(28, "md"),
                    rows(27, "id"),
                    rows(25, "me"),
                    rows(25, "al"));

            // ASC sort - tied values, relax assertion to check order only
            response =
                    executeQuery(
                            String.format(
                                    "source=%s | eval new_state = lower(state) | stats"
                                            + " bucket_nullable=false count() by new_state | sort"
                                            + " `count()` | head 5",
                                    TEST_INDEX_ACCOUNT));
            JSONArray datarows = response.getJSONArray("datarows");
            assertEquals(5, datarows.length());
            for (int i = 1; i < datarows.length(); i++) {
                assertTrue(
                        "Rows should be in ascending order by count",
                        datarows.getJSONArray(i - 1).getInt(0)
                                <= datarows.getJSONArray(i).getInt(0));
            }
        } finally {
            resetQueryBucketSize();
        }
    }

    @Override
    @Test
    public void testStatsSpanSortOnMeasureMultiTerms() throws IOException {
        try {
            setQueryBucketSize(5);
            // DESC sort - unique values, no tie-breaking issue
            JSONObject response =
                    executeQuery(
                            String.format(
                                    "source=%s | stats bucket_nullable=false count() by gender, state"
                                            + " | sort - `count()` | head 5",
                                    TEST_INDEX_ACCOUNT));
            verifyDataRows(
                    response,
                    rows(18, "M", "MD"),
                    rows(17, "M", "ID"),
                    rows(17, "F", "TX"),
                    rows(16, "M", "ME"),
                    rows(15, "M", "OK"));

            // ASC sort - tied values at count=5, relax assertion
            response =
                    executeQuery(
                            String.format(
                                    "source=%s | stats bucket_nullable=false count() by gender, state"
                                            + " | sort `count()` | head 5",
                                    TEST_INDEX_ACCOUNT));
            JSONArray datarows = response.getJSONArray("datarows");
            assertEquals(5, datarows.length());
            // First row should have count=3 (only one with that count)
            assertEquals(3, datarows.getJSONArray(0).getInt(0));
            for (int i = 1; i < datarows.length(); i++) {
                assertTrue(
                        "Rows should be in ascending order by count",
                        datarows.getJSONArray(i - 1).getInt(0)
                                <= datarows.getJSONArray(i).getInt(0));
            }

            // SUM sort ASC - unique sums, no tie-breaking issue
            response =
                    executeQuery(
                            String.format(
                                    "source=%s | stats bucket_nullable=false sum(balance) as sum by"
                                            + " gender, state | sort sum | head 5",
                                    TEST_INDEX_ACCOUNT));
            verifyDataRows(
                    response,
                    rows(85753, "F", "OR"),
                    rows(86793, "F", "DE"),
                    rows(100197, "F", "WI"),
                    rows(105693, "M", "NV"),
                    rows(124878, "M", "IN"));

            // SUM sort DESC - unique sums, no tie-breaking issue
            response =
                    executeQuery(
                            String.format(
                                    "source=%s | stats bucket_nullable=false sum(balance) as sum by"
                                            + " gender, state | sort - sum | head 5",
                                    TEST_INDEX_ACCOUNT));
            verifyDataRows(
                    response,
                    rows(505688, "F", "TX"),
                    rows(484567, "M", "MD"),
                    rows(432776, "M", "OK"),
                    rows(388568, "F", "AL"),
                    rows(382314, "F", "RI"));
        } finally {
            resetQueryBucketSize();
        }
    }

    @Override
    @Test
    public void testStatsSpanSortOnMeasureMultiTermsWithScript() throws IOException {
        try {
            setQueryBucketSize(5);
            // DESC sort - unique values, no tie-breaking issue
            JSONObject response =
                    executeQuery(
                            String.format(
                                    "source=%s | eval new_gender = lower(gender), new_state ="
                                            + " lower(state) | stats bucket_nullable=false count() by"
                                            + " new_gender, new_state | sort - `count()` | head 5",
                                    TEST_INDEX_ACCOUNT));
            verifyDataRows(
                    response,
                    rows(18, "m", "md"),
                    rows(17, "m", "id"),
                    rows(17, "f", "tx"),
                    rows(16, "m", "me"),
                    rows(15, "m", "ok"));

            // ASC sort - tied values at count=5, relax assertion
            response =
                    executeQuery(
                            String.format(
                                    "source=%s | eval new_gender = lower(gender), new_state ="
                                            + " lower(state) | stats bucket_nullable=false count() by"
                                            + " new_gender, new_state | sort `count()` | head 5",
                                    TEST_INDEX_ACCOUNT));
            JSONArray datarows = response.getJSONArray("datarows");
            assertEquals(5, datarows.length());
            assertEquals(3, datarows.getJSONArray(0).getInt(0));
            for (int i = 1; i < datarows.length(); i++) {
                assertTrue(
                        "Rows should be in ascending order by count",
                        datarows.getJSONArray(i - 1).getInt(0)
                                <= datarows.getJSONArray(i).getInt(0));
            }
        } finally {
            resetQueryBucketSize();
        }
    }

    @Override
    @Test
    public void testSumWithNull() throws IOException {
        JSONObject response =
                executeQuery(
                        String.format(
                                "source=%s | where age = 36 | stats sum(balance)",
                                TEST_INDEX_BANK_WITH_NULL_VALUES));
        verifySchema(response, schema("sum(balance)", null, "bigint"));
        // Under DQE, SUM of all-null values returns null (standard SQL behavior)
        verifyDataRows(response, rows((Integer) null));
    }

    @Override
    @Test
    public void testStatsPercentileByNullValue() throws IOException {
        JSONObject response =
                executeQuery(
                        String.format(
                                "source=%s | stats percentile(balance, 50) as p50 by age",
                                TEST_INDEX_BANK_WITH_NULL_VALUES));
        verifySchema(response, schema("p50", null, "bigint"), schema("age", null, "int"));
        // Under DQE, percentile of all-null values returns null (standard SQL behavior)
        verifyDataRows(
                response,
                rows(null, null),
                rows(32838, 28),
                rows(39225, 32),
                rows(4180, 33),
                rows(48086, 34),
                rows(null, 36));
    }

    @Override
    @Test
    public void testStatsPercentileByNullValueNonNullBucket() throws IOException {
        JSONObject response =
                executeQuery(
                        String.format(
                                "source=%s | stats bucket_nullable=false percentile(balance, 50) as p50"
                                        + " by age",
                                TEST_INDEX_BANK_WITH_NULL_VALUES));
        verifySchema(response, schema("p50", null, "bigint"), schema("age", null, "int"));
        // Under DQE, percentile of all-null values returns null (standard SQL behavior)
        verifyDataRows(
                response,
                rows(32838, 28),
                rows(39225, 32),
                rows(4180, 33),
                rows(48086, 34),
                rows(null, 36));
    }

    @Override
    @Test
    public void testStatsWithLimit() throws IOException {
        // Under DQE, ordering of stats results without explicit sort is non-deterministic.
        // Verify row count and schema only (matching pushdown-disabled behavior).
        JSONObject response =
                executeQuery(
                        String.format(
                                "source=%s | stats avg(balance) as a by age | head 5",
                                TEST_INDEX_BANK_WITH_NULL_VALUES));
        verifySchema(response, schema("a", null, "double"), schema("age", null, "int"));
        assertEquals(5, (int) response.get("size"));

        response =
                executeQuery(
                        String.format(
                                "source=%s | stats avg(balance) as a by age | head 5 | head 2 from 1",
                                TEST_INDEX_BANK_WITH_NULL_VALUES));
        verifySchema(response, schema("a", null, "double"), schema("age", null, "int"));
        assertEquals(2, (int) response.get("size"));

        response =
                executeQuery(
                        String.format(
                                "source=%s | stats avg(balance) as a by age | sort - age | head 5 |"
                                        + " head 2 from 1",
                                TEST_INDEX_BANK_WITH_NULL_VALUES));
        verifySchema(response, schema("a", null, "double"), schema("age", null, "int"));
        verifyDataRows(response, rows(48086D, 34), rows(4180D, 33));

        response =
                executeQuery(
                        String.format(
                                "source=%s | stats avg(balance) as a by age | sort - a | head 5 |"
                                        + " head 2 from 1",
                                TEST_INDEX_BANK_WITH_NULL_VALUES));
        verifySchema(response, schema("a", null, "double"), schema("age", null, "int"));
        verifyDataRows(response, rows(39225D, 32), rows(32838D, 28));
    }
}
