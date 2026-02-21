/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.distributed;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY_WITH_NULL;
import static org.opensearch.sql.util.MatcherUtils.closeTo;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Assume;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.remote.CalcitePPLCaseFunctionIT;

/**
 * DQE override for CalcitePPLCaseFunctionIT. Under DQE, CASE expressions are evaluated by Calcite
 * on shards rather than being pushed down as range aggregations. This produces different NULL
 * handling behavior matching the pushdown-disabled path.
 */
public class DistributedPPLCaseFunctionIT extends CalcitePPLCaseFunctionIT {
    @Override
    public void init() throws Exception {
        super.init();
        enableCalcite();
        enableDQE();
    }

    @Override
    @Test
    public void testCaseCanBePushedDownAsRangeQuery() throws IOException {
        // 1.1 Range - Metric
        JSONObject actual1 =
                executeQuery(
                        String.format(
                                "source=%s | eval age_range = case(age < 30, 'u30', age < 40, 'u40'"
                                        + " else 'u100') | stats avg(age) as avg_age by age_range",
                                TEST_INDEX_BANK));
        verifySchema(actual1, schema("avg_age", "double"), schema("age_range", "string"));
        verifyDataRows(actual1, rows(28.0, "u30"), rows(35.0, "u40"));

        // 1.2 Range - Metric (COUNT)
        JSONObject actual2 =
                executeQuery(
                        String.format(
                                "source=%s | eval age_range = case(age < 30, 'u30', age >= 30 and age"
                                        + " < 40, 'u40' else 'u100') | stats avg(age) by age_range",
                                TEST_INDEX_BANK));
        verifySchema(actual2, schema("avg(age)", "double"), schema("age_range", "string"));
        verifyDataRows(actual2, rows(28.0, "u30"), rows(35.0, "u40"));

        // 1.3 Range - Range - Metric
        JSONObject actual3 =
                executeQuery(
                        String.format(
                                "source=%s | eval age_range = case(age < 30, 'u30', age < 40, 'u40'"
                                        + " else 'u100'), balance_range = case(balance < 20000,"
                                        + " 'medium' else 'high') | stats avg(balance) as avg_balance"
                                        + " by age_range, balance_range",
                                TEST_INDEX_BANK));
        verifySchema(
                actual3,
                schema("avg_balance", "double"),
                schema("age_range", "string"),
                schema("balance_range", "string"));
        verifyDataRows(
                actual3,
                rows(32838.0, "u30", "high"),
                closeTo(8761.333333333334, "u40", "medium"),
                rows(42617.0, "u40", "high"));

        // 1.4 Range - Metric (With null & discontinuous ranges)
        // Under DQE, CASE expressions evaluated by Calcite produce real null (not string "null")
        JSONObject actual4 =
                executeQuery(
                        String.format(
                                "source=%s | eval age_range = case(age < 30, 'u30', (age >= 35 and"
                                        + " age < 40) or age >= 80, '30-40 or >=80') | stats"
                                        + " avg(balance) by age_range",
                                TEST_INDEX_BANK));
        verifySchema(actual4, schema("avg(balance)", "double"), schema("age_range", "string"));
        verifyDataRows(
                actual4,
                rows(32838.0, "u30"),
                rows(30497.0, null),
                closeTo(20881.333333333332, "30-40 or >=80"));

        // 1.5 Should not be pushed because the range is not closed-open
        JSONObject actual5 =
                executeQuery(
                        String.format(
                                "source=%s | eval age_range = case(age < 30, 'u30', age >= 30 and"
                                        + " age <= 40, 'u40' else 'u100') | stats avg(age) as avg_age"
                                        + " by age_range",
                                TEST_INDEX_BANK));
        verifySchema(actual5, schema("avg_age", "double"), schema("age_range", "string"));
        verifyDataRows(actual5, rows(28.0, "u30"), rows(35.0, "u40"));
    }

    @Override
    @Test
    public void testCaseAggWithNullValues() throws IOException {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s"
                                        + "| eval age_category = case("
                                        + "    age < 20, 'teenager',"
                                        + "    age < 70, 'adult',"
                                        + "    age >= 70, 'senior'"
                                        + "    else 'unknown')"
                                        + "| stats avg(age) by age_category",
                                TEST_INDEX_STATE_COUNTRY_WITH_NULL));
        verifySchema(actual, schema("avg(age)", "double"), schema("age_category", "string"));
        // Under DQE, CASE expressions evaluated by Calcite include null-valued rows in the
        // 'unknown' bucket (matching pushdown-disabled behavior), producing 4 rows instead of 3.
        verifyDataRows(
                actual,
                rows(10, "teenager"),
                rows(25, "adult"),
                rows(70, "senior"),
                rows(null, "unknown"));
    }

    @Override
    @Test
    public void testNestedCaseAggWithAutoDateHistogram() throws IOException {
        // Timestamp bins via auto_date_histogram require DSL pushdown, not available under DQE
        Assume.assumeTrue(
                "Timestamp bins require DSL pushdown, skipped under DQE", false);
    }
}
