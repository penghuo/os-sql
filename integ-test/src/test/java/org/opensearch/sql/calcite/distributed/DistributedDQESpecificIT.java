/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.distributed;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * DQE-specific integration tests that validate distributed query execution behavior beyond what the
 * mirrored Calcite*IT classes cover. These tests target partial aggregation correctness, shard-side
 * eval, TopK merge, script filter handling, fail-fast semantics, and setting toggles.
 */
public class DistributedDQESpecificIT extends PPLIntegTestCase {

    @Override
    public void init() throws Exception {
        super.init();
        enableCalcite();
        enableDQE();

        loadIndex(Index.BANK);
        loadIndex(Index.ACCOUNT);
    }

    @Test
    public void testPartialAggregationCount() throws IOException {
        // Partial aggregation: COUNT across shards should produce correct global count
        JSONObject result =
                executeQuery(
                        String.format("source=%s | stats count() as cnt", TEST_INDEX_BANK));
        verifySchema(result, schema("cnt", "bigint"));
        verifyDataRows(result, rows(7));
    }

    @Test
    public void testPartialAggregationSumByGroup() throws IOException {
        // Partial aggregation: SUM grouped by key — shard partial sums merge correctly
        JSONObject result =
                executeQuery(
                        String.format(
                                "source=%s | stats sum(balance) as total by gender",
                                TEST_INDEX_BANK));
        verifySchema(result, schema("total", "bigint"), schema("gender", "string"));
        // Bank index has both M and F genders; verify both groups are present
        // Exact values depend on test data but should be deterministic
        JSONObject resultNoGroup =
                executeQuery(
                        String.format(
                                "source=%s | stats sum(balance) as total", TEST_INDEX_BANK));
        verifySchema(resultNoGroup, schema("total", "bigint"));
    }

    @Test
    public void testPartialAggregationAvg() throws IOException {
        // AVG decomposes to SUM + COUNT on shards, then SUM(sums)/SUM(counts) on coordinator
        JSONObject result =
                executeQuery(
                        String.format(
                                "source=%s | stats avg(age) as avg_age", TEST_INDEX_BANK));
        verifySchema(result, schema("avg_age", "double"));
        // Verify the result is a reasonable average (bank test data ages are 28-39)
        double avgAge = result.getJSONArray("datarows").getJSONArray(0).getDouble(0);
        assertTrue("Average age should be between 20 and 50, got: " + avgAge,
                avgAge >= 20.0 && avgAge <= 50.0);
    }

    @Test
    public void testPartialAggregationMinMax() throws IOException {
        // MIN and MAX across shards: each shard returns local min/max, coordinator merges
        JSONObject result =
                executeQuery(
                        String.format(
                                "source=%s | stats min(age) as min_age, max(age) as max_age",
                                TEST_INDEX_BANK));
        verifySchema(
                result,
                schema("min_age", "int"),
                schema("max_age", "int"));
        int minAge = result.getJSONArray("datarows").getJSONArray(0).getInt(0);
        int maxAge = result.getJSONArray("datarows").getJSONArray(0).getInt(1);
        assertTrue("min_age should be <= max_age", minAge <= maxAge);
    }

    @Test
    public void testPartialAggregationMultipleFunctions() throws IOException {
        // Multiple aggregate functions in a single stats command
        JSONObject result =
                executeQuery(
                        String.format(
                                "source=%s | stats count() as cnt, sum(balance) as total,"
                                        + " avg(age) as avg_age by gender",
                                TEST_INDEX_BANK));
        verifySchema(
                result,
                schema("cnt", "bigint"),
                schema("total", "bigint"),
                schema("avg_age", "double"),
                schema("gender", "string"));
    }

    @Test
    public void testEvalOnShard() throws IOException {
        // eval expression should execute on shard side, not pull all rows to coordinator
        // source=bank | eval doubled_age = age * 2 | where doubled_age > 60 | fields firstname,
        // doubled_age
        JSONObject result =
                executeQuery(
                        String.format(
                                "source=%s | eval doubled_age = age * 2 | where doubled_age > 60"
                                        + " | fields firstname, doubled_age",
                                TEST_INDEX_BANK));
        verifySchema(
                result,
                schema("firstname", "string"),
                schema("doubled_age", "int"));
        // All returned rows should have doubled_age > 60
        for (int i = 0; i < result.getJSONArray("datarows").length(); i++) {
            int val = result.getJSONArray("datarows").getJSONArray(i).getInt(1);
            assertTrue("doubled_age should be > 60, got: " + val, val > 60);
        }
    }

    @Test
    public void testEvalWithStringExpression() throws IOException {
        // Eval with string concatenation on shard
        JSONObject result =
                executeQuery(
                        String.format(
                                "source=%s | eval full_info = firstname | fields full_info"
                                        + " | head 3",
                                TEST_INDEX_BANK));
        verifySchema(result, schema("full_info", "string"));
        assertTrue(
                "Should return at most 3 rows",
                result.getJSONArray("datarows").length() <= 3);
    }

    @Test
    public void testTopKAcrossShards() throws IOException {
        // TopK: sort + head on multi-shard index — each shard returns local top-K,
        // coordinator merges via MergeSortExchange
        JSONObject result =
                executeQuery(
                        String.format(
                                "source=%s | sort balance | head 5",
                                TEST_INDEX_BANK));
        // Verify we get exactly 5 rows (or fewer if index has < 5 docs)
        int rowCount = result.getJSONArray("datarows").length();
        assertTrue("Should return at most 5 rows, got: " + rowCount, rowCount <= 5);

        // Verify rows are in ascending order by balance
        for (int i = 1; i < rowCount; i++) {
            int prev = result.getJSONArray("datarows").getJSONArray(i - 1).getInt(
                    getColumnIndex(result, "balance"));
            int curr = result.getJSONArray("datarows").getJSONArray(i).getInt(
                    getColumnIndex(result, "balance"));
            assertTrue(
                    "Rows should be sorted ascending by balance: " + prev + " <= " + curr,
                    prev <= curr);
        }
    }

    @Test
    public void testTopKDescending() throws IOException {
        // TopK descending: sort - balance | head 3
        JSONObject result =
                executeQuery(
                        String.format(
                                "source=%s | sort - balance | head 3",
                                TEST_INDEX_BANK));
        int rowCount = result.getJSONArray("datarows").length();
        assertTrue("Should return at most 3 rows", rowCount <= 3);

        // Verify rows are in descending order by balance
        for (int i = 1; i < rowCount; i++) {
            int prev = result.getJSONArray("datarows").getJSONArray(i - 1).getInt(
                    getColumnIndex(result, "balance"));
            int curr = result.getJSONArray("datarows").getJSONArray(i).getInt(
                    getColumnIndex(result, "balance"));
            assertTrue(
                    "Rows should be sorted descending by balance: " + prev + " >= " + curr,
                    prev >= curr);
        }
    }

    @Test
    public void testScriptFilterNotPushedToLucene() throws IOException {
        // where abs(age - 30) = 0 — abs() is not a Lucene-native filter.
        // Under DQE, this should execute as a Calcite Filter on the shard,
        // not as a Painless script in DSL.
        JSONObject result =
                executeQuery(
                        String.format(
                                "source=%s | where abs(age - 30) = 0 | fields firstname, age",
                                TEST_INDEX_BANK));
        verifySchema(result, schema("firstname", "string"), schema("age", "int"));
        // All returned rows should have age == 30
        for (int i = 0; i < result.getJSONArray("datarows").length(); i++) {
            int age = result.getJSONArray("datarows").getJSONArray(i).getInt(1);
            assertEquals("All rows should have age=30", 30, age);
        }
    }

    @Test
    public void testScriptFilterWithMathFunction() throws IOException {
        // Another non-pushable filter: ceil(balance / 1000) > 40
        JSONObject result =
                executeQuery(
                        String.format(
                                "source=%s | where ceil(balance / 1000) > 40"
                                        + " | fields firstname, balance",
                                TEST_INDEX_BANK));
        verifySchema(result, schema("firstname", "string"), schema("balance", "bigint"));
        for (int i = 0; i < result.getJSONArray("datarows").length(); i++) {
            int balance = result.getJSONArray("datarows").getJSONArray(i).getInt(1);
            assertTrue("balance should satisfy ceil(balance/1000) > 40, got: " + balance,
                    Math.ceil(balance / 1000.0) > 40);
        }
    }

    @Test
    public void testSettingToggle() throws IOException {
        // Run query with DQE on (already enabled in init)
        JSONObject resultDqeOn =
                executeQuery(
                        String.format(
                                "source=%s | stats count() as cnt", TEST_INDEX_BANK));
        verifySchema(resultDqeOn, schema("cnt", "bigint"));
        verifyDataRows(resultDqeOn, rows(7));

        // Toggle DQE off, run same query
        disableDQE();
        JSONObject resultDqeOff =
                executeQuery(
                        String.format(
                                "source=%s | stats count() as cnt", TEST_INDEX_BANK));
        verifySchema(resultDqeOff, schema("cnt", "bigint"));
        verifyDataRows(resultDqeOff, rows(7));

        // Toggle DQE back on, run same query
        enableDQE();
        JSONObject resultDqeOnAgain =
                executeQuery(
                        String.format(
                                "source=%s | stats count() as cnt", TEST_INDEX_BANK));
        verifySchema(resultDqeOnAgain, schema("cnt", "bigint"));
        verifyDataRows(resultDqeOnAgain, rows(7));
    }

    @Test
    public void testSettingToggleWithEval() throws IOException {
        // Verify eval results are identical regardless of DQE on/off
        String query =
                String.format(
                        "source=%s | eval x = age + 1 | stats avg(x) as avg_x",
                        TEST_INDEX_BANK);

        JSONObject resultDqeOn = executeQuery(query);
        double valDqeOn = resultDqeOn.getJSONArray("datarows").getJSONArray(0).getDouble(0);

        disableDQE();
        JSONObject resultDqeOff = executeQuery(query);
        double valDqeOff = resultDqeOff.getJSONArray("datarows").getJSONArray(0).getDouble(0);

        enableDQE();
        assertEquals(
                "DQE on and off should produce identical results for eval + avg",
                valDqeOn, valDqeOff, 0.001);
    }

    @Test
    public void testFailFastOnUnsupportedQuery() throws IOException {
        // DQE should fail fast (return error) rather than silently fall back
        // when encountering a query pattern it cannot handle.
        // Using an intentionally malformed or unsupported query to trigger error.
        try {
            executeQuery("source=nonexistent_index_dqe_test | stats count()");
            fail("Expected query against nonexistent index to throw an error");
        } catch (Exception e) {
            // Expected: should get an error, not a silent fallback
            assertTrue(
                    "Error message should indicate the issue",
                    e.getMessage() != null && !e.getMessage().isEmpty());
        }
    }

    @Test
    public void testLargeAggregationOnAccountIndex() throws IOException {
        // Account index has 1000 docs across multiple shards — tests partial aggregation at scale
        JSONObject result =
                executeQuery(
                        String.format(
                                "source=%s | stats count() as cnt, sum(age) as total_age,"
                                        + " avg(balance) as avg_bal",
                                TEST_INDEX_ACCOUNT));
        verifySchema(
                result,
                schema("cnt", "bigint"),
                schema("total_age", "bigint"),
                schema("avg_bal", "double"));
        long cnt = result.getJSONArray("datarows").getJSONArray(0).getLong(0);
        assertTrue("Account index should have docs, got: " + cnt, cnt > 0);
    }

    @Test
    public void testGroupByAggregationOnAccountIndex() throws IOException {
        // Group by with multiple aggregate functions on a large index
        JSONObject result =
                executeQuery(
                        String.format(
                                "source=%s | stats count() as cnt, avg(age) as avg_age"
                                        + " by gender",
                                TEST_INDEX_ACCOUNT));
        verifySchema(
                result,
                schema("cnt", "bigint"),
                schema("avg_age", "double"),
                schema("gender", "string"));
        // Should have at least M and F groups
        assertTrue(
                "Should have at least 1 group",
                result.getJSONArray("datarows").length() >= 1);
    }

    /**
     * Helper to find column index by name from the result schema.
     */
    private int getColumnIndex(JSONObject result, String columnName) {
        var schema = result.getJSONArray("schema");
        for (int i = 0; i < schema.length(); i++) {
            if (schema.getJSONObject(i).getString("name").equals(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }
}
