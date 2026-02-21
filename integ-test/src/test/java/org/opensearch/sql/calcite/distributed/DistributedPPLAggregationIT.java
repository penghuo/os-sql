/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.distributed;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.remote.CalcitePPLAggregationIT;

/**
 * DQE override for CalcitePPLAggregationIT. Under DQE, SUM of all-null values returns null
 * (matching standard SQL and the pushdown-disabled path) rather than 0.
 */
public class DistributedPPLAggregationIT extends CalcitePPLAggregationIT {
    @Override
    public void init() throws Exception {
        super.init();
        enableCalcite();
        enableDQE();
    }

    @Override
    @Test
    public void testSumNull() throws IOException {
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
    public void testSumGroupByNullValue() throws IOException {
        JSONObject response =
                executeQuery(
                        String.format(
                                "source=%s | stats sum(balance) as a by age",
                                TEST_INDEX_BANK_WITH_NULL_VALUES));
        verifySchema(response, schema("a", null, "bigint"), schema("age", null, "int"));
        // Under DQE, SUM of all-null values returns null (standard SQL behavior)
        verifyDataRows(
                response,
                rows(null, null),
                rows(32838, 28),
                rows(39225, 32),
                rows(4180, 33),
                rows(48086, 34),
                rows(null, 36));
    }
}
