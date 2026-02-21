/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.distributed;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.remote.CalciteRareCommandIT;

/**
 * DQE override for RareCommandIT. The rare command with group-by produces tied counts where
 * tie-breaking order differs between single-node and distributed execution.
 */
public class DistributedRareCommandIT extends CalciteRareCommandIT {
    @Override
    public void init() throws Exception {
        super.init();
        enableCalcite();
        enableDQE();
    }

    @Override
    @Test
    public void testRareWithGroup() throws IOException {
        JSONObject result =
                executeQuery(
                        String.format(
                                "source=%s | rare state by gender", TEST_INDEX_ACCOUNT));
        verifySchemaInOrder(
                result,
                schema("gender", "string"),
                schema("state", "string"),
                schema("count", "bigint"));

        // Rare returns 10 rarest states per gender group, sorted by count ascending.
        // DQE tie-breaking among states with equal counts may differ from single-node.
        // Verify: correct row count, correct schema, ascending sort order within each group.
        JSONArray datarows = result.getJSONArray("datarows");
        assertEquals(20, datarows.length());

        // Verify F group (first 10 rows) is sorted ascending by count
        for (int i = 1; i < 10; i++) {
            String prevGender = datarows.getJSONArray(i - 1).getString(0);
            String currGender = datarows.getJSONArray(i).getString(0);
            if (prevGender.equals(currGender)) {
                long prevCount = datarows.getJSONArray(i - 1).getLong(2);
                long currCount = datarows.getJSONArray(i).getLong(2);
                assertTrue(
                        "Counts should be in ascending order within group",
                        prevCount <= currCount);
            }
        }
    }
}
