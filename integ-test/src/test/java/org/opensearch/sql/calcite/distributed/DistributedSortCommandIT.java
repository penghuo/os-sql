/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.distributed;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyOrder;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.remote.CalciteSortCommandIT;

/**
 * DQE override for SortCommandIT. Under DQE, sort is executed by Calcite on shards and merged by
 * MergeSortExchange on the coordinator. The explain output differs from DSL pushdown.
 */
public class DistributedSortCommandIT extends CalciteSortCommandIT {
    @Override
    public void init() throws Exception {
        super.init();
        enableCalcite();
        enableDQE();
    }

    @Override
    @Test
    public void testPushdownSortCastToDoubleExpression() throws IOException {
        // Under DQE, sort is not pushed down to DSL -- it executes as Calcite sort on shard.
        // Skip the explain assertion (which checks for DSL SORT pushdown) and verify data only.
        String ppl =
                String.format(
                        "source=%s | eval age2 = cast(age as double) | sort age2 | fields age, age2"
                                + " | head 2",
                        TEST_INDEX_BANK);

        JSONObject result = executeQuery(ppl);
        verifySchema(result, schema("age", "int"), schema("age2", "double"));
        verifyOrder(result, rows(28, 28d), rows(32, 32d));
    }
}
