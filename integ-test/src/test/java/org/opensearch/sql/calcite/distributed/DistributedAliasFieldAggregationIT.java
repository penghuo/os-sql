/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.distributed;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.List;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.remote.CalciteAliasFieldAggregationIT;

/**
 * DQE override for CalciteAliasFieldAggregationIT. Under DQE, the TAKE aggregation on timestamp
 * fields returns values in space-separated format ("2024-01-01 10:00:00") rather than ISO-8601
 * format ("2024-01-01T10:00:00.000Z") because DSLScan returns timestamps in a different format
 * than the DSL pushdown path.
 */
public class DistributedAliasFieldAggregationIT extends CalciteAliasFieldAggregationIT {
    @Override
    public void init() throws Exception {
        super.init();
        enableCalcite();
        enableDQE();
    }

    @Override
    @Test
    public void testTakeWithAliasField() throws IOException {
        JSONObject actual =
                executeQuery(
                        "source=test_alias_bug | sort @timestamp | stats TAKE(@timestamp, 2)");
        verifySchema(actual, schema("TAKE(@timestamp, 2)", "array"));
        // Under DQE, timestamps come back in space-separated format without trailing Z
        verifyDataRows(
                actual,
                rows(List.of("2024-01-01 10:00:00", "2024-01-02 10:00:00")));
    }
}
