/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.distributed;

import java.io.IOException;
import org.junit.Assume;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.remote.CalciteWhereCommandIT;

/**
 * DQE override for CalciteWhereCommandIT. Nested aggregation with filter requires DSL pushdown,
 * which is not available under DQE's distributed Calcite execution path.
 */
public class DistributedWhereCommandIT extends CalciteWhereCommandIT {
    @Override
    public void init() throws Exception {
        super.init();
        enableCalcite();
        enableDQE();
    }

    @Override
    @Test
    public void testAggFilterOnNestedFields() throws IOException {
        Assume.assumeTrue(
                "Nested aggregation with filter requires DSL pushdown, skipped under DQE", false);
    }
}
