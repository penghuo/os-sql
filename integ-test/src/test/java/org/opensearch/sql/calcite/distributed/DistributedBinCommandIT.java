/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.distributed;

import java.io.IOException;
import org.junit.Assume;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.remote.CalciteBinCommandIT;

/**
 * DQE override for CalciteBinCommandIT. Timestamp-based bin operations require DSL pushdown via
 * auto_date_histogram, which is not available under DQE's Calcite execution path.
 */
public class DistributedBinCommandIT extends CalciteBinCommandIT {
    @Override
    public void init() throws Exception {
        super.init();
        enableCalcite();
        enableDQE();
    }

    @Override
    @Test
    public void testStatsWithBinsOnTimeField_Count() throws IOException {
        // Timestamp bins require auto_date_histogram pushdown, not available under DQE
        Assume.assumeTrue("Timestamp bins require DSL pushdown, skipped under DQE", false);
    }

    @Override
    @Test
    public void testStatsWithBinsOnTimeField_Avg() throws IOException {
        Assume.assumeTrue("Timestamp bins require DSL pushdown, skipped under DQE", false);
    }

    @Override
    @Test
    public void testStatsWithBinsOnTimeAndTermField_Count() throws IOException {
        Assume.assumeTrue("Timestamp bins require DSL pushdown, skipped under DQE", false);
    }

    @Override
    @Test
    public void testStatsWithBinsOnTimeAndTermField_Avg() throws IOException {
        Assume.assumeTrue("Timestamp bins require DSL pushdown, skipped under DQE", false);
    }
}
