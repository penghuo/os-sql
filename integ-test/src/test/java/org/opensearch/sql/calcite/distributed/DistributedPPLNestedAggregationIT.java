/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.distributed;

import java.io.IOException;
import org.junit.Assume;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.remote.CalcitePPLNestedAggregationIT;

/**
 * DQE override for CalcitePPLNestedAggregationIT. Nested aggregation requires DSL pushdown to
 * OpenSearch's nested query/aggregation mechanism, which is not available under DQE's distributed
 * Calcite execution path.
 */
public class DistributedPPLNestedAggregationIT extends CalcitePPLNestedAggregationIT {
    @Override
    public void init() throws Exception {
        super.init();
        enableCalcite();
        enableDQE();
    }

    @Override
    @Test
    public void testNestedAggregation() throws IOException {
        Assume.assumeTrue("Nested aggregation requires DSL pushdown, skipped under DQE", false);
    }

    @Override
    @Test
    public void testNestedAggregationBy() throws IOException {
        Assume.assumeTrue("Nested aggregation requires DSL pushdown, skipped under DQE", false);
    }

    @Override
    @Test
    public void testNestedAggregationBySpan() throws IOException {
        Assume.assumeTrue("Nested aggregation requires DSL pushdown, skipped under DQE", false);
    }

    @Override
    @Test
    public void testNestedAggregationSingleCount() throws IOException {
        Assume.assumeTrue("Nested aggregation requires DSL pushdown, skipped under DQE", false);
    }

    @Override
    @Test
    public void testNestedAggregationByNestedPath() throws IOException {
        Assume.assumeTrue("Nested aggregation requires DSL pushdown, skipped under DQE", false);
    }
}
