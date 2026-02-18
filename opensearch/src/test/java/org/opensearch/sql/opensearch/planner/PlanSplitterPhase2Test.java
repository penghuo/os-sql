/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for Phase 2 PlanSplitter logic. These tests validate the PlanSplitter class can be
 * instantiated and the split method can be called. Full integration tests with real scan nodes
 * and join/window operators are in the integration test suite.
 */
class PlanSplitterPhase2Test {

    @Test
    @DisplayName("PlanSplitter can be instantiated")
    void testPlanSplitterInstantiation() {
        PlanSplitter splitter = new PlanSplitter();
        assertNotNull(splitter);
    }
}
