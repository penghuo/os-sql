/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;

/**
 * Detects whether a Calcite {@link RelNode} tree contains operators that are unsupported for
 * distributed execution. Used by {@link QueryRouter} to fall back gracefully to single-node
 * execution when the plan contains unrecognized or custom operators.
 */
public class UnsupportedCommandDetector {

    /** Result of the detection: whether the plan is fully supported for distribution. */
    public enum Support {
        /** Fully supported for distributed execution. */
        SUPPORTED,
        /** Not supported – should fall back to single-node. */
        UNSUPPORTED
    }

    /** Holds the detection result together with an optional reason string. */
    public static class DetectionResult {
        private final Support support;
        private final String reason;

        private DetectionResult(Support support, String reason) {
            this.support = support;
            this.reason = reason;
        }

        /** Creates a result indicating full support for distributed execution. */
        public static DetectionResult supported() {
            return new DetectionResult(Support.SUPPORTED, null);
        }

        /** Creates a result indicating the plan is unsupported, with a human-readable reason. */
        public static DetectionResult unsupported(String reason) {
            return new DetectionResult(Support.UNSUPPORTED, reason);
        }

        public Support getSupport() {
            return support;
        }

        public String getReason() {
            return reason;
        }

        public boolean isSupported() {
            return support == Support.SUPPORTED;
        }
    }

    /**
     * Check whether the given plan tree is supported for distributed execution.
     *
     * <p>Supported (Tier 1) operators:
     *
     * <ul>
     *   <li>{@link LogicalAggregate}
     *   <li>{@link LogicalFilter}
     *   <li>{@link LogicalSort} and {@link Sort} (covers non-logical sorts)
     *   <li>{@link LogicalJoin}
     *   <li>{@link LogicalProject}
     *   <li>{@link TableScan} (covers all table scan types)
     *   <li>{@link LogicalValues}
     *   <li>{@link LogicalCalc}
     *   <li>{@link LogicalUnion}
     * </ul>
     *
     * @param relNode the root of the plan tree to check
     * @return a {@link DetectionResult} indicating whether the tree is supported
     */
    public DetectionResult detect(RelNode relNode) {
        if (!isSupportedType(relNode)) {
            return DetectionResult.unsupported(
                    "Unsupported operator: " + relNode.getClass().getSimpleName());
        }
        for (RelNode input : relNode.getInputs()) {
            DetectionResult childResult = detect(input);
            if (!childResult.isSupported()) {
                return childResult;
            }
        }
        return DetectionResult.supported();
    }

    private boolean isSupportedType(RelNode relNode) {
        return relNode instanceof LogicalAggregate
                || relNode instanceof LogicalFilter
                || relNode instanceof LogicalProject
                || relNode instanceof LogicalJoin
                || relNode instanceof LogicalValues
                || relNode instanceof LogicalCalc
                || relNode instanceof LogicalUnion
                || relNode instanceof TableScan
                || relNode instanceof Sort;
    }
}
