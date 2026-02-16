/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.distributed;

import java.util.Locale;

/**
 * Aggregation types supported by the distributed partial/final aggregation operators. Maps from
 * function names used by {@link org.opensearch.sql.expression.aggregation.NamedAggregator} to
 * the corresponding partial aggregation strategy.
 */
public enum AggregationType {
    COUNT,
    SUM,
    AVG,
    MIN,
    MAX;

    /**
     * Resolves an {@link AggregationType} from the function name string returned by
     * {@code aggregator.getDelegated().getFunctionName().getFunctionName()}.
     *
     * @param functionName the lowercase function name (e.g. "count", "sum")
     * @return the corresponding AggregationType
     * @throws IllegalArgumentException if the function name is not supported
     */
    public static AggregationType fromFunctionName(String functionName) {
        try {
            return valueOf(functionName.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unsupported aggregation function for distributed execution: " + functionName,
                    e);
        }
    }
}
