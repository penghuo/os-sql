/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.distributed;

import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.aggregation.NamedAggregator;

/**
 * Factory for creating {@link PartialAggState} instances from {@link NamedAggregator} metadata.
 */
public class PartialAggStateFactory {

    private PartialAggStateFactory() {}

    /**
     * Create a {@link PartialAggState} for the given aggregation type.
     *
     * @param type the aggregation type
     * @param returnType the return type of the aggregation (used by SUM to preserve type)
     * @return a new partial aggregation state
     */
    public static PartialAggState create(AggregationType type, ExprCoreType returnType) {
        switch (type) {
            case COUNT:
                return new CountPartialAggState();
            case SUM:
                return new SumPartialAggState(returnType);
            case AVG:
                return new AvgPartialAggState();
            case MIN:
                return new MinPartialAggState();
            case MAX:
                return new MaxPartialAggState();
            default:
                throw new IllegalArgumentException(
                        "Unsupported aggregation type: " + type);
        }
    }

    /**
     * Create a {@link PartialAggState} from a {@link NamedAggregator}.
     *
     * @param aggregator the named aggregator
     * @return a new partial aggregation state
     */
    public static PartialAggState create(NamedAggregator aggregator) {
        String functionName =
                aggregator.getDelegated().getFunctionName().getFunctionName();
        AggregationType type = AggregationType.fromFunctionName(functionName);
        ExprCoreType returnType = (ExprCoreType) aggregator.getDelegated().type();
        return create(type, returnType);
    }
}
