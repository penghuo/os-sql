/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.distributed;

import java.util.Map;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;

/**
 * Partial aggregation state for SUM. The partial phase computes a local sum per partition; the
 * final phase sums the partial sums.
 *
 * <p>Serialization format: {@code {aggName_sum: numericValue}}
 */
public class SumPartialAggState implements PartialAggState {
    private static final String SUM_SUFFIX = "_sum";

    private final ExprCoreType type;
    private double sum;
    private boolean hasValue;

    public SumPartialAggState(ExprCoreType type) {
        this.type = type;
        this.sum = 0.0;
        this.hasValue = false;
    }

    @Override
    public void accumulate(ExprValue value) {
        hasValue = true;
        sum += value.doubleValue();
    }

    @Override
    public void toPartialResult(String aggName, Map<String, ExprValue> result) {
        if (hasValue) {
            result.put(aggName + SUM_SUFFIX, ExprValueUtils.doubleValue(sum));
        } else {
            result.put(aggName + SUM_SUFFIX, ExprNullValue.of());
        }
    }

    @Override
    public void mergePartialResult(String aggName, Map<String, ExprValue> partialRow) {
        ExprValue partialSum = partialRow.get(aggName + SUM_SUFFIX);
        if (partialSum != null && !partialSum.isNull() && !partialSum.isMissing()) {
            hasValue = true;
            sum += partialSum.doubleValue();
        }
    }

    @Override
    public ExprValue finalResult() {
        if (!hasValue) {
            return ExprNullValue.of();
        }
        switch (type) {
            case INTEGER:
                return ExprValueUtils.integerValue((int) sum);
            case LONG:
                return ExprValueUtils.longValue((long) sum);
            case FLOAT:
                return ExprValueUtils.floatValue((float) sum);
            case DOUBLE:
                return ExprValueUtils.doubleValue(sum);
            default:
                return ExprValueUtils.doubleValue(sum);
        }
    }
}
