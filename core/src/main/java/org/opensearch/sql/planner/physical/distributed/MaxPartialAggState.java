/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.distributed;

import java.util.Map;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValue;

/**
 * Partial aggregation state for MAX. The partial phase computes a local maximum per partition;
 * the final phase takes the maximum of all partial maximums.
 *
 * <p>Serialization format: {@code {aggName_max: value}}
 */
public class MaxPartialAggState implements PartialAggState {
    private static final String MAX_SUFFIX = "_max";

    private ExprValue maxValue;

    public MaxPartialAggState() {
        this.maxValue = ExprNullValue.of();
    }

    @Override
    public void accumulate(ExprValue value) {
        if (maxValue.isNull() || value.compareTo(maxValue) > 0) {
            maxValue = value;
        }
    }

    @Override
    public void toPartialResult(String aggName, Map<String, ExprValue> result) {
        result.put(aggName + MAX_SUFFIX, maxValue);
    }

    @Override
    public void mergePartialResult(String aggName, Map<String, ExprValue> partialRow) {
        ExprValue partialMax = partialRow.get(aggName + MAX_SUFFIX);
        if (partialMax != null && !partialMax.isNull() && !partialMax.isMissing()) {
            if (maxValue.isNull() || partialMax.compareTo(maxValue) > 0) {
                maxValue = partialMax;
            }
        }
    }

    @Override
    public ExprValue finalResult() {
        return maxValue;
    }
}
