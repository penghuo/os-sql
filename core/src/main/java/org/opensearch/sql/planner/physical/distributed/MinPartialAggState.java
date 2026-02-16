/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.distributed;

import java.util.Map;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValue;

/**
 * Partial aggregation state for MIN. The partial phase computes a local minimum per partition;
 * the final phase takes the minimum of all partial minimums.
 *
 * <p>Serialization format: {@code {aggName_min: value}}
 */
public class MinPartialAggState implements PartialAggState {
    private static final String MIN_SUFFIX = "_min";

    private ExprValue minValue;

    public MinPartialAggState() {
        this.minValue = ExprNullValue.of();
    }

    @Override
    public void accumulate(ExprValue value) {
        if (minValue.isNull() || value.compareTo(minValue) < 0) {
            minValue = value;
        }
    }

    @Override
    public void toPartialResult(String aggName, Map<String, ExprValue> result) {
        result.put(aggName + MIN_SUFFIX, minValue);
    }

    @Override
    public void mergePartialResult(String aggName, Map<String, ExprValue> partialRow) {
        ExprValue partialMin = partialRow.get(aggName + MIN_SUFFIX);
        if (partialMin != null && !partialMin.isNull() && !partialMin.isMissing()) {
            if (minValue.isNull() || partialMin.compareTo(minValue) < 0) {
                minValue = partialMin;
            }
        }
    }

    @Override
    public ExprValue finalResult() {
        return minValue;
    }
}
