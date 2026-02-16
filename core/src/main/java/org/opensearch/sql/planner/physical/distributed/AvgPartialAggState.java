/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.distributed;

import java.util.Map;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;

/**
 * Partial aggregation state for AVG. The partial phase tracks both sum and count per partition;
 * the final phase sums the partial sums and counts, then divides.
 *
 * <p>Serialization format: {@code {aggName_sum: doubleValue, aggName_count: longValue}}
 */
public class AvgPartialAggState implements PartialAggState {
    private static final String SUM_SUFFIX = "_sum";
    private static final String COUNT_SUFFIX = "_count";

    private double sum;
    private long count;

    @Override
    public void accumulate(ExprValue value) {
        sum += value.doubleValue();
        count++;
    }

    @Override
    public void toPartialResult(String aggName, Map<String, ExprValue> result) {
        result.put(aggName + SUM_SUFFIX, ExprValueUtils.doubleValue(sum));
        result.put(aggName + COUNT_SUFFIX, ExprValueUtils.longValue(count));
    }

    @Override
    public void mergePartialResult(String aggName, Map<String, ExprValue> partialRow) {
        ExprValue partialSum = partialRow.get(aggName + SUM_SUFFIX);
        ExprValue partialCount = partialRow.get(aggName + COUNT_SUFFIX);
        if (partialSum != null && !partialSum.isNull() && !partialSum.isMissing()) {
            sum += partialSum.doubleValue();
        }
        if (partialCount != null && !partialCount.isNull() && !partialCount.isMissing()) {
            count += partialCount.longValue();
        }
    }

    @Override
    public ExprValue finalResult() {
        if (count == 0) {
            return ExprNullValue.of();
        }
        return ExprValueUtils.doubleValue(sum / count);
    }
}
