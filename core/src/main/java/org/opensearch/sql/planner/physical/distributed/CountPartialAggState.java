/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.distributed;

import java.util.Map;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;

/**
 * Partial aggregation state for COUNT. The partial phase counts rows per partition; the final
 * phase sums the partial counts.
 *
 * <p>Serialization format: {@code {aggName_count: longValue}}
 */
public class CountPartialAggState implements PartialAggState {
    private static final String COUNT_SUFFIX = "_count";

    private long count;

    @Override
    public void accumulate(ExprValue value) {
        count++;
    }

    @Override
    public void toPartialResult(String aggName, Map<String, ExprValue> result) {
        result.put(aggName + COUNT_SUFFIX, ExprValueUtils.longValue(count));
    }

    @Override
    public void mergePartialResult(String aggName, Map<String, ExprValue> partialRow) {
        ExprValue partialCount = partialRow.get(aggName + COUNT_SUFFIX);
        if (partialCount != null && !partialCount.isNull() && !partialCount.isMissing()) {
            count += partialCount.longValue();
        }
    }

    @Override
    public ExprValue finalResult() {
        return ExprValueUtils.longValue(count);
    }
}
