/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.distributed;

import java.util.Map;
import org.opensearch.sql.data.model.ExprValue;

/**
 * Represents the intermediate state of a distributed aggregation. In a distributed execution plan,
 * aggregation is split into two phases:
 *
 * <ol>
 *   <li><b>Partial phase</b> (runs on each shard): reads raw rows, calls {@link #accumulate} for
 *       each value, then serializes the state via {@link #toPartialResult}.
 *   <li><b>Final phase</b> (runs on coordinator): deserializes partial states via
 *       {@link #mergePartialResult}, then produces the final value via {@link #finalResult}.
 * </ol>
 *
 * <p>Partial state is serialized as named fields in an {@link
 * org.opensearch.sql.data.model.ExprTupleValue} row, using the aggregation name as a prefix to
 * avoid collisions when multiple aggregations are computed in the same operator.
 */
public interface PartialAggState {

    /**
     * Accumulate a raw input value into this partial state. Called during the partial phase for
     * each non-null, non-missing value from the input rows.
     *
     * @param value the input value to accumulate
     */
    void accumulate(ExprValue value);

    /**
     * Serialize the current partial state into named fields in the given map. The field names are
     * prefixed with {@code aggName} to allow multiple aggregations per row.
     *
     * @param aggName the aggregation name prefix
     * @param result the map to write partial state fields into
     */
    void toPartialResult(String aggName, Map<String, ExprValue> result);

    /**
     * Merge a serialized partial state from another partition into this state. Called during the
     * final phase for each partial result row received from the exchange.
     *
     * @param aggName the aggregation name prefix used in {@link #toPartialResult}
     * @param partialRow the tuple value map containing partial state fields
     */
    void mergePartialResult(String aggName, Map<String, ExprValue> partialRow);

    /**
     * Compute and return the final aggregated result from the accumulated/merged state.
     *
     * @return the final aggregation result as an {@link ExprValue}
     */
    ExprValue finalResult();
}
