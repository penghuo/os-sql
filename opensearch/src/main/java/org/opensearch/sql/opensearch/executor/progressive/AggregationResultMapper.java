/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progressive;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.TotalHits;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.agg.CountAsTotalHitsParser;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;

/**
 * Converts completed and reduce-callback aggregation data through the existing parser and value
 * factory.
 */
public final class AggregationResultMapper {
  private final OpenSearchExprValueFactory valueFactory;
  private final OpenSearchAggregationResponseParser parser;

  public AggregationResultMapper(OpenSearchExprValueFactory valueFactory) {
    this.valueFactory = valueFactory;
    this.parser = valueFactory.getParser();
  }

  public List<ExprValue> map(TotalHits totalHits, Aggregations aggregations) {
    List<Map<String, Object>> parsed =
        parser instanceof CountAsTotalHitsParser
            ? parser.parse(new SearchHits(new SearchHit[0], totalHits, Float.NaN))
            : parser.parse(aggregations);
    return parsed.stream().map(this::toExprTuple).toList();
  }

  private ExprValue toExprTuple(Map<String, Object> values) {
    ImmutableMap.Builder<String, ExprValue> tuple = ImmutableMap.builder();
    values.forEach((field, value) -> tuple.put(field, valueFactory.construct(field, value, true)));
    return ExprTupleValue.fromExprValueMap(tuple.build());
  }
}
