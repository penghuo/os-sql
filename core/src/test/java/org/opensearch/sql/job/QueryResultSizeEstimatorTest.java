/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponseNode;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.pagination.Cursor;

class QueryResultSizeEstimatorTest {

  private static final Schema SCHEMA = new Schema(List.of());

  @Test
  void emptyRowsAndRunningEstimateZero() {
    assertEquals(0L, QueryResultSizeEstimator.estimate(rows(List.of())));
    assertEquals(
        0L,
        QueryResultSizeEstimator.estimate(new QueryResult.Running(new QueryJobId("node", "ctx"))));
  }

  @Test
  void rowCostsOverheadPlusItsValues() {
    ExprValue row =
        tuple(Map.of("s", new ExprStringValue("abc"), "n", ExprValueUtils.integerValue(1)));
    long expected =
        QueryResultSizeEstimator.ROW_OVERHEAD
            + QueryResultSizeEstimator.TUPLE_OVERHEAD
            + 2 * QueryResultSizeEstimator.TUPLE_ENTRY_OVERHEAD
            + QueryResultSizeEstimator.STRING_OVERHEAD
            + 2 * 3
            + QueryResultSizeEstimator.SCALAR;
    assertEquals(expected, QueryResultSizeEstimator.estimate(rows(List.of(row))));
  }

  @Test
  void growsWithRowCountAndStringLength() {
    ExprValue shortRow = tuple(Map.of("s", new ExprStringValue("a")));
    ExprValue longRow = tuple(Map.of("s", new ExprStringValue("a".repeat(1000))));
    long one = QueryResultSizeEstimator.estimate(rows(List.of(shortRow)));
    assertEquals(2 * one, QueryResultSizeEstimator.estimate(rows(List.of(shortRow, shortRow))));
    assertEquals(one + 2 * 999, QueryResultSizeEstimator.estimate(rows(List.of(longRow))));
  }

  @Test
  void walksNestedTuplesAndCollections() {
    ExprValue inner =
        new ExprCollectionValue(List.of(new ExprStringValue("x"), ExprNullValue.of()));
    ExprValue row = tuple(Map.of("arr", inner));
    long expected =
        QueryResultSizeEstimator.ROW_OVERHEAD
            + QueryResultSizeEstimator.TUPLE_OVERHEAD
            + QueryResultSizeEstimator.TUPLE_ENTRY_OVERHEAD
            + QueryResultSizeEstimator.COLLECTION_OVERHEAD
            + 2 * QueryResultSizeEstimator.COLLECTION_ELEMENT_OVERHEAD
            + QueryResultSizeEstimator.STRING_OVERHEAD
            + 2
            + QueryResultSizeEstimator.SCALAR;
    assertEquals(expected, QueryResultSizeEstimator.estimate(rows(List.of(row))));
  }

  @Test
  void temporalValuesAreScalarsNotFormattedStrings() {
    ExprValue row = tuple(Map.of("ts", new ExprTimestampValue(Instant.ofEpochMilli(0))));
    long expected =
        QueryResultSizeEstimator.ROW_OVERHEAD
            + QueryResultSizeEstimator.TUPLE_OVERHEAD
            + QueryResultSizeEstimator.TUPLE_ENTRY_OVERHEAD
            + QueryResultSizeEstimator.SCALAR;
    assertEquals(expected, QueryResultSizeEstimator.estimate(rows(List.of(row))));
  }

  @Test
  void deepNestingIsCappedInsteadOfWalked() {
    ExprValue value = new ExprStringValue("leaf");
    for (int i = 0; i < QueryResultSizeEstimator.MAX_DEPTH * 4; i++) {
      value = new ExprCollectionValue(List.of(value));
    }
    long estimate = QueryResultSizeEstimator.estimate(rows(List.of(value)));
    assertTrue(estimate > 0);
    assertTrue(
        estimate
            < QueryResultSizeEstimator.MAX_DEPTH
                * (QueryResultSizeEstimator.COLLECTION_OVERHEAD
                    + QueryResultSizeEstimator.COLLECTION_ELEMENT_OVERHEAD)
                * 2);
  }

  @Test
  void explainCostsOverheadPlusTextLength() {
    ExplainResponse response = new ExplainResponse(new ExplainResponseNode("plan"));
    long estimate = QueryResultSizeEstimator.estimate(new QueryResult.Explain(response, 0));
    assertEquals(
        QueryResultSizeEstimator.EXPLAIN_OVERHEAD + 2L * response.toString().length(), estimate);
  }

  @Test
  void tenThousandLogRowsEstimateNearMeasuredHeap() {
    // Shape of the rows measured at ~8.9 MB of live heap for 10,000 rows on a 4 GB node.
    List<ExprValue> rows = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      Map<String, ExprValue> fields = new LinkedHashMap<>();
      fields.put("ts", new ExprTimestampValue(Instant.ofEpochMilli(1_700_000_000_000L + i)));
      fields.put("host", new ExprStringValue("host-" + (i % 200)));
      fields.put("status", ExprValueUtils.integerValue(200));
      fields.put("bytes", ExprValueUtils.longValue(12_345L));
      fields.put("latency", ExprValueUtils.doubleValue(12.5));
      fields.put("msg", new ExprStringValue("alpha bravo charlie delta echo foxtrot golf hotel"));
      fields.put("user", new ExprStringValue("user-" + (i % 5000)));
      fields.put("path", new ExprStringValue("/api/v1/charlie/" + (i % 1000)));
      rows.add(ExprTupleValue.fromExprValueMap(fields));
    }
    long estimate = QueryResultSizeEstimator.estimate(rows(rows));
    assertTrue(estimate > 6_000_000L && estimate < 12_000_000L, "estimate=" + estimate);
  }

  private static QueryResult rows(List<ExprValue> rows) {
    return new QueryResult.Rows(SCHEMA, rows, Cursor.None, List.of(), 0);
  }

  private static ExprValue tuple(Map<String, ExprValue> fields) {
    return ExprTupleValue.fromExprValueMap(new LinkedHashMap<>(fields));
  }
}
