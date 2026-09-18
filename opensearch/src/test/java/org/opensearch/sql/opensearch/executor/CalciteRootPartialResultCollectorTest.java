/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.PartialResultResponseListener;
import org.opensearch.sql.executor.PartialResultResponseListener.UpdateMode;

class CalciteRootPartialResultCollectorTest {

  @Test
  void append_publications_contain_only_new_rows() throws Exception {
    RecordingListener listener = new RecordingListener();
    CalciteRootPartialResultCollector collector =
        new CalciteRootPartialResultCollector(listener, UpdateMode.APPEND, materializer());
    List<ExprValue> allRows = new ArrayList<>();

    for (int value = 1; value <= 3; value++) {
      ExprValue row = row(value);
      allRows.add(row);
      collector.onRow(row, allRows);
    }
    collector.finish();

    assertEquals(List.of(row(1)), listener.partials.get(0).getResults());
    assertEquals(List.of(row(2), row(3)), listener.partials.get(1).getResults());
  }

  @Test
  void replace_publication_contains_the_complete_current_snapshot() throws Exception {
    RecordingListener listener = new RecordingListener();
    CalciteRootPartialResultCollector collector =
        new CalciteRootPartialResultCollector(listener, UpdateMode.REPLACE, materializer());
    List<ExprValue> allRows = new ArrayList<>();

    ExprValue first = row(1);
    allRows.add(first);
    collector.onRow(first, allRows);

    assertEquals(List.of(first), listener.partials.getFirst().getResults());
  }

  private static CalciteRootResultMaterializer materializer() throws Exception {
    JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    RelDataType rowType =
        typeFactory.builder().add("value", typeFactory.createSqlType(SqlTypeName.INTEGER)).build();
    ResultSetMetaData metadata = mock(ResultSetMetaData.class);
    when(metadata.getColumnCount()).thenReturn(1);
    when(metadata.getColumnName(1)).thenReturn("value");
    return new CalciteRootResultMaterializer(metadata, rowType);
  }

  private static ExprValue row(int value) {
    return ExprTupleValue.fromExprValueMap(Map.of("value", new ExprIntegerValue(value)));
  }

  private static final class RecordingListener implements PartialResultResponseListener {
    private final List<QueryResponse> partials = new ArrayList<>();

    @Override
    public void onPartial(QueryResponse response) {
      partials.add(response);
    }

    @Override
    public void onResponse(QueryResponse response) {}

    @Override
    public void onFailure(Exception e) {
      fail(e);
    }
  }
}
