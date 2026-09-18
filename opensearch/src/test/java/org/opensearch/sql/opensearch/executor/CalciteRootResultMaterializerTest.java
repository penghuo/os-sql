/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;

class CalciteRootResultMaterializerTest {

  @Test
  void source_and_jdbc_rows_use_the_same_root_descriptor() throws Exception {
    JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    RelDataType rowType =
        typeFactory
            .builder()
            .add("total", typeFactory.createSqlType(SqlTypeName.BIGINT))
            .add("product", typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .build();
    ResultSetMetaData metadata = metadata("total", "product");
    CalciteRootResultMaterializer materializer =
        new CalciteRootResultMaterializer(metadata, rowType);

    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getObject("total")).thenReturn(42L);
    when(resultSet.getObject("product")).thenReturn("p1");
    ExprValue jdbcRow = materializer.readRow(resultSet);

    Map<String, ExprValue> sourceOrder = new LinkedHashMap<>();
    sourceOrder.put("product", new ExprStringValue("p1"));
    sourceOrder.put("total", new ExprLongValue(42L));
    ExprValue sourceRow = ExprTupleValue.fromExprValueMap(sourceOrder);
    ExprValue normalized = materializer.materializeSourceRows(List.of(sourceRow)).getFirst();

    assertEquals(jdbcRow, normalized);
    assertEquals(
        List.of("total", "product"),
        materializer.response(List.of(normalized)).getSchema().getColumns().stream()
            .map(column -> column.getName())
            .toList());
  }

  @Test
  void rejects_source_snapshot_that_does_not_match_root_columns() throws Exception {
    JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    RelDataType rowType =
        typeFactory.builder().add("total", typeFactory.createSqlType(SqlTypeName.BIGINT)).build();
    CalciteRootResultMaterializer materializer =
        new CalciteRootResultMaterializer(metadata("total"), rowType);
    ExprValue wrongRow = ExprTupleValue.fromExprValueMap(Map.of("other", new ExprLongValue(1L)));

    assertThrows(
        IllegalArgumentException.class,
        () -> materializer.materializeSourceRows(List.of(wrongRow)));
  }

  private static ResultSetMetaData metadata(String... names) throws Exception {
    ResultSetMetaData metadata = mock(ResultSetMetaData.class);
    when(metadata.getColumnCount()).thenReturn(names.length);
    for (int i = 0; i < names.length; i++) {
      when(metadata.getColumnName(i + 1)).thenReturn(names[i]);
    }
    return metadata;
  }
}
