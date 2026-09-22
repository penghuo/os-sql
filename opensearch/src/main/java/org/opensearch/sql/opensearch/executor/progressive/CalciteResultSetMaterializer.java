/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progressive;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.calcite.avatica.util.StructImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.locationtech.jts.geom.Point;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.executor.Warning;
import org.opensearch.sql.executor.analytics.TimewrapSignals;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprGeoPointValue;

/**
 * Shared conversion boundary for synchronous final rows and progressive StateStore rows.
 *
 * <p>Only this class interprets JDBC values and derives the public schema.
 */
final class CalciteResultSetMaterializer {
  private final RelDataType rowType;
  private final List<RelDataType> fieldTypes;
  private final TimewrapSignals timewrapSignals;
  private final List<Warning> warnings;

  CalciteResultSetMaterializer(
      RelDataType rowType, TimewrapSignals timewrapSignals, List<Warning> warnings) {
    this.rowType = rowType;
    this.fieldTypes = rowType.getFieldList().stream().map(RelDataTypeField::getType).toList();
    this.timewrapSignals = timewrapSignals;
    this.warnings = List.copyOf(warnings);
  }

  List<ExprValue> drain(
      ResultSet resultSet, Integer querySizeLimit, Consumer<ExprValue> rowConsumer)
      throws SQLException {
    List<ExprValue> rows = new ArrayList<>();
    while (resultSet.next() && (querySizeLimit == null || rows.size() < querySizeLimit)) {
      ExprValue row = readCurrentRow(resultSet);
      rows.add(row);
      rowConsumer.accept(row);
    }
    return rows;
  }

  QueryResponse materialize(ResultSet resultSet, Integer querySizeLimit) throws SQLException {
    return materialize(drain(resultSet, querySizeLimit, ignored -> {}));
  }

  QueryResponse materialize(List<ExprValue> rows) {
    List<Column> columns = buildColumns(rows);
    QueryResponse response = new QueryResponse(new Schema(columns), List.copyOf(rows), null);
    response.setWarnings(warnings);
    QueryResponse pivoted = timewrapSignals.pivot(response);
    pivoted.setWarnings(warnings);
    return pivoted;
  }

  private ExprValue readCurrentRow(ResultSet resultSet) throws SQLException {
    ResultSetMetaData metaData = resultSet.getMetaData();
    Map<String, ExprValue> row = new LinkedHashMap<>();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      String columnName = metaData.getColumnName(i);
      Object converted = processValue(resultSet.getObject(columnName), fieldTypes.get(i - 1));
      row.put(columnName, ExprValueUtils.fromObjectValue(converted));
    }
    return ExprTupleValue.fromExprValueMap(row);
  }

  private List<Column> buildColumns(List<ExprValue> rows) {
    List<Column> columns = new ArrayList<>(rowType.getFieldCount());
    List<RelDataTypeField> fields = rowType.getFieldList();
    for (int i = 0; i < fields.size(); i++) {
      RelDataTypeField field = fields.get(i);
      ExprType exprType;
      if (field.getType().getSqlTypeName() == SqlTypeName.ANY) {
        exprType =
            rows.isEmpty()
                ? ExprCoreType.UNDEFINED
                : rows.getFirst().tupleValue().get(field.getName()).type();
      } else {
        exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(field.getType());
      }
      columns.add(new Column(field.getName(), null, exprType));
    }
    return columns;
  }

  @SuppressWarnings("unchecked")
  private static Object processValue(Object value, RelDataType type) throws SQLException {
    if (value == null) {
      return null;
    }
    if (value instanceof Point point) {
      return new OpenSearchExprGeoPointValue(point.getY(), point.getX());
    }
    if (value instanceof Map<?, ?> rawMap) {
      Map<String, Object> convertedMap = new HashMap<>();
      for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
        convertedMap.put(String.valueOf(entry.getKey()), processValue(entry.getValue(), null));
      }
      return convertedMap;
    }
    if (value instanceof StructImpl struct) {
      Object[] attributes = struct.getAttributes();
      if (type != null && type.getSqlTypeName() == SqlTypeName.ROW) {
        Map<String, Object> converted = new LinkedHashMap<>();
        List<RelDataTypeField> fields = type.getFieldList();
        for (int i = 0; i < fields.size() && i < attributes.length; i++) {
          converted.put(
              fields.get(i).getName(), processValue(attributes[i], fields.get(i).getType()));
        }
        return converted;
      }
      return Arrays.asList(attributes);
    }
    if (value instanceof List<?> values) {
      RelDataType componentType = type == null ? null : type.getComponentType();
      List<Object> converted = new ArrayList<>(values.size());
      for (Object item : values) {
        converted.add(processValue(item, componentType));
      }
      return converted;
    }
    return value;
  }
}
