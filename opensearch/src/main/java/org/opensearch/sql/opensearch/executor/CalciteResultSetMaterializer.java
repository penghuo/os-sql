/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.util.StructImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.locationtech.jts.geom.Point;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.TimewrapPivot;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprGeoPointValue;

/** Standard ResultSet-to-QueryResponse materialization shared by final and state queries. */
final class CalciteResultSetMaterializer {
  interface RowObserver {
    void onRow(ExprValue row);

    default void finish() {}
  }

  private CalciteResultSetMaterializer() {}

  static QueryResponse materialize(
      ResultSet resultSet,
      RelDataType rowTypes,
      Integer querySizeLimit,
      RowObserver rowObserver,
      boolean finalResponse)
      throws SQLException {
    ResultSetMetaData metaData = resultSet.getMetaData();
    int columnCount = metaData.getColumnCount();
    List<RelDataType> fieldTypes =
        rowTypes.getFieldList().stream().map(RelDataTypeField::getType).toList();
    List<ExprValue> values = new ArrayList<>();
    while (resultSet.next() && (querySizeLimit == null || values.size() < querySizeLimit)) {
      Map<String, ExprValue> row = new LinkedHashMap<>();
      for (int i = 1; i <= columnCount; i++) {
        String columnName = metaData.getColumnName(i);
        Object converted = processValue(resultSet.getObject(columnName), fieldTypes.get(i - 1));
        row.put(columnName, ExprValueUtils.fromObjectValue(converted));
      }
      ExprValue value = ExprTupleValue.fromExprValueMap(row);
      values.add(value);
      if (rowObserver != null) {
        rowObserver.onRow(value);
      }
    }
    if (rowObserver != null) {
      rowObserver.finish();
    }

    List<Column> columns = buildColumns(metaData, fieldTypes, values);
    if (finalResponse && TimewrapPivot.isTimewrap()) {
      try {
        TimewrapPivot.Result pivoted =
            TimewrapPivot.pivot(
                columns,
                values,
                CalcitePlanContext.timewrapUnitName.get(),
                CalcitePlanContext.timewrapSeries.get());
        columns = pivoted.columns();
        values = pivoted.values();
      } finally {
        CalcitePlanContext.clearTimewrapSignals();
      }
    }

    QueryResponse response = new QueryResponse(new Schema(columns), values, null);
    if (finalResponse) {
      response.setWarnings(CalcitePlanContext.drainWarnings());
    }
    return response;
  }

  static QueryResponse materialize(
      Enumerable<?> enumerable, RelDataType rowTypes, Integer querySizeLimit) throws SQLException {
    List<RelDataTypeField> fields = rowTypes.getFieldList();
    List<RelDataType> fieldTypes = fields.stream().map(RelDataTypeField::getType).toList();
    List<String> fieldNames = fields.stream().map(RelDataTypeField::getName).toList();
    List<ExprValue> values = new ArrayList<>();
    Enumerator<?> enumerator = enumerable.enumerator();
    try {
      while (enumerator.moveNext() && (querySizeLimit == null || values.size() < querySizeLimit)) {
        Object current = enumerator.current();
        Object[] columns = current instanceof Object[] array ? array : new Object[] {current};
        Map<String, ExprValue> row = new LinkedHashMap<>();
        for (int i = 0; i < fieldNames.size(); i++) {
          row.put(
              fieldNames.get(i),
              ExprValueUtils.fromObjectValue(processValue(columns[i], fieldTypes.get(i))));
        }
        values.add(ExprTupleValue.fromExprValueMap(row));
      }
    } finally {
      enumerator.close();
    }
    return new QueryResponse(
        new Schema(buildColumns(fieldNames, fieldTypes, values)), values, null);
  }

  private static List<Column> buildColumns(
      ResultSetMetaData metaData, List<RelDataType> fieldTypes, List<ExprValue> values)
      throws SQLException {
    List<String> fieldNames = new ArrayList<>(metaData.getColumnCount());
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      fieldNames.add(metaData.getColumnName(i));
    }
    return buildColumns(fieldNames, fieldTypes, values);
  }

  private static List<Column> buildColumns(
      List<String> fieldNames, List<RelDataType> fieldTypes, List<ExprValue> values) {
    List<Column> columns = new ArrayList<>(fieldNames.size());
    for (int i = 0; i < fieldNames.size(); i++) {
      String columnName = fieldNames.get(i);
      RelDataType fieldType = fieldTypes.get(i);
      ExprType exprType;
      if (fieldType.getSqlTypeName() == SqlTypeName.ANY) {
        ExprValue value = values.isEmpty() ? null : values.getFirst().tupleValue().get(columnName);
        exprType = value == null ? ExprCoreType.UNDEFINED : value.type();
      } else {
        exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(fieldType);
      }
      columns.add(new Column(columnName, null, exprType));
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
    if (value instanceof Map) {
      Map<String, Object> converted = new HashMap<>();
      for (Map.Entry<String, Object> entry : ((Map<String, Object>) value).entrySet()) {
        converted.put(entry.getKey(), processValue(entry.getValue(), null));
      }
      return converted;
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
    if (value instanceof List) {
      RelDataType componentType =
          type != null && type.getComponentType() != null ? type.getComponentType() : null;
      List<Object> converted = new ArrayList<>();
      for (Object item : (List<Object>) value) {
        converted.add(processValue(item, componentType));
      }
      return converted;
    }
    return value;
  }
}
