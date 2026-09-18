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
import org.opensearch.sql.opensearch.data.value.OpenSearchExprGeoPointValue;

/**
 * Canonical materializer for both final JDBC rows and source-native partial snapshots.
 *
 * <p>Column names come from Calcite JDBC metadata, while types come from the Calcite root row type.
 * Source callbacks must not construct public schemas or reorder columns independently.
 */
final class CalciteRootResultMaterializer {
  private final List<String> columnNames;
  private final List<RelDataType> fieldTypes;
  private List<Column> materializedColumns;

  CalciteRootResultMaterializer(ResultSetMetaData metaData, RelDataType rowType)
      throws SQLException {
    int columnCount = metaData.getColumnCount();
    if (columnCount != rowType.getFieldCount()) {
      throw new SQLException(
          "Calcite result metadata has "
              + columnCount
              + " columns but root row type has "
              + rowType.getFieldCount());
    }
    this.columnNames = new ArrayList<>(columnCount);
    for (int i = 1; i <= columnCount; i++) {
      columnNames.add(metaData.getColumnName(i));
    }
    this.fieldTypes = rowType.getFieldList().stream().map(RelDataTypeField::getType).toList();
  }

  ExprValue readRow(ResultSet resultSet) throws SQLException {
    Map<String, ExprValue> row = new LinkedHashMap<>();
    for (int i = 0; i < columnNames.size(); i++) {
      String columnName = columnNames.get(i);
      Object converted = processValue(resultSet.getObject(columnName), fieldTypes.get(i));
      row.put(columnName, ExprValueUtils.fromObjectValue(converted));
    }
    return ExprTupleValue.fromExprValueMap(row);
  }

  /**
   * Reorders and validates source-native rows against the same root descriptor used for JDBC rows.
   */
  List<ExprValue> materializeSourceRows(List<ExprValue> sourceRows) {
    List<ExprValue> materialized = new ArrayList<>(sourceRows.size());
    for (ExprValue sourceRow : sourceRows) {
      Map<String, ExprValue> source = sourceRow.tupleValue();
      Map<String, ExprValue> ordered = new LinkedHashMap<>();
      for (String columnName : columnNames) {
        ExprValue value = source.get(columnName);
        if (value == null) {
          throw new IllegalArgumentException(
              "Aggregation snapshot is missing root column [" + columnName + "]");
        }
        ordered.put(columnName, value);
      }
      materialized.add(ExprTupleValue.fromExprValueMap(ordered));
    }
    return List.copyOf(materialized);
  }

  synchronized QueryResponse response(List<ExprValue> rows) {
    if (materializedColumns == null) {
      materializedColumns = List.copyOf(buildColumns(rows));
    }
    return new QueryResponse(new Schema(materializedColumns), List.copyOf(rows), null);
  }

  private List<Column> buildColumns(List<ExprValue> rows) {
    List<Column> columns = new ArrayList<>(columnNames.size());
    for (int i = 0; i < columnNames.size(); i++) {
      String columnName = columnNames.get(i);
      RelDataType fieldType = fieldTypes.get(i);
      ExprType exprType;
      if (fieldType.getSqlTypeName() == SqlTypeName.ANY) {
        ExprValue value = rows.isEmpty() ? null : rows.getFirst().tupleValue().get(columnName);
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
      Map<String, Object> map = (Map<String, Object>) value;
      Map<String, Object> convertedMap = new HashMap<>();
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        convertedMap.put(entry.getKey(), processValue(entry.getValue(), null));
      }
      return convertedMap;
    }
    if (value instanceof StructImpl structImpl) {
      Object[] attributes = structImpl.getAttributes();
      if (type != null && type.getSqlTypeName() == SqlTypeName.ROW) {
        List<RelDataTypeField> fields = type.getFieldList();
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < fields.size() && i < attributes.length; i++) {
          map.put(fields.get(i).getName(), processValue(attributes[i], fields.get(i).getType()));
        }
        return map;
      }
      return Arrays.asList(attributes);
    }
    if (value instanceof List) {
      List<Object> list = (List<Object>) value;
      RelDataType componentType =
          type != null && type.getComponentType() != null ? type.getComponentType() : null;
      List<Object> convertedList = new ArrayList<>(list.size());
      for (Object item : list) {
        convertedList.add(processValue(item, componentType));
      }
      return convertedList;
    }
    return value;
  }
}
