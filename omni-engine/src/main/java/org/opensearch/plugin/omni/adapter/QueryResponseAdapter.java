/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.omni.adapter;

import io.trino.client.Column;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTimestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.pagination.Cursor;

/** Converts Trino client-protocol results into {@link ExecutionEngine.QueryResponse}. */
public final class QueryResponseAdapter {

  private QueryResponseAdapter() {}

  /**
   * Adapts Trino results to QueryResponse, using Calcite's RelDataType (from PPL's type inference)
   * when available.
   *
   * @param columns Trino column metadata
   * @param rows result rows
   * @param pplRowType Calcite RelDataType from the planned RelNode (nullable)
   * @return QueryResponse with schema derived from PPL types (preferred) or Trino types (fallback)
   */
  public static ExecutionEngine.QueryResponse adapt(
      List<Column> columns, List<List<Object>> rows, RelDataType pplRowType) {
    int n = columns.size();
    List<Schema.Column> schemaCols = new ArrayList<>(n);
    List<String> names = new ArrayList<>(n);
    List<ExprType> types = new ArrayList<>(n);

    // Build a map from column name → RelDataTypeField for fast lookup
    Map<String, RelDataTypeField> pplFields = new LinkedHashMap<>();
    if (pplRowType != null) {
      for (RelDataTypeField field : pplRowType.getFieldList()) {
        pplFields.put(field.getName(), field);
      }
    }

    for (Column c : columns) {
      String name = c.getName();
      ExprType t;
      // Prefer Calcite's type if available (it has PPL's type inference baked in)
      RelDataTypeField pplField = pplFields.get(name);
      if (pplField != null) {
        t = CalciteToExprTypeMapper.toExprType(pplField.getType());
      } else {
        // Fallback to Trino type mapping
        t = TrinoTypeMapper.toExprType(c.getType());
      }
      names.add(name);
      types.add(t);
      schemaCols.add(new Schema.Column(name, null, t));
    }

    List<ExprValue> results = new ArrayList<>(rows.size());
    for (List<Object> row : rows) {
      Map<String, ExprValue> tuple = new LinkedHashMap<>();
      int cells = Math.min(row.size(), n);
      for (int i = 0; i < cells; i++) {
        Object raw = row.get(i);
        tuple.put(names.get(i), toExprValue(raw, types.get(i)));
      }
      results.add(ExprTupleValue.fromExprValueMap(tuple));
    }

    return new ExecutionEngine.QueryResponse(new Schema(schemaCols), results, Cursor.None);
  }

  /**
   * Backward-compatible overload without RelDataType (uses Trino type mapping only).
   */
  public static ExecutionEngine.QueryResponse adapt(
      List<Column> columns, List<List<Object>> rows) {
    return adapt(columns, rows, null);
  }

  private static ExprValue toExprValue(Object raw, ExprType type) {
    if (raw == null) {
      return ExprValueUtils.nullValue();
    }
    // Unwrap Trino SPI types to Java primitives/strings that ExprValueUtils expects
    Object normalized = normalize(raw);
    return ExprValueUtils.fromObjectValue(normalized, type);
  }

  /**
   * Converts Trino SPI types (SqlDecimal, SqlTimestamp, etc.) to plain Java types that
   * ExprValueUtils.fromObjectValue() expects.
   */
  private static Object normalize(Object raw) {
    if (raw instanceof SqlDecimal decimal) {
      // TrinoTypeMapper maps DECIMAL→DOUBLE, so convert to double
      return decimal.toBigDecimal().doubleValue();
    }
    if (raw instanceof SqlTimestamp timestamp) {
      // Return ISO-8601 string format expected by ExprValueUtils for TIMESTAMP
      return timestamp.toString();
    }
    return raw;
  }
}
