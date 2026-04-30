/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.omni.adapter;

import io.trino.client.Column;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.SqlVarbinary;
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
    // Trino returns columns in its own order (often alphabetical for SELECT *). PPL expects
    // columns in the order declared by the planned RelNode (pplRowType). When pplRowType is
    // available we build the schema in PPL order and reorder each row's values to match.
    int n = columns.size();

    // Index Trino columns by name for reorder lookup.
    Map<String, Integer> trinoIndexByName = new LinkedHashMap<>();
    for (int i = 0; i < n; i++) {
      trinoIndexByName.put(columns.get(i).getName(), i);
    }

    // Determine the authoritative column order + types.
    List<String> pplNames;
    List<ExprType> pplTypes;
    List<Schema.Column> schemaCols = new ArrayList<>(n);

    if (pplRowType != null) {
      List<RelDataTypeField> fields = pplRowType.getFieldList();
      pplNames = new ArrayList<>(fields.size());
      pplTypes = new ArrayList<>(fields.size());
      for (RelDataTypeField field : fields) {
        String name = field.getName();
        ExprType t = CalciteToExprTypeMapper.toExprType(field.getType());
        pplNames.add(name);
        pplTypes.add(t);
        schemaCols.add(new Schema.Column(name, null, t));
      }
    } else {
      pplNames = new ArrayList<>(n);
      pplTypes = new ArrayList<>(n);
      for (Column c : columns) {
        String name = c.getName();
        ExprType t = TrinoTypeMapper.toExprType(c.getType());
        pplNames.add(name);
        pplTypes.add(t);
        schemaCols.add(new Schema.Column(name, null, t));
      }
    }

    // Precompute Trino column index for each PPL column (by name match).
    int[] trinoIdx = new int[pplNames.size()];
    for (int i = 0; i < pplNames.size(); i++) {
      Integer idx = trinoIndexByName.get(pplNames.get(i));
      trinoIdx[i] = idx == null ? i : idx;
    }

    List<ExprValue> results = new ArrayList<>(rows.size());
    for (List<Object> row : rows) {
      Map<String, ExprValue> tuple = new LinkedHashMap<>();
      for (int i = 0; i < pplNames.size(); i++) {
        int src = trinoIdx[i];
        Object raw = src < row.size() ? row.get(src) : null;
        tuple.put(pplNames.get(i), toExprValue(raw, pplTypes.get(i)));
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
      return decimal.toBigDecimal().doubleValue();
    }
    if (raw instanceof SqlTimestamp timestamp) {
      return timestamp.toString();
    }
    if (raw instanceof SqlTimestampWithTimeZone ts) {
      return ts.toString();
    }
    if (raw instanceof SqlDate date) {
      return date.toString();
    }
    if (raw instanceof SqlTime time) {
      return time.toString();
    }
    if (raw instanceof SqlTimeWithTimeZone time) {
      return time.toString();
    }
    if (raw instanceof SqlVarbinary bin) {
      return bin.getBytes();
    }
    return raw;
  }
}
