/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.omni.adapter;

import io.trino.client.Column;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

  public static ExecutionEngine.QueryResponse adapt(
      List<Column> columns, List<List<Object>> rows) {
    int n = columns.size();
    List<Schema.Column> schemaCols = new ArrayList<>(n);
    List<String> names = new ArrayList<>(n);
    List<ExprType> types = new ArrayList<>(n);
    for (Column c : columns) {
      ExprType t = TrinoTypeMapper.toExprType(c.getType());
      names.add(c.getName());
      types.add(t);
      schemaCols.add(new Schema.Column(c.getName(), null, t));
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

  private static ExprValue toExprValue(Object raw, ExprType type) {
    if (raw == null) {
      return ExprValueUtils.nullValue();
    }
    return ExprValueUtils.fromObjectValue(raw, type);
  }
}
