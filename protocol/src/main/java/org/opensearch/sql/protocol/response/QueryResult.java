/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response;

import java.time.ZoneId;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import lombok.Getter;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.lang.LangSpec;

/**
 * Query response that encapsulates query results and isolate {@link ExprValue} related from
 * formatter implementation.
 */
public class QueryResult implements Iterable<Object[]> {

  @Getter private final ExecutionEngine.Schema schema;

  /** Results which are collection of expression. */
  private final Collection<ExprValue> exprValues;

  @Getter private final Cursor cursor;

  private final LangSpec langSpec;

  /** Session timezone for formatting timestamp values. */
  private final ZoneId sessionTimezone;

  public QueryResult(ExecutionEngine.Schema schema, Collection<ExprValue> exprValues) {
    this(schema, exprValues, Cursor.None, LangSpec.SQL_SPEC, ZoneId.systemDefault());
  }

  public QueryResult(
      ExecutionEngine.Schema schema, Collection<ExprValue> exprValues, Cursor cursor) {
    this(schema, exprValues, cursor, LangSpec.SQL_SPEC, ZoneId.systemDefault());
  }

  public QueryResult(
      ExecutionEngine.Schema schema,
      Collection<ExprValue> exprValues,
      Cursor cursor,
      LangSpec langSpec) {
    this(schema, exprValues, cursor, langSpec, ZoneId.systemDefault());
  }

  /** Constructor with timezone support for timestamp formatting. */
  public QueryResult(
      ExecutionEngine.Schema schema,
      Collection<ExprValue> exprValues,
      Cursor cursor,
      LangSpec langSpec,
      ZoneId sessionTimezone) {
    this.schema = schema;
    this.exprValues = exprValues;
    this.cursor = cursor;
    this.langSpec = langSpec;
    this.sessionTimezone = sessionTimezone != null ? sessionTimezone : ZoneId.systemDefault();
  }

  /**
   * size of results.
   *
   * @return size of results
   */
  public int size() {
    return exprValues.size();
  }

  /**
   * Parse column name from results.
   *
   * @return mapping from column names to its expression type. note that column name could be
   *     original name or its alias if any.
   */
  public Map<String, String> columnNameTypes() {
    Map<String, String> colNameTypes = new LinkedHashMap<>();
    schema
        .getColumns()
        .forEach(
            column ->
                colNameTypes.put(
                    getColumnName(column),
                    langSpec.typeName(column.getExprType()).toLowerCase(Locale.ROOT)));
    return colNameTypes;
  }

  @Override
  public Iterator<Object[]> iterator() {
    // Any chance to avoid copy for json response generation?
    return exprValues.stream()
        .map(ExprValueUtils::getTupleValue)
        .map(Map::values)
        .map(this::convertExprValuesToValues)
        .iterator();
  }

  private String getColumnName(Column column) {
    return (column.getAlias() != null) ? column.getAlias() : column.getName();
  }

  private Object[] convertExprValuesToValues(Collection<ExprValue> exprValues) {
    return exprValues.stream().map(this::convertExprValueToValue).toArray(Object[]::new);
  }

  /**
   * Convert ExprValue to display value using session timezone for timestamps. This implements the
   * pure Instant architecture where timezone is applied only at display boundary.
   */
  private Object convertExprValueToValue(ExprValue exprValue) {
    if (exprValue instanceof ExprTimestampValue) {
      // Apply session timezone for timestamp display - key part of Instant-based architecture
      return ((ExprTimestampValue) exprValue).value(sessionTimezone);
    }
    return exprValue.value();
  }
}
