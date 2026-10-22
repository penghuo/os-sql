/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.util.Map;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;

/**
 * Approximates the heap retained by a {@link QueryResult}.
 *
 * <p>The estimate is the input to {@link ResultBudget}, so it only needs to be proportional to the
 * real footprint, not exact. Constants approximate 64-bit JVM object sizes with compressed oops; a
 * 10,000-row, eight-column result measured about 8.9 MB of live heap and estimates within the same
 * order of magnitude.
 */
public final class QueryResultSizeEstimator {

  /** Row list slot plus per-row bookkeeping. */
  static final long ROW_OVERHEAD = 16;

  /** {@link ExprTupleValue} and its backing map. */
  static final long TUPLE_OVERHEAD = 64;

  /** One map entry; keys are shared schema strings and are not counted. */
  static final long TUPLE_ENTRY_OVERHEAD = 48;

  /** {@link ExprCollectionValue} and its backing list. */
  static final long COLLECTION_OVERHEAD = 40;

  /** One list slot. */
  static final long COLLECTION_ELEMENT_OVERHEAD = 8;

  /** String wrapper, {@link String} header, and backing array header. */
  static final long STRING_OVERHEAD = 56;

  /** Boxed scalar: numbers, booleans, temporals, null and missing. */
  static final long SCALAR = 24;

  /** Fixed cost of an explain result. */
  static final long EXPLAIN_OVERHEAD = 1024;

  /** Nesting beyond this depth is charged as a scalar rather than walked. */
  static final int MAX_DEPTH = 32;

  private static final String CORE_MODEL_PACKAGE = ExprValue.class.getPackageName();

  private QueryResultSizeEstimator() {}

  /**
   * Estimates the retained size of {@code result} in bytes.
   *
   * @param result terminal result produced by a runner
   * @return estimated size; never negative
   */
  public static long estimate(QueryResult result) {
    return switch (result) {
      case QueryResult.Rows rows -> {
        long total = 0;
        for (ExprValue row : rows.rows()) {
          total += ROW_OVERHEAD + estimate(row, 0);
        }
        yield total;
      }
      case QueryResult.Explain explain ->
          EXPLAIN_OVERHEAD + 2L * String.valueOf(explain.response()).length();
      case QueryResult.Running running -> 0L;
    };
  }

  private static long estimate(ExprValue value, int depth) {
    if (value == null || depth >= MAX_DEPTH) {
      return SCALAR;
    }
    if (value instanceof ExprTupleValue tuple) {
      long total = TUPLE_OVERHEAD;
      for (Map.Entry<String, ExprValue> entry : tuple.tupleValue().entrySet()) {
        total += TUPLE_ENTRY_OVERHEAD + estimate(entry.getValue(), depth + 1);
      }
      return total;
    }
    if (value instanceof ExprCollectionValue collection) {
      long total = COLLECTION_OVERHEAD;
      for (ExprValue element : collection.collectionValue()) {
        total += COLLECTION_ELEMENT_OVERHEAD + estimate(element, depth + 1);
      }
      return total;
    }
    if (value instanceof ExprStringValue string) {
      return stringSize(string.stringValue());
    }
    if (value.getClass().getPackageName().equals(CORE_MODEL_PACKAGE)) {
      // Numbers, booleans, temporals, IPs, null and missing. Temporal value() formats a new
      // string on every call, so it must not be used for sizing.
      return SCALAR;
    }
    // Engine-specific values (binary, geo point, ...) expose their payload through value().
    return value.value() instanceof String text ? stringSize(text) : SCALAR;
  }

  private static long stringSize(String text) {
    return text == null ? SCALAR : STRING_OVERHEAD + 2L * text.length();
  }
}
