/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc;

import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;

import java.util.List;
import java.util.Map;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.storage.TableScanOperator;

public class JDBCQueryOperator extends TableScanOperator {

  private String jdbcQuery = "jdbcQuery";

  private boolean flag = true;

  @Override
  public String explain() {
    return jdbcQuery;
  }

  @Override
  public boolean hasNext() {
    if (flag) {
      flag = false;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public ExprValue next() {
    return ExprTupleValue.fromExprValueMap(Map.of("field", LITERAL_TRUE));
  }

  /**
   * Schema is determined at query execution time.
   */
  @Override
  public ExecutionEngine.Schema schema() {
    return new ExecutionEngine.Schema(
        List.of(new ExecutionEngine.Schema.Column("field", "field", ExprCoreType.BOOLEAN)));
  }
}
