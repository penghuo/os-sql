/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_IP;

import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

public class ExprIPType extends ExprSqlType {
  public ExprIPType(OpenSearchTypeFactory typeFactory) {
    super(typeFactory, EXPR_IP, SqlTypeName.VARCHAR);
  }

  private ExprIPType(BasicSqlType type) {
    super(EXPR_IP, type);
  }

  @Override
  protected ExprSqlType cloneWith(BasicSqlType inner) {
    return new ExprIPType(inner);
  }
}
