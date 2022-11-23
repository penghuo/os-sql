/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.statement.ddl;

import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.statement.Statement;

@Getter
@RequiredArgsConstructor
public class CreateTable extends Statement {

  private final QualifiedName tableName;

  private final List<Column> columns;

  private final String fileFormat;

  private final String location;

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> visitor, C context) {
    return visitor.visitCreateTable(this, context);
  }
}
