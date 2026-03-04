/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.trino.parser;

import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import java.util.HashSet;
import java.util.Set;

public class DqeSqlParser {

  private final SqlParser parser = new SqlParser();

  public Statement parse(String sql) {
    return parser.createStatement(sql);
  }

  public Set<String> extractTableNames(String sql) {
    Statement stmt = parse(sql);
    Set<String> tables = new HashSet<>();
    new DefaultTraversalVisitor<Void>() {
      @Override
      protected Void visitTable(Table node, Void context) {
        tables.add(node.getName().toString());
        return null;
      }
    }.process(stmt, null);
    return tables;
  }
}
