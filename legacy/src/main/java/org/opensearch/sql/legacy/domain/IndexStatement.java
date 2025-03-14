/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.domain;

/** Class used to differentiate SHOW and DESCRIBE statements */
public class IndexStatement implements QueryStatement {

  private final StatementType statementType;
  private final String query;
  private String indexPattern;
  private String columnPattern;

  public IndexStatement(StatementType statementType, String query) {
    this.statementType = statementType;
    this.query = query;

    parseQuery();
  }

  private void parseQuery() {
    String[] statement = query.split(" ");

    int tokenLength = statement.length;
    try {
      for (int i = 1; i < tokenLength; i++) {
        switch (statement[i].toUpperCase()) {
          case "TABLES":
            if (i + 1 < tokenLength && statement[i + 1].equalsIgnoreCase("LIKE")) {
              if (i + 2 < tokenLength) {
                indexPattern = replaceWildcard(statement[i + 2]);
                i += 2;
              }
            }
            break;
          case "COLUMNS":
            if (i + 1 < tokenLength && statement[i + 1].equalsIgnoreCase("LIKE")) {
              if (i + 2 < tokenLength) {
                columnPattern = replaceWildcard(statement[i + 2]);
                i += 2;
              }
            }
            break;
        }
      }

      if (indexPattern == null) {
        throw new IllegalArgumentException();
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Expected syntax example: " + syntaxString(), e);
    }
  }

  private String replaceWildcard(String str) {
    return str.replace("%", ".*").replace("_", ".");
  }

  private String syntaxString() {
    if (statementType.equals(StatementType.SHOW)) {
      return "'SHOW TABLES LIKE <table pattern>'";
    } else {
      return "'DESCRIBE TABLES LIKE <table pattern> [COLUMNS LIKE <column pattern>]'";
    }
  }

  public StatementType getStatementType() {
    return statementType;
  }

  public String getQuery() {
    return query;
  }

  public String getIndexPattern() {
    return indexPattern;
  }

  public String getColumnPattern() {
    return columnPattern;
  }

  public enum StatementType {
    SHOW,
    DESCRIBE
  }
}
