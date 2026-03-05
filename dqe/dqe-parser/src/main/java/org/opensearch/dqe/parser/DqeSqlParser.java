/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.parser;

import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;

/**
 * Wraps Trino's {@link SqlParser} with DQE-specific configuration. Parses SQL strings into Trino
 * {@link Statement} ASTs.
 *
 * <p>This is the entry point for all SQL parsing in the DQE engine. It catches Trino's {@link
 * ParsingException} and wraps it in a {@link DqeParsingException} with structured line/column
 * information.
 */
public class DqeSqlParser {

  private final SqlParser sqlParser;

  /**
   * Creates a parser with default DQE options.
   *
   * <p>TODO: When upgrading from Trino 439 to 479+, add {@code ParsingOptions} with {@code
   * DecimalLiteralTreatment.AS_DOUBLE} to ensure decimal literals are parsed as DOUBLE rather than
   * DECIMAL. Trino 439 does not support ParsingOptions.
   */
  public DqeSqlParser() {
    this.sqlParser = new SqlParser();
  }

  /**
   * Parses a SQL string into a Trino {@link Statement} AST.
   *
   * @param sql the SQL query string
   * @return the parsed Statement AST
   * @throws DqeParsingException if the SQL has syntax errors (wraps Trino ParsingException with
   *     line/column info)
   */
  public Statement parse(String sql) throws DqeParsingException {
    try {
      return sqlParser.createStatement(sql);
    } catch (ParsingException e) {
      throw new DqeParsingException(e.getErrorMessage(), e.getLineNumber(), e.getColumnNumber(), e);
    }
  }
}
