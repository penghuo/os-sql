/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.parser;

/**
 * Thrown when SQL parsing fails. Wraps Trino's {@code ParsingException} with structured
 * line/column information for the REST error response.
 */
public class DqeParsingException extends DqeException {

  private final int lineNumber;
  private final int columnNumber;

  /**
   * Creates a parsing exception with line and column information.
   *
   * @param message the parser error message
   * @param lineNumber 1-based line number where the error occurred
   * @param columnNumber 1-based column number where the error occurred
   */
  public DqeParsingException(String message, int lineNumber, int columnNumber) {
    super(message, DqeErrorCode.PARSING_ERROR);
    this.lineNumber = lineNumber;
    this.columnNumber = columnNumber;
  }

  /**
   * Creates a parsing exception with line/column information and an underlying cause.
   *
   * @param message the parser error message
   * @param lineNumber 1-based line number where the error occurred
   * @param columnNumber 1-based column number where the error occurred
   * @param cause the underlying Trino {@code ParsingException}
   */
  public DqeParsingException(String message, int lineNumber, int columnNumber, Throwable cause) {
    super(message, DqeErrorCode.PARSING_ERROR, cause);
    this.lineNumber = lineNumber;
    this.columnNumber = columnNumber;
  }

  /** Returns the 1-based line number of the parse error. */
  public int getLineNumber() {
    return lineNumber;
  }

  /** Returns the 1-based column number of the parse error. */
  public int getColumnNumber() {
    return columnNumber;
  }
}
