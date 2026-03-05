/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.parser;

/**
 * Thrown when a type mismatch is detected during semantic analysis. Reports the column/expression,
 * expected type, and actual type.
 */
public class DqeTypeMismatchException extends DqeException {

  private final String expression;
  private final String expectedType;
  private final String actualType;

  /**
   * Creates a type mismatch exception.
   *
   * @param expression the expression or column name with the type error
   * @param expectedType the expected type name (e.g., "INTEGER")
   * @param actualType the actual type name (e.g., "VARCHAR")
   */
  public DqeTypeMismatchException(String expression, String expectedType, String actualType) {
    super(
        "Type mismatch for '"
            + expression
            + "': expected "
            + expectedType
            + " but got "
            + actualType,
        DqeErrorCode.TYPE_MISMATCH);
    this.expression = expression;
    this.expectedType = expectedType;
    this.actualType = actualType;
  }

  /** Returns the expression or column name. */
  public String getExpression() {
    return expression;
  }

  /** Returns the expected type name. */
  public String getExpectedType() {
    return expectedType;
  }

  /** Returns the actual type name. */
  public String getActualType() {
    return actualType;
  }
}
