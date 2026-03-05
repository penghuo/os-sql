/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.type;

import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Literal;
import java.util.Objects;
import org.opensearch.dqe.types.DqeType;

/**
 * An expression paired with its resolved DQE type. Wraps the original Trino Expression AST node.
 */
public class TypedExpression {

  private final Expression expression;
  private final DqeType type;

  public TypedExpression(Expression expression, DqeType type) {
    this.expression = Objects.requireNonNull(expression, "expression must not be null");
    this.type = Objects.requireNonNull(type, "type must not be null");
  }

  public Expression getExpression() {
    return expression;
  }

  public DqeType getType() {
    return type;
  }

  /** Returns true if this expression is a simple column reference. */
  public boolean isColumnReference() {
    return expression instanceof Identifier || expression instanceof DereferenceExpression;
  }

  /** Returns true if this is a literal value. */
  public boolean isLiteral() {
    return expression instanceof Literal;
  }

  @Override
  public String toString() {
    return "TypedExpression{" + expression + " : " + type + "}";
  }
}
