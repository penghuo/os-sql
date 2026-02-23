/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.predicate;

import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.StringLiteral;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.opensearch.dqe.analyzer.scope.Scope;
import org.opensearch.dqe.analyzer.type.TypedExpression;
import org.opensearch.dqe.metadata.DqeColumnHandle;

/**
 * Classifies individual expression nodes into pushdown predicate types. Used internally by {@link
 * PredicateAnalyzer}.
 */
public class PushdownClassifier {

  public PushdownClassifier() {}

  /**
   * Attempts to classify a typed expression as a pushdown predicate.
   *
   * @param expression the typed expression
   * @param scope the current scope
   * @return the classified predicate, or empty if the expression cannot be pushed down
   */
  public Optional<PushdownPredicate> classify(TypedExpression expression, Scope scope) {
    return classifyExpression(expression.getExpression(), scope);
  }

  /**
   * Checks if an expression is a simple column-vs-literal comparison suitable for pushdown.
   *
   * @param expression the typed expression
   * @return true if it is a column-literal comparison
   */
  public boolean isColumnLiteralComparison(TypedExpression expression) {
    if (expression.getExpression() instanceof ComparisonExpression cmp) {
      return (isColumnRef(cmp.getLeft()) && isLiteralValue(cmp.getRight()))
          || (isLiteralValue(cmp.getLeft()) && isColumnRef(cmp.getRight()));
    }
    return false;
  }

  private Optional<PushdownPredicate> classifyExpression(Expression expr, Scope scope) {
    if (expr instanceof ComparisonExpression cmp) {
      return classifyComparison(cmp, scope);
    }

    if (expr instanceof IsNullPredicate isNull) {
      return classifyIsNull(isNull, scope);
    }

    if (expr instanceof IsNotNullPredicate isNotNull) {
      return classifyIsNotNull(isNotNull, scope);
    }

    if (expr instanceof BetweenPredicate between) {
      return classifyBetween(between, scope);
    }

    if (expr instanceof InPredicate in) {
      return classifyIn(in, scope);
    }

    if (expr instanceof LikePredicate like) {
      return classifyLike(like, scope);
    }

    if (expr instanceof LogicalExpression logical) {
      return classifyLogical(logical, scope);
    }

    if (expr instanceof NotExpression not) {
      return classifyNot(not, scope);
    }

    return Optional.empty();
  }

  private Optional<PushdownPredicate> classifyComparison(
      ComparisonExpression cmp, Scope scope) {
    // We can push down column = literal, column < literal, etc.
    Expression left = cmp.getLeft();
    Expression right = cmp.getRight();

    String fieldName = null;
    Object literalValue = null;
    DqeColumnHandle colHandle = null;
    boolean reversed = false;

    if (isColumnRef(left) && isLiteralValue(right)) {
      fieldName = resolveFieldName(left, scope);
      colHandle = resolveColumnHandle(left, scope);
      literalValue = extractLiteralValue(right);
    } else if (isLiteralValue(left) && isColumnRef(right)) {
      fieldName = resolveFieldName(right, scope);
      colHandle = resolveColumnHandle(right, scope);
      literalValue = extractLiteralValue(left);
      reversed = true;
    } else {
      return Optional.empty();
    }

    if (fieldName == null || literalValue == null) {
      return Optional.empty();
    }

    // Use keyword sub-field for text fields in term/range queries
    if (colHandle != null && colHandle.getKeywordSubField() != null) {
      fieldName = colHandle.getKeywordSubField();
    }

    var colType = colHandle != null ? colHandle.getType() : null;

    ComparisonExpression.Operator op = reversed ? reverseOperator(cmp.getOperator()) : cmp.getOperator();

    return switch (op) {
      case EQUAL -> Optional.of(PushdownPredicate.termEquality(fieldName, colType, literalValue));
      case NOT_EQUAL -> {
        // NOT (field = value) -> bool.must_not term query
        PushdownPredicate eq = PushdownPredicate.termEquality(fieldName, colType, literalValue);
        yield Optional.of(PushdownPredicate.not(eq));
      }
      case LESS_THAN ->
          Optional.of(PushdownPredicate.range(fieldName, colType, null, false, literalValue, false));
      case LESS_THAN_OR_EQUAL ->
          Optional.of(PushdownPredicate.range(fieldName, colType, null, false, literalValue, true));
      case GREATER_THAN ->
          Optional.of(PushdownPredicate.range(fieldName, colType, literalValue, false, null, false));
      case GREATER_THAN_OR_EQUAL ->
          Optional.of(PushdownPredicate.range(fieldName, colType, literalValue, true, null, false));
      default -> Optional.empty();
    };
  }

  private Optional<PushdownPredicate> classifyIsNull(IsNullPredicate isNull, Scope scope) {
    if (isColumnRef(isNull.getValue())) {
      String fieldName = resolveFieldName(isNull.getValue(), scope);
      return Optional.of(PushdownPredicate.notExists(fieldName));
    }
    return Optional.empty();
  }

  private Optional<PushdownPredicate> classifyIsNotNull(
      IsNotNullPredicate isNotNull, Scope scope) {
    if (isColumnRef(isNotNull.getValue())) {
      String fieldName = resolveFieldName(isNotNull.getValue(), scope);
      return Optional.of(PushdownPredicate.exists(fieldName));
    }
    return Optional.empty();
  }

  private Optional<PushdownPredicate> classifyBetween(BetweenPredicate between, Scope scope) {
    if (isColumnRef(between.getValue())
        && isLiteralValue(between.getMin())
        && isLiteralValue(between.getMax())) {
      String fieldName = resolveFieldName(between.getValue(), scope);
      DqeColumnHandle colHandle = resolveColumnHandle(between.getValue(), scope);
      if (colHandle != null && colHandle.getKeywordSubField() != null) {
        fieldName = colHandle.getKeywordSubField();
      }
      Object lower = extractLiteralValue(between.getMin());
      Object upper = extractLiteralValue(between.getMax());
      var colType = colHandle != null ? colHandle.getType() : null;
      return Optional.of(PushdownPredicate.between(fieldName, colType, lower, upper));
    }
    return Optional.empty();
  }

  private Optional<PushdownPredicate> classifyIn(InPredicate in, Scope scope) {
    if (!isColumnRef(in.getValue())) {
      return Optional.empty();
    }

    if (!(in.getValueList() instanceof InListExpression inList)) {
      return Optional.empty();
    }

    List<Object> values = new ArrayList<>();
    for (Expression item : inList.getValues()) {
      if (!isLiteralValue(item)) {
        return Optional.empty();
      }
      values.add(extractLiteralValue(item));
    }

    String fieldName = resolveFieldName(in.getValue(), scope);
    DqeColumnHandle colHandle = resolveColumnHandle(in.getValue(), scope);
    if (colHandle != null && colHandle.getKeywordSubField() != null) {
      fieldName = colHandle.getKeywordSubField();
    }
    var colType = colHandle != null ? colHandle.getType() : null;
    return Optional.of(PushdownPredicate.inSet(fieldName, colType, values));
  }

  private Optional<PushdownPredicate> classifyLike(LikePredicate like, Scope scope) {
    if (isColumnRef(like.getValue()) && like.getPattern() instanceof StringLiteral pattern) {
      String fieldName = resolveFieldName(like.getValue(), scope);
      DqeColumnHandle colHandle = resolveColumnHandle(like.getValue(), scope);
      if (colHandle != null && colHandle.getKeywordSubField() != null) {
        fieldName = colHandle.getKeywordSubField();
      }
      return Optional.of(PushdownPredicate.likePattern(fieldName, pattern.getValue()));
    }
    return Optional.empty();
  }

  private Optional<PushdownPredicate> classifyLogical(LogicalExpression logical, Scope scope) {
    List<PushdownPredicate> classifiedChildren = new ArrayList<>();
    for (Expression term : logical.getTerms()) {
      Optional<PushdownPredicate> classified = classifyExpression(term, scope);
      if (classified.isEmpty()) {
        return Optional.empty();
      }
      classifiedChildren.add(classified.get());
    }

    return switch (logical.getOperator()) {
      case AND -> Optional.of(PushdownPredicate.and(classifiedChildren));
      case OR -> Optional.of(PushdownPredicate.or(classifiedChildren));
    };
  }

  private Optional<PushdownPredicate> classifyNot(NotExpression not, Scope scope) {
    Optional<PushdownPredicate> inner = classifyExpression(not.getValue(), scope);
    return inner.map(PushdownPredicate::not);
  }

  // -- Helpers --

  private boolean isColumnRef(Expression expr) {
    return expr instanceof Identifier || expr instanceof DereferenceExpression;
  }

  private boolean isLiteralValue(Expression expr) {
    return expr instanceof Literal;
  }

  private String resolveFieldName(Expression expr, Scope scope) {
    if (expr instanceof Identifier id) {
      return id.getValue();
    }
    if (expr instanceof DereferenceExpression deref) {
      return flattenDereference(deref);
    }
    return null;
  }

  private DqeColumnHandle resolveColumnHandle(Expression expr, Scope scope) {
    String name = resolveFieldName(expr, scope);
    if (name == null) {
      return null;
    }
    for (DqeColumnHandle col : scope.getColumns()) {
      if (col.getFieldName().equalsIgnoreCase(name)
          || col.getFieldPath().equalsIgnoreCase(name)) {
        return col;
      }
    }
    return null;
  }

  private String flattenDereference(DereferenceExpression deref) {
    if (deref.getBase() instanceof Identifier base && deref.getField().isPresent()) {
      return base.getValue() + "." + deref.getField().get().getValue();
    }
    if (deref.getBase() instanceof DereferenceExpression inner && deref.getField().isPresent()) {
      return flattenDereference(inner) + "." + deref.getField().get().getValue();
    }
    return deref.toString();
  }

  static Object extractLiteralValue(Expression expr) {
    if (expr instanceof StringLiteral s) {
      return s.getValue();
    }
    if (expr instanceof LongLiteral l) {
      return l.getValue();
    }
    if (expr instanceof DoubleLiteral d) {
      return d.getValue();
    }
    if (expr instanceof BooleanLiteral b) {
      return b.getValue();
    }
    if (expr instanceof NullLiteral) {
      return null;
    }
    return null;
  }

  private ComparisonExpression.Operator reverseOperator(ComparisonExpression.Operator op) {
    return switch (op) {
      case LESS_THAN -> ComparisonExpression.Operator.GREATER_THAN;
      case LESS_THAN_OR_EQUAL -> ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
      case GREATER_THAN -> ComparisonExpression.Operator.LESS_THAN;
      case GREATER_THAN_OR_EQUAL -> ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
      default -> op;
    };
  }
}
