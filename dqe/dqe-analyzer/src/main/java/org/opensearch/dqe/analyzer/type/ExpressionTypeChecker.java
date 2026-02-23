/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.type;

import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.WhenClause;
import java.util.List;
import java.util.Optional;
import org.opensearch.dqe.analyzer.DqeAnalysisException;
import org.opensearch.dqe.analyzer.scope.ResolvedField;
import org.opensearch.dqe.analyzer.scope.Scope;
import org.opensearch.dqe.analyzer.scope.ScopeResolver;
import org.opensearch.dqe.parser.DqeTypeMismatchException;
import org.opensearch.dqe.parser.DqeUnsupportedOperationException;
import org.opensearch.dqe.types.DqeType;
import org.opensearch.dqe.types.DqeTypeCoercion;
import org.opensearch.dqe.types.DqeTypes;

/**
 * Walks expression trees, resolves column references, and assigns/verifies types. Reports type
 * errors with column name, expected type, and actual type.
 *
 * <p>Phase 1 supported expressions: arithmetic (+,-,*,/,%,unary-), comparison (=,!=,<,>,<=,>=),
 * boolean (AND/OR/NOT), CAST/TRY_CAST, IS NULL/IS NOT NULL, BETWEEN, IN, LIKE, CASE
 * (searched+simple), COALESCE, NULLIF, literals, column references.
 */
public class ExpressionTypeChecker {

  private final ScopeResolver scopeResolver;

  public ExpressionTypeChecker() {
    this.scopeResolver = new ScopeResolver();
  }

  /**
   * Type-check an expression tree within the given scope.
   *
   * @param expression the Trino expression AST node
   * @param scope the current scope for name resolution
   * @return the expression annotated with resolved type
   * @throws DqeTypeMismatchException on type errors
   * @throws DqeUnsupportedOperationException on Phase 2+ expressions
   */
  public TypedExpression check(Expression expression, Scope scope) {
    if (expression instanceof LongLiteral) {
      return new TypedExpression(expression, DqeTypes.BIGINT);
    }

    if (expression instanceof DoubleLiteral) {
      return new TypedExpression(expression, DqeTypes.DOUBLE);
    }

    if (expression instanceof StringLiteral) {
      return new TypedExpression(expression, DqeTypes.VARCHAR);
    }

    if (expression instanceof BooleanLiteral) {
      return new TypedExpression(expression, DqeTypes.BOOLEAN);
    }

    if (expression instanceof NullLiteral) {
      // NULL has an unknown type; we represent it as VARCHAR (standard SQL convention)
      return new TypedExpression(expression, DqeTypes.VARCHAR);
    }

    if (expression instanceof Identifier id) {
      return checkIdentifier(id, scope);
    }

    if (expression instanceof DereferenceExpression deref) {
      return checkDereference(deref, scope);
    }

    if (expression instanceof ArithmeticBinaryExpression arith) {
      return checkArithmeticBinary(arith, scope);
    }

    if (expression instanceof ArithmeticUnaryExpression unary) {
      return checkArithmeticUnary(unary, scope);
    }

    if (expression instanceof ComparisonExpression cmp) {
      return checkComparison(cmp, scope);
    }

    if (expression instanceof LogicalExpression logical) {
      return checkLogical(logical, scope);
    }

    if (expression instanceof NotExpression not) {
      return checkNot(not, scope);
    }

    if (expression instanceof IsNullPredicate isNull) {
      TypedExpression value = check(isNull.getValue(), scope);
      return new TypedExpression(expression, DqeTypes.BOOLEAN);
    }

    if (expression instanceof IsNotNullPredicate isNotNull) {
      TypedExpression value = check(isNotNull.getValue(), scope);
      return new TypedExpression(expression, DqeTypes.BOOLEAN);
    }

    if (expression instanceof BetweenPredicate between) {
      return checkBetween(between, scope);
    }

    if (expression instanceof InPredicate in) {
      return checkIn(in, scope);
    }

    if (expression instanceof LikePredicate like) {
      return checkLike(like, scope);
    }

    if (expression instanceof Cast cast) {
      return checkCast(cast, scope);
    }

    if (expression instanceof SearchedCaseExpression searchedCase) {
      return checkSearchedCase(searchedCase, scope);
    }

    if (expression instanceof SimpleCaseExpression simpleCase) {
      return checkSimpleCase(simpleCase, scope);
    }

    if (expression instanceof CoalesceExpression coalesce) {
      return checkCoalesce(coalesce, scope);
    }

    if (expression instanceof NullIfExpression nullIf) {
      return checkNullIf(nullIf, scope);
    }

    if (expression instanceof FunctionCall fc) {
      throw new DqeUnsupportedOperationException(
          "function call '" + fc.getName() + "'",
          "named function calls are not supported in Phase 1");
    }

    throw new DqeUnsupportedOperationException(
        expression.getClass().getSimpleName(),
        "expression type not supported in Phase 1");
  }

  private TypedExpression checkIdentifier(Identifier id, Scope scope) {
    ResolvedField resolved = scopeResolver.resolveColumn(id.getValue(), scope);
    return new TypedExpression(id, resolved.getType());
  }

  private TypedExpression checkDereference(DereferenceExpression deref, Scope scope) {
    // Handles qualified references like "t.column" or "table.column"
    if (deref.getBase() instanceof Identifier baseId && deref.getField().isPresent()) {
      String qualifier = baseId.getValue();
      String columnName = deref.getField().get().getValue();
      ResolvedField resolved = scopeResolver.resolveQualifiedColumn(qualifier, columnName, scope);
      return new TypedExpression(deref, resolved.getType());
    }
    // Nested field access like "obj.nested_field"
    String fullPath = flattenDereference(deref);
    ResolvedField resolved = scopeResolver.resolveColumn(fullPath, scope);
    return new TypedExpression(deref, resolved.getType());
  }

  private String flattenDereference(DereferenceExpression deref) {
    if (deref.getBase() instanceof Identifier base && deref.getField().isPresent()) {
      return base.getValue() + "." + deref.getField().get().getValue();
    }
    if (deref.getBase() instanceof DereferenceExpression inner && deref.getField().isPresent()) {
      return flattenDereference(inner) + "." + deref.getField().get().getValue();
    }
    throw new DqeAnalysisException("Cannot resolve nested field reference: " + deref);
  }

  private TypedExpression checkArithmeticBinary(
      ArithmeticBinaryExpression arith, Scope scope) {
    TypedExpression left = check(arith.getLeft(), scope);
    TypedExpression right = check(arith.getRight(), scope);

    if (!left.getType().isNumeric()) {
      throw new DqeTypeMismatchException(
          arith.getLeft().toString(), "numeric", left.getType().getDisplayName());
    }
    if (!right.getType().isNumeric()) {
      throw new DqeTypeMismatchException(
          arith.getRight().toString(), "numeric", right.getType().getDisplayName());
    }

    Optional<DqeType> common = DqeTypeCoercion.getCommonSuperType(left.getType(), right.getType());
    DqeType resultType = common.orElseThrow(
        () -> new DqeTypeMismatchException(
            arith.toString(),
            left.getType().getDisplayName(),
            right.getType().getDisplayName()));

    return new TypedExpression(arith, resultType);
  }

  private TypedExpression checkArithmeticUnary(
      ArithmeticUnaryExpression unary, Scope scope) {
    TypedExpression value = check(unary.getValue(), scope);
    if (!value.getType().isNumeric()) {
      throw new DqeTypeMismatchException(
          unary.getValue().toString(), "numeric", value.getType().getDisplayName());
    }
    return new TypedExpression(unary, value.getType());
  }

  private TypedExpression checkComparison(ComparisonExpression cmp, Scope scope) {
    TypedExpression left = check(cmp.getLeft(), scope);
    TypedExpression right = check(cmp.getRight(), scope);

    // Both sides must be comparable
    if (!left.getType().isComparable()) {
      throw new DqeTypeMismatchException(
          cmp.getLeft().toString(), "comparable type", left.getType().getDisplayName());
    }
    if (!right.getType().isComparable()) {
      throw new DqeTypeMismatchException(
          cmp.getRight().toString(), "comparable type", right.getType().getDisplayName());
    }

    // Must have a common supertype
    if (!left.getType().equals(right.getType())) {
      Optional<DqeType> common =
          DqeTypeCoercion.getCommonSuperType(left.getType(), right.getType());
      if (common.isEmpty()) {
        throw new DqeTypeMismatchException(
            cmp.toString(),
            left.getType().getDisplayName(),
            right.getType().getDisplayName());
      }
    }

    return new TypedExpression(cmp, DqeTypes.BOOLEAN);
  }

  private TypedExpression checkLogical(LogicalExpression logical, Scope scope) {
    for (Expression term : logical.getTerms()) {
      TypedExpression checked = check(term, scope);
      if (!checked.getType().equals(DqeTypes.BOOLEAN)) {
        throw new DqeTypeMismatchException(
            term.toString(), "BOOLEAN", checked.getType().getDisplayName());
      }
    }
    return new TypedExpression(logical, DqeTypes.BOOLEAN);
  }

  private TypedExpression checkNot(NotExpression not, Scope scope) {
    TypedExpression value = check(not.getValue(), scope);
    if (!value.getType().equals(DqeTypes.BOOLEAN)) {
      throw new DqeTypeMismatchException(
          not.getValue().toString(), "BOOLEAN", value.getType().getDisplayName());
    }
    return new TypedExpression(not, DqeTypes.BOOLEAN);
  }

  private TypedExpression checkBetween(BetweenPredicate between, Scope scope) {
    TypedExpression value = check(between.getValue(), scope);
    TypedExpression min = check(between.getMin(), scope);
    TypedExpression max = check(between.getMax(), scope);

    // All three must be compatible
    Optional<DqeType> commonMinMax =
        DqeTypeCoercion.getCommonSuperType(min.getType(), max.getType());
    if (commonMinMax.isEmpty()) {
      throw new DqeTypeMismatchException(
          "BETWEEN bounds", min.getType().getDisplayName(), max.getType().getDisplayName());
    }
    Optional<DqeType> commonAll =
        DqeTypeCoercion.getCommonSuperType(value.getType(), commonMinMax.get());
    if (commonAll.isEmpty()) {
      throw new DqeTypeMismatchException(
          between.toString(),
          value.getType().getDisplayName(),
          commonMinMax.get().getDisplayName());
    }

    return new TypedExpression(between, DqeTypes.BOOLEAN);
  }

  private TypedExpression checkIn(InPredicate in, Scope scope) {
    TypedExpression value = check(in.getValue(), scope);

    if (in.getValueList() instanceof InListExpression inList) {
      for (Expression item : inList.getValues()) {
        TypedExpression checkedItem = check(item, scope);
        if (!value.getType().equals(checkedItem.getType())) {
          Optional<DqeType> common =
              DqeTypeCoercion.getCommonSuperType(value.getType(), checkedItem.getType());
          if (common.isEmpty()) {
            throw new DqeTypeMismatchException(
                "IN list item",
                value.getType().getDisplayName(),
                checkedItem.getType().getDisplayName());
          }
        }
      }
    } else {
      throw new DqeUnsupportedOperationException("subquery in IN clause");
    }

    return new TypedExpression(in, DqeTypes.BOOLEAN);
  }

  private TypedExpression checkLike(LikePredicate like, Scope scope) {
    TypedExpression value = check(like.getValue(), scope);
    TypedExpression pattern = check(like.getPattern(), scope);

    if (!value.getType().equals(DqeTypes.VARCHAR)) {
      throw new DqeTypeMismatchException(
          like.getValue().toString(), "VARCHAR", value.getType().getDisplayName());
    }
    if (!pattern.getType().equals(DqeTypes.VARCHAR)) {
      throw new DqeTypeMismatchException(
          like.getPattern().toString(), "VARCHAR", pattern.getType().getDisplayName());
    }

    return new TypedExpression(like, DqeTypes.BOOLEAN);
  }

  private TypedExpression checkCast(Cast cast, Scope scope) {
    TypedExpression value = check(cast.getExpression(), scope);
    DqeType targetType = resolveDataType(cast.getType().toString());

    if (!DqeTypeCoercion.canCast(value.getType(), targetType)) {
      throw new DqeTypeMismatchException(
          "CAST(" + cast.getExpression() + " AS " + cast.getType() + ")",
          targetType.getDisplayName(),
          value.getType().getDisplayName());
    }

    return new TypedExpression(cast, targetType);
  }

  private TypedExpression checkSearchedCase(SearchedCaseExpression searchedCase, Scope scope) {
    DqeType resultType = null;

    for (WhenClause when : searchedCase.getWhenClauses()) {
      TypedExpression condition = check(when.getOperand(), scope);
      if (!condition.getType().equals(DqeTypes.BOOLEAN)) {
        throw new DqeTypeMismatchException(
            "CASE WHEN condition", "BOOLEAN", condition.getType().getDisplayName());
      }

      TypedExpression result = check(when.getResult(), scope);
      if (resultType == null) {
        resultType = result.getType();
      } else {
        resultType = findCommonType(resultType, result.getType(), "CASE branch");
      }
    }

    if (searchedCase.getDefaultValue().isPresent()) {
      TypedExpression defaultVal = check(searchedCase.getDefaultValue().get(), scope);
      resultType = findCommonType(resultType, defaultVal.getType(), "CASE ELSE");
    }

    return new TypedExpression(searchedCase, resultType != null ? resultType : DqeTypes.VARCHAR);
  }

  private TypedExpression checkSimpleCase(SimpleCaseExpression simpleCase, Scope scope) {
    TypedExpression operand = check(simpleCase.getOperand(), scope);
    DqeType resultType = null;

    for (WhenClause when : simpleCase.getWhenClauses()) {
      TypedExpression whenValue = check(when.getOperand(), scope);
      // Validate the WHEN value is compatible with the operand
      if (!operand.getType().equals(whenValue.getType())) {
        Optional<DqeType> common =
            DqeTypeCoercion.getCommonSuperType(operand.getType(), whenValue.getType());
        if (common.isEmpty()) {
          throw new DqeTypeMismatchException(
              "CASE WHEN value",
              operand.getType().getDisplayName(),
              whenValue.getType().getDisplayName());
        }
      }

      TypedExpression result = check(when.getResult(), scope);
      if (resultType == null) {
        resultType = result.getType();
      } else {
        resultType = findCommonType(resultType, result.getType(), "CASE branch");
      }
    }

    if (simpleCase.getDefaultValue().isPresent()) {
      TypedExpression defaultVal = check(simpleCase.getDefaultValue().get(), scope);
      resultType = findCommonType(resultType, defaultVal.getType(), "CASE ELSE");
    }

    return new TypedExpression(simpleCase, resultType != null ? resultType : DqeTypes.VARCHAR);
  }

  private TypedExpression checkCoalesce(CoalesceExpression coalesce, Scope scope) {
    DqeType resultType = null;
    for (Expression operand : coalesce.getOperands()) {
      TypedExpression checked = check(operand, scope);
      if (resultType == null) {
        resultType = checked.getType();
      } else {
        resultType = findCommonType(resultType, checked.getType(), "COALESCE");
      }
    }
    return new TypedExpression(coalesce, resultType != null ? resultType : DqeTypes.VARCHAR);
  }

  private TypedExpression checkNullIf(NullIfExpression nullIf, Scope scope) {
    TypedExpression first = check(nullIf.getFirst(), scope);
    TypedExpression second = check(nullIf.getSecond(), scope);

    // Both arguments must have compatible types
    if (!first.getType().equals(second.getType())) {
      Optional<DqeType> common =
          DqeTypeCoercion.getCommonSuperType(first.getType(), second.getType());
      if (common.isEmpty()) {
        throw new DqeTypeMismatchException(
            "NULLIF",
            first.getType().getDisplayName(),
            second.getType().getDisplayName());
      }
    }

    // Result type is first argument's type
    return new TypedExpression(nullIf, first.getType());
  }

  private DqeType findCommonType(DqeType existing, DqeType incoming, String context) {
    if (existing.equals(incoming)) {
      return existing;
    }
    // NULL literal (VARCHAR) is compatible with anything
    if (incoming.equals(DqeTypes.VARCHAR) || existing.equals(DqeTypes.VARCHAR)) {
      // If one is VARCHAR (from null literal), prefer the other concrete type
      // But if both are different non-null types, we need coercion
    }
    Optional<DqeType> common = DqeTypeCoercion.getCommonSuperType(existing, incoming);
    return common.orElseThrow(
        () -> new DqeTypeMismatchException(
            context, existing.getDisplayName(), incoming.getDisplayName()));
  }

  /** Resolves a SQL type name string to a DqeType. */
  static DqeType resolveDataType(String typeName) {
    String upper = typeName.toUpperCase().trim();
    return switch (upper) {
      case "VARCHAR", "STRING", "TEXT", "CHAR" -> DqeTypes.VARCHAR;
      case "BIGINT", "LONG" -> DqeTypes.BIGINT;
      case "INTEGER", "INT" -> DqeTypes.INTEGER;
      case "SMALLINT", "SHORT" -> DqeTypes.SMALLINT;
      case "TINYINT", "BYTE" -> DqeTypes.TINYINT;
      case "DOUBLE", "DOUBLE PRECISION" -> DqeTypes.DOUBLE;
      case "REAL", "FLOAT" -> DqeTypes.REAL;
      case "BOOLEAN", "BOOL" -> DqeTypes.BOOLEAN;
      case "VARBINARY" -> DqeTypes.VARBINARY;
      case "TIMESTAMP", "TIMESTAMP(3)" -> DqeTypes.TIMESTAMP_MILLIS;
      case "TIMESTAMP(9)" -> DqeTypes.TIMESTAMP_NANOS;
      default -> {
        // Handle DECIMAL(p,s) and TIMESTAMP(n)
        if (upper.startsWith("DECIMAL")) {
          yield parseDecimalType(upper);
        }
        if (upper.startsWith("TIMESTAMP")) {
          yield parseTimestampType(upper);
        }
        throw new DqeAnalysisException("Unknown type: " + typeName);
      }
    };
  }

  private static DqeType parseDecimalType(String typeName) {
    // DECIMAL(p,s) or DECIMAL(p, s)
    int lparen = typeName.indexOf('(');
    int rparen = typeName.indexOf(')');
    if (lparen == -1 || rparen == -1) {
      return DqeTypes.decimal(38, 0);
    }
    String params = typeName.substring(lparen + 1, rparen).trim();
    String[] parts = params.split(",");
    int precision = Integer.parseInt(parts[0].trim());
    int scale = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
    return DqeTypes.decimal(precision, scale);
  }

  private static DqeType parseTimestampType(String typeName) {
    int lparen = typeName.indexOf('(');
    int rparen = typeName.indexOf(')');
    if (lparen == -1 || rparen == -1) {
      return DqeTypes.TIMESTAMP_MILLIS;
    }
    int precision = Integer.parseInt(typeName.substring(lparen + 1, rparen).trim());
    return DqeTypes.timestamp(precision);
  }
}
