/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.type;

import static org.junit.jupiter.api.Assertions.*;

import io.trino.sql.tree.*;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.analyzer.DqeAnalysisException;
import org.opensearch.dqe.analyzer.scope.Scope;
import org.opensearch.dqe.metadata.DqeColumnHandle;
import org.opensearch.dqe.metadata.DqeTableHandle;
import org.opensearch.dqe.parser.DqeTypeMismatchException;
import org.opensearch.dqe.parser.DqeUnsupportedOperationException;
import org.opensearch.dqe.types.DqeTypes;

@DisplayName("ExpressionTypeChecker")
class ExpressionTypeCheckerTest {

  private ExpressionTypeChecker typeChecker;
  private Scope scope;

  @BeforeEach
  void setUp() {
    typeChecker = new ExpressionTypeChecker();
    DqeTableHandle table = new DqeTableHandle("test_table", null, List.of("test_table"), 1L, null);
    DqeColumnHandle nameCol =
        new DqeColumnHandle("name", "name", DqeTypes.VARCHAR, true, "name.keyword", false);
    DqeColumnHandle ageCol = new DqeColumnHandle("age", "age", DqeTypes.INTEGER, true, null, false);
    DqeColumnHandle salaryCol =
        new DqeColumnHandle("salary", "salary", DqeTypes.DOUBLE, true, null, false);
    DqeColumnHandle activeCol =
        new DqeColumnHandle("active", "active", DqeTypes.BOOLEAN, true, null, false);
    scope = new Scope(table, List.of(nameCol, ageCol, salaryCol, activeCol), Optional.empty());
  }

  @Nested
  @DisplayName("Literals")
  class Literals {

    @Test
    @DisplayName("Long literal returns BIGINT")
    void longLiteral() {
      TypedExpression result = typeChecker.check(new LongLiteral("42"), scope);
      assertEquals(DqeTypes.BIGINT, result.getType());
      assertTrue(result.isLiteral());
    }

    @Test
    @DisplayName("Double literal returns DOUBLE")
    void doubleLiteral() {
      TypedExpression result = typeChecker.check(new DoubleLiteral("3.14"), scope);
      assertEquals(DqeTypes.DOUBLE, result.getType());
    }

    @Test
    @DisplayName("String literal returns VARCHAR")
    void stringLiteral() {
      TypedExpression result = typeChecker.check(new StringLiteral("hello"), scope);
      assertEquals(DqeTypes.VARCHAR, result.getType());
    }

    @Test
    @DisplayName("Boolean literal returns BOOLEAN")
    void booleanLiteral() {
      TypedExpression result = typeChecker.check(new BooleanLiteral("true"), scope);
      assertEquals(DqeTypes.BOOLEAN, result.getType());
    }

    @Test
    @DisplayName("Null literal returns VARCHAR")
    void nullLiteral() {
      TypedExpression result = typeChecker.check(new NullLiteral(), scope);
      assertEquals(DqeTypes.VARCHAR, result.getType());
    }
  }

  @Nested
  @DisplayName("Column references")
  class ColumnReferences {

    @Test
    @DisplayName("Identifier resolves to column type")
    void identifierResolvesToColumn() {
      TypedExpression result = typeChecker.check(new Identifier("age"), scope);
      assertEquals(DqeTypes.INTEGER, result.getType());
      assertTrue(result.isColumnReference());
    }

    @Test
    @DisplayName("Unknown column throws")
    void unknownColumnThrows() {
      assertThrows(
          DqeAnalysisException.class, () -> typeChecker.check(new Identifier("unknown"), scope));
    }
  }

  @Nested
  @DisplayName("Arithmetic expressions")
  class ArithmeticExpressions {

    @Test
    @DisplayName("INTEGER + BIGINT -> BIGINT")
    void integerPlusBigint() {
      ArithmeticBinaryExpression expr =
          new ArithmeticBinaryExpression(
              ArithmeticBinaryExpression.Operator.ADD,
              new Identifier("age"),
              new LongLiteral("10"));
      TypedExpression result = typeChecker.check(expr, scope);
      // INTEGER + BIGINT -> BIGINT
      assertEquals(DqeTypes.BIGINT, result.getType());
    }

    @Test
    @DisplayName("DOUBLE * INTEGER -> DOUBLE")
    void doubleMulInteger() {
      ArithmeticBinaryExpression expr =
          new ArithmeticBinaryExpression(
              ArithmeticBinaryExpression.Operator.MULTIPLY,
              new Identifier("salary"),
              new Identifier("age"));
      TypedExpression result = typeChecker.check(expr, scope);
      assertEquals(DqeTypes.DOUBLE, result.getType());
    }

    @Test
    @DisplayName("Arithmetic on non-numeric throws")
    void arithmeticOnNonNumericThrows() {
      ArithmeticBinaryExpression expr =
          new ArithmeticBinaryExpression(
              ArithmeticBinaryExpression.Operator.ADD,
              new Identifier("name"),
              new LongLiteral("1"));
      assertThrows(DqeTypeMismatchException.class, () -> typeChecker.check(expr, scope));
    }

    @Test
    @DisplayName("Unary minus on numeric returns same type")
    void unaryMinus() {
      ArithmeticUnaryExpression expr =
          new ArithmeticUnaryExpression(
              ArithmeticUnaryExpression.Sign.MINUS, new Identifier("age"));
      TypedExpression result = typeChecker.check(expr, scope);
      assertEquals(DqeTypes.INTEGER, result.getType());
    }

    @Test
    @DisplayName("Unary minus on non-numeric throws")
    void unaryMinusOnNonNumeric() {
      ArithmeticUnaryExpression expr =
          new ArithmeticUnaryExpression(
              ArithmeticUnaryExpression.Sign.MINUS, new Identifier("name"));
      assertThrows(DqeTypeMismatchException.class, () -> typeChecker.check(expr, scope));
    }
  }

  @Nested
  @DisplayName("Comparison expressions")
  class ComparisonExpressions {

    @Test
    @DisplayName("INTEGER = BIGINT -> BOOLEAN")
    void integerEqualsBigint() {
      ComparisonExpression expr =
          new ComparisonExpression(
              ComparisonExpression.Operator.EQUAL, new Identifier("age"), new LongLiteral("25"));
      TypedExpression result = typeChecker.check(expr, scope);
      assertEquals(DqeTypes.BOOLEAN, result.getType());
    }

    @Test
    @DisplayName("VARCHAR comparison returns BOOLEAN")
    void varcharComparison() {
      ComparisonExpression expr =
          new ComparisonExpression(
              ComparisonExpression.Operator.EQUAL,
              new Identifier("name"),
              new StringLiteral("Alice"));
      TypedExpression result = typeChecker.check(expr, scope);
      assertEquals(DqeTypes.BOOLEAN, result.getType());
    }
  }

  @Nested
  @DisplayName("Logical expressions")
  class LogicalExpressions {

    @Test
    @DisplayName("AND of booleans returns BOOLEAN")
    void andExpression() {
      LogicalExpression expr =
          new LogicalExpression(
              LogicalExpression.Operator.AND,
              List.of(
                  new ComparisonExpression(
                      ComparisonExpression.Operator.EQUAL,
                      new Identifier("age"),
                      new LongLiteral("25")),
                  new ComparisonExpression(
                      ComparisonExpression.Operator.EQUAL,
                      new Identifier("name"),
                      new StringLiteral("Alice"))));
      TypedExpression result = typeChecker.check(expr, scope);
      assertEquals(DqeTypes.BOOLEAN, result.getType());
    }

    @Test
    @DisplayName("NOT of boolean returns BOOLEAN")
    void notExpression() {
      NotExpression expr = new NotExpression(new Identifier("active"));
      TypedExpression result = typeChecker.check(expr, scope);
      assertEquals(DqeTypes.BOOLEAN, result.getType());
    }

    @Test
    @DisplayName("NOT of non-boolean throws")
    void notOfNonBooleanThrows() {
      NotExpression expr = new NotExpression(new Identifier("age"));
      assertThrows(DqeTypeMismatchException.class, () -> typeChecker.check(expr, scope));
    }
  }

  @Nested
  @DisplayName("IS NULL / IS NOT NULL")
  class NullPredicates {

    @Test
    @DisplayName("IS NULL returns BOOLEAN")
    void isNull() {
      IsNullPredicate expr = new IsNullPredicate(new Identifier("name"));
      TypedExpression result = typeChecker.check(expr, scope);
      assertEquals(DqeTypes.BOOLEAN, result.getType());
    }

    @Test
    @DisplayName("IS NOT NULL returns BOOLEAN")
    void isNotNull() {
      IsNotNullPredicate expr = new IsNotNullPredicate(new Identifier("name"));
      TypedExpression result = typeChecker.check(expr, scope);
      assertEquals(DqeTypes.BOOLEAN, result.getType());
    }
  }

  @Nested
  @DisplayName("BETWEEN")
  class BetweenTests {

    @Test
    @DisplayName("BETWEEN with compatible types returns BOOLEAN")
    void betweenCompatible() {
      BetweenPredicate expr =
          new BetweenPredicate(new Identifier("age"), new LongLiteral("18"), new LongLiteral("65"));
      TypedExpression result = typeChecker.check(expr, scope);
      assertEquals(DqeTypes.BOOLEAN, result.getType());
    }
  }

  @Nested
  @DisplayName("IN predicate")
  class InTests {

    @Test
    @DisplayName("IN with compatible literals returns BOOLEAN")
    void inWithLiterals() {
      InPredicate expr =
          new InPredicate(
              new Identifier("age"),
              new InListExpression(
                  List.of(new LongLiteral("20"), new LongLiteral("30"), new LongLiteral("40"))));
      TypedExpression result = typeChecker.check(expr, scope);
      assertEquals(DqeTypes.BOOLEAN, result.getType());
    }
  }

  @Nested
  @DisplayName("LIKE")
  class LikeTests {

    @Test
    @DisplayName("LIKE on VARCHAR returns BOOLEAN")
    void likeOnVarchar() {
      LikePredicate expr =
          new LikePredicate(new Identifier("name"), new StringLiteral("A%"), Optional.empty());
      TypedExpression result = typeChecker.check(expr, scope);
      assertEquals(DqeTypes.BOOLEAN, result.getType());
    }

    @Test
    @DisplayName("LIKE on non-VARCHAR throws")
    void likeOnNonVarchar() {
      LikePredicate expr =
          new LikePredicate(new Identifier("age"), new StringLiteral("1%"), Optional.empty());
      assertThrows(DqeTypeMismatchException.class, () -> typeChecker.check(expr, scope));
    }
  }

  @Nested
  @DisplayName("CAST")
  class CastTests {

    @Test
    @DisplayName("CAST(age AS VARCHAR) resolves to VARCHAR")
    void castIntegerToVarchar() {
      Cast expr =
          new Cast(
              new Identifier("age"),
              new GenericDataType(Optional.empty(), new Identifier("VARCHAR"), List.of()));
      TypedExpression result = typeChecker.check(expr, scope);
      assertEquals(DqeTypes.VARCHAR, result.getType());
    }
  }

  @Nested
  @DisplayName("COALESCE")
  class CoalesceTests {

    @Test
    @DisplayName("COALESCE with same types returns that type")
    void coalesceSameTypes() {
      CoalesceExpression expr =
          new CoalesceExpression(List.of(new Identifier("name"), new StringLiteral("unknown")));
      TypedExpression result = typeChecker.check(expr, scope);
      assertEquals(DqeTypes.VARCHAR, result.getType());
    }
  }

  @Nested
  @DisplayName("NULLIF")
  class NullIfTests {

    @Test
    @DisplayName("NULLIF returns type of first argument")
    void nullifReturnType() {
      NullIfExpression expr =
          new NullIfExpression(new Identifier("name"), new StringLiteral("unknown"));
      TypedExpression result = typeChecker.check(expr, scope);
      assertEquals(DqeTypes.VARCHAR, result.getType());
    }
  }

  @Nested
  @DisplayName("Unsupported expressions")
  class UnsupportedExpressions {

    @Test
    @DisplayName("Function call throws")
    void functionCallThrows() {
      FunctionCall fc =
          new FunctionCall(
              Optional.empty(),
              QualifiedName.of("my_func"),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              false,
              Optional.empty(),
              Optional.empty(),
              List.of());
      assertThrows(DqeUnsupportedOperationException.class, () -> typeChecker.check(fc, scope));
    }
  }

  @Nested
  @DisplayName("resolveDataType")
  class ResolveDataType {

    @Test
    @DisplayName("VARCHAR resolved correctly")
    void varchar() {
      assertEquals(DqeTypes.VARCHAR, ExpressionTypeChecker.resolveDataType("VARCHAR"));
    }

    @Test
    @DisplayName("BIGINT resolved correctly")
    void bigint() {
      assertEquals(DqeTypes.BIGINT, ExpressionTypeChecker.resolveDataType("BIGINT"));
    }

    @Test
    @DisplayName("INTEGER resolved correctly")
    void integer() {
      assertEquals(DqeTypes.INTEGER, ExpressionTypeChecker.resolveDataType("INTEGER"));
    }

    @Test
    @DisplayName("DOUBLE resolved correctly")
    void doubleType() {
      assertEquals(DqeTypes.DOUBLE, ExpressionTypeChecker.resolveDataType("DOUBLE"));
    }

    @Test
    @DisplayName("BOOLEAN resolved correctly")
    void booleanType() {
      assertEquals(DqeTypes.BOOLEAN, ExpressionTypeChecker.resolveDataType("BOOLEAN"));
    }

    @Test
    @DisplayName("Case insensitive resolution")
    void caseInsensitive() {
      assertEquals(DqeTypes.VARCHAR, ExpressionTypeChecker.resolveDataType("varchar"));
    }

    @Test
    @DisplayName("Unknown type throws")
    void unknownTypeThrows() {
      assertThrows(
          DqeAnalysisException.class, () -> ExpressionTypeChecker.resolveDataType("UNKNOWN"));
    }
  }
}
