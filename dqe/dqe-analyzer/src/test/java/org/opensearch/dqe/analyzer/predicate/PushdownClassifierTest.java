/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.predicate;

import static org.junit.jupiter.api.Assertions.*;

import io.trino.sql.tree.*;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.analyzer.scope.Scope;
import org.opensearch.dqe.analyzer.type.TypedExpression;
import org.opensearch.dqe.metadata.DqeColumnHandle;
import org.opensearch.dqe.metadata.DqeTableHandle;
import org.opensearch.dqe.types.DqeTypes;

@DisplayName("PushdownClassifier")
class PushdownClassifierTest {

  private PushdownClassifier classifier;
  private Scope scope;

  @BeforeEach
  void setUp() {
    classifier = new PushdownClassifier();
    DqeTableHandle table =
        new DqeTableHandle("test_table", null, List.of("test_table"), 1L, null);
    DqeColumnHandle nameCol =
        new DqeColumnHandle("name", "name", DqeTypes.VARCHAR, true, "name.keyword", false);
    DqeColumnHandle ageCol =
        new DqeColumnHandle("age", "age", DqeTypes.INTEGER, true, null, false);
    scope = new Scope(table, List.of(nameCol, ageCol), Optional.empty());
  }

  @Test
  @DisplayName("Equality: column = literal classified as TERM_EQUALITY")
  void termEquality() {
    ComparisonExpression expr =
        new ComparisonExpression(
            ComparisonExpression.Operator.EQUAL,
            new Identifier("name"),
            new StringLiteral("Alice"));
    TypedExpression typed = new TypedExpression(expr, DqeTypes.BOOLEAN);
    Optional<PushdownPredicate> result = classifier.classify(typed, scope);

    assertTrue(result.isPresent());
    assertEquals(PushdownPredicate.PredicateType.TERM_EQUALITY, result.get().getType());
    assertEquals("name.keyword", result.get().getFieldName());
    assertEquals("Alice", result.get().getValue());
  }

  @Test
  @DisplayName("Reversed: literal = column classified as TERM_EQUALITY")
  void reversedEquality() {
    ComparisonExpression expr =
        new ComparisonExpression(
            ComparisonExpression.Operator.EQUAL,
            new StringLiteral("Alice"),
            new Identifier("name"));
    TypedExpression typed = new TypedExpression(expr, DqeTypes.BOOLEAN);
    Optional<PushdownPredicate> result = classifier.classify(typed, scope);

    assertTrue(result.isPresent());
    assertEquals(PushdownPredicate.PredicateType.TERM_EQUALITY, result.get().getType());
  }

  @Test
  @DisplayName("Not-equal: column != literal classified as BOOL_NOT(TERM_EQUALITY)")
  void notEqual() {
    ComparisonExpression expr =
        new ComparisonExpression(
            ComparisonExpression.Operator.NOT_EQUAL,
            new Identifier("age"),
            new LongLiteral("25"));
    TypedExpression typed = new TypedExpression(expr, DqeTypes.BOOLEAN);
    Optional<PushdownPredicate> result = classifier.classify(typed, scope);

    assertTrue(result.isPresent());
    assertEquals(PushdownPredicate.PredicateType.BOOL_NOT, result.get().getType());
    assertEquals(1, result.get().getChildren().size());
    assertEquals(
        PushdownPredicate.PredicateType.TERM_EQUALITY,
        result.get().getChildren().get(0).getType());
  }

  @Test
  @DisplayName("Less-than: column < literal classified as RANGE")
  void lessThan() {
    ComparisonExpression expr =
        new ComparisonExpression(
            ComparisonExpression.Operator.LESS_THAN,
            new Identifier("age"),
            new LongLiteral("30"));
    TypedExpression typed = new TypedExpression(expr, DqeTypes.BOOLEAN);
    Optional<PushdownPredicate> result = classifier.classify(typed, scope);

    assertTrue(result.isPresent());
    assertEquals(PushdownPredicate.PredicateType.RANGE, result.get().getType());
    PushdownPredicate.RangeBounds bounds =
        (PushdownPredicate.RangeBounds) result.get().getValue();
    assertNull(bounds.lowerBound());
    assertFalse(bounds.upperInclusive());
  }

  @Test
  @DisplayName("Greater-than-or-equal: column >= literal classified as RANGE")
  void greaterThanOrEqual() {
    ComparisonExpression expr =
        new ComparisonExpression(
            ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
            new Identifier("age"),
            new LongLiteral("18"));
    TypedExpression typed = new TypedExpression(expr, DqeTypes.BOOLEAN);
    Optional<PushdownPredicate> result = classifier.classify(typed, scope);

    assertTrue(result.isPresent());
    assertEquals(PushdownPredicate.PredicateType.RANGE, result.get().getType());
    PushdownPredicate.RangeBounds bounds =
        (PushdownPredicate.RangeBounds) result.get().getValue();
    assertTrue(bounds.lowerInclusive());
  }

  @Test
  @DisplayName("Column-to-column comparison cannot be pushed down")
  void columnToColumnNotPushable() {
    ComparisonExpression expr =
        new ComparisonExpression(
            ComparisonExpression.Operator.EQUAL,
            new Identifier("name"),
            new Identifier("age"));
    TypedExpression typed = new TypedExpression(expr, DqeTypes.BOOLEAN);
    Optional<PushdownPredicate> result = classifier.classify(typed, scope);

    assertTrue(result.isEmpty());
  }

  @Test
  @DisplayName("isColumnLiteralComparison returns true for column = literal")
  void isColumnLiteralComparison() {
    ComparisonExpression expr =
        new ComparisonExpression(
            ComparisonExpression.Operator.EQUAL,
            new Identifier("name"),
            new StringLiteral("Alice"));
    TypedExpression typed = new TypedExpression(expr, DqeTypes.BOOLEAN);
    assertTrue(classifier.isColumnLiteralComparison(typed));
  }

  @Test
  @DisplayName("isColumnLiteralComparison returns false for column = column")
  void isNotColumnLiteralComparison() {
    ComparisonExpression expr =
        new ComparisonExpression(
            ComparisonExpression.Operator.EQUAL,
            new Identifier("name"),
            new Identifier("age"));
    TypedExpression typed = new TypedExpression(expr, DqeTypes.BOOLEAN);
    assertFalse(classifier.isColumnLiteralComparison(typed));
  }

  @Test
  @DisplayName("NOT(expression) pushdown")
  void notPushdown() {
    NotExpression expr =
        new NotExpression(
            new ComparisonExpression(
                ComparisonExpression.Operator.EQUAL,
                new Identifier("age"),
                new LongLiteral("25")));
    TypedExpression typed = new TypedExpression(expr, DqeTypes.BOOLEAN);
    Optional<PushdownPredicate> result = classifier.classify(typed, scope);

    assertTrue(result.isPresent());
    assertEquals(PushdownPredicate.PredicateType.BOOL_NOT, result.get().getType());
  }
}
