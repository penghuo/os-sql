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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.analyzer.scope.Scope;
import org.opensearch.dqe.analyzer.type.TypedExpression;
import org.opensearch.dqe.metadata.DqeColumnHandle;
import org.opensearch.dqe.metadata.DqeTableHandle;
import org.opensearch.dqe.types.DqeTypes;

@DisplayName("PredicateAnalyzer")
class PredicateAnalyzerTest {

  private PredicateAnalyzer analyzer;
  private Scope scope;

  @BeforeEach
  void setUp() {
    analyzer = new PredicateAnalyzer();
    DqeTableHandle table =
        new DqeTableHandle("test_table", null, List.of("test_table"), 1L, null);
    DqeColumnHandle nameCol =
        new DqeColumnHandle("name", "name", DqeTypes.VARCHAR, true, "name.keyword", false);
    DqeColumnHandle ageCol =
        new DqeColumnHandle("age", "age", DqeTypes.INTEGER, true, null, false);
    DqeColumnHandle activeCol =
        new DqeColumnHandle("active", "active", DqeTypes.BOOLEAN, true, null, false);
    scope = new Scope(table, List.of(nameCol, ageCol, activeCol), Optional.empty());
  }

  @Nested
  @DisplayName("Single predicate pushdown")
  class SinglePredicate {

    @Test
    @DisplayName("Equality pushdown: name = 'Alice'")
    void equalityPushdown() {
      ComparisonExpression expr =
          new ComparisonExpression(
              ComparisonExpression.Operator.EQUAL,
              new Identifier("name"),
              new StringLiteral("Alice"));
      TypedExpression typed = new TypedExpression(expr, DqeTypes.BOOLEAN);
      PredicateAnalysisResult result = analyzer.analyze(typed, scope);

      assertTrue(result.isFullyPushedDown());
      assertEquals(1, result.getPushdownPredicates().size());
      PushdownPredicate pred = result.getPushdownPredicates().get(0);
      assertEquals(PushdownPredicate.PredicateType.TERM_EQUALITY, pred.getType());
      // Uses keyword sub-field
      assertEquals("name.keyword", pred.getFieldName());
    }

    @Test
    @DisplayName("Range pushdown: age > 18")
    void rangePushdown() {
      ComparisonExpression expr =
          new ComparisonExpression(
              ComparisonExpression.Operator.GREATER_THAN,
              new Identifier("age"),
              new LongLiteral("18"));
      TypedExpression typed = new TypedExpression(expr, DqeTypes.BOOLEAN);
      PredicateAnalysisResult result = analyzer.analyze(typed, scope);

      assertTrue(result.isFullyPushedDown());
      PushdownPredicate pred = result.getPushdownPredicates().get(0);
      assertEquals(PushdownPredicate.PredicateType.RANGE, pred.getType());
      assertEquals("age", pred.getFieldName());
    }

    @Test
    @DisplayName("IS NULL pushdown")
    void isNullPushdown() {
      IsNullPredicate expr = new IsNullPredicate(new Identifier("name"));
      TypedExpression typed = new TypedExpression(expr, DqeTypes.BOOLEAN);
      PredicateAnalysisResult result = analyzer.analyze(typed, scope);

      assertTrue(result.isFullyPushedDown());
      PushdownPredicate pred = result.getPushdownPredicates().get(0);
      assertEquals(PushdownPredicate.PredicateType.NOT_EXISTS, pred.getType());
    }

    @Test
    @DisplayName("IS NOT NULL pushdown")
    void isNotNullPushdown() {
      IsNotNullPredicate expr = new IsNotNullPredicate(new Identifier("name"));
      TypedExpression typed = new TypedExpression(expr, DqeTypes.BOOLEAN);
      PredicateAnalysisResult result = analyzer.analyze(typed, scope);

      assertTrue(result.isFullyPushedDown());
      PushdownPredicate pred = result.getPushdownPredicates().get(0);
      assertEquals(PushdownPredicate.PredicateType.EXISTS, pred.getType());
    }

    @Test
    @DisplayName("BETWEEN pushdown")
    void betweenPushdown() {
      BetweenPredicate expr =
          new BetweenPredicate(
              new Identifier("age"), new LongLiteral("18"), new LongLiteral("65"));
      TypedExpression typed = new TypedExpression(expr, DqeTypes.BOOLEAN);
      PredicateAnalysisResult result = analyzer.analyze(typed, scope);

      assertTrue(result.isFullyPushedDown());
      PushdownPredicate pred = result.getPushdownPredicates().get(0);
      assertEquals(PushdownPredicate.PredicateType.BETWEEN, pred.getType());
    }

    @Test
    @DisplayName("IN list pushdown")
    void inListPushdown() {
      InPredicate expr =
          new InPredicate(
              new Identifier("age"),
              new InListExpression(
                  List.of(new LongLiteral("20"), new LongLiteral("30"))));
      TypedExpression typed = new TypedExpression(expr, DqeTypes.BOOLEAN);
      PredicateAnalysisResult result = analyzer.analyze(typed, scope);

      assertTrue(result.isFullyPushedDown());
      PushdownPredicate pred = result.getPushdownPredicates().get(0);
      assertEquals(PushdownPredicate.PredicateType.IN_SET, pred.getType());
    }

    @Test
    @DisplayName("LIKE pushdown")
    void likePushdown() {
      LikePredicate expr =
          new LikePredicate(new Identifier("name"), new StringLiteral("A%"), Optional.empty());
      TypedExpression typed = new TypedExpression(expr, DqeTypes.BOOLEAN);
      PredicateAnalysisResult result = analyzer.analyze(typed, scope);

      assertTrue(result.isFullyPushedDown());
      PushdownPredicate pred = result.getPushdownPredicates().get(0);
      assertEquals(PushdownPredicate.PredicateType.LIKE_PATTERN, pred.getType());
    }
  }

  @Nested
  @DisplayName("Compound predicates")
  class CompoundPredicates {

    @Test
    @DisplayName("AND splits into separate pushdown predicates")
    void andSplitsPredicates() {
      LogicalExpression expr =
          new LogicalExpression(
              LogicalExpression.Operator.AND,
              List.of(
                  new ComparisonExpression(
                      ComparisonExpression.Operator.EQUAL,
                      new Identifier("name"),
                      new StringLiteral("Alice")),
                  new ComparisonExpression(
                      ComparisonExpression.Operator.GREATER_THAN,
                      new Identifier("age"),
                      new LongLiteral("18"))));
      TypedExpression typed = new TypedExpression(expr, DqeTypes.BOOLEAN);
      PredicateAnalysisResult result = analyzer.analyze(typed, scope);

      assertTrue(result.isFullyPushedDown());
      assertEquals(2, result.getPushdownPredicates().size());
    }

    @Test
    @DisplayName("OR becomes a single BOOL_OR pushdown")
    void orBecomesBoolOr() {
      LogicalExpression expr =
          new LogicalExpression(
              LogicalExpression.Operator.OR,
              List.of(
                  new ComparisonExpression(
                      ComparisonExpression.Operator.EQUAL,
                      new Identifier("name"),
                      new StringLiteral("Alice")),
                  new ComparisonExpression(
                      ComparisonExpression.Operator.EQUAL,
                      new Identifier("name"),
                      new StringLiteral("Bob"))));
      TypedExpression typed = new TypedExpression(expr, DqeTypes.BOOLEAN);
      PredicateAnalysisResult result = analyzer.analyze(typed, scope);

      assertTrue(result.isFullyPushedDown());
      assertEquals(1, result.getPushdownPredicates().size());
      assertEquals(
          PushdownPredicate.PredicateType.BOOL_OR,
          result.getPushdownPredicates().get(0).getType());
    }
  }

  @Nested
  @DisplayName("Residual predicates")
  class ResidualPredicates {

    @Test
    @DisplayName("Non-pushable expression becomes residual")
    void nonPushableBecomesResidual() {
      // Expression between two columns (not column vs literal)
      ComparisonExpression expr =
          new ComparisonExpression(
              ComparisonExpression.Operator.EQUAL,
              new Identifier("name"),
              new Identifier("name"));
      TypedExpression typed = new TypedExpression(expr, DqeTypes.BOOLEAN);
      PredicateAnalysisResult result = analyzer.analyze(typed, scope);

      assertFalse(result.isFullyPushedDown());
      assertEquals(0, result.getPushdownPredicates().size());
      assertEquals(1, result.getResidualPredicates().size());
    }
  }
}
