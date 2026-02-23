/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.analyzer.predicate.PushdownPredicate;
import org.opensearch.dqe.types.DqeTypes;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;

class PredicateToQueryDslConverterTests {

  private PredicateToQueryDslConverter converter;

  @BeforeEach
  void setUp() {
    converter = new PredicateToQueryDslConverter();
  }

  @Test
  @DisplayName("null predicate returns null")
  void nullPredicateReturnsNull() {
    assertNull(converter.convert(null));
  }

  @Test
  @DisplayName("TERM_EQUALITY converts to term query")
  void termEqualityConvertsToTermQuery() {
    PushdownPredicate pred =
        PushdownPredicate.termEquality("name.keyword", DqeTypes.VARCHAR, "Alice");
    QueryBuilder result = converter.convert(pred);

    assertTrue(result instanceof TermQueryBuilder);
    TermQueryBuilder term = (TermQueryBuilder) result;
    assertEquals("name.keyword", term.fieldName());
    assertEquals("Alice", term.value());
  }

  @Test
  @DisplayName("RANGE with lower and upper bounds converts to range query")
  void rangeConvertsToRangeQuery() {
    PushdownPredicate pred = PushdownPredicate.range("age", DqeTypes.BIGINT, 18, false, 65, true);
    QueryBuilder result = converter.convert(pred);

    assertTrue(result instanceof RangeQueryBuilder);
    RangeQueryBuilder range = (RangeQueryBuilder) result;
    assertEquals("age", range.fieldName());
  }

  @Test
  @DisplayName("BETWEEN converts to inclusive range query")
  void betweenConvertsToInclusiveRange() {
    PushdownPredicate pred = PushdownPredicate.between("price", DqeTypes.DOUBLE, 10.0, 100.0);
    QueryBuilder result = converter.convert(pred);

    assertTrue(result instanceof RangeQueryBuilder);
    RangeQueryBuilder range = (RangeQueryBuilder) result;
    assertEquals("price", range.fieldName());
  }

  @Test
  @DisplayName("IN_SET converts to terms query")
  void inSetConvertsToTermsQuery() {
    PushdownPredicate pred =
        PushdownPredicate.inSet("status", DqeTypes.VARCHAR, List.of("active", "pending"));
    QueryBuilder result = converter.convert(pred);

    assertTrue(result instanceof TermsQueryBuilder);
    TermsQueryBuilder terms = (TermsQueryBuilder) result;
    assertEquals("status", terms.fieldName());
  }

  @Test
  @DisplayName("EXISTS converts to exists query")
  void existsConvertsToExistsQuery() {
    PushdownPredicate pred = PushdownPredicate.exists("email");
    QueryBuilder result = converter.convert(pred);

    assertTrue(result instanceof ExistsQueryBuilder);
    assertEquals("email", ((ExistsQueryBuilder) result).fieldName());
  }

  @Test
  @DisplayName("NOT_EXISTS converts to bool must_not exists")
  void notExistsConvertsToBoolMustNotExists() {
    PushdownPredicate pred = PushdownPredicate.notExists("email");
    QueryBuilder result = converter.convert(pred);

    assertTrue(result instanceof BoolQueryBuilder);
    BoolQueryBuilder bool = (BoolQueryBuilder) result;
    assertEquals(1, bool.mustNot().size());
    assertTrue(bool.mustNot().get(0) instanceof ExistsQueryBuilder);
  }

  @Test
  @DisplayName("LIKE_PATTERN converts to wildcard query")
  void likePatternConvertsToWildcardQuery() {
    PushdownPredicate pred = PushdownPredicate.likePattern("name.keyword", "Jo%");
    QueryBuilder result = converter.convert(pred);

    assertTrue(result instanceof WildcardQueryBuilder);
    WildcardQueryBuilder wildcard = (WildcardQueryBuilder) result;
    assertEquals("name.keyword", wildcard.fieldName());
    assertEquals("Jo*", wildcard.value());
  }

  @Test
  @DisplayName("BOOL_AND converts to bool must")
  void boolAndConvertsToBoolMust() {
    PushdownPredicate child1 = PushdownPredicate.termEquality("a", DqeTypes.VARCHAR, "1");
    PushdownPredicate child2 = PushdownPredicate.termEquality("b", DqeTypes.VARCHAR, "2");
    PushdownPredicate pred = PushdownPredicate.and(List.of(child1, child2));

    QueryBuilder result = converter.convert(pred);
    assertTrue(result instanceof BoolQueryBuilder);
    BoolQueryBuilder bool = (BoolQueryBuilder) result;
    assertEquals(2, bool.must().size());
  }

  @Test
  @DisplayName("BOOL_OR converts to bool should with minimumShouldMatch 1")
  void boolOrConvertsToBoolShould() {
    PushdownPredicate child1 = PushdownPredicate.termEquality("a", DqeTypes.VARCHAR, "1");
    PushdownPredicate child2 = PushdownPredicate.termEquality("b", DqeTypes.VARCHAR, "2");
    PushdownPredicate pred = PushdownPredicate.or(List.of(child1, child2));

    QueryBuilder result = converter.convert(pred);
    assertTrue(result instanceof BoolQueryBuilder);
    BoolQueryBuilder bool = (BoolQueryBuilder) result;
    assertEquals(2, bool.should().size());
    assertEquals("1", bool.minimumShouldMatch());
  }

  @Test
  @DisplayName("BOOL_NOT converts to bool must_not")
  void boolNotConvertsToBoolMustNot() {
    PushdownPredicate child = PushdownPredicate.termEquality("a", DqeTypes.VARCHAR, "1");
    PushdownPredicate pred = PushdownPredicate.not(child);

    QueryBuilder result = converter.convert(pred);
    assertTrue(result instanceof BoolQueryBuilder);
    BoolQueryBuilder bool = (BoolQueryBuilder) result;
    assertEquals(1, bool.mustNot().size());
  }

  @Test
  @DisplayName("convertAll returns null for empty list")
  void convertAllEmptyList() {
    assertNull(converter.convertAll(List.of()));
  }

  @Test
  @DisplayName("convertAll returns null for null list")
  void convertAllNullList() {
    assertNull(converter.convertAll(null));
  }

  @Test
  @DisplayName("convertAll with single predicate returns direct query")
  void convertAllSinglePredicate() {
    PushdownPredicate pred = PushdownPredicate.termEquality("a", DqeTypes.VARCHAR, "1");
    QueryBuilder result = converter.convertAll(List.of(pred));

    assertTrue(result instanceof TermQueryBuilder);
  }

  @Test
  @DisplayName("convertAll with multiple predicates wraps in bool must")
  void convertAllMultiplePredicates() {
    PushdownPredicate pred1 = PushdownPredicate.termEquality("a", DqeTypes.VARCHAR, "1");
    PushdownPredicate pred2 = PushdownPredicate.exists("b");
    QueryBuilder result = converter.convertAll(List.of(pred1, pred2));

    assertTrue(result instanceof BoolQueryBuilder);
    BoolQueryBuilder bool = (BoolQueryBuilder) result;
    assertEquals(2, bool.must().size());
  }

  @Test
  @DisplayName("convertLikeToWildcard: % -> * and _ -> ?")
  void convertLikeToWildcard() {
    assertEquals("Jo*", PredicateToQueryDslConverter.convertLikeToWildcard("Jo%"));
    assertEquals("?o?", PredicateToQueryDslConverter.convertLikeToWildcard("_o_"));
    assertEquals("*a?b*", PredicateToQueryDslConverter.convertLikeToWildcard("%a_b%"));
  }

  @Test
  @DisplayName("convertLikeToWildcard handles escaped characters")
  void convertLikeToWildcardEscaped() {
    assertEquals("a%b", PredicateToQueryDslConverter.convertLikeToWildcard("a\\%b"));
    assertEquals("a_b", PredicateToQueryDslConverter.convertLikeToWildcard("a\\_b"));
  }

  @Test
  @DisplayName("nested boolean predicates convert correctly")
  void nestedBooleanPredicates() {
    PushdownPredicate eq1 = PushdownPredicate.termEquality("a", DqeTypes.VARCHAR, "1");
    PushdownPredicate eq2 = PushdownPredicate.termEquality("b", DqeTypes.VARCHAR, "2");
    PushdownPredicate orPred = PushdownPredicate.or(List.of(eq1, eq2));
    PushdownPredicate notPred = PushdownPredicate.not(orPred);

    QueryBuilder result = converter.convert(notPred);
    assertTrue(result instanceof BoolQueryBuilder);
    BoolQueryBuilder outer = (BoolQueryBuilder) result;
    assertEquals(1, outer.mustNot().size());
    assertTrue(outer.mustNot().get(0) instanceof BoolQueryBuilder);
  }
}
