/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.predicate;

import java.util.List;
import java.util.Objects;
import org.opensearch.dqe.types.DqeType;

/**
 * Represents a single predicate classified for pushdown to OpenSearch Query DSL. The execution
 * layer's {@code PredicateToQueryDslConverter} consumes this. The fieldName is pre-resolved (e.g.,
 * "name.keyword" for text fields with keyword sub-fields).
 */
public class PushdownPredicate {

  /** Classification of pushdown predicate types that map to OpenSearch Query DSL constructs. */
  public enum PredicateType {
    /** column = literal -> term query */
    TERM_EQUALITY,
    /** column < / > / <= / >= literal -> range query */
    RANGE,
    /** column BETWEEN a AND b -> range query */
    BETWEEN,
    /** column IN (a, b, c) -> terms query */
    IN_SET,
    /** column IS NOT NULL -> exists query */
    EXISTS,
    /** column IS NULL -> bool.must_not.exists */
    NOT_EXISTS,
    /** column LIKE 'pattern' -> wildcard/regexp query */
    LIKE_PATTERN,
    /** AND combination -> bool.must */
    BOOL_AND,
    /** OR combination -> bool.should */
    BOOL_OR,
    /** NOT negation -> bool.must_not */
    BOOL_NOT
  }

  private final PredicateType type;
  private final String fieldName;
  private final DqeType fieldType;
  private final Object value;
  private final List<PushdownPredicate> children;

  public PushdownPredicate(
      PredicateType type,
      String fieldName,
      DqeType fieldType,
      Object value,
      List<PushdownPredicate> children) {
    this.type = Objects.requireNonNull(type, "type must not be null");
    this.fieldName = fieldName;
    this.fieldType = fieldType;
    this.value = value;
    this.children = children != null ? List.copyOf(children) : List.of();
  }

  public PredicateType getType() {
    return type;
  }

  /** Returns the field name to query against. Null for boolean combinators. */
  public String getFieldName() {
    return fieldName;
  }

  /** Returns the field type. Null for boolean combinators. */
  public DqeType getFieldType() {
    return fieldType;
  }

  /** Returns the literal value, range bounds, or list for IN. */
  public Object getValue() {
    return value;
  }

  /** Returns children for BOOL_AND/BOOL_OR/BOOL_NOT. */
  public List<PushdownPredicate> getChildren() {
    return children;
  }

  // -- Factory methods --

  public static PushdownPredicate termEquality(String field, DqeType type, Object value) {
    return new PushdownPredicate(PredicateType.TERM_EQUALITY, field, type, value, null);
  }

  public static PushdownPredicate range(
      String field,
      DqeType type,
      Object lowerBound,
      boolean lowerInclusive,
      Object upperBound,
      boolean upperInclusive) {
    RangeBounds bounds = new RangeBounds(lowerBound, lowerInclusive, upperBound, upperInclusive);
    return new PushdownPredicate(PredicateType.RANGE, field, type, bounds, null);
  }

  public static PushdownPredicate between(String field, DqeType type, Object lower, Object upper) {
    RangeBounds bounds = new RangeBounds(lower, true, upper, true);
    return new PushdownPredicate(PredicateType.BETWEEN, field, type, bounds, null);
  }

  public static PushdownPredicate inSet(String field, DqeType type, List<Object> values) {
    return new PushdownPredicate(PredicateType.IN_SET, field, type, values, null);
  }

  public static PushdownPredicate exists(String field) {
    return new PushdownPredicate(PredicateType.EXISTS, field, null, null, null);
  }

  public static PushdownPredicate notExists(String field) {
    return new PushdownPredicate(PredicateType.NOT_EXISTS, field, null, null, null);
  }

  public static PushdownPredicate likePattern(String field, String pattern) {
    return new PushdownPredicate(PredicateType.LIKE_PATTERN, field, null, pattern, null);
  }

  public static PushdownPredicate and(List<PushdownPredicate> children) {
    return new PushdownPredicate(PredicateType.BOOL_AND, null, null, null, children);
  }

  public static PushdownPredicate or(List<PushdownPredicate> children) {
    return new PushdownPredicate(PredicateType.BOOL_OR, null, null, null, children);
  }

  public static PushdownPredicate not(PushdownPredicate child) {
    return new PushdownPredicate(PredicateType.BOOL_NOT, null, null, null, List.of(child));
  }

  @Override
  public String toString() {
    return "PushdownPredicate{" + type + ", field=" + fieldName + "}";
  }

  /** Holder for range query bounds. */
  public record RangeBounds(
      Object lowerBound, boolean lowerInclusive, Object upperBound, boolean upperInclusive) {}
}
