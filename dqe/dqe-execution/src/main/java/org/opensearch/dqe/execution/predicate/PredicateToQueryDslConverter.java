/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.predicate;

import java.util.List;
import javax.annotation.Nullable;
import org.opensearch.dqe.analyzer.predicate.PushdownPredicate;
import org.opensearch.dqe.analyzer.predicate.PushdownPredicate.RangeBounds;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;

/**
 * Converts {@link PushdownPredicate} trees into OpenSearch QueryBuilder objects. The fieldName in
 * PushdownPredicate is pre-resolved by the analyzer (e.g., "name.keyword" for text fields).
 */
public class PredicateToQueryDslConverter {

  public PredicateToQueryDslConverter() {}

  /**
   * Converts a PushdownPredicate to an OpenSearch QueryBuilder. Returns null if predicate is null.
   */
  @Nullable
  public QueryBuilder convert(@Nullable PushdownPredicate predicate) {
    if (predicate == null) {
      return null;
    }
    return convertInternal(predicate);
  }

  /** Converts a list of predicates combined with AND. Returns null if the list is empty. */
  @Nullable
  public QueryBuilder convertAll(List<PushdownPredicate> predicates) {
    if (predicates == null || predicates.isEmpty()) {
      return null;
    }
    if (predicates.size() == 1) {
      return convert(predicates.get(0));
    }
    BoolQueryBuilder bool = QueryBuilders.boolQuery();
    for (PushdownPredicate pred : predicates) {
      bool.must(convertInternal(pred));
    }
    return bool;
  }

  @SuppressWarnings("unchecked")
  private QueryBuilder convertInternal(PushdownPredicate predicate) {
    return switch (predicate.getType()) {
      case TERM_EQUALITY -> QueryBuilders.termQuery(predicate.getFieldName(), predicate.getValue());

      case RANGE -> {
        RangeBounds bounds = (RangeBounds) predicate.getValue();
        RangeQueryBuilder range = QueryBuilders.rangeQuery(predicate.getFieldName());
        if (bounds.lowerBound() != null) {
          if (bounds.lowerInclusive()) {
            range.gte(bounds.lowerBound());
          } else {
            range.gt(bounds.lowerBound());
          }
        }
        if (bounds.upperBound() != null) {
          if (bounds.upperInclusive()) {
            range.lte(bounds.upperBound());
          } else {
            range.lt(bounds.upperBound());
          }
        }
        yield range;
      }

      case BETWEEN -> {
        RangeBounds bounds = (RangeBounds) predicate.getValue();
        yield QueryBuilders.rangeQuery(predicate.getFieldName())
            .gte(bounds.lowerBound())
            .lte(bounds.upperBound());
      }

      case IN_SET -> {
        List<Object> values = (List<Object>) predicate.getValue();
        yield QueryBuilders.termsQuery(predicate.getFieldName(), values);
      }

      case EXISTS -> QueryBuilders.existsQuery(predicate.getFieldName());

      case NOT_EXISTS ->
          QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(predicate.getFieldName()));

      case LIKE_PATTERN -> {
        String pattern = (String) predicate.getValue();
        // Convert SQL LIKE pattern to OpenSearch wildcard pattern
        // SQL: % -> *, _ -> ?
        String wildcardPattern = convertLikeToWildcard(pattern);
        yield QueryBuilders.wildcardQuery(predicate.getFieldName(), wildcardPattern);
      }

      case BOOL_AND -> {
        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        for (PushdownPredicate child : predicate.getChildren()) {
          bool.must(convertInternal(child));
        }
        yield bool;
      }

      case BOOL_OR -> {
        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        for (PushdownPredicate child : predicate.getChildren()) {
          bool.should(convertInternal(child));
        }
        bool.minimumShouldMatch(1);
        yield bool;
      }

      case BOOL_NOT -> {
        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        for (PushdownPredicate child : predicate.getChildren()) {
          bool.mustNot(convertInternal(child));
        }
        yield bool;
      }
    };
  }

  /** Converts SQL LIKE pattern to OpenSearch wildcard pattern. SQL % -> *, _ -> ? */
  static String convertLikeToWildcard(String likePattern) {
    StringBuilder sb = new StringBuilder(likePattern.length());
    boolean escaped = false;
    for (int i = 0; i < likePattern.length(); i++) {
      char c = likePattern.charAt(i);
      if (escaped) {
        sb.append(c);
        escaped = false;
      } else if (c == '\\') {
        escaped = true;
      } else if (c == '%') {
        sb.append('*');
      } else if (c == '_') {
        sb.append('?');
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }
}
