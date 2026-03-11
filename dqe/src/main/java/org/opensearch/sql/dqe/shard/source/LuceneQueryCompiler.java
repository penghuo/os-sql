/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;

/**
 * Compiles OpenSearch DSL filter JSON strings into Lucene {@link Query} objects. Supports term,
 * range, terms (set), and bool (must/should/must_not) queries.
 *
 * <p>Uses field type information to choose the correct Lucene query type: numeric fields use {@link
 * LongPoint} queries while keyword/text fields use {@link TermQuery}.
 */
public class LuceneQueryCompiler {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Types stored as LongPoint (8 bytes per dimension). */
  private static final Set<String> LONG_TYPES = Set.of("long", "date");

  /** Types stored as IntPoint (4 bytes per dimension). */
  private static final Set<String> INT_TYPES = Set.of("integer", "short", "byte");

  private static final Set<String> NUMERIC_TYPES =
      Set.of("long", "integer", "short", "byte", "date");
  private static final Set<String> FLOAT_TYPES =
      Set.of("double", "float", "half_float", "scaled_float");

  private final Map<String, String> fieldTypes;

  /**
   * Create a compiler with field type mappings.
   *
   * @param fieldTypes mapping from field name to OpenSearch type (e.g., "integer", "keyword")
   */
  public LuceneQueryCompiler(Map<String, String> fieldTypes) {
    this.fieldTypes = fieldTypes;
  }

  /**
   * Compile a DSL filter JSON string into a Lucene Query.
   *
   * @param dslJson the DSL filter JSON, or null for match-all
   * @return the compiled Lucene query
   */
  public Query compile(String dslJson) {
    if (dslJson == null || dslJson.isBlank()) {
      return new MatchAllDocsQuery();
    }
    try {
      JsonNode root = MAPPER.readTree(dslJson);
      return compileNode(root);
    } catch (Exception e) {
      // If parsing fails, fall back to match-all to avoid blocking queries
      return new MatchAllDocsQuery();
    }
  }

  private Query compileNode(JsonNode node) {
    if (node.has("term")) {
      return compileTerm(node.get("term"));
    } else if (node.has("terms")) {
      return compileTerms(node.get("terms"));
    } else if (node.has("range")) {
      return compileRange(node.get("range"));
    } else if (node.has("wildcard")) {
      return compileWildcard(node.get("wildcard"));
    } else if (node.has("bool")) {
      return compileBool(node.get("bool"));
    } else if (node.has("match_all")) {
      return new MatchAllDocsQuery();
    }
    return new MatchAllDocsQuery();
  }

  private Query compileTerm(JsonNode termNode) {
    Iterator<String> fields = termNode.fieldNames();
    if (!fields.hasNext()) {
      return new MatchAllDocsQuery();
    }
    String field = fields.next();
    JsonNode value = termNode.get(field);
    return buildTermQuery(field, value);
  }

  private Query buildTermQuery(String field, JsonNode value) {
    String osType = fieldTypes.getOrDefault(field, "keyword");

    if (INT_TYPES.contains(osType)) {
      int intVal = value.asInt();
      return IntPoint.newExactQuery(field, intVal);
    } else if (LONG_TYPES.contains(osType)) {
      long longVal = value.asLong();
      return LongPoint.newExactQuery(field, longVal);
    } else if (FLOAT_TYPES.contains(osType)) {
      // Float fields are stored as double-encoded longs in Lucene
      double doubleVal = value.asDouble();
      return LongPoint.newExactQuery(field, Double.doubleToRawLongBits(doubleVal));
    } else {
      // keyword, text, ip, etc. → inverted index term query
      return new TermQuery(new Term(field, value.asText()));
    }
  }

  private Query compileTerms(JsonNode termsNode) {
    Iterator<String> fields = termsNode.fieldNames();
    if (!fields.hasNext()) {
      return new MatchAllDocsQuery();
    }
    String field = fields.next();
    JsonNode values = termsNode.get(field);
    String osType = fieldTypes.getOrDefault(field, "keyword");

    if (INT_TYPES.contains(osType) && values.isArray()) {
      int[] arr = new int[values.size()];
      for (int i = 0; i < values.size(); i++) {
        arr[i] = values.get(i).asInt();
      }
      return IntPoint.newSetQuery(field, arr);
    } else if (LONG_TYPES.contains(osType) && values.isArray()) {
      long[] arr = new long[values.size()];
      for (int i = 0; i < values.size(); i++) {
        arr[i] = values.get(i).asLong();
      }
      return LongPoint.newSetQuery(field, arr);
    } else if (values.isArray()) {
      // Keyword terms → OR of TermQueries
      BooleanQuery.Builder bqb = new BooleanQuery.Builder();
      for (JsonNode v : values) {
        bqb.add(new TermQuery(new Term(field, v.asText())), BooleanClause.Occur.SHOULD);
      }
      return bqb.build();
    }
    return new MatchAllDocsQuery();
  }

  private Query compileWildcard(JsonNode wildcardNode) {
    Iterator<String> fields = wildcardNode.fieldNames();
    if (!fields.hasNext()) return new MatchAllDocsQuery();
    String field = fields.next();
    JsonNode config = wildcardNode.get(field);
    String pattern = config.has("value") ? config.get("value").asText() : config.asText();
    return new WildcardQuery(new Term(field, pattern));
  }

  private Query compileRange(JsonNode rangeNode) {
    Iterator<String> fields = rangeNode.fieldNames();
    if (!fields.hasNext()) {
      return new MatchAllDocsQuery();
    }
    String field = fields.next();
    JsonNode bounds = rangeNode.get(field);
    String osType = fieldTypes.getOrDefault(field, "keyword");

    if (INT_TYPES.contains(osType)) {
      int lower = bounds.has("gte") ? bounds.get("gte").asInt() : Integer.MIN_VALUE;
      int upper = bounds.has("lte") ? bounds.get("lte").asInt() : Integer.MAX_VALUE;
      if (bounds.has("gt")) {
        lower = bounds.get("gt").asInt() + 1;
      }
      if (bounds.has("lt")) {
        upper = bounds.get("lt").asInt() - 1;
      }
      return IntPoint.newRangeQuery(field, lower, upper);
    } else if (LONG_TYPES.contains(osType)) {
      long lower = bounds.has("gte") ? bounds.get("gte").asLong() : Long.MIN_VALUE;
      long upper = bounds.has("lte") ? bounds.get("lte").asLong() : Long.MAX_VALUE;
      if (bounds.has("gt")) {
        lower = bounds.get("gt").asLong() + 1;
      }
      if (bounds.has("lt")) {
        upper = bounds.get("lt").asLong() - 1;
      }
      return LongPoint.newRangeQuery(field, lower, upper);
    }
    // Range on keyword fields: fall back to match all
    return new MatchAllDocsQuery();
  }

  private Query compileBool(JsonNode boolNode) {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    boolean hasPositive = false;
    boolean hasMustNot = false;

    // Collect range queries on the same field for merging.
    // When two separate range clauses target the same field (e.g., gte and lte on EventDate),
    // merging them into a single LongPoint/IntPoint range query is more efficient for Lucene
    // because it uses a single BKD tree traversal instead of intersecting two.
    java.util.Map<String, long[]> longRangeBounds = null;
    java.util.Map<String, int[]> intRangeBounds = null;
    java.util.List<JsonNode> nonRangeMustClauses = null;

    if (boolNode.has("must")) {
      // First pass: separate range clauses (for merging) from non-range clauses
      for (JsonNode clause : boolNode.get("must")) {
        if (clause.has("range")) {
          JsonNode rangeNode = clause.get("range");
          Iterator<String> fields = rangeNode.fieldNames();
          if (fields.hasNext()) {
            String field = fields.next();
            String osType = fieldTypes.getOrDefault(field, "keyword");
            if (LONG_TYPES.contains(osType)) {
              if (longRangeBounds == null) longRangeBounds = new java.util.HashMap<>();
              long[] existing =
                  longRangeBounds.computeIfAbsent(
                      field, k -> new long[] {Long.MIN_VALUE, Long.MAX_VALUE});
              JsonNode bounds = rangeNode.get(field);
              if (bounds.has("gte"))
                existing[0] = Math.max(existing[0], bounds.get("gte").asLong());
              if (bounds.has("gt"))
                existing[0] = Math.max(existing[0], bounds.get("gt").asLong() + 1);
              if (bounds.has("lte"))
                existing[1] = Math.min(existing[1], bounds.get("lte").asLong());
              if (bounds.has("lt"))
                existing[1] = Math.min(existing[1], bounds.get("lt").asLong() - 1);
              continue;
            } else if (INT_TYPES.contains(osType)) {
              if (intRangeBounds == null) intRangeBounds = new java.util.HashMap<>();
              int[] existing =
                  intRangeBounds.computeIfAbsent(
                      field, k -> new int[] {Integer.MIN_VALUE, Integer.MAX_VALUE});
              JsonNode bounds = rangeNode.get(field);
              if (bounds.has("gte")) existing[0] = Math.max(existing[0], bounds.get("gte").asInt());
              if (bounds.has("gt"))
                existing[0] = Math.max(existing[0], bounds.get("gt").asInt() + 1);
              if (bounds.has("lte")) existing[1] = Math.min(existing[1], bounds.get("lte").asInt());
              if (bounds.has("lt"))
                existing[1] = Math.min(existing[1], bounds.get("lt").asInt() - 1);
              continue;
            }
          }
        }
        if (nonRangeMustClauses == null) nonRangeMustClauses = new java.util.ArrayList<>();
        nonRangeMustClauses.add(clause);
      }

      // Add merged range queries (one BKD traversal per field instead of per-bound)
      if (longRangeBounds != null) {
        for (java.util.Map.Entry<String, long[]> entry : longRangeBounds.entrySet()) {
          builder.add(
              LongPoint.newRangeQuery(entry.getKey(), entry.getValue()[0], entry.getValue()[1]),
              BooleanClause.Occur.MUST);
          hasPositive = true;
        }
      }
      if (intRangeBounds != null) {
        for (java.util.Map.Entry<String, int[]> entry : intRangeBounds.entrySet()) {
          builder.add(
              IntPoint.newRangeQuery(entry.getKey(), entry.getValue()[0], entry.getValue()[1]),
              BooleanClause.Occur.MUST);
          hasPositive = true;
        }
      }

      // Add non-range must clauses, flattening nested bool(must_not-only) patterns.
      // When a MUST clause is a bool with only must_not (e.g., from NOT_EQUAL like URL <> ''),
      // promote the must_not clauses directly to avoid a redundant nested BooleanQuery
      // with MatchAllDocsQuery.
      java.util.List<JsonNode> mustClauses =
          nonRangeMustClauses != null ? nonRangeMustClauses : new java.util.ArrayList<>();
      if (nonRangeMustClauses == null && longRangeBounds == null && intRangeBounds == null) {
        for (JsonNode clause : boolNode.get("must")) {
          mustClauses.add(clause);
        }
      }

      for (JsonNode clause : mustClauses) {
        if (clause.has("bool") && isOnlyMustNot(clause.get("bool"))) {
          for (JsonNode innerMustNot : clause.get("bool").get("must_not")) {
            builder.add(compileNode(innerMustNot), BooleanClause.Occur.MUST_NOT);
            hasMustNot = true;
          }
        } else {
          builder.add(compileNode(clause), BooleanClause.Occur.MUST);
          hasPositive = true;
        }
      }
    }

    if (boolNode.has("should")) {
      for (JsonNode clause : boolNode.get("should")) {
        builder.add(compileNode(clause), BooleanClause.Occur.SHOULD);
        hasPositive = true;
      }
    }
    if (boolNode.has("must_not")) {
      for (JsonNode clause : boolNode.get("must_not")) {
        builder.add(compileNode(clause), BooleanClause.Occur.MUST_NOT);
        hasMustNot = true;
      }
    }
    if (boolNode.has("filter")) {
      for (JsonNode clause : boolNode.get("filter")) {
        builder.add(compileNode(clause), BooleanClause.Occur.FILTER);
        hasPositive = true;
      }
    }

    // A BooleanQuery with only must_not clauses matches nothing in Lucene.
    // Add MatchAllDocsQuery as a positive clause to match all docs except excluded.
    if (!hasPositive && hasMustNot) {
      builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
    }

    return builder.build();
  }

  /**
   * Check if a bool node contains only must_not clauses (no must, should, or filter). This pattern
   * arises from NOT_EQUAL comparisons like {@code URL <> ''} and can be flattened into the parent
   * bool to eliminate a redundant nested BooleanQuery with MatchAllDocsQuery.
   */
  private static boolean isOnlyMustNot(JsonNode boolNode) {
    return boolNode.has("must_not")
        && !boolNode.has("must")
        && !boolNode.has("should")
        && !boolNode.has("filter");
  }
}
