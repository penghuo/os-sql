/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    if (NUMERIC_TYPES.contains(osType)) {
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

    if (NUMERIC_TYPES.contains(osType) && values.isArray()) {
      List<Long> longVals = new ArrayList<>();
      for (JsonNode v : values) {
        longVals.add(v.asLong());
      }
      long[] arr = new long[longVals.size()];
      for (int i = 0; i < longVals.size(); i++) {
        arr[i] = longVals.get(i);
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

    if (NUMERIC_TYPES.contains(osType)) {
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

    if (boolNode.has("must")) {
      for (JsonNode clause : boolNode.get("must")) {
        builder.add(compileNode(clause), BooleanClause.Occur.MUST);
      }
    }
    if (boolNode.has("should")) {
      for (JsonNode clause : boolNode.get("should")) {
        builder.add(compileNode(clause), BooleanClause.Occur.SHOULD);
      }
    }
    if (boolNode.has("must_not")) {
      for (JsonNode clause : boolNode.get("must_not")) {
        builder.add(compileNode(clause), BooleanClause.Occur.MUST_NOT);
      }
    }
    if (boolNode.has("filter")) {
      for (JsonNode clause : boolNode.get("filter")) {
        builder.add(compileNode(clause), BooleanClause.Occur.FILTER);
      }
    }

    return builder.build();
  }
}
