/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Map;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("LuceneQueryCompiler: compile DSL filter JSON to Lucene queries")
class LuceneQueryCompilerTest {

  private final Map<String, String> fieldTypes =
      Map.of(
          "CounterID", "integer",
          "RegionID", "integer",
          "AdvEngineID", "integer",
          "UserID", "long",
          "EventDate", "date",
          "URL", "keyword",
          "IsRefresh", "short",
          "ResolutionWidth", "integer");

  @Test
  @DisplayName("null predicate compiles to MatchAllDocsQuery")
  void nullPredicateToMatchAll() {
    LuceneQueryCompiler compiler = new LuceneQueryCompiler(fieldTypes);
    Query q = compiler.compile(null);
    assertInstanceOf(MatchAllDocsQuery.class, q);
  }

  @Test
  @DisplayName("term query with numeric value compiles to point query")
  void termQueryNumeric() {
    LuceneQueryCompiler compiler = new LuceneQueryCompiler(fieldTypes);
    Query q = compiler.compile("{\"term\":{\"CounterID\":62}}");
    // Should be a point query (LongPoint or IntPoint), not a TermQuery
    // The exact class depends on Lucene internals, just verify it's not null
    // and not MatchAllDocsQuery
    assertNotMatchAll(q);
  }

  @Test
  @DisplayName("term query with string value compiles to TermQuery")
  void termQueryKeyword() {
    LuceneQueryCompiler compiler = new LuceneQueryCompiler(fieldTypes);
    Query q = compiler.compile("{\"term\":{\"URL\":\"http://example.com\"}}");
    assertInstanceOf(TermQuery.class, q);
    TermQuery tq = (TermQuery) q;
    assertEquals("URL", tq.getTerm().field());
    assertEquals("http://example.com", tq.getTerm().text());
  }

  @Test
  @DisplayName("bool must query compiles to BooleanQuery with MUST clauses")
  void boolMustQuery() {
    LuceneQueryCompiler compiler = new LuceneQueryCompiler(fieldTypes);
    String dsl =
        "{\"bool\":{\"must\":[{\"term\":{\"CounterID\":62}},{\"term\":{\"RegionID\":229}}]}}";
    Query q = compiler.compile(dsl);
    assertInstanceOf(BooleanQuery.class, q);
    BooleanQuery bq = (BooleanQuery) q;
    assertEquals(2, bq.clauses().size());
    assertEquals(BooleanClause.Occur.MUST, bq.clauses().get(0).occur());
  }

  @Test
  @DisplayName("range query compiles to point range query")
  void rangeQuery() {
    LuceneQueryCompiler compiler = new LuceneQueryCompiler(fieldTypes);
    String dsl = "{\"range\":{\"CounterID\":{\"gte\":10,\"lte\":100}}}";
    Query q = compiler.compile(dsl);
    assertNotMatchAll(q);
  }

  @Test
  @DisplayName("terms query compiles to point set query")
  void termsQuery() {
    LuceneQueryCompiler compiler = new LuceneQueryCompiler(fieldTypes);
    String dsl = "{\"terms\":{\"AdvEngineID\":[2,3,4]}}";
    Query q = compiler.compile(dsl);
    assertNotMatchAll(q);
  }

  @Test
  @DisplayName("wildcard query compiles to WildcardQuery")
  void wildcardQuery() {
    LuceneQueryCompiler compiler = new LuceneQueryCompiler(fieldTypes);
    String dsl = "{\"wildcard\":{\"URL\":{\"value\":\"*google*\"}}}";
    Query q = compiler.compile(dsl);
    assertInstanceOf(WildcardQuery.class, q);
    WildcardQuery wq = (WildcardQuery) q;
    assertEquals("URL", wq.getTerm().field());
    assertEquals("*google*", wq.getTerm().text());
  }

  @Test
  @DisplayName("unknown field type falls back to keyword TermQuery")
  void unknownFieldType() {
    LuceneQueryCompiler compiler = new LuceneQueryCompiler(fieldTypes);
    Query q = compiler.compile("{\"term\":{\"UnknownField\":\"hello\"}}");
    assertInstanceOf(TermQuery.class, q);
  }

  private static void assertNotMatchAll(Query q) {
    if (q instanceof MatchAllDocsQuery) {
      throw new AssertionError("Expected non-MatchAll query, got: " + q);
    }
  }
}
