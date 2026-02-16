/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests that verify Calcite PPL queries produce correct results when the distributed
 * execution engine is enabled. On a single-node test cluster, the DistributedExecutionEngine
 * detects all shards are local and falls back to the single-node engine.
 */
public class CalciteDistributedExecutionIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    enableDistributedExecution();

    Request request1 = new Request("PUT", "/test/_doc/1?refresh=true");
    request1.setJsonEntity("{\"name\": \"hello\", \"age\": 20}");
    client().performRequest(request1);
    Request request2 = new Request("PUT", "/test/_doc/2?refresh=true");
    request2.setJsonEntity("{\"name\": \"world\", \"age\": 30}");
    client().performRequest(request2);

    loadIndex(Index.BANK);
  }

  @Test
  public void testBasicSourceQuery() throws IOException {
    JSONObject actual = executeQuery("source=test");
    verifySchema(actual, schema("name", "string"), schema("age", "bigint"));
    verifyDataRows(actual, rows("hello", 20), rows("world", 30));
  }

  @Test
  public void testFilterQuery() throws IOException {
    JSONObject actual = executeQuery("source=test | where age = 30 | fields name, age");
    verifySchema(actual, schema("name", "string"), schema("age", "bigint"));
    verifyDataRows(actual, rows("world", 30));
  }

  @Test
  public void testAggregation() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | stats count() as cnt", TEST_INDEX_BANK));
    verifySchema(actual, schema("cnt", "bigint"));
    verifyDataRows(actual, rows(7));
  }

  @Test
  public void testGroupByAggregation() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats count() as cnt by gender | sort gender",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("cnt", "bigint"), schema("gender", "string"));
    verifyDataRows(actual, rows(3, "F"), rows(4, "M"));
  }

  @Test
  public void testSort() throws IOException {
    JSONObject actual = executeQuery("source=test | sort age | fields name, age");
    verifySchema(actual, schema("name", "string"), schema("age", "bigint"));
    verifyDataRows(actual, rows("hello", 20), rows("world", 30));
  }

  @Test
  public void testSortWithLimit() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort age | head 3 | fields firstname, age",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("age", "int"));
    verifyDataRows(actual, rows("Nanette", 28), rows("Amber JOHnny", 32), rows("Dale", 33));
  }

  @Test
  public void testEval() throws IOException {
    JSONObject actual =
        executeQuery("source=test | eval doubled_age = age * 2 | fields name, doubled_age");
    verifySchema(actual, schema("name", "string"), schema("doubled_age", "bigint"));
    verifyDataRows(actual, rows("hello", 40), rows("world", 60));
  }

  @Test
  public void testFieldsProjection() throws IOException {
    JSONObject actual = executeQuery("source=test | fields name");
    verifySchema(actual, schema("name", "string"));
    verifyDataRows(actual, rows("hello"), rows("world"));
  }
}
