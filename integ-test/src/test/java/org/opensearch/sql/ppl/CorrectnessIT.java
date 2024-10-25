/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import java.util.List;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class CorrectnessIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.BANK);
  }

  @After
  public void afterTest() throws IOException {
    resetQuerySizeLimit();
    resetMaxResultWindow(TEST_INDEX_BANK);
  }

  // https://github.com/opensearch-project/sql/issues/2802
  @Test
  public void issue2802() throws IOException {
    for (Integer querySizeLimit : querySizeLimits()) {
      for (Integer maxResultWindow : maxResultWindow()) {
        setQuerySizeLimit(querySizeLimit);
        setMaxResultWindow(TEST_INDEX_BANK, maxResultWindow);

        JSONObject result =
            executeQuery(
                String.format("source=%s | eval age = age | sort -age | head 5 | fields age",
                    TEST_INDEX_BANK));
        verifyDataRows(
            result,
            rows(39),
            rows(36),
            rows(36),
            rows(34),
            rows(33));
      }
    }
  }

  @Test
  public void returnQuerySizeLimitRows() throws IOException {
    setQuerySizeLimit(1);
    setMaxResultWindow(TEST_INDEX_BANK, 1);

    JSONObject result =
        executeQuery(
            String.format("source=%s | eval age = age | sort -age | head 5 | fields age",
                TEST_INDEX_BANK));
    verifyDataRows(
        result,
        rows(39));

    result = executeQuery(String.format("source=%s | stats count() by age | sort -age | fields age",
        TEST_INDEX_BANK));
    verifyDataRows(
        result,
        rows(39));
  }

  @Test
  public void limitPushDown() throws IOException {
    // if limit less or equal to maxResultWindow = 5, push down limit, no scroll.
    setMaxResultWindow(TEST_INDEX_BANK, 5);
    String explainResult = explainQueryToString(
        String.format("source=%s | head 1 | fields age",
            TEST_INDEX_BANK));
    Assert.assertTrue(explainResult.contains("OpenSearchIndexScan"));
    Assert.assertTrue(explainResult.contains("\\\"size\\\":1"));

    explainResult = explainQueryToString(
        String.format("source=%s | head 5 | fields age",
            TEST_INDEX_BANK));
    Assert.assertTrue(explainResult.contains("OpenSearchIndexScan"));
    Assert.assertTrue(explainResult.contains("\\\"size\\\":5"));

    // if limit larger than maxResultWindow = 5, no limit push down, using scroll api.
    explainResult = explainQueryToString(
        String.format("source=%s | head 6 | fields age",
            TEST_INDEX_BANK));
    Assert.assertTrue(explainResult.contains("\"limit\": 6"));
    Assert.assertTrue(explainResult.contains("OpenSearchScrollRequest"));
    Assert.assertTrue(explainResult.contains("\\\"size\\\":5"));
  }

  @Test
  public void aggQueryShouldNotIncludeScrollAndQuerySizeLimitIsPushed() throws IOException {
    setQuerySizeLimit(10000);
    String explainResult = explainQueryToString(
        String.format("source=%s | stats count() by age | sort -age | fields age",
            TEST_INDEX_BANK));
    Assert.assertTrue(explainResult.contains("OpenSearchIndexScan"));
    Assert.assertTrue(explainResult.contains("{\\\"composite\\\":{\\\"size\\\":10000"));
  }

  private List<Integer> querySizeLimits() {
    return List.of(5, 10);
  }

  private List<Integer> maxResultWindow() {
    return List.of(1, 10, 100);
  }
}
