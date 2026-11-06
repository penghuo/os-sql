/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.deleteAsyncQuery;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.pollUntilTerminal;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.postPpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongFunction;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.common.setting.Settings.Key;

/**
 * End-to-end IT for the limits on what a node retains for asynchronous PPL queries: {@code
 * plugins.ppl.async.max_retained_jobs} and {@code plugins.ppl.async.max_retained_result_size}.
 *
 * <p>Jobs left by earlier test classes may still be retained on the node, so each test first finds
 * the smallest limit at which the node admits a submit, which is one more than the node's usage.
 */
public class AsyncPPLRetentionLimitsIT extends PPLIntegTestCase {

  private static final String MAX_JOBS = Key.PPL_ASYNC_MAX_RETAINED_JOBS.getKeyValue();
  private static final String MAX_RESULT_SIZE =
      Key.PPL_ASYNC_MAX_RETAINED_RESULT_SIZE.getKeyValue();
  private static final String ROWS_QUERY =
      "source=" + TEST_INDEX_ACCOUNT + " | fields firstname, lastname, email | head 100";

  /** Returns no rows, so a retained result would be charged nothing. */
  private static final String NO_ROWS_QUERY =
      "source=" + TEST_INDEX_ACCOUNT + " | where age < 0 | fields firstname";

  private static final long POLL_TIMEOUT_MILLIS = 30_000;

  private final List<String> jobIds = new ArrayList<>();

  @Override
  protected void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    enableCalcite();
  }

  @After
  public void cleanUp() throws IOException {
    setLimit(MAX_JOBS, null);
    setLimit(MAX_RESULT_SIZE, null);
    for (String id : jobIds) {
      try {
        deleteAsyncQuery(client(), id);
      } catch (ResponseException e) {
        // Already deleted or evicted.
      }
    }
    jobIds.clear();
  }

  @Test
  public void retainedJobLimitRejectsSubmitWith429UntilAJobIsDeleted() throws Exception {
    // The node retains one finished job less than this limit, so one more finished job fills it.
    setLimit(MAX_JOBS, smallestAdmittingLimit(MAX_JOBS, jobs -> jobs));
    String id = submitRetained(ROWS_QUERY);
    pollUntilTerminal(client(), id, POLL_TIMEOUT_MILLIS);

    ResponseException rejected =
        Assert.assertThrows(ResponseException.class, () -> submitRetained(ROWS_QUERY));
    assertRejectedWith429(rejected, MAX_JOBS);

    deleteAsyncQuery(client(), id);
    submitRetained(ROWS_QUERY);
  }

  @Test
  public void retainedResultOverBudgetFailsTheJobWithoutRows() throws Exception {
    // One byte more than the retained results: the submit is admitted, its result is not.
    setLimit(MAX_RESULT_SIZE, smallestAdmittingLimit(MAX_RESULT_SIZE, bytes -> bytes + "b") + "b");

    String id = submitRetained(ROWS_QUERY);
    JSONObject job = pollUntilTerminal(client(), id, POLL_TIMEOUT_MILLIS);

    Assert.assertEquals(job.toString(), "FAILED", job.getString("status"));
    JSONObject error = job.getJSONObject("error");
    Assert.assertEquals(job.toString(), "QueryJobCapacityException", error.getString("type"));
    Assert.assertEquals(job.toString(), "Too Many Requests", error.getString("reason"));
    Assert.assertTrue(job.toString(), error.getString("details").contains(MAX_RESULT_SIZE));
    Assert.assertTrue(
        job.toString(), !job.has("datarows") || job.getJSONArray("datarows").isEmpty());
  }

  @Test
  public void fullResultBudgetRejectsSubmitWith429UntilAJobIsDeleted() throws Exception {
    long budget = smallestAdmittingLimit(MAX_RESULT_SIZE, bytes -> bytes + "b");
    setLimit(MAX_RESULT_SIZE, null);
    String id = submitRetained(ROWS_QUERY);
    Assert.assertEquals(
        "SUCCEEDED", pollUntilTerminal(client(), id, POLL_TIMEOUT_MILLIS).getString("status"));

    // The retained result now fills a budget that only had room for one more byte.
    setLimit(MAX_RESULT_SIZE, budget + "b");
    ResponseException rejected =
        Assert.assertThrows(ResponseException.class, () -> submitRetained(ROWS_QUERY));
    assertRejectedWith429(rejected, MAX_RESULT_SIZE);

    // Deleting the job returns its charge.
    deleteAsyncQuery(client(), id);
    submitRetained(ROWS_QUERY);
  }

  /**
   * Finds the smallest value of {@code setting} at which the node admits a submit, by binary search
   * with a query that returns its empty result inline and so is never retained.
   *
   * @param value renders a limit as the setting value
   */
  private long smallestAdmittingLimit(String setting, LongFunction<Object> value)
      throws IOException {
    long admitting = 1;
    while (!admits(setting, value.apply(admitting))) {
      admitting *= 2;
    }
    long refusing = admitting / 2;
    while (admitting - refusing > 1) {
      long middle = refusing + (admitting - refusing) / 2;
      if (admits(setting, value.apply(middle))) {
        admitting = middle;
      } else {
        refusing = middle;
      }
    }
    return admitting;
  }

  private boolean admits(String setting, Object limit) throws IOException {
    setLimit(setting, limit);
    JSONObject body =
        new JSONObject().put("query", NO_ROWS_QUERY).put("wait_for_completion_timeout", "30s");
    try {
      postPpl(client(), body);
      return true;
    } catch (ResponseException e) {
      assertRejectedWith429(e, setting);
      return false;
    }
  }

  /** Submits {@code query} with a zero wait and returns the id of the retained job. */
  private String submitRetained(String query) throws IOException {
    JSONObject body = new JSONObject().put("query", query).put("wait_for_completion_timeout", "0");
    JSONObject response = new JSONObject(postPpl(client(), body));
    String id = response.getString("id");
    jobIds.add(id);
    return id;
  }

  private static void assertRejectedWith429(ResponseException rejected, String setting)
      throws IOException {
    Assert.assertEquals(429, rejected.getResponse().getStatusLine().getStatusCode());
    String body = getResponseBody(rejected.getResponse(), true);
    Assert.assertTrue(body, body.contains(setting));
  }

  private void setLimit(String key, Object value) throws IOException {
    Request request = new Request("PUT", "/_cluster/settings");
    request.setJsonEntity(
        new JSONObject()
            .put("transient", new JSONObject().put(key, value == null ? JSONObject.NULL : value))
            .toString());
    client().performRequest(request);
  }
}
