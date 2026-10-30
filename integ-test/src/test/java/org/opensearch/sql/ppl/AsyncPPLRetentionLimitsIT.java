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
 * <p>Jobs left by earlier test classes may still be retained on the node, so each test sets its
 * limits relative to the usage the node reports in {@code /_plugins/_ppl/stats}.
 */
public class AsyncPPLRetentionLimitsIT extends PPLIntegTestCase {

  private static final String MAX_JOBS = Key.PPL_ASYNC_MAX_RETAINED_JOBS.getKeyValue();
  private static final String MAX_RESULT_SIZE =
      Key.PPL_ASYNC_MAX_RETAINED_RESULT_SIZE.getKeyValue();
  private static final String ROWS_QUERY =
      "source=" + TEST_INDEX_ACCOUNT + " | fields firstname, lastname, email | head 100";
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
    setLimit(MAX_JOBS, stat("ppl_async_retained_jobs") + 1);
    long rejectedBefore = stat("ppl_async_rejected_submit_count");

    // Fill the node to the limit; a job evicted meanwhile only makes room for one more.
    ResponseException rejected = null;
    for (int i = 0; i < 5 && rejected == null; i++) {
      try {
        submitRetained(ROWS_QUERY);
      } catch (ResponseException e) {
        rejected = e;
      }
    }

    Assert.assertNotNull("a submit over the retained job limit must be rejected", rejected);
    assertRejectedWith429(rejected, MAX_JOBS);
    Assert.assertEquals(rejectedBefore + 1, stat("ppl_async_rejected_submit_count"));

    deleteAsyncQuery(client(), jobIds.removeLast());
    submitRetained(ROWS_QUERY);
  }

  @Test
  public void retainedResultOverBudgetFailsTheJobWithoutRows() throws Exception {
    // Room for the node's current usage plus 100 bytes: the submit is admitted, the result is not.
    setLimit(MAX_RESULT_SIZE, (stat("ppl_async_retained_result_bytes") + 100) + "b");
    long rejectedBefore = stat("ppl_async_rejected_result_count");

    String id = submitRetained(ROWS_QUERY);
    JSONObject job = pollUntilTerminal(client(), id, POLL_TIMEOUT_MILLIS);

    Assert.assertEquals(job.toString(), "FAILED", job.getString("status"));
    JSONObject error = job.getJSONObject("error");
    Assert.assertEquals(job.toString(), "QueryJobCapacityException", error.getString("type"));
    Assert.assertEquals(job.toString(), "Too Many Requests", error.getString("reason"));
    Assert.assertTrue(job.toString(), error.getString("details").contains(MAX_RESULT_SIZE));
    Assert.assertTrue(
        job.toString(), !job.has("datarows") || job.getJSONArray("datarows").isEmpty());
    Assert.assertEquals(rejectedBefore + 1, stat("ppl_async_rejected_result_count"));
  }

  @Test
  public void fullResultBudgetRejectsSubmitWith429UntilAJobIsDeleted() throws Exception {
    long usageBefore = stat("ppl_async_retained_result_bytes");
    String id = submitRetained(ROWS_QUERY);
    Assert.assertEquals(
        "SUCCEEDED", pollUntilTerminal(client(), id, POLL_TIMEOUT_MILLIS).getString("status"));
    long usage = stat("ppl_async_retained_result_bytes");
    Assert.assertTrue("the retained result must be charged", usage > usageBefore);

    // Retained results now fill the budget exactly.
    setLimit(MAX_RESULT_SIZE, usage + "b");
    ResponseException rejected =
        Assert.assertThrows(ResponseException.class, () -> submitRetained(ROWS_QUERY));
    assertRejectedWith429(rejected, MAX_RESULT_SIZE);

    deleteAsyncQuery(client(), id);
    Assert.assertTrue(
        "deleting the job must return its charge", stat("ppl_async_retained_result_bytes") < usage);
    submitRetained(ROWS_QUERY);
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

  private long stat(String name) throws IOException {
    String body =
        getResponseBody(client().performRequest(new Request("GET", "/_plugins/_ppl/stats")), true);
    return new JSONObject(body).getLong(name);
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
