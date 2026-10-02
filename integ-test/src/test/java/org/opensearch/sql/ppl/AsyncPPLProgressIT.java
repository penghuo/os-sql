/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.ASYNC_QUERY_ENDPOINT;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.getAsyncQuery;
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

/**
 * End-to-end progress contract for asynchronous Calcite PPL queries (issue #5797).
 *
 * <p>Asserts what the REST contract promises, across the source shapes the design enumerates:
 * {@code fraction_done} is always present and finite, running values stay within {@code [0.0,
 * 0.8]}, the sequence never decreases, {@code SUCCEEDED} is exactly {@code 1.0}, and a failed job
 * keeps its last running value instead of being rounded up.
 *
 * <p>Two things this class is deliberate about:
 *
 * <ul>
 *   <li><b>Calcite is enabled explicitly.</b> {@link PPLIntegTestCase#init()} disables it, and
 *       progress is only produced on the Calcite path — without this the whole class would pass
 *       while exercising the V2 engine.
 *   <li><b>Page and bucket sizes are shrunk.</b> The test indices hold a thousand documents and
 *       around a hundred state/gender pairs, so at the default result window and bucket size every
 *       paged shape would resolve in one round trip and the multi-page arithmetic would never run.
 * </ul>
 *
 * <p>No case asserts a specific intermediate value: progress is a best-effort estimate derived from
 * shard timing, so pinning a number would measure cluster speed rather than the contract.
 */
public class AsyncPPLProgressIT extends PPLIntegTestCase {

  /** Highest value a running query may publish; the top 20% is reserved for coordinator work. */
  private static final double RUNNING_CEILING = 0.8;

  private static final double EPSILON = 1e-9;

  /**
   * Small enough that the 1000-document test index needs many pages, and the ~100 state/gender
   * pairs need many Composite pages. At the defaults (10,000-row window, 1,000-bucket page) every
   * paged shape resolves in one round trip and the multi-page arithmetic never runs.
   */
  private static final int SMALL_PAGE_SIZE = 40;

  @Override
  protected void init() throws Exception {
    super.init();
    // super.init() disables Calcite. Progress is produced only on the Calcite scan path, so without
    // this every
    // assertion below would be checking the V2 engine instead.
    enableCalcite();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK);
    setQueryBucketSize(SMALL_PAGE_SIZE);
    setMaxResultWindow(TEST_INDEX_ACCOUNT, SMALL_PAGE_SIZE);
    setMaxResultWindow(TEST_INDEX_BANK, SMALL_PAGE_SIZE);
  }

  @After
  public void restoreWindows() throws IOException {
    resetQueryBucketSize();
    resetMaxResultWindow(TEST_INDEX_ACCOUNT);
    resetMaxResultWindow(TEST_INDEX_BANK);
  }

  // ------------------------------------------------------------------ submit shapes

  @Test
  public void submitRunningSnapshotCarriesProgress() throws IOException {
    JSONObject response = submitAsync("source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    if (!response.has("id")) {
      // The runner can legitimately beat even a zero wait: QueryJob registers its completion
      // callback before it
      // completes the zero-budget running snapshot, so a fast query may settle first. That is a
      // valid outcome, not
      // a missing id, and it must still report completion.
      assertInlineSuccess(response);
      return;
    }
    Assert.assertEquals("RUNNING", response.getString("status"));
    assertRunningProgress(response);
  }

  @Test
  public void inlineSuccessCarriesCompleteProgress() throws IOException {
    JSONObject response =
        submitAsyncWithWait("source=" + TEST_INDEX_ACCOUNT + " | stats count() as c", "60s");

    Assert.assertTrue("inline result must keep its schema", response.has("schema"));
    assertInlineSuccess(response);
  }

  @Test
  public void inlineExplainCarriesCompleteProgress() throws IOException {
    JSONObject response =
        submitAsyncWithWait(
            "explain source=" + TEST_INDEX_ACCOUNT + " | stats count() as c", "60s");

    Assert.assertTrue(
        "inline explain must keep its plan tree", response.has("calcite") || response.has("root"));
    assertInlineSuccess(response);
  }

  @Test
  public void retainedExplainCarriesCompleteProgressOnGet() throws Exception {
    JSONObject submitted =
        submitAsync("explain source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    if (!submitted.has("id")) {
      assertInlineSuccess(submitted);
      return;
    }
    String queryId = submitted.getString("id");
    List<Double> observed = new ArrayList<>();
    assertRunningProgress(submitted);
    observed.add(fractionDone(submitted));

    long deadline = System.currentTimeMillis() + 60_000L;
    JSONObject explain = null;
    while (System.currentTimeMillis() < deadline) {
      JSONObject polled = new JSONObject(getAsyncQuery(client(), queryId));
      if (polled.has("calcite") || polled.has("root")) {
        explain = polled;
        break;
      }
      // A running explain response is still an async response: it carries progress, in range, and
      // monotonically.
      Assert.assertEquals(
          "an explain job must not fail: " + polled, "RUNNING", polled.optString("status"));
      assertRunningProgress(polled);
      observed.add(fractionDone(polled));
      Thread.sleep(25);
    }
    Assert.assertNotNull("explain body never arrived for [" + queryId + "]", explain);
    double finalFraction = fractionDone(explain);
    Assert.assertEquals(1.0, finalFraction, EPSILON);
    observed.add(finalFraction);
    assertMonotonic(observed, "explain");
  }

  @Test
  public void inlineFailureKeepsItsErrorSemantics() {
    // Progress must not change how a fast failure is reported: the status and payload stay those of
    // the
    // synchronous path.
    JSONObject body = new JSONObject();
    body.put("query", "source=");
    body.put("wait_for_completion_timeout", "60s");
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(body.toString());
    ResponseException ex =
        Assert.assertThrows(ResponseException.class, () -> client().performRequest(request));
    Assert.assertEquals(400, ex.getResponse().getStatusLine().getStatusCode());
  }

  // ------------------------------------------------------------------ source shapes

  @Test
  public void singleRequestAggregationReachesOneOnSuccess() throws Exception {
    assertSucceedsWithProgressContract("source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
  }

  @Test
  public void singleRequestHitSearchReachesOneOnSuccess() throws Exception {
    assertSucceedsWithProgressContract(
        "source=" + TEST_INDEX_ACCOUNT + " | fields firstname | head 20");
  }

  @Test
  public void compositeAggregationReportsProgress() throws Exception {
    // ~100 state/gender pairs against a 40-bucket page: several non-empty Composite pages.
    assertSucceedsWithProgressContract(
        "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c by state, gender");
  }

  @Test
  public void pagedScanReportsProgress() throws Exception {
    // Grouping by a text field cannot be pushed down, so the engine opens a point-in-time and pages
    // the scan.
    // With a 40-row window over 1000 documents that is many pages — the shape §8.5 governs.
    assertSucceedsWithProgressContract(
        "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c by address");
  }

  @Test
  public void filteredSourceReportsProgress() throws Exception {
    // A filtered source covers fewer documents than the index-size denominator implies, so its
    // fraction rises
    // faster than the estimate suggests. It must still stay in range.
    assertSucceedsWithProgressContract(
        "source=" + TEST_INDEX_ACCOUNT + " | where age > 30 | stats count() as c");
  }

  @Test
  public void wildcardSourceReportsProgress() throws Exception {
    assertSucceedsWithProgressContract("source=" + TEST_INDEX_ACCOUNT + "* | stats count() as c");
  }

  @Test
  public void multipleDistinctSourcesReportProgress() throws Exception {
    assertSucceedsWithProgressContract(
        "source="
            + TEST_INDEX_ACCOUNT
            + " | fields account_number | join left=l right=r on l.account_number ="
            + " r.account_number "
            + TEST_INDEX_BANK);
  }

  @Test
  public void selfJoinOverOneIndexReportsProgress() throws Exception {
    // Two physical positions over one index, which the planner canonicalizes onto a single plan
    // node. They are
    // two independent reads, so neither may claim the other's progress.
    assertSucceedsWithProgressContract(
        "source="
            + TEST_INDEX_BANK
            + " | join left=l right=r on l.account_number = r.account_number "
            + TEST_INDEX_BANK);
  }

  @Test
  public void uncorrelatedSubqueryReportsProgress() throws Exception {
    // The inner scan survives into the physical plan inside a subquery expression rather than as a
    // plan input.
    // Unregistered it would report nothing and hold the query below the ceiling.
    assertSucceedsWithProgressContract(
        "source="
            + TEST_INDEX_BANK
            + " | where account_number in [ source="
            + TEST_INDEX_BANK
            + " | fields account_number ] | stats count() as c");
  }

  @Test
  public void coordinatorLimitedInnerSourceReachesOneOnSuccess() throws Exception {
    // The filtered coordinator-limit shape: `head` then a filter then `head` again, inside a joined
    // subsearch. The
    // inner limit's cap cannot be pushed into the scan and cannot be derived statically — two raw
    // rows may yield
    // fewer than two qualifying rows — so the source is completed by the limit itself when it has
    // its rows, while
    // the outer side still has everything to read.
    assertSucceedsWithProgressContract(
        "source="
            + TEST_INDEX_BANK
            + " | join left=l right=r on l.account_number = r.account_number [ source="
            + TEST_INDEX_BANK
            + " | head 100 | where account_number > 0 | head 2 ] | stats count() as c");
  }

  @Test
  public void nestedCoordinatorLimitsReachOneOnSuccess() throws Exception {
    // Nested limits with offsets: each reports only what sits beneath it, and the outer's demand
    // must not rewrite
    // the inner's cap.
    assertSucceedsWithProgressContract(
        "source="
            + TEST_INDEX_BANK
            + " | join left=l right=r on l.account_number = r.account_number [ source="
            + TEST_INDEX_BANK
            + " | head 10 from 2 | where account_number > 0 | head 3 from 5 ] | stats count() as"
            + " c");
  }

  @Test
  public void oneRowLimitReachesOneOnSuccess() throws Exception {
    assertSucceedsWithProgressContract(
        "source=" + TEST_INDEX_BANK + " | head 1 | stats count() as c");
  }

  // A physical `fetch = 0` branch is not reachable from PPL: `head 0` fails in the planner
  // (PlanUtils.findTable returns null) on the plain synchronous path too, independently of
  // progress. The zero-quota
  // boundary is therefore pinned at the wrapper instead — see
  // ProgressLimitSignalTest.zeroQuotaCompletesOnVisit —
  // and ProgressAwareEnumerableLimit no longer skips a zero fetch, so the case is handled if it
  // ever becomes
  // reachable.

  @Test
  public void emptyResultReportsOneOnSuccess() throws Exception {
    assertSucceedsWithProgressContract(
        "source=" + TEST_INDEX_ACCOUNT + " | where age > 10000 | stats count() as c");
  }

  // ------------------------------------------------------------------ failure

  @Test
  public void failedQueryFreezesItsLastRunningValue() throws Exception {
    // Fails while evaluating rows, not while parsing or planning, so the job is retained as FAILED
    // and its source has
    // already done real work by then. `unix_timestamp` of a non-date string is a deterministic
    // execution-time failure.
    String query =
        "source=" + TEST_INDEX_ACCOUNT + " | eval bad = unix_timestamp(firstname) | fields bad";
    JSONObject submitted;
    try {
      submitted = submitAsync(query);
    } catch (ResponseException inlineFailure) {
      // The failure beat the zero wait and surfaced inline, which is a valid outcome: it must keep
      // the synchronous
      // path's error status rather than becoming a retained job.
      int code = inlineFailure.getResponse().getStatusLine().getStatusCode();
      Assert.assertTrue("expected a 4xx/5xx inline failure, got " + code, code >= 400);
      return;
    }
    if (!submitted.has("id")) {
      Assert.fail("a failing query returned an inline success body: " + submitted);
    }
    String queryId = submitted.getString("id");

    List<Double> observed = new ArrayList<>();
    assertRunningProgress(submitted);
    observed.add(fractionDone(submitted));
    JSONObject terminal = pollUntilTerminal(queryId, observed);

    Assert.assertEquals(
        "this query must fail during execution: " + terminal,
        "FAILED",
        terminal.getString("status"));
    JSONObject error = terminal.getJSONObject("error");
    Assert.assertTrue(
        "failed jobs must include the structured failure reason", error.has("reason"));
    double frozen = fractionDone(terminal);
    assertFinite(frozen);
    Assert.assertTrue(
        "a failed query must not report completion, got " + frozen, frozen < 1.0 - EPSILON);
    // A frozen value is a running value that stopped moving, so the running ceiling still binds it.
    // Checking only
    // "< 1.0" would accept 0.9, which no running sample could ever have published.
    Assert.assertTrue(
        "a frozen fraction must respect the running ceiling, got " + frozen,
        frozen <= RUNNING_CEILING + EPSILON);
    Assert.assertTrue(
        "the source finished reading before the row-level failure, so the frozen value must be"
            + " above zero, got "
            + frozen,
        frozen > 0.0);

    // The whole observed sequence must be monotonic, not just the endpoints, and the frozen value
    // must be at least
    // every value already published.
    double highestRunning = observed.stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
    Assert.assertTrue(
        "FAILED froze at " + frozen + " below already-published " + highestRunning,
        frozen >= highestRunning - EPSILON);
    observed.add(frozen);
    assertMonotonic(observed, query);

    for (int i = 0; i < 3; i++) {
      JSONObject repeat = new JSONObject(getAsyncQuery(client(), queryId));
      Assert.assertEquals("FAILED", repeat.getString("status"));
      Assert.assertTrue(
          "the structured failure must stay unchanged across polls",
          error.similar(repeat.getJSONObject("error")));
      Assert.assertEquals(
          "a frozen fraction must not move across polls", frozen, fractionDone(repeat), EPSILON);
    }
  }

  @Test
  public void planningFailureReportsFailedWithoutCompletion() throws Exception {
    // Fails before any source runs, so progress is zero — which is correct: nothing was observed.
    // Asserted
    // separately from the row-level failure so neither case can quietly stand in for the other.
    JSONObject submitted;
    try {
      submitted =
          submitAsync("source=" + TEST_INDEX_ACCOUNT + " | stats count() by span(firstname, 1)");
    } catch (ResponseException inlineFailure) {
      int code = inlineFailure.getResponse().getStatusLine().getStatusCode();
      Assert.assertTrue("expected a 4xx/5xx inline failure, got " + code, code >= 400);
      return;
    }
    if (!submitted.has("id")) {
      Assert.fail("a failing query returned an inline success body: " + submitted);
    }
    List<Double> observed = new ArrayList<>();
    observed.add(fractionDone(submitted));
    JSONObject terminal = pollUntilTerminal(submitted.getString("id"), observed);

    Assert.assertEquals("FAILED", terminal.getString("status"));
    Assert.assertEquals(0.0, fractionDone(terminal), EPSILON);
    observed.add(fractionDone(terminal));
    assertMonotonic(observed, "planning failure");
  }

  @Test
  public void unknownJobStillReturns4xx() {
    Request request = new Request("GET", ASYNC_QUERY_ENDPOINT + "nodeX%3Amissing");
    ResponseException ex =
        Assert.assertThrows(ResponseException.class, () -> client().performRequest(request));
    int code = ex.getResponse().getStatusLine().getStatusCode();
    Assert.assertTrue("expected 4xx, got " + code, code >= 400 && code < 500);
  }

  // ------------------------------------------------------------------ helpers

  /**
   * Submits {@code query}, polls it to a terminal state, and asserts the full published contract.
   *
   * <p>Requires {@code SUCCEEDED}: these are all valid queries, so anything else is a defect in the
   * query or the cluster, not an outcome to tolerate. Accepting a terminal failure here is how a
   * progress regression hides.
   */
  private void assertSucceedsWithProgressContract(String query) throws Exception {
    JSONObject submitted = submitAsync(query);
    if (!submitted.has("id")) {
      // Settled inside the zero wait. The inline contract is the thing to assert; requiring an id
      // here would make
      // the test depend on losing a race.
      assertInlineSuccess(submitted);
      return;
    }
    String queryId = submitted.getString("id");

    List<Double> observed = new ArrayList<>();
    assertRunningProgress(submitted);
    observed.add(fractionDone(submitted));

    JSONObject terminal = pollUntilTerminal(queryId, observed);
    Assert.assertEquals(
        "query must succeed for [" + query + "], got " + terminal,
        "SUCCEEDED",
        terminal.getString("status"));
    double finalFraction = fractionDone(terminal);
    Assert.assertEquals(
        "SUCCEEDED must report exactly 1.0 for [" + query + "]", 1.0, finalFraction, EPSILON);
    observed.add(finalFraction);
    assertMonotonic(observed, query);
  }

  /** The inline outcome: no polling id, the ordinary result body, and completion. */
  private static void assertInlineSuccess(JSONObject response) {
    Assert.assertFalse("an inline result must not carry a polling id", response.has("id"));
    Assert.assertTrue(
        "an inline result must carry rows or a plan: " + response,
        response.has("datarows") || response.has("calcite") || response.has("root"));
    Assert.assertEquals(
        "an inline result must report completion", 1.0, fractionDone(response), EPSILON);
  }

  private JSONObject submitAsync(String query) throws IOException {
    return submitAsyncWithWait(query, "0");
  }

  private JSONObject submitAsyncWithWait(String query, String wait) throws IOException {
    JSONObject body = new JSONObject();
    body.put("query", query);
    body.put("wait_for_completion_timeout", wait);
    body.put("keep_alive", "5m");
    return new JSONObject(postPpl(client(), body));
  }

  /** Polls until terminal, asserting the running contract on every intermediate snapshot. */
  private JSONObject pollUntilTerminal(String queryId, List<Double> observed) throws Exception {
    long deadline = System.currentTimeMillis() + 60_000L;
    JSONObject last = null;
    while (System.currentTimeMillis() < deadline) {
      last = new JSONObject(getAsyncQuery(client(), queryId));
      String status = last.optString("status", "");
      if ("RUNNING".equals(status) || "PENDING".equals(status)) {
        assertRunningProgress(last);
        observed.add(fractionDone(last));
        Thread.sleep(25);
        continue;
      }
      return last;
    }
    Assert.fail("async job [" + queryId + "] did not reach a terminal state within 60s: " + last);
    return last; // unreachable
  }

  private static void assertRunningProgress(JSONObject snapshot) {
    double fraction = fractionDone(snapshot);
    assertFinite(fraction);
    Assert.assertTrue("fraction_done must not be negative, got " + fraction, fraction >= 0.0);
    Assert.assertTrue(
        "a running query must not exceed " + RUNNING_CEILING + ", got " + fraction,
        fraction <= RUNNING_CEILING + EPSILON);
  }

  private static double fractionDone(JSONObject snapshot) {
    Assert.assertTrue(
        "every async response must carry a progress object: " + snapshot, snapshot.has("progress"));
    JSONObject progress = snapshot.getJSONObject("progress");
    Assert.assertTrue(
        "progress must carry fraction_done: " + progress, progress.has("fraction_done"));
    return progress.getDouble("fraction_done");
  }

  private static void assertFinite(double fraction) {
    Assert.assertTrue("fraction_done must be finite, got " + fraction, Double.isFinite(fraction));
    Assert.assertTrue(
        "fraction_done must not exceed 1.0, got " + fraction, fraction <= 1.0 + EPSILON);
  }

  private static void assertMonotonic(List<Double> observed, String query) {
    for (int i = 1; i < observed.size(); i++) {
      Assert.assertTrue(
          "fraction_done decreased from "
              + observed.get(i - 1)
              + " to "
              + observed.get(i)
              + " for ["
              + query
              + "]",
          observed.get(i) >= observed.get(i - 1) - EPSILON);
    }
  }
}
