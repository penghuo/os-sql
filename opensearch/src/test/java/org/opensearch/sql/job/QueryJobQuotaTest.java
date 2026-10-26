/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.common.setting.Settings;

class QueryJobQuotaTest {

  @Test
  void admitRejectsOnceRetainedFinishedJobLimitIsReached() {
    QueryJobQuota quota = new QueryJobQuota(settings(10, 1, 1_000));
    quota.admit();
    quota.admit();
    quota.addFinished();
    OpenSearchStatusException e = assertThrows(OpenSearchStatusException.class, quota::admit);
    assertEquals(RestStatus.TOO_MANY_REQUESTS, e.status());
    assertEquals(
        "Async query capacity on this node is exhausted"
            + " [plugins.ppl.async.max_retained_jobs]; retry later",
        e.getMessage());
    quota.releaseFinished();
    quota.admit();
    assertEquals(0, quota.finished());
  }

  @Test
  void admitRejectsOnceResultBudgetIsExhausted() {
    QueryJobQuota quota = new QueryJobQuota(settings(10, 10, 100));
    quota.reserve(100);
    OpenSearchStatusException e = assertThrows(OpenSearchStatusException.class, quota::admit);
    assertEquals(
        "Async query capacity on this node is exhausted"
            + " [plugins.ppl.async.max_retained_result_size]; retry later",
        e.getMessage());
    quota.release(1);
    quota.admit();
  }

  @Test
  void reserveRejectsChargeThatDoesNotFit() {
    QueryJobQuota quota = new QueryJobQuota(settings(10, 10, 100));
    quota.reserve(60);
    OpenSearchStatusException e =
        assertThrows(OpenSearchStatusException.class, () -> quota.reserve(41));
    assertEquals(RestStatus.TOO_MANY_REQUESTS, e.status());
    assertEquals(
        "Async query result does not fit in the retained result budget"
            + " [plugins.ppl.async.max_retained_result_size]; reduce the result size, or delete"
            + " completed queries and retry",
        e.getMessage());
    assertEquals(60, quota.resultBytes());
    quota.reserve(40);
    assertEquals(100, quota.resultBytes());
  }

  @Test
  void loweredLimitAppliesToNextAdmission() {
    MutableSettings settings = settings(5, 10, 1_000);
    QueryJobQuota quota = new QueryJobQuota(settings);
    quota.addFinished();
    quota.addFinished();
    quota.admit();
    settings.put(Settings.Key.PPL_ASYNC_MAX_RETAINED_JOBS, 2);
    assertThrows(OpenSearchStatusException.class, quota::admit);
    assertEquals(2, quota.finished());
  }

  static MutableSettings settings(int maxRunning, int maxJobs, long maxResultBytes) {
    MutableSettings settings = new MutableSettings();
    settings.put(Settings.Key.PPL_ASYNC_NODE_CONCURRENT_RUNNING_QUERIES, maxRunning);
    settings.put(Settings.Key.PPL_ASYNC_MAX_RETAINED_JOBS, maxJobs);
    settings.put(Settings.Key.PPL_ASYNC_MAX_QUEUED_QUERIES, 100);
    settings.put(Settings.Key.PPL_ASYNC_MAX_QUEUED_TIME, TimeValue.timeValueMinutes(5));
    settings.put(
        Settings.Key.PPL_ASYNC_MAX_RETAINED_RESULT_SIZE,
        ByteSizeValue.parseBytesSizeValue(maxResultBytes + "b", "test"));
    return settings;
  }

  /** {@link Settings} backed by a map the test can change between calls. */
  static final class MutableSettings extends Settings {
    private final Map<Key, Object> values = new HashMap<>();

    void put(Key key, Object value) {
      values.put(key, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getSettingValue(Key key) {
      return (T) values.get(key);
    }

    @Override
    public List<?> getSettings() {
      return List.of();
    }
  }
}
