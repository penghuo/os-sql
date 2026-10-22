/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.common.setting.Settings;

class QueryJobQuotaTest {

  @Test
  void admitRejectsOnceRunningLimitIsReached() {
    QueryJobQuota quota = new QueryJobQuota(settings(1, 10, 1_000));
    quota.admit();
    OpenSearchStatusException e = assertThrows(OpenSearchStatusException.class, quota::admit);
    assertEquals(RestStatus.TOO_MANY_REQUESTS, e.status());
    assertEquals(
        "Async query capacity on this node is exhausted"
            + " [plugins.ppl.async.node_concurrent_running_queries]; retry later",
        e.getMessage());
    quota.releaseRunning();
    quota.admit();
    assertEquals(1, quota.running());
    assertEquals(2, quota.jobs());
  }

  @Test
  void admitRejectsOnceRetainedJobLimitIsReached() {
    QueryJobQuota quota = new QueryJobQuota(settings(10, 1, 1_000));
    quota.admit();
    quota.releaseRunning();
    OpenSearchStatusException e = assertThrows(OpenSearchStatusException.class, quota::admit);
    assertEquals(RestStatus.TOO_MANY_REQUESTS, e.status());
    assertEquals(
        "Async query capacity on this node is exhausted"
            + " [plugins.ppl.async.max_retained_jobs]; retry later",
        e.getMessage());
    quota.releaseJob();
    quota.admit();
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
  void rollbackRevertsAdmission() {
    QueryJobQuota quota = new QueryJobQuota(settings(1, 1, 100));
    quota.admit();
    quota.rollback();
    assertEquals(0, quota.running());
    assertEquals(0, quota.jobs());
    quota.admit();
  }

  @Test
  void loweredLimitAppliesToNextAdmission() {
    MutableSettings settings = settings(5, 10, 1_000);
    QueryJobQuota quota = new QueryJobQuota(settings);
    quota.admit();
    quota.admit();
    settings.put(Settings.Key.PPL_ASYNC_NODE_CONCURRENT_RUNNING_QUERIES, 2);
    assertThrows(OpenSearchStatusException.class, quota::admit);
    assertEquals(2, quota.running());
  }

  @Test
  void concurrentAdmissionNeverExceedsTheLimit() throws Exception {
    int limit = 7;
    QueryJobQuota quota = new QueryJobQuota(settings(limit, 1_000, 1_000));
    int threads = 32;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<Boolean>> results = new ArrayList<>();
    try {
      for (int i = 0; i < threads; i++) {
        results.add(
            pool.submit(
                () -> {
                  start.await();
                  try {
                    quota.admit();
                    return true;
                  } catch (OpenSearchStatusException e) {
                    return false;
                  }
                }));
      }
      start.countDown();
      int admitted = 0;
      for (Future<Boolean> result : results) {
        admitted += result.get(5, TimeUnit.SECONDS) ? 1 : 0;
      }
      assertEquals(limit, admitted);
      assertEquals(limit, quota.running());
    } finally {
      pool.shutdownNow();
    }
  }

  static MutableSettings settings(int maxRunning, int maxJobs, long maxResultBytes) {
    MutableSettings settings = new MutableSettings();
    settings.put(Settings.Key.PPL_ASYNC_NODE_CONCURRENT_RUNNING_QUERIES, maxRunning);
    settings.put(Settings.Key.PPL_ASYNC_MAX_RETAINED_JOBS, maxJobs);
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
