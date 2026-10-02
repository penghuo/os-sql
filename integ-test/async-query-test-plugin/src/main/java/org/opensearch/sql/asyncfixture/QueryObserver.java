/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.asyncfixture;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolStats;

/**
 * Node-local observer for the searches one async PPL query sends to an armed index.
 *
 * <p>Once armed, it captures the task of the next PPL request, counts searches that target the
 * index directly or through a point-in-time created on it, and holds the first such search until
 * the test releases it. Batches run on a background pool without a parent task, so searches are
 * matched by index and PIT rather than by task.
 */
final class QueryObserver implements ActionFilter {

  static final String PPL_ACTION = "cluster:admin/opensearch/ppl";
  static final List<String> SQL_POOLS =
      List.of("sql-worker", "sql-complex-worker", "sql_background_io");
  private static final TimeValue HOLD_TIMEOUT = TimeValue.timeValueSeconds(30);

  private volatile ThreadPool threadPool;
  private String index;
  private boolean holdNextSearch;
  private Task pplTask;
  private int searches;
  private final Set<String> pits = new LinkedHashSet<>();
  private HeldSearch held;

  void setThreadPool(ThreadPool threadPool) {
    this.threadPool = threadPool;
  }

  @Override
  public int order() {
    // Run after the security filter so a held request has already been authorized.
    return Integer.MAX_VALUE;
  }

  @Override
  public <Request extends ActionRequest, Response extends ActionResponse> void apply(
      Task task,
      String action,
      Request request,
      ActionRequestMetadata<Request, Response> actionRequestMetadata,
      ActionListener<Response> listener,
      ActionFilterChain<Request, Response> chain) {
    synchronized (this) {
      if (index == null) {
        // Not armed: fall through to proceed below.
      } else if (PPL_ACTION.equals(action)) {
        if (pplTask == null) {
          pplTask = task;
        }
      } else if (CreatePitAction.NAME.equals(action)
          && request instanceof CreatePitRequest pitRequest
          && targetsIndex(pitRequest.indices())) {
        chain.proceed(task, action, request, recordPit(listener));
        return;
      } else if (SearchAction.NAME.equals(action)
          && request instanceof SearchRequest search
          && targetsIndex(search)) {
        searches++;
        if (holdNextSearch) {
          holdNextSearch = false;
          HeldSearch heldSearch = new HeldSearch(task, action, request, listener, chain);
          held = heldSearch;
          threadPool.schedule(
              () -> releaseIfHeld(heldSearch), HOLD_TIMEOUT, ThreadPool.Names.GENERIC);
          return;
        }
      }
    }
    chain.proceed(task, action, request, listener);
  }

  /** Arms the observer for {@code targetIndex}, dropping any earlier state. */
  synchronized void arm(String targetIndex, boolean hold) {
    release(false);
    index = targetIndex;
    holdNextSearch = hold;
    pplTask = null;
    searches = 0;
    pits.clear();
  }

  /** Disarms the observer, letting any held search proceed. */
  synchronized void disarm() {
    release(false);
    index = null;
    holdNextSearch = false;
  }

  /**
   * Lets the held search proceed, or fails it when {@code fail} is true. Runs on the generic pool
   * with the request's original thread context. Returns whether a search was held.
   */
  synchronized boolean release(boolean fail) {
    HeldSearch search = held;
    if (search == null) {
      return false;
    }
    held = null;
    threadPool.generic().execute(() -> search.resume(fail));
    return true;
  }

  private synchronized void releaseIfHeld(HeldSearch search) {
    if (held == search) {
      release(false);
    }
  }

  synchronized Map<String, Object> status() {
    Map<String, Object> status = new LinkedHashMap<>();
    status.put("index", index);
    status.put("held", held != null);
    status.put("task_captured", pplTask != null);
    status.put("task_cancelled", pplTask instanceof CancellableTask t && t.isCancelled());
    status.put("searches", searches);
    status.put("pits", List.copyOf(pits));
    Map<String, Integer> active = new LinkedHashMap<>();
    for (ThreadPoolStats.Stats stats : threadPool.stats()) {
      if (SQL_POOLS.contains(stats.getName())) {
        active.put(stats.getName(), stats.getActive());
      }
    }
    status.put("active", active);
    return status;
  }

  private boolean targetsIndex(String[] indices) {
    return indices != null && Arrays.asList(indices).contains(index);
  }

  private boolean targetsIndex(SearchRequest search) {
    if (targetsIndex(search.indices())) {
      return true;
    }
    return search.source() != null
        && search.source().pointInTimeBuilder() != null
        && pits.contains(search.source().pointInTimeBuilder().getId());
  }

  private <Response> ActionListener<Response> recordPit(ActionListener<Response> listener) {
    return ActionListener.wrap(
        response -> {
          if (response instanceof CreatePitResponse pit) {
            synchronized (this) {
              pits.add(pit.getId());
            }
          }
          listener.onResponse(response);
        },
        listener::onFailure);
  }

  private final class HeldSearch {
    private final Task task;
    private final String action;
    private final ActionRequest request;
    private final ActionListener<ActionResponse> listener;
    private final ActionFilterChain<ActionRequest, ActionResponse> chain;
    private final ThreadContext.StoredContext context;

    @SuppressWarnings("unchecked")
    <Request extends ActionRequest, Response extends ActionResponse> HeldSearch(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain) {
      this.task = task;
      this.action = action;
      this.request = request;
      this.listener = (ActionListener<ActionResponse>) listener;
      this.chain = (ActionFilterChain<ActionRequest, ActionResponse>) chain;
      this.context = threadPool.getThreadContext().newStoredContext(false);
    }

    void resume(boolean fail) {
      try (ThreadContext.StoredContext ignored = threadPool.getThreadContext().stashContext()) {
        context.restore();
        if (fail) {
          listener.onFailure(
              new IllegalStateException("search failed by async query test fixture"));
        } else {
          chain.proceed(task, action, request, listener);
        }
      }
    }
  }
}
