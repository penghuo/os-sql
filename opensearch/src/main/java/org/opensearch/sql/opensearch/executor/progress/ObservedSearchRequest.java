/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import java.util.Map;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchTask;
import org.opensearch.core.tasks.TaskId;

/**
 * Attaches a progress listener to a search request.
 *
 * <p>The listener has to be in place when the search task is created, before the query phase
 * dispatches to the shards; setting it afterwards would miss {@code onListShards} and leave shard
 * progress with no denominator. Overriding {@code createTask} is the hook OpenSearch provides for
 * exactly that — the task manager calls it on the request while registering the search — and it is
 * the mechanism OpenSearch's own asynchronous search uses.
 */
public final class ObservedSearchRequest {

  private ObservedSearchRequest() {
    throw new AssertionError(
        ObservedSearchRequest.class.getCanonicalName()
            + " is a utility class and must not be initialized");
  }

  /**
   * Returns a request whose search task reports shard progress into {@code channel}, or {@code
   * request} itself when nothing is observing — so the synchronous path allocates nothing and
   * behaves exactly as it does today.
   */
  public static SearchRequest wrap(SearchRequest request, SourceChannel channel) {
    if (channel.isNoop()) {
      return request;
    }
    SearchProgressTracker tracker = new SearchProgressTracker(channel);
    SearchRequest observed =
        new SearchRequest(request) {
          @Override
          public SearchTask createTask(
              long id,
              String type,
              String action,
              TaskId parentTaskId,
              Map<String, String> headers) {
            SearchTask task = super.createTask(id, type, action, parentTaskId, headers);
            task.setProgressListener(tracker);
            return task;
          }
        };
    // SearchRequest's copy constructor copies search state but not the ActionRequest-level parent
    // task, so the
    // wrapper would reach the task manager unparented. That link is what ties this search to the
    // PPL task for
    // cancellation and resource accounting; losing it would make progress reporting silently break
    // query
    // cancellation.
    observed.setParentTask(request.getParentTask());
    return observed;
  }
}
