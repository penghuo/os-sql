/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.action.search.SearchProgressListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.SearchTask;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.executor.progress.ProgressUnit;
import org.opensearch.sql.executor.progress.ProgressiveSourceProgress;
import org.opensearch.sql.executor.progress.QueryProgress;
import org.opensearch.sql.executor.progress.SourceShardKey;
import org.opensearch.sql.opensearch.executor.progress.ProgressiveQueryContext;
import org.opensearch.sql.opensearch.executor.progress.SearchProgressTracker;
import org.opensearch.sql.opensearch.executor.progress.SearchProgressTrackerTestAccess;
import org.opensearch.sql.opensearch.executor.progress.SourceChannel;

/**
 * Covers the client's own search wiring: that the request it hands to the transport carries both
 * the progress listener and the parent task, and that the response it reports uses the source's
 * declared shape.
 *
 * <p>These go through {@code executeObservedSearch}, the production per-search body, rather than
 * asserting on the pieces in isolation — the defects worth a regression here were in the wiring
 * between those pieces.
 */
class OpenSearchNodeClientProgressTest {

  private static final String INDEX_UUID = "uuid-accounts";
  private static final double CEILING = QueryProgress.PUBLIC_CEILING.fractionDone();
  private static final double EPSILON = 1e-9;

  @Test
  @DisplayName("the observed request preserves the parent task the copy constructor drops")
  void observedRequestPreservesParentTask() {
    TaskId parent = new TaskId("owner_node", 123L);
    SearchRequest original = new SearchRequest().source(new SearchSourceBuilder().size(10));
    original.setParentTask(parent);

    SearchRequest observed = observedRequest(original, observedChannel());

    assertTrue(observed != original, "an observed query must be wrapped");
    assertEquals(parent, observed.getParentTask(), "parent task must survive wrapping");
    assertTrue(observed.getParentTask().isSet(), "parent task must remain set");
  }

  @Test
  @DisplayName("an unobserved request is handed to the transport untouched")
  void unobservedRequestNotWrapped() {
    SearchRequest original = new SearchRequest().source(new SearchSourceBuilder().size(10));
    assertSame(original, observedRequest(original, SourceChannel.NOOP));
  }

  @Test
  @DisplayName("createTask on the observed request attaches the progress listener")
  void observedRequestAttachesProgressListener() {
    SearchRequest observed =
        observedRequest(
            new SearchRequest().source(new SearchSourceBuilder().size(10)), observedChannel());

    SearchProgressListener listener = createTask(observed).getProgressListener();

    assertNotNull(listener);
    assertTrue(
        listener instanceof SearchProgressTracker,
        "expected the progress tracker, got " + listener.getClass());
  }

  @Test
  @DisplayName("shard weights registered before execution reach the listener the client attaches")
  void registeredShardWeightsReachTheAttachedListener() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    ProgressiveQueryContext context = ProgressiveQueryContext.create(progress);
    assertNotNull(context);

    Object occurrence = new Object();
    progress.register(0, OptionalLong.of(10_000_000L));
    context.registerPosition(occurrence, 0, Map.of(shard(0), 9_000_000L, shard(1), 1_000_000L));
    progress.seal();

    try (ProgressiveQueryContext.Scope queryScope = ProgressiveQueryContext.open(context)) {
      ProgressiveQueryContext.Binding binding =
          ProgressiveQueryContext.bindingFor(ProgressiveQueryContext.claimPosition(occurrence));
      assertNotNull(binding);
      ProgressiveQueryContext.reportShape(binding, ProgressUnit.SINGLE_REQUEST, 10);

      try (ProgressiveQueryContext.Scope sourceScope = ProgressiveQueryContext.restore(binding)) {
        SearchRequest observed =
            observedRequest(
                new SearchRequest().source(new SearchSourceBuilder().size(10)),
                ProgressiveQueryContext.openChannel());
        SearchProgressTracker tracker =
            (SearchProgressTracker) createTask(observed).getProgressListener();
        // Only the 1M-document shard reports.
        SearchProgressTrackerTestAccess.listShards(tracker, List.of(shard(0), shard(1)), List.of());
        SearchProgressTrackerTestAccess.queryResult(tracker, 1);
      }
    }

    // 1M of 10M documents, scaled by the public ceiling. Equal weighting would report 0.4 here,
    // which is what
    // the weights-never-reach-the-listener defect produced.
    assertEquals(CEILING * 0.1, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("a single-request source stays shard-weighted when its response lands")
  void singleRequestKeepsShardWeightingOnResponse() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    ProgressiveQueryContext context = ProgressiveQueryContext.create(progress);
    assertNotNull(context);

    Object occurrence = new Object();
    progress.register(0, OptionalLong.of(1_000L));
    context.registerPosition(occurrence, 0, Map.of(shard(0), 1_000L));
    progress.seal();

    try (ProgressiveQueryContext.Scope queryScope = ProgressiveQueryContext.open(context)) {
      ProgressiveQueryContext.Binding binding =
          ProgressiveQueryContext.bindingFor(ProgressiveQueryContext.claimPosition(occurrence));
      assertNotNull(binding);
      // Non-paged request over a 1000-document index, physical page size 10.
      ProgressiveQueryContext.reportShape(binding, ProgressUnit.SINGLE_REQUEST, 10);

      try (ProgressiveQueryContext.Scope sourceScope = ProgressiveQueryContext.restore(binding)) {
        SourceChannel channel = ProgressiveQueryContext.openChannel();
        assertEquals(ProgressUnit.SINGLE_REQUEST, channel.declaredUnit());

        SearchRequest observed =
            observedRequest(
                new SearchRequest().source(new SearchSourceBuilder().size(10)), channel, 10);
        SearchProgressTracker tracker =
            (SearchProgressTracker) createTask(observed).getProgressListener();
        SearchProgressTrackerTestAccess.listShards(tracker, List.of(shard(0)), List.of());
        SearchProgressTrackerTestAccess.queryResult(tracker, 0);
      }
    }

    // The client reported the response's rows. Progress must stay at full source coverage rather
    // than
    // collapsing to one page of an estimated hundred.
    assertEquals(CEILING, progress.current().fractionDone(), EPSILON);
  }

  // ---------------------------------------------------------------- helpers

  /** Runs the production per-search body, capturing the request handed to the transport. */
  private static SearchRequest observedRequest(SearchRequest original, SourceChannel channel) {
    return observedRequest(original, channel, 0);
  }

  private static SearchRequest observedRequest(
      SearchRequest original, SourceChannel channel, int hits) {
    AtomicReference<SearchRequest> captured = new AtomicReference<>();
    OpenSearchNodeClient client = new OpenSearchNodeClient(null);
    Function<SearchRequest, SearchResponse> transport =
        request -> {
          captured.set(request);
          return response(hits);
        };
    client.executeObservedSearch(original, channel, transport);
    return captured.get();
  }

  private static SourceChannel observedChannel() {
    return new SourceChannel(
        new ProgressiveSourceProgress(), 0L, 1L, Map.of(), ProgressUnit.SINGLE_REQUEST);
  }

  private static SearchTask createTask(SearchRequest request) {
    return request.createTask(
        1L, "transport", "indices:data/read/search", TaskId.EMPTY_TASK_ID, Map.of());
  }

  private static SourceShardKey shard(int id) {
    return new SourceShardKey(INDEX_UUID, id);
  }

  private static SearchResponse response(int hits) {
    SearchHit[] searchHits = new SearchHit[hits];
    for (int i = 0; i < hits; i++) {
      searchHits[i] = new SearchHit(i);
    }
    SearchHits searchHitsHolder =
        new SearchHits(searchHits, new TotalHits(1_000, TotalHits.Relation.EQUAL_TO), Float.NaN);
    return new SearchResponse(
        new SearchResponseSections(searchHitsHolder, null, null, false, null, null, 1),
        null,
        1,
        1,
        0,
        1L,
        new ShardSearchFailure[0],
        SearchResponse.Clusters.EMPTY);
  }
}
