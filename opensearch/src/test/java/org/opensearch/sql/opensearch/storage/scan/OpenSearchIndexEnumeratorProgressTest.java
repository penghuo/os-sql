/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.NonFallbackCalciteException;
import org.opensearch.sql.executor.progress.ProgressiveSourceProgress;
import org.opensearch.sql.executor.progress.QueryProgress;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.monitor.ResourceStatus;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.executor.progress.ProgressiveQueryContext;
import org.opensearch.sql.opensearch.request.OpenSearchQueryRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

/**
 * What a scan's close means for progress.
 *
 * <p>Linq4j closes a source from its own {@code finally}, which runs identically whether the query
 * is finishing or blowing up — and before the exception reaches the execution engine. So a close on
 * its own cannot be read as "the consumer stopped on purpose": the scan records it provisionally
 * and the engine confirms it only once draining succeeded. These tests pin each outcome.
 */
class OpenSearchIndexEnumeratorProgressTest {

  private static final double CEILING = QueryProgress.PUBLIC_CEILING.fractionDone();
  private static final double EPSILON = 1e-9;

  private ProgressiveSourceProgress progress;
  private ProgressiveQueryContext context;
  private ProgressiveQueryContext.Scope queryScope;
  private ProgressiveQueryContext.Binding binding;
  private Object planNode;

  @BeforeEach
  void setUp() {
    progress = new ProgressiveSourceProgress();
    context = ProgressiveQueryContext.create(progress);
    assertNotNull(context);
    planNode = new Object();
    progress.register(0, OptionalLong.of(1_000L));
    // A second, still-running source keeps the published value below the ceiling, so a wrongly
    // completed first
    // source is visible rather than masked by saturation.
    progress.register(1, OptionalLong.of(1_000L));
    context.registerPosition(planNode, 0, Map.of());
    context.registerPosition(new Object(), 1, Map.of());
    progress.seal();
    queryScope = ProgressiveQueryContext.open(context);
    binding = ProgressiveQueryContext.bindingFor(ProgressiveQueryContext.claimPosition(planNode));
    assertNotNull(binding);
  }

  @AfterEach
  void tearDown() {
    queryScope.close();
  }

  @Test
  @DisplayName("a consumer that stops early completes the source once draining succeeds")
  void consumerCloseCompletesAfterSuccessfulDraining() {
    OpenSearchIndexEnumerator enumerator = enumerator(pagedRequest(), healthyMonitor(), 10_000);

    // Read one row, then stop and close — what a coordinator-side `take`/`head` does to an inner
    // source.
    assertTrue(enumerator.moveNext());
    enumerator.close();

    // Nothing published yet: this close is indistinguishable from one during an abort.
    double afterClose = progress.current().fractionDone();

    context.completeConsumerClosedSources();

    // Confirmed: the source is done, which is half of this two-source plan.
    assertEquals(CEILING * 0.5, progress.current().fractionDone(), EPSILON);
    assertTrue(
        afterClose < CEILING * 0.5, "close alone must not publish completion, got " + afterClose);
  }

  @Test
  @DisplayName("a close after this scan's own failure is never completed")
  void closeAfterLocalFailureIsNotCompleted() {
    ResourceStatus healthy = mock(ResourceStatus.class);
    when(healthy.isHealthy()).thenReturn(true);
    ResourceStatus unhealthy = mock(ResourceStatus.class);
    when(unhealthy.isHealthy()).thenReturn(false);
    when(unhealthy.getFormattedDescription()).thenReturn("memory limit");
    ResourceMonitor monitor = mock(ResourceMonitor.class);
    // Healthy at construction, unhealthy on the first moveNext resource check.
    when(monitor.getStatus()).thenReturn(healthy, unhealthy);

    OpenSearchIndexEnumerator enumerator = enumerator(pagedRequest(), monitor, 10_000);

    assertThrows(NonFallbackCalciteException.class, enumerator::moveNext);
    enumerator.close();
    context.completeConsumerClosedSources();

    assertEquals(0.0, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("a close while execution is flagged aborted is never completed")
  void closeDuringAbortIsNotCompleted() {
    OpenSearchIndexEnumerator enumerator = enumerator(pagedRequest(), healthyMonitor(), 10_000);
    assertTrue(enumerator.moveNext());

    // A coordinator exception reached the engine, which flags the context before anything unwinds.
    context.markAborted();
    enumerator.close();
    context.completeConsumerClosedSources();

    assertEquals(0.0, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("a close on an interrupted thread is never completed")
  void closeWhileInterruptedIsNotCompleted() {
    OpenSearchIndexEnumerator enumerator = enumerator(pagedRequest(), healthyMonitor(), 10_000);
    assertTrue(enumerator.moveNext());

    // The query timeout interrupts the execution thread; teardown then runs interrupted.
    Thread.currentThread().interrupt();
    try {
      enumerator.close();
      context.completeConsumerClosedSources();
      assertEquals(0.0, progress.current().fractionDone(), EPSILON);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  @DisplayName("the scan's own query size limit completes the source immediately")
  void ownUpstreamLimitCompletesImmediately() {
    // maxResponseSize of 1: the scan itself is the thing that stops, which needs no confirmation.
    OpenSearchIndexEnumerator enumerator = enumerator(pagedRequest(), healthyMonitor(), 1);

    assertTrue(enumerator.moveNext());
    assertTrue(!enumerator.moveNext());

    assertEquals(CEILING * 0.5, progress.current().fractionDone(), EPSILON);
  }

  // ---------------------------------------------------------------- helpers

  private OpenSearchIndexEnumerator enumerator(
      OpenSearchRequest request, ResourceMonitor monitor, int maxResponseSize) {
    // Build the stub values first: Mockito rejects creating a mock inside an unfinished when(...)
    // chain.
    // Two full pages before exhaustion, so a test can read past a two-row quota without the scan
    // finishing first.
    OpenSearchResponse first = response();
    OpenSearchResponse second = response();
    OpenSearchClient client = mock(OpenSearchClient.class);
    when(client.getNodeClient()).thenReturn(Optional.empty());
    when(client.search(any())).thenReturn(first, second, OpenSearchResponse.EMPTY);
    return new OpenSearchIndexEnumerator(
        client, List.of("id"), maxResponseSize, 10, 10, request, monitor, binding);
  }

  private static OpenSearchRequest pagedRequest() {
    SearchSourceBuilder source = new SearchSourceBuilder().size(10);
    OpenSearchQueryRequest request = mock(OpenSearchQueryRequest.class);
    when(request.getSourceBuilder()).thenReturn(source);
    when(request.getPitId()).thenReturn("pit-id");
    return request;
  }

  private static ResourceMonitor healthyMonitor() {
    ResourceStatus status = mock(ResourceStatus.class);
    when(status.isHealthy()).thenReturn(true);
    ResourceMonitor monitor = mock(ResourceMonitor.class);
    when(monitor.getStatus()).thenReturn(status);
    return monitor;
  }

  /** A response carrying fewer hits than the page size, so one more page is still expected. */
  private static OpenSearchResponse response() {
    var rows =
        List.of(
            ExprValueUtils.tupleValue(Map.of("id", 1)),
            ExprValueUtils.tupleValue(Map.of("id", 2)),
            ExprValueUtils.tupleValue(Map.of("id", 3)));
    OpenSearchResponse response = mock(OpenSearchResponse.class);
    when(response.isEmpty()).thenReturn(false);
    when(response.isAggregationResponse()).thenReturn(false);
    when(response.isCompositeAggregationResponse()).thenReturn(false);
    when(response.isCountResponse()).thenReturn(false);
    when(response.getHitsSize()).thenReturn(10);
    when(response.getTotalHitsLowerBound()).thenReturn(OptionalLong.of(1_000L));
    when(response.iterator()).thenAnswer(invocation -> rows.iterator());
    return response;
  }
}
