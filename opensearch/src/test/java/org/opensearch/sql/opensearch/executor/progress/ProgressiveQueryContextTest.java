/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.progress.ProgressObserver;
import org.opensearch.sql.executor.progress.ProgressUnit;
import org.opensearch.sql.executor.progress.ProgressiveSourceProgress;
import org.opensearch.sql.executor.progress.QueryProgress;
import org.opensearch.sql.executor.progress.SourceShardKey;

/**
 * Binding semantics that the source-attribution defects came down to: one physical occurrence is
 * one source, two occurrences over the same index are two, and neither the built request's index
 * names nor the order enumerators happen to start in can change that.
 */
class ProgressiveQueryContextTest {

  private static final double CEILING = QueryProgress.PUBLIC_CEILING.fractionDone();
  private static final double EPSILON = 1e-9;

  @Test
  @DisplayName("an unobserved query creates no context, so nothing is installed")
  void noopObserverCreatesNoContext() {
    assertNull(ProgressiveQueryContext.create(ProgressObserver.NOOP));
    try (ProgressiveQueryContext.Scope scope = ProgressiveQueryContext.open(null)) {
      assertNull(ProgressiveQueryContext.capture());
      assertSame(SourceChannel.NOOP, ProgressiveQueryContext.openChannel());
    }
  }

  @Test
  @DisplayName("one claim per position; the enumerable reuses it across re-enumeration")
  void positionClaimedOncePerPosition() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    ProgressiveQueryContext context = ProgressiveQueryContext.create(progress);
    assertNotNull(context);
    Object node = new Object();
    progress.register(0, OptionalLong.empty());
    context.registerPosition(node, 0, Map.of());
    progress.seal();

    try (ProgressiveQueryContext.Scope scope = ProgressiveQueryContext.open(context)) {
      // Code generation claims once for the position; the id it gets is then baked into the
      // generated call, so
      // every enumerator that position creates resolves the same binding.
      long claimed = ProgressiveQueryContext.claimPosition(node);
      assertEquals(0L, claimed);
      assertEquals(0L, ProgressiveQueryContext.bindingFor(claimed).sourceId());
      assertEquals(0L, ProgressiveQueryContext.bindingFor(claimed).sourceId());
    }
  }

  @Test
  @DisplayName("a node Calcite canonicalized into two positions hands out two sources")
  void canonicalizedNodeWithTwoPositions() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    ProgressiveQueryContext context = ProgressiveQueryContext.create(progress);
    assertNotNull(context);
    // A same-index equijoin: the planner collapses both scan positions onto one plan node, but
    // execution still
    // runs two independent searches, so the node is registered twice and must hand out two ids.
    Object sharedNode = new Object();
    progress.register(0, OptionalLong.empty());
    progress.register(1, OptionalLong.empty());
    context.registerPosition(sharedNode, 0, Map.of());
    context.registerPosition(sharedNode, 1, Map.of());
    progress.seal();

    try (ProgressiveQueryContext.Scope scope = ProgressiveQueryContext.open(context)) {
      long first = ProgressiveQueryContext.claimPosition(sharedNode);
      long second = ProgressiveQueryContext.claimPosition(sharedNode);
      assertNotEquals(first, second);

      // Finishing one position is half the plan's source work. Collapsing the two onto one id would
      // report the
      // whole plan done here.
      ProgressiveQueryContext.completeSource(
          ProgressiveQueryContext.bindingFor(first),
          org.opensearch.sql.executor.progress.CompletionReason.EXHAUSTED);
      assertEquals(CEILING * 0.5, progress.current().fractionDone(), EPSILON);

      ProgressiveQueryContext.completeSource(
          ProgressiveQueryContext.bindingFor(second),
          org.opensearch.sql.executor.progress.CompletionReason.EXHAUSTED);
      assertEquals(CEILING, progress.current().fractionDone(), EPSILON);
    }
  }

  @Test
  @DisplayName("two occurrences over the same index stay distinct sources")
  void twoOccurrencesOverOneIndexAreDistinct() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    ProgressiveQueryContext context = ProgressiveQueryContext.create(progress);
    assertNotNull(context);
    Object buildSide = new Object();
    Object probeSide = new Object();
    progress.register(0, OptionalLong.empty());
    progress.register(1, OptionalLong.empty());
    context.registerPosition(buildSide, 0, Map.of());
    context.registerPosition(probeSide, 1, Map.of());
    progress.seal();

    try (ProgressiveQueryContext.Scope scope = ProgressiveQueryContext.open(context)) {
      ProgressiveQueryContext.Binding build =
          ProgressiveQueryContext.bindingFor(ProgressiveQueryContext.claimPosition(buildSide));
      ProgressiveQueryContext.Binding probe =
          ProgressiveQueryContext.bindingFor(ProgressiveQueryContext.claimPosition(probeSide));
      assertNotNull(build);
      assertNotNull(probe);
      assertNotEquals(build.sourceId(), probe.sourceId());

      // The first occurrence finishing is half the plan's source work, not all of it. An
      // index-keyed claim
      // pool would have let this occurrence take the other's id and report the query nearly done.
      ProgressiveQueryContext.completeSource(
          build, org.opensearch.sql.executor.progress.CompletionReason.EXHAUSTED);
      assertEquals(CEILING * 0.5, progress.current().fractionDone(), EPSILON);
    }
  }

  @Test
  @DisplayName("a late-starting occurrence over an already-finished index still reports")
  void lateOccurrenceOverSameIndexStillReports() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    ProgressiveQueryContext context = ProgressiveQueryContext.create(progress);
    assertNotNull(context);
    Object first = new Object();
    Object second = new Object();
    progress.register(0, OptionalLong.empty());
    progress.register(1, OptionalLong.empty());
    context.registerPosition(first, 0, Map.of());
    context.registerPosition(second, 1, Map.of());
    progress.seal();

    try (ProgressiveQueryContext.Scope scope = ProgressiveQueryContext.open(context)) {
      // First occurrence runs and finishes; it is even re-enumerated, as a nested-loop inner side
      // would be.
      ProgressiveQueryContext.Binding firstBinding =
          ProgressiveQueryContext.bindingFor(ProgressiveQueryContext.claimPosition(first));
      ProgressiveQueryContext.bindingFor(ProgressiveQueryContext.claimPosition(first));
      ProgressiveQueryContext.completeSource(
          firstBinding, org.opensearch.sql.executor.progress.CompletionReason.EXHAUSTED);
      assertEquals(CEILING * 0.5, progress.current().fractionDone(), EPSILON);

      // Only now does the second occurrence start. Its id must still be available.
      ProgressiveQueryContext.Binding secondBinding =
          ProgressiveQueryContext.bindingFor(ProgressiveQueryContext.claimPosition(second));
      assertNotNull(secondBinding);
      assertNotEquals(firstBinding.sourceId(), secondBinding.sourceId());
      ProgressiveQueryContext.completeSource(
          secondBinding, org.opensearch.sql.executor.progress.CompletionReason.EXHAUSTED);
      assertEquals(CEILING, progress.current().fractionDone(), EPSILON);
    }
  }

  @Test
  @DisplayName("an unregistered occurrence binds to nothing rather than another source's counters")
  void unregisteredOccurrenceBindsToNothing() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    ProgressiveQueryContext context = ProgressiveQueryContext.create(progress);
    assertNotNull(context);
    progress.register(0, OptionalLong.empty());
    context.registerPosition(new Object(), 0, Map.of());
    progress.seal();

    try (ProgressiveQueryContext.Scope scope = ProgressiveQueryContext.open(context)) {
      assertNull(
          ProgressiveQueryContext.bindingFor(ProgressiveQueryContext.claimPosition(new Object())));
      assertNull(ProgressiveQueryContext.bindingFor(ProgressiveQueryContext.claimPosition(null)));
    }
  }

  @Test
  @DisplayName("a consumer close is confirmed only when draining succeeds")
  void consumerCloseConfirmedOnlyOnSuccess() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    ProgressiveQueryContext context = ProgressiveQueryContext.create(progress);
    assertNotNull(context);
    Object node = new Object();
    progress.register(0, OptionalLong.empty());
    progress.register(1, OptionalLong.empty());
    context.registerPosition(node, 0, Map.of());
    context.registerPosition(node, 1, Map.of());
    progress.seal();

    try (ProgressiveQueryContext.Scope scope = ProgressiveQueryContext.open(context)) {
      ProgressiveQueryContext.Binding capped =
          ProgressiveQueryContext.bindingFor(ProgressiveQueryContext.claimPosition(node));
      // A `head` closed this source early. Recording alone must not move the published value: a
      // close during an
      // abort is indistinguishable from this one.
      ProgressiveQueryContext.recordConsumerClose(capped);
      assertEquals(0.0, progress.current().fractionDone(), EPSILON);

      // Draining succeeded, which is the signal that the stop was deliberate.
      context.completeConsumerClosedSources();
      assertEquals(CEILING * 0.5, progress.current().fractionDone(), EPSILON);
    }
  }

  @Test
  @DisplayName("an aborted execution never confirms a consumer close")
  void abortedExecutionNeverConfirmsConsumerClose() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    ProgressiveQueryContext context = ProgressiveQueryContext.create(progress);
    assertNotNull(context);
    Object node = new Object();
    progress.register(0, OptionalLong.empty());
    context.registerPosition(node, 0, Map.of());
    progress.seal();

    try (ProgressiveQueryContext.Scope scope = ProgressiveQueryContext.open(context)) {
      ProgressiveQueryContext.Binding binding =
          ProgressiveQueryContext.bindingFor(ProgressiveQueryContext.claimPosition(node));
      context.markAborted();
      assertTrue(ProgressiveQueryContext.isAborted(binding));
      // A failing query never reaches the confirmation, so its abandoned source is not rounded up
      // to done.
      assertEquals(0.0, progress.current().fractionDone(), EPSILON);
    }
  }

  @Test
  @DisplayName("the channel carries the registered shard weights and the declared shape")
  void channelCarriesRegisteredFacts() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    ProgressiveQueryContext context = ProgressiveQueryContext.create(progress);
    assertNotNull(context);
    Object occurrence = new Object();
    SourceShardKey shard = new SourceShardKey("uuid", 0);
    progress.register(0, OptionalLong.of(5L));
    context.registerPosition(occurrence, 0, Map.of(shard, 5L));
    progress.seal();

    try (ProgressiveQueryContext.Scope scope = ProgressiveQueryContext.open(context)) {
      ProgressiveQueryContext.Binding binding =
          ProgressiveQueryContext.bindingFor(ProgressiveQueryContext.claimPosition(occurrence));
      ProgressiveQueryContext.reportShape(binding, ProgressUnit.BUCKET_COVERAGE, 0);
      try (ProgressiveQueryContext.Scope sourceScope = ProgressiveQueryContext.restore(binding)) {
        SourceChannel channel = ProgressiveQueryContext.openChannel();
        assertEquals(Map.of(shard, 5L), channel.shardDocs());
        assertEquals(ProgressUnit.BUCKET_COVERAGE, channel.declaredUnit());
      }
    }
  }

  @Test
  @DisplayName("each search on one source gets its own request id")
  void channelsGetDistinctRequestIds() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    ProgressiveQueryContext context = ProgressiveQueryContext.create(progress);
    assertNotNull(context);
    Object occurrence = new Object();
    progress.register(0, OptionalLong.empty());
    context.registerPosition(occurrence, 0, Map.of());
    progress.seal();

    try (ProgressiveQueryContext.Scope scope = ProgressiveQueryContext.open(context)) {
      ProgressiveQueryContext.Binding binding =
          ProgressiveQueryContext.bindingFor(ProgressiveQueryContext.claimPosition(occurrence));
      try (ProgressiveQueryContext.Scope sourceScope = ProgressiveQueryContext.restore(binding)) {
        SourceChannel first = ProgressiveQueryContext.openChannel();
        SourceChannel second = ProgressiveQueryContext.openChannel();
        // Distinct request ids are what stop page N's shard list from being read as a replay of
        // page N-1's.
        assertNotEquals(first, second);
        assertTrue(!first.isNoop() && !second.isNoop());
      }
    }
  }

  @Test
  @DisplayName("scopes restore the previous binding and leave pooled threads clean")
  void scopesRestorePreviousBinding() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    ProgressiveQueryContext context = ProgressiveQueryContext.create(progress);
    assertNotNull(context);
    Object occurrence = new Object();
    progress.register(0, OptionalLong.empty());
    context.registerPosition(occurrence, 0, Map.of());
    progress.seal();

    assertNull(ProgressiveQueryContext.capture());
    try (ProgressiveQueryContext.Scope queryScope = ProgressiveQueryContext.open(context)) {
      assertEquals(ProgressiveQueryContext.NO_SOURCE, ProgressiveQueryContext.capture().sourceId());
      ProgressiveQueryContext.Binding binding =
          ProgressiveQueryContext.bindingFor(ProgressiveQueryContext.claimPosition(occurrence));
      try (ProgressiveQueryContext.Scope sourceScope = ProgressiveQueryContext.restore(binding)) {
        assertEquals(0L, ProgressiveQueryContext.capture().sourceId());
      }
      assertEquals(ProgressiveQueryContext.NO_SOURCE, ProgressiveQueryContext.capture().sourceId());
    }
    assertNull(ProgressiveQueryContext.capture());
  }

  @Test
  @DisplayName("a captured binding replays onto a pool thread that inherits nothing")
  void capturedBindingReplaysOnAnotherThread() throws Exception {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    ProgressiveQueryContext context = ProgressiveQueryContext.create(progress);
    assertNotNull(context);
    Object occurrence = new Object();
    progress.register(0, OptionalLong.empty());
    context.registerPosition(occurrence, 0, Map.of());
    progress.seal();

    ProgressiveQueryContext.Binding binding;
    try (ProgressiveQueryContext.Scope queryScope = ProgressiveQueryContext.open(context)) {
      binding =
          ProgressiveQueryContext.bindingFor(ProgressiveQueryContext.claimPosition(occurrence));
    }
    assertNotNull(binding);

    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Callable<Boolean> task =
          () -> {
            // Pool thread starts with nothing bound, exactly like sql_background_io.
            boolean cleanBefore = ProgressiveQueryContext.capture() == null;
            boolean observedInside;
            try (ProgressiveQueryContext.Scope replay = ProgressiveQueryContext.restore(binding)) {
              observedInside = !ProgressiveQueryContext.openChannel().isNoop();
            }
            boolean cleanAfter = ProgressiveQueryContext.capture() == null;
            return cleanBefore && observedInside && cleanAfter;
          };
      assertTrue(pool.submit(task).get(10, TimeUnit.SECONDS));
    } finally {
      pool.shutdownNow();
    }
  }
}
