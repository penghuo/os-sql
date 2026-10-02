/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.progress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Progress matrix for the published fraction, per the detailed design's §8.
 *
 * <p>Every case asserts the same invariants the REST contract leans on: finite, within {@code [0.0,
 * 0.8]} while running, never decreasing, and never reaching {@code 1.0} from source accounting
 * alone.
 */
class ProgressiveSourceProgressTest {

  private static final double CEILING = QueryProgress.PUBLIC_CEILING.fractionDone();
  private static final double EPSILON = 1e-9;

  private static final String UUID_A = "index-a-uuid";
  private static final String UUID_B = "index-b-uuid";

  // ---------------------------------------------------------------- registration and sealing

  @Test
  @DisplayName("publishes nothing until the source set is sealed")
  void unsealedPublishesZero() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    progress.register(0, OptionalLong.of(100));
    singleRequestSource(progress, 0, shard(UUID_A, 0), shard(UUID_A, 1));
    completeShard(progress, 0, shard(UUID_A, 0));

    assertEquals(0.0, progress.current().fractionDone(), EPSILON);

    progress.seal();
    assertTrue(progress.current().fractionDone() > 0.0);
  }

  @Test
  @DisplayName("rejects registration after sealing so the denominator cannot change mid-query")
  void registerAfterSealRejected() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    progress.seal();
    assertThrows(IllegalStateException.class, () -> progress.register(0, OptionalLong.empty()));
  }

  @Test
  @DisplayName("a source-less plan reports 0.0 while running")
  void sourcelessPlanReportsZero() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    progress.seal();
    assertEquals(0.0, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("registering the same source id twice keeps the first registration")
  void duplicateRegistrationIgnored() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    progress.register(0, OptionalLong.of(10));
    progress.register(0, OptionalLong.of(999));
    progress.seal();
    assertEquals(1, progress.sourceCount());
  }

  @Test
  @DisplayName("events for an unregistered source are dropped, not fatal")
  void unknownSourceIgnored() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    progress.register(0, OptionalLong.empty());
    progress.seal();
    singleRequestSource(progress, 42, shard(UUID_A, 0));
    completeShard(progress, 42, shard(UUID_A, 0));
    assertEquals(0.0, progress.current().fractionDone(), EPSILON);
  }

  // ---------------------------------------------------------------- §8.3 single-request hit search

  @Test
  @DisplayName("§8.3 weights shard completion by primary document counts")
  void singleRequestWeightsByDocCount() {
    ProgressiveSourceProgress progress = sealedSingleSource(OptionalLong.of(10_000_000L));
    progress.accept(
        new SourceProgressEvent.ShardsListed(
            0,
            1,
            List.of(
                new ShardWeight(shard(UUID_A, 0), 9_000_000L),
                new ShardWeight(shard(UUID_A, 1), 1_000_000L)),
            Set.of()));
    declareShape(progress, 0, ProgressUnit.SINGLE_REQUEST, 10);

    completeShard(progress, 0, shard(UUID_A, 1));

    // 1M of 10M documents: the small shard finishing is 10% of the source, not 50%.
    assertEquals(CEILING * 0.1, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("§8.3 falls back to equal shard weight when no estimate is available")
  void singleRequestEqualWeightWithoutEstimate() {
    ProgressiveSourceProgress progress = sealedSingleSource(OptionalLong.empty());
    singleRequestSource(progress, 0, shard(UUID_A, 0), shard(UUID_A, 1), shard(UUID_A, 2));
    completeShard(progress, 0, shard(UUID_A, 0));

    assertEquals(CEILING / 3.0, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("§8.3 skipped shards contribute no work in either direction")
  void skippedShardsExcluded() {
    ProgressiveSourceProgress progress = sealedSingleSource(OptionalLong.empty());
    progress.accept(
        new SourceProgressEvent.ShardsListed(
            0,
            1,
            List.of(
                new ShardWeight(shard(UUID_A, 0), 0L),
                new ShardWeight(shard(UUID_A, 1), 0L),
                new ShardWeight(shard(UUID_A, 2), 0L)),
            Set.of(shard(UUID_A, 2))));
    declareShape(progress, 0, ProgressUnit.SINGLE_REQUEST, 10);

    completeShard(progress, 0, shard(UUID_A, 0));
    completeShard(progress, 0, shard(UUID_A, 1));

    // Two of two participating shards, not two of three.
    assertEquals(CEILING, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("a shard failure counts as completion so progress cannot stall")
  void shardFailureCountsAsCompletion() {
    ProgressiveSourceProgress progress = sealedSingleSource(OptionalLong.empty());
    singleRequestSource(progress, 0, shard(UUID_A, 0), shard(UUID_A, 1));
    completeShard(progress, 0, shard(UUID_A, 0));
    completeShard(progress, 0, shard(UUID_A, 1));
    assertEquals(CEILING, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("repeated shard completions are idempotent")
  void duplicateShardCompletionIdempotent() {
    ProgressiveSourceProgress progress = sealedSingleSource(OptionalLong.empty());
    singleRequestSource(progress, 0, shard(UUID_A, 0), shard(UUID_A, 1));
    completeShard(progress, 0, shard(UUID_A, 0));
    completeShard(progress, 0, shard(UUID_A, 0));
    assertEquals(CEILING * 0.5, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName(
      "a single-request response closes the shard callback gap without waiting for SourceCompleted")
  void singleRequestResponseClosesCallbackGap() {
    ProgressiveSourceProgress progress = sealedSingleSource(OptionalLong.of(1_000L));
    singleRequestSource(progress, 0, shard(UUID_A, 0), shard(UUID_A, 1));
    // Only one of the two query-result callbacks was observed; the other was dropped.
    completeShard(progress, 0, shard(UUID_A, 0));

    progress.accept(
        new SourceProgressEvent.RowsObserved(0, 1, ProgressUnit.PAGED_ROWS, 10, 10, 1_000));

    // The response proves both shards finished, so the source is fully covered. It must not be
    // dragged down
    // to one page of an estimated hundred by the report's unit.
    assertEquals(CEILING, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("a declared shape survives a response that would otherwise relabel it")
  void declaredShapeWinsOverReportedUnit() {
    ProgressiveSourceProgress progress = sealedSingleSource(OptionalLong.of(1_000L));
    singleRequestSource(progress, 0, shard(UUID_A, 0));
    completeShard(progress, 0, shard(UUID_A, 0));
    double beforeResponse = progress.current().fractionDone();

    progress.accept(
        new SourceProgressEvent.RowsObserved(0, 1, ProgressUnit.PAGED_ROWS, 10, 10, 1_000));

    assertEquals(CEILING, beforeResponse, EPSILON);
    assertEquals(CEILING, progress.current().fractionDone(), EPSILON);
  }

  // ---------------------------------------------------------------- §8.5 PIT-paged hit search

  @Test
  @DisplayName("§8.5 paged search advances by expected pages")
  void pagedSearchAdvancesByPage() {
    ProgressiveSourceProgress progress = sealedSingleSource(OptionalLong.of(1_000L));
    declareShape(progress, 0, ProgressUnit.PAGED_ROWS, 100);

    List<Double> observed = new ArrayList<>();
    for (int page = 1; page <= 5; page++) {
      listShards(progress, 0, page, shard(UUID_A, 0));
      completeShard(progress, 0, page, shard(UUID_A, 0));
      observed.add(progress.current().fractionDone());
      progress.accept(
          new SourceProgressEvent.RowsObserved(0, page, ProgressUnit.PAGED_ROWS, 100, 100, 1_000));
      observed.add(progress.current().fractionDone());
    }

    assertMonotonic(observed);
    // 1000 docs over a 100-row page is 10 expected pages; 5 landed.
    assertEquals(CEILING * 0.5, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("§8.5 a landing page does not double-count the shards that produced it")
  void pageBoundaryIsContinuous() {
    ProgressiveSourceProgress progress = sealedSingleSource(OptionalLong.of(1_000L));
    declareShape(progress, 0, ProgressUnit.PAGED_ROWS, 100);

    listShards(progress, 0, 1, shard(UUID_A, 0));
    completeShard(progress, 0, 1, shard(UUID_A, 0));
    double allShardsReported = progress.current().fractionDone();

    progress.accept(
        new SourceProgressEvent.RowsObserved(0, 1, ProgressUnit.PAGED_ROWS, 100, 100, 1_000));
    double pageLanded = progress.current().fractionDone();

    listShards(progress, 0, 2, shard(UUID_A, 0));
    double nextPageOpened = progress.current().fractionDone();

    // One page of work, counted once: in-flight before the response, completed after, and unchanged
    // when the
    // next page re-lists its shards.
    assertEquals(CEILING * 0.1, allShardsReported, EPSILON);
    assertEquals(CEILING * 0.1, pageLanded, EPSILON);
    assertEquals(CEILING * 0.1, nextPageOpened, EPSILON);
  }

  @Test
  @DisplayName("§8.5 uses a TotalHits value from the response when no estimate exists")
  void pagedSearchUsesObservedTotal() {
    ProgressiveSourceProgress progress = sealedSingleSource(OptionalLong.empty());
    declareShape(progress, 0, ProgressUnit.PAGED_ROWS, 100);
    progress.accept(
        new SourceProgressEvent.RowsObserved(0, 1, ProgressUnit.PAGED_ROWS, 100, 100, 400));

    // 400 hits over a 100-row page is 4 expected pages; 1 landed.
    assertEquals(CEILING * 0.25, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("§8.5 adaptive fallback assumes one more page when nothing is known")
  void pagedSearchAdaptiveFallback() {
    ProgressiveSourceProgress progress = sealedSingleSource(OptionalLong.empty());
    declareShape(progress, 0, ProgressUnit.PAGED_ROWS, 100);

    progress.accept(
        new SourceProgressEvent.RowsObserved(0, 1, ProgressUnit.PAGED_ROWS, 100, 100, 0));
    assertEquals(CEILING * 0.5, progress.current().fractionDone(), EPSILON);

    progress.accept(
        new SourceProgressEvent.RowsObserved(0, 2, ProgressUnit.PAGED_ROWS, 100, 100, 0));
    assertEquals(CEILING * (200.0 / 300.0), progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("§8.5 a scan that runs past a TotalHits lower bound stays within range")
  void pagedSearchOvershootClamped() {
    ProgressiveSourceProgress progress = sealedSingleSource(OptionalLong.empty());
    declareShape(progress, 0, ProgressUnit.PAGED_ROWS, 100);
    for (int page = 1; page <= 30; page++) {
      progress.accept(
          new SourceProgressEvent.RowsObserved(0, page, ProgressUnit.PAGED_ROWS, 100, 100, 1_000));
      assertWithinRunningRange(progress.current());
    }
  }

  // ---------------------------------------------------------------- §8.6 Composite aggregation

  @Test
  @DisplayName("§8.6 composite coverage divides bucket doc_count by the index estimate")
  void compositeUsesCoverageOverEstimate() {
    ProgressiveSourceProgress progress = sealedSingleSource(OptionalLong.of(1_000L));
    declareShape(progress, 0, ProgressUnit.BUCKET_COVERAGE, 0);

    progress.accept(
        new SourceProgressEvent.RowsObserved(0, 1, ProgressUnit.BUCKET_COVERAGE, 250, 0, 0));
    assertEquals(CEILING * 0.25, progress.current().fractionDone(), EPSILON);

    progress.accept(
        new SourceProgressEvent.RowsObserved(0, 2, ProgressUnit.BUCKET_COVERAGE, 250, 0, 0));
    assertEquals(CEILING * 0.5, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("§8.6 adaptive fallback assumes one more page the size of the last")
  void compositeAdaptiveFallback() {
    ProgressiveSourceProgress progress = sealedSingleSource(OptionalLong.empty());
    declareShape(progress, 0, ProgressUnit.BUCKET_COVERAGE, 0);

    progress.accept(
        new SourceProgressEvent.RowsObserved(0, 1, ProgressUnit.BUCKET_COVERAGE, 100, 0, 0));
    assertEquals(CEILING * 0.5, progress.current().fractionDone(), EPSILON);

    progress.accept(
        new SourceProgressEvent.RowsObserved(0, 2, ProgressUnit.BUCKET_COVERAGE, 100, 0, 0));
    assertEquals(CEILING * (200.0 / 300.0), progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("§8.6 multi-valued group keys can over-count coverage; the clamp absorbs it")
  void compositeOverCountClamped() {
    ProgressiveSourceProgress progress = sealedSingleSource(OptionalLong.of(100L));
    declareShape(progress, 0, ProgressUnit.BUCKET_COVERAGE, 0);
    progress.accept(
        new SourceProgressEvent.RowsObserved(0, 1, ProgressUnit.BUCKET_COVERAGE, 10_000, 0, 0));

    assertEquals(CEILING, progress.current().fractionDone(), EPSILON);
    assertWithinRunningRange(progress.current());
  }

  // ---------------------------------------------------------------- §8.7 multiple sources

  @Test
  @DisplayName("§8.7 combines sources weighted by their document estimates")
  void multipleSourcesWeightedByEstimate() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    progress.register(0, OptionalLong.of(9_000_000L));
    progress.register(1, OptionalLong.of(1_000_000L));
    progress.seal();

    // Source 0 half done.
    progress.accept(
        new SourceProgressEvent.ShardsListed(
            0,
            1,
            List.of(new ShardWeight(shard(UUID_A, 0), 1L), new ShardWeight(shard(UUID_A, 1), 1L)),
            Set.of()));
    declareShape(progress, 0, ProgressUnit.SINGLE_REQUEST, 10);
    completeShard(progress, 0, shard(UUID_A, 0));
    // Source 1 complete.
    progress.accept(new SourceProgressEvent.SourceCompleted(1, CompletionReason.EXHAUSTED));

    // (9M * 0.5 + 1M * 1.0) / 10M = 0.55, scaled by 0.8 = 0.44.
    assertEquals(0.44, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("§8.1 falls back to equal weight when any source has no estimate")
  void mixedEstimatesUseEqualWeight() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    progress.register(0, OptionalLong.of(9_000_000L));
    progress.register(1, OptionalLong.empty());
    progress.seal();

    progress.accept(new SourceProgressEvent.SourceCompleted(1, CompletionReason.EXHAUSTED));

    // One of two sources complete under equal weight.
    assertEquals(CEILING * 0.5, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("§8.1 all-zero estimates use equal weight rather than dividing by zero")
  void zeroEstimatesUseEqualWeight() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    progress.register(0, OptionalLong.of(0L));
    progress.register(1, OptionalLong.of(0L));
    progress.seal();

    progress.accept(new SourceProgressEvent.SourceCompleted(0, CompletionReason.EXHAUSTED));

    assertEquals(CEILING * 0.5, progress.current().fractionDone(), EPSILON);
    assertWithinRunningRange(progress.current());
  }

  @Test
  @DisplayName("a self-join's two occurrences over one index track independently")
  void selfJoinTracksTwoOccurrences() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    progress.register(0, OptionalLong.of(1_000L));
    progress.register(1, OptionalLong.of(1_000L));
    progress.seal();

    progress.accept(new SourceProgressEvent.SourceCompleted(0, CompletionReason.EXHAUSTED));
    assertEquals(CEILING * 0.5, progress.current().fractionDone(), EPSILON);

    progress.accept(new SourceProgressEvent.SourceCompleted(1, CompletionReason.UPSTREAM_LIMIT));
    assertEquals(CEILING, progress.current().fractionDone(), EPSILON);
  }

  // ---------------------------------------------------------------- global invariants

  @Test
  @DisplayName("draining every source reports the ceiling, never 1.0")
  void drainedSourcesSaturateAtCeiling() {
    ProgressiveSourceProgress progress = sealedSingleSource(OptionalLong.of(10L));
    progress.accept(new SourceProgressEvent.SourceCompleted(0, CompletionReason.EXHAUSTED));

    assertEquals(CEILING, progress.current().fractionDone(), EPSILON);
    assertTrue(progress.current().fractionDone() < 1.0);
  }

  @Test
  @DisplayName("a source re-enumerated from zero cannot pull the published value down")
  void reEnumerationCannotRegress() {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    progress.register(0, OptionalLong.empty());
    progress.register(1, OptionalLong.empty());
    progress.seal();

    progress.accept(new SourceProgressEvent.SourceCompleted(0, CompletionReason.EXHAUSTED));
    double high = progress.current().fractionDone();

    // Source 1 starts a fresh search; its shard view resets to nothing observed.
    listShards(progress, 1, 7, shard(UUID_B, 0), shard(UUID_B, 1));
    assertTrue(progress.current().fractionDone() >= high);
  }

  @Test
  @DisplayName("concurrent producers and pollers never observe a decrease")
  void concurrentPollsAreMonotonic() throws Exception {
    ProgressiveSourceProgress progress = sealedSingleSource(OptionalLong.empty());
    declareShape(progress, 0, ProgressUnit.PAGED_ROWS, 10);

    ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      CountDownLatch start = new CountDownLatch(1);
      AtomicReference<Double> violation = new AtomicReference<>();
      List<java.util.concurrent.Future<?>> tasks = new ArrayList<>();
      tasks.add(
          pool.submit(
              () -> {
                awaitQuietly(start);
                for (int page = 1; page <= 500; page++) {
                  progress.accept(
                      new SourceProgressEvent.RowsObserved(
                          0, page, ProgressUnit.PAGED_ROWS, 10, 10, 0));
                }
              }));
      for (int reader = 0; reader < 3; reader++) {
        tasks.add(
            pool.submit(
                () -> {
                  awaitQuietly(start);
                  double previous = 0.0;
                  for (int i = 0; i < 2_000; i++) {
                    double seen = progress.current().fractionDone();
                    if (seen < previous) {
                      violation.set(seen);
                    }
                    previous = seen;
                  }
                }));
      }
      start.countDown();
      for (java.util.concurrent.Future<?> task : tasks) {
        task.get(30, TimeUnit.SECONDS);
      }
      assertEquals(null, violation.get(), "published fraction decreased");
      assertWithinRunningRange(progress.current());
    } finally {
      pool.shutdownNow();
    }
  }

  // ---------------------------------------------------------------- helpers

  private static SourceShardKey shard(String uuid, int id) {
    return new SourceShardKey(uuid, id);
  }

  private static ProgressiveSourceProgress sealedSingleSource(OptionalLong estimatedDocs) {
    ProgressiveSourceProgress progress = new ProgressiveSourceProgress();
    progress.register(0, estimatedDocs);
    progress.seal();
    return progress;
  }

  private static void declareShape(
      ProgressiveSourceProgress progress, long sourceId, ProgressUnit unit, long pageSize) {
    progress.accept(new SourceProgressEvent.SourceShapeObserved(sourceId, unit, pageSize));
  }

  private static void singleRequestSource(
      ProgressiveSourceProgress progress, long sourceId, SourceShardKey... shards) {
    listShards(progress, sourceId, 1, shards);
    declareShape(progress, sourceId, ProgressUnit.SINGLE_REQUEST, 10);
  }

  private static void listShards(
      ProgressiveSourceProgress progress, long sourceId, long requestId, SourceShardKey... shards) {
    List<ShardWeight> weights = new ArrayList<>();
    for (SourceShardKey shard : shards) {
      weights.add(new ShardWeight(shard, 0L));
    }
    progress.accept(new SourceProgressEvent.ShardsListed(sourceId, requestId, weights, Set.of()));
  }

  private static void completeShard(
      ProgressiveSourceProgress progress, long sourceId, SourceShardKey shard) {
    completeShard(progress, sourceId, 1, shard);
  }

  private static void completeShard(
      ProgressiveSourceProgress progress, long sourceId, long requestId, SourceShardKey shard) {
    progress.accept(new SourceProgressEvent.ShardCompleted(sourceId, requestId, shard));
  }

  private static void assertWithinRunningRange(QueryProgress progress) {
    assertTrue(Double.isFinite(progress.fractionDone()), "fraction must be finite");
    assertTrue(progress.fractionDone() >= 0.0, "fraction must not be negative");
    assertTrue(
        progress.fractionDone() <= CEILING,
        "running fraction must not exceed " + CEILING + ", got " + progress.fractionDone());
  }

  private static void assertMonotonic(List<Double> observed) {
    for (int i = 1; i < observed.size(); i++) {
      assertTrue(
          observed.get(i) >= observed.get(i - 1),
          "fraction decreased from " + observed.get(i - 1) + " to " + observed.get(i));
    }
    for (Double value : observed) {
      assertWithinRunningRange(new QueryProgress(value));
    }
  }

  private static void awaitQuietly(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
