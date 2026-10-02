/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.progress.ProgressUnit;
import org.opensearch.sql.executor.progress.ProgressiveSourceProgress;
import org.opensearch.sql.executor.progress.QueryProgress;
import org.opensearch.sql.executor.progress.SourceProgressEvent;

/**
 * The limit-driven completion signal, which is what §8.5's "intentional upstream limit" means in
 * practice.
 *
 * <p>The timing assertion is the point of this class: a capped source must reach its full share at
 * the instant the limit has its rows, while the plan's other sources have done nothing. Checking
 * only the terminal value cannot establish that, because a successful job reports 1.0 regardless.
 */
class ProgressLimitSignalTest {

  private static final double CEILING = QueryProgress.PUBLIC_CEILING.fractionDone();
  private static final double EPSILON = 1e-9;

  private ProgressiveSourceProgress progress;
  private ProgressiveQueryContext context;
  private ProgressiveQueryContext.Scope scope;

  @BeforeEach
  void setUp() {
    progress = new ProgressiveSourceProgress();
    context = ProgressiveQueryContext.create(progress);
    assertNotNull(context);
    // Two sources: a capped inner read and an outer read that has not started. Without the second
    // source the first
    // would saturate the ceiling and a premature completion would be invisible.
    progress.register(0, OptionalLong.of(1_000L));
    progress.register(1, OptionalLong.of(1_000L));
    progress.seal();
    scope = ProgressiveQueryContext.open(context);
  }

  @AfterEach
  void tearDown() {
    scope.close();
  }

  @Test
  @DisplayName(
      "reaching the limit's output quota completes its sources before any other source runs")
  void quotaExhaustionCompletesBeneathSourcesImmediately() {
    // The inner source has read a little — one page of an estimated hundred — and the outer has
    // read nothing.
    observeInnerPage();
    double beforeLimit = progress.current().fractionDone();
    assertTrue(
        beforeLimit < CEILING * 0.5, "the inner source is not finished yet, got " + beforeLimit);

    Enumerator<Integer> limited =
        signal(2, 0L).observe(Linq4j.asEnumerable(List.of(10, 20, 30, 40))).enumerator();

    assertTrue(limited.moveNext());
    assertTrue(
        progress.current().fractionDone() < CEILING * 0.5,
        "one row of a two-row quota is not an exhausted limit");

    assertTrue(limited.moveNext());

    // Asserted here: the limit has its rows, nothing has been closed, no other source has issued a
    // search, and the
    // query is nowhere near finished. This is the moment the design requires.
    assertEquals(
        CEILING * 0.5,
        progress.current().fractionDone(),
        EPSILON,
        "a satisfied limit must complete its sources at once");
  }

  @Test
  @DisplayName("a limit over two sources completes both")
  void quotaExhaustionCompletesEverySourceBeneath() {
    Enumerator<Integer> limited =
        signal(1, 0L, 1L).observe(Linq4j.asEnumerable(List.of(1, 2, 3))).enumerator();

    assertTrue(limited.moveNext());

    assertEquals(CEILING, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("an input that runs dry before the quota completes nothing")
  void unreachedQuotaCompletesNothing() {
    Enumerator<Integer> limited =
        signal(5, 0L).observe(Linq4j.asEnumerable(List.of(1, 2))).enumerator();

    while (limited.moveNext()) {
      // drain
    }
    limited.close();

    // The limit never filled, so this was end-of-input, not an intentional stop. The scan reports
    // its own
    // exhaustion; the limit must not speak for it.
    assertEquals(0.0, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("an exception unwinding through the counter completes nothing")
  void failureDuringIterationCompletesNothing() {
    Enumerable<Integer> exploding = failingAfter(1);
    Enumerator<Integer> limited = signal(5, 0L).observe(exploding).enumerator();

    assertTrue(limited.moveNext());
    assertThrows(IllegalStateException.class, limited::moveNext);
    limited.close();

    assertEquals(0.0, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("close alone completes nothing")
  void closeCompletesNothing() {
    Enumerator<Integer> limited =
        signal(5, 0L).observe(Linq4j.asEnumerable(List.of(1, 2, 3, 4, 5, 6))).enumerator();

    assertTrue(limited.moveNext());
    limited.close();

    // Linq4j closes a source from its own finally block whether the query is finishing or failing,
    // so close is not
    // evidence of anything on its own.
    assertEquals(0.0, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("a visited zero-row limit completes its sources: it is already at its quota")
  void zeroQuotaCompletesOnVisit() {
    // The signal wraps the limit's output, and Calcite's take(0) produces an empty enumerable.
    Enumerator<Integer> limited =
        signal(0, 0L).observe(Linq4j.<Integer>asEnumerable(List.of())).enumerator();

    // A limit that asks for nothing is satisfied the moment it is reached. Bypassing it would leave
    // the work beneath
    // it pending until the query ended.
    assertFalse(limited.moveNext());
    assertEquals(CEILING * 0.5, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("a zero-row limit that is never visited reports nothing")
  void zeroQuotaNeverVisitedReportsNothing() {
    signal(0, 0L).observe(Linq4j.<Integer>asEnumerable(List.of()));
    // Wrapping is not visiting. A branch the plan never reaches has no intentional stop to report.
    assertEquals(0.0, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("a zero-row limit whose delegate fails reports nothing")
  void zeroQuotaFailureReportsNothing() {
    Enumerator<Integer> limited = signal(0, 0L).observe(failingAfter(0)).enumerator();

    assertThrows(IllegalStateException.class, limited::moveNext);
    limited.close();

    assertEquals(0.0, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("a limit with no source beneath it leaves the enumerable untouched")
  void noSourcesIsTransparent() {
    Enumerable<Integer> source = Linq4j.asEnumerable(List.of(1, 2, 3));
    assertSame(source, signal(2).observe(source));
  }

  @Test
  @DisplayName("the counter passes rows through unchanged")
  void rowsArePreserved() {
    List<Integer> rows = List.of(1, 2, 3, 4, 5);
    Enumerator<Integer> limited = signal(2, 0L).observe(Linq4j.asEnumerable(rows)).enumerator();

    List<Integer> seen = new ArrayList<>();
    while (limited.moveNext()) {
      seen.add(limited.current());
    }
    limited.close();

    // The signal observes; Calcite's own skip/take decides what rows exist. Nothing is dropped or
    // reordered here.
    assertEquals(rows, seen);
  }

  @Test
  @DisplayName("re-enumeration restarts the quota and reports again idempotently")
  void reEnumerationRestartsTheQuota() {
    Enumerable<Integer> limited = signal(2, 0L).observe(Linq4j.asEnumerable(List.of(1, 2, 3, 4)));
    Enumerator<Integer> enumerator = limited.enumerator();

    assertTrue(enumerator.moveNext());
    assertTrue(enumerator.moveNext());
    double afterFirst = progress.current().fractionDone();
    assertEquals(CEILING * 0.5, afterFirst, EPSILON);

    // A nested-loop join re-drives its inner side per outer row.
    enumerator.reset();
    assertTrue(enumerator.moveNext());
    assertTrue(enumerator.moveNext());

    assertEquals(afterFirst, progress.current().fractionDone(), EPSILON);
  }

  @Test
  @DisplayName("a second enumerator of the same limit reports the same sources")
  void secondEnumeratorReportsTheSameSources() {
    Enumerable<Integer> limited = signal(1, 0L).observe(Linq4j.asEnumerable(List.of(1, 2, 3)));

    assertTrue(limited.enumerator().moveNext());
    assertEquals(CEILING * 0.5, progress.current().fractionDone(), EPSILON);
    assertTrue(limited.enumerator().moveNext());
    assertEquals(CEILING * 0.5, progress.current().fractionDone(), EPSILON);
  }

  // ---------------------------------------------------------------- helpers

  private ProgressLimitSignal signal(long quota, Long... sourceIds) {
    return new ProgressLimitSignal(context, List.of(sourceIds), quota);
  }

  /** Simulates the inner source having read one page, so it is partway rather than at zero. */
  private void observeInnerPage() {
    progress.accept(new SourceProgressEvent.SourceShapeObserved(0, ProgressUnit.PAGED_ROWS, 10));
    progress.accept(
        new SourceProgressEvent.RowsObserved(
            0, 1, ProgressUnit.PAGED_ROWS, 10, 10, 1_000, false, false));
  }

  /** An enumerable whose enumerator throws once it has produced {@code rows} rows. */
  private static Enumerable<Integer> failingAfter(int rows) {
    return new org.apache.calcite.linq4j.AbstractEnumerable<>() {
      @Override
      public Enumerator<Integer> enumerator() {
        return new Enumerator<>() {
          private int produced;

          @Override
          public Integer current() {
            return produced;
          }

          @Override
          public boolean moveNext() {
            if (produced >= rows) {
              throw new IllegalStateException("downstream blew up");
            }
            produced++;
            return true;
          }

          @Override
          public void reset() {
            produced = 0;
          }

          @Override
          public void close() {}
        };
      }
    };
  }

  @Test
  @DisplayName("the signal carries the sources and quota it was built with")
  void signalExposesItsConfiguration() {
    ProgressLimitSignal signal = signal(3, 0L, 1L);
    assertEquals(List.of(0L, 1L), signal.sourceIds());
    assertEquals(3L, signal.outputQuota());
    assertFalse(signal.sourceIds().isEmpty());
  }
}
