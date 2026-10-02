/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import java.util.List;
import java.util.Objects;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.opensearch.sql.executor.progress.CompletionReason;

/**
 * Marks the sources beneath a coordinator limit complete the moment that limit has produced its
 * full output.
 *
 * <h2>Why the signal belongs here</h2>
 *
 * A limit is the one place where a deliberate early stop is directly knowable. Everywhere else it
 * is ambiguous or too late:
 *
 * <ul>
 *   <li><b>A scan cannot infer it.</b> A limit separated from the scan by a filter bounds the
 *       limit's <em>output</em>, not the scan's input — two raw rows may yield fewer than two
 *       qualifying rows — so no static row budget derived from the plan can tell the scan when to
 *       stop counting.
 *   <li><b>A close cannot prove it.</b> Linq4j closes a source from its own {@code finally} block,
 *       which runs identically whether the query is finishing or failing, and before the failure
 *       reaches the execution engine.
 *   <li><b>End of execution is too late.</b> A joined subsearch capped at two rows is finished with
 *       its work while the outer side still has everything to read; waiting for the query to end
 *       would hold the published fraction down for that entire window and then be overwritten by
 *       the terminal transition anyway.
 * </ul>
 *
 * Counting the limit's own emitted rows settles it exactly: reaching the output quota is normal
 * exhaustion, by definition, at the instant it happens.
 *
 * <h2>What it does not do</h2>
 *
 * Nothing but count and report. Rows, ordering, control flow, and cleanup all stay with Calcite's
 * own skip/take code, which this wraps rather than replaces. A quota that is never reached —
 * because the input ran dry, the query failed, or it was cancelled — signals nothing, so an
 * abandoned source is never rounded up to done.
 *
 * <p>Instances are created only for observed (asynchronous) queries and are stashed into generated
 * code, which is why the context is held directly rather than read from a thread binding: the
 * limit's rows may be pulled on a thread that has no binding installed.
 */
public final class ProgressLimitSignal {

  private final ProgressiveQueryContext context;
  private final List<Long> sourceIds;
  private final long outputQuota;

  /**
   * @param context query context to report into
   * @param sourceIds progress sources that sit beneath this limit
   * @param outputQuota rows this limit emits before it stops — its {@code fetch}
   */
  ProgressLimitSignal(ProgressiveQueryContext context, List<Long> sourceIds, long outputQuota) {
    this.context = Objects.requireNonNull(context, "context must not be null");
    this.sourceIds = List.copyOf(Objects.requireNonNull(sourceIds, "sourceIds must not be null"));
    this.outputQuota = outputQuota;
  }

  /** Sources this signal completes. Visible for tests. */
  List<Long> sourceIds() {
    return sourceIds;
  }

  /** Rows the limit emits before stopping. Visible for tests. */
  long outputQuota() {
    return outputQuota;
  }

  /**
   * Returns {@code source} with row counting attached.
   *
   * <p>Called from generated code with the enumerable Calcite's own limit implementation produced.
   * Returns the input unchanged when there is nothing to report, so an uninteresting limit costs
   * one branch at plan time and nothing at run time.
   */
  public <T> Enumerable<T> observe(Enumerable<T> source) {
    // A zero-row quota is still a quota: a limit that asks for nothing and is visited is already
    // satisfied, and
    // bypassing it would leave the work beneath it pending until the query ends. Only "no source to
    // report" is a
    // reason to return the input untouched.
    if (sourceIds.isEmpty()) {
      return source;
    }
    return new AbstractEnumerable<T>() {
      @Override
      public Enumerator<T> enumerator() {
        return new QuotaCountingEnumerator<>(source.enumerator());
      }
    };
  }

  private void signalQuotaReached() {
    context.completeSources(sourceIds, CompletionReason.UPSTREAM_LIMIT);
  }

  /**
   * Pass-through enumerator that reports once the limit's output quota has been met.
   *
   * <p>Every method delegates. The only added behaviour is the counter, so a failure or
   * cancellation unwinding through here behaves exactly as it did before — including {@link
   * #close()}, which reports nothing.
   */
  private final class QuotaCountingEnumerator<T> implements Enumerator<T> {

    private final Enumerator<T> delegate;
    private long emitted;
    private boolean signalled;

    QuotaCountingEnumerator(Enumerator<T> delegate) {
      this.delegate = delegate;
    }

    @Override
    public T current() {
      return delegate.current();
    }

    @Override
    public boolean moveNext() {
      // Delegate first and signal only after it returns normally, so an exception or cancellation
      // unwinding through
      // here reports nothing.
      boolean hasNext = delegate.moveNext();
      if (!signalled && (outputQuota == 0L || (hasNext && ++emitted >= outputQuota))) {
        // The limit has everything it asked for — which for a zero-row limit is true as soon as it
        // is visited at
        // all. Its input will not be pulled again, so the sources beneath it are done; reported
        // here, while the
        // rest of the plan is still running.
        signalled = true;
        signalQuotaReached();
      }
      return hasNext;
    }

    @Override
    public void reset() {
      delegate.reset();
      // Re-enumeration (a nested-loop join re-driving its inner side) starts the quota over.
      // Reporting again is
      // harmless: source completion is idempotent.
      emitted = 0L;
      signalled = false;
    }

    @Override
    public void close() {
      delegate.close();
    }
  }
}
