/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.progress;

import java.util.OptionalLong;

/**
 * Sink for {@link SourceProgressEvent}s and reader of the published fraction.
 *
 * <p>Implementations are thread-safe and non-blocking. Events arrive from OpenSearch search-phase
 * callbacks on transport threads, from page fetches on background IO threads, and from the Calcite
 * worker thread; {@link #current()} is read from whichever transport thread is serving a poll. None
 * of those may be delayed by progress accounting, so an implementation must never do IO or take a
 * lock held across a call-out.
 *
 * <p>Registration happens before execution and is one-shot: {@link #register} every physical source
 * occurrence, then {@link #seal}. Until the set is sealed the observer publishes {@link
 * QueryProgress#ZERO}, which is what prevents a plan from starting at "half done" with a
 * denominator holding only its first source and then appearing to move backwards when the second
 * appears.
 */
public interface ProgressObserver {

  /** Observer for queries that are not tracked. Accepts everything and always reports zero. */
  ProgressObserver NOOP =
      new ProgressObserver() {
        @Override
        public void register(long sourceId, OptionalLong estimatedDocs) {}

        @Override
        public void seal() {}

        @Override
        public void accept(SourceProgressEvent event) {}

        @Override
        public QueryProgress current() {
          return QueryProgress.ZERO;
        }
      };

  /**
   * Declares a physical source occurrence.
   *
   * <p>Must be called before {@link #seal()}. Registering the same {@code sourceId} twice keeps the
   * first registration, so a planning hook that runs more than once is harmless.
   *
   * @param sourceId stable id of the source occurrence
   * @param estimatedDocs live primary-shard document total for the source's concrete indices, or
   *     empty when the estimate was unavailable — the weighting then falls back to equal weight
   */
  void register(long sourceId, OptionalLong estimatedDocs);

  /**
   * Closes registration and allows non-zero publication. Idempotent; registrations after sealing
   * are rejected rather than silently changing the denominator mid-query.
   */
  void seal();

  /** Records one source event. Unknown source ids are ignored rather than failing execution. */
  void accept(SourceProgressEvent event);

  /**
   * Returns the currently published fraction: finite, within {@code [0.0, }{@link
   * QueryProgress#PUBLIC_CEILING}{@code ]}, and never lower than any value previously returned.
   */
  QueryProgress current();
}
