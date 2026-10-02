/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.progress;

import org.opensearch.sql.common.response.ResponseListener;

/**
 * Response listener that also carries the query's progress sink.
 *
 * <p>The listener is the one object that already travels from the submitting thread all the way
 * into the execution engine, so it is how the engine learns that this query is being observed. A
 * plain {@link ResponseListener} means "not observed", and the engine substitutes {@link
 * ProgressObserver#NOOP} — which is what keeps synchronous queries on exactly the path they are on
 * today, with no progress bookkeeping at all.
 *
 * <p>This is the progress slice of the design's progressive listener. Partial-result publication,
 * cancellable-work registration, and authoritative-result delivery are separate members of the same
 * contract and land with their own sub-tasks; adding them here does not change this one.
 *
 * @param <T> response payload type
 */
public interface ProgressiveQueryResponseListener<T> extends ResponseListener<T> {

  /**
   * Returns the sink for this query's {@link SourceProgressEvent}s. Must return the same instance
   * on every call: the engine registers sources against it and the lifecycle layer polls it.
   */
  ProgressObserver progressObserver();

  /**
   * Resolves the observer behind an arbitrary listener, yielding {@link ProgressObserver#NOOP} for
   * listeners that do not participate.
   */
  static ProgressObserver observerOf(ResponseListener<?> listener) {
    return listener instanceof ProgressiveQueryResponseListener<?> progressive
        ? progressive.progressObserver()
        : ProgressObserver.NOOP;
  }
}
