/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.io.Closeable;
import java.util.Collection;
import java.util.Optional;

/**
 * Registry of live jobs on the owner node.
 *
 * <p>Implementations must be safe for concurrent use. The MVP ships an in-memory implementation;
 * follow-up work can add a persistent variant without changing the interface or the callers.
 */
public interface QueryJobStore extends Closeable {

  /**
   * Publishes {@code job} under its own ID. Returns {@code null} on success. If another job with
   * the same ID is already registered, that pre-existing job is returned and the caller must
   * discard the incoming duplicate.
   *
   * @throws IllegalStateException when the store has been closed
   */
  QueryJob register(QueryJob job);

  /** Looks up a job by ID. */
  Optional<QueryJob> find(QueryJobId id);

  /**
   * Removes {@code id} only when it still maps to {@code job}. Returns {@code true} when the
   * mapping was removed.
   */
  boolean remove(QueryJobId id, QueryJob job);

  /** Snapshot of every job registered at the moment of the call. */
  Collection<QueryJob> jobs();

  /**
   * Takes a slot for a job that finished while retained for polling, and charges the estimated size
   * of its result. A {@link QueryJob} calls this once, while it holds its own monitor, so
   * implementations must not call back into a job. Accepts every charge by default.
   *
   * @param bytes estimated size of a successful result; {@code 0}, which always fits, for a job
   *     that failed or was cancelled
   * @throws org.opensearch.sql.job.exceptions.QueryJobCapacityException when {@code bytes} does not
   *     fit; no slot is taken, and the exception becomes the job's failure cause
   */
  default void retain(long bytes) {}

  /**
   * Returns a slot taken by {@link #retain(long)} together with its charge.
   *
   * @param bytes charge to return; never negative
   */
  default void release(long bytes) {}

  /**
   * Stops accepting new jobs and cancels every remaining job. Idempotent; safe to invoke from
   * shutdown hooks.
   */
  @Override
  void close();
}
