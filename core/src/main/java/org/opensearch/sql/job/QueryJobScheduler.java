/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

/**
 * Decides when a published {@link QueryJob} starts.
 *
 * <p>A scheduler bounds how many jobs run at once. A job beyond the bound waits in {@link
 * QueryJobState#PENDING} until a running job finishes. A job whose runner is refused for lack of
 * capacity goes back to waiting instead of failing.
 */
public interface QueryJobScheduler {

  /**
   * Starts {@code job} now or queues it. The job is {@code PENDING} and already registered in the
   * {@link QueryJobStore}.
   *
   * @param job the newly published job
   * @throws RuntimeException when the queue is full; the job is neither started nor queued
   */
  void submit(QueryJob job);

  /**
   * Whether {@code failure} means the runner was refused for lack of capacity before doing any
   * work, so the query can run again unchanged.
   *
   * @param failure the runner's failure, unwrapped
   */
  boolean isCapacityRejection(Throwable failure);

  /**
   * Puts {@code job} back in the queue after its runner was refused for lack of capacity. The job
   * is {@code PENDING} again and holds a fresh runner.
   *
   * @param job the refused job
   */
  void requeue(QueryJob job);
}
