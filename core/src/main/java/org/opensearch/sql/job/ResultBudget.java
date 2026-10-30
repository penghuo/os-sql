/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

/**
 * Memory budget that retained query results are charged against.
 *
 * <p>A {@link QueryJob} charges its result when it succeeds while retained for polling, and returns
 * the charge when the store removes it. {@link #reserve(long)} runs while the job holds its own
 * monitor, so implementations must not call back into a {@link QueryJob}.
 */
public interface ResultBudget {

  /** Budget that accepts every charge. */
  ResultBudget UNLIMITED =
      new ResultBudget() {
        @Override
        public void reserve(long bytes) {}

        @Override
        public void release(long bytes) {}
      };

  /**
   * Charges {@code bytes} against the budget.
   *
   * @param bytes estimated size of the retained result; never negative
   * @throws org.opensearch.sql.job.exceptions.QueryJobCapacityException when {@code bytes} does not
   *     fit; it becomes the job's failure cause
   */
  void reserve(long bytes);

  /**
   * Returns {@code bytes} previously charged by {@link #reserve(long)}.
   *
   * @param bytes charge to return; never negative
   */
  void release(long bytes);
}
