/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

/**
 * Memory budget that retained query results are charged against.
 *
 * <p>A {@link QueryJob} charges its result once it is both {@link QueryJobState#SUCCEEDED} and
 * retained for polling, and releases the charge when the job leaves the store. Implementations must
 * not call back into a {@link QueryJob}: {@link #reserve(long)} runs while the job holds its own
 * monitor.
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
   * @throws RuntimeException when the budget cannot fit {@code bytes}; the exception becomes the
   *     job's failure cause, so its message must be safe to return to the client
   */
  void reserve(long bytes);

  /**
   * Returns {@code bytes} previously charged by {@link #reserve(long)}.
   *
   * @param bytes charge to return; never negative
   */
  void release(long bytes);
}
