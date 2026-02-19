/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.spill;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Interface for operators that support cooperative memory revocation (spill-to-disk). When the
 * {@link org.opensearch.sql.distributed.memory.MemoryPool} detects memory pressure, it signals
 * operators implementing this interface to spill their in-memory data to disk.
 *
 * <p>The revocation protocol is cooperative:
 *
 * <ol>
 *   <li>MemoryPool signals pressure by calling {@link #startMemoryRevoke()}
 *   <li>The operator begins spilling data asynchronously and returns a future
 *   <li>When the future completes, the driver calls {@link #finishMemoryRevoke()}
 *   <li>The operator frees its revocable memory back to the pool
 * </ol>
 *
 * <p>Operators decide when to spill — they are never forced to drop data.
 */
public interface MemoryRevocableOperator {

  /**
   * Returns the amount of revocable memory currently held by this operator. This is memory that can
   * be freed by spilling to disk.
   */
  long getRevocableMemoryUsage();

  /**
   * Initiates memory revocation. The operator should begin spilling its in-memory data to disk and
   * return a future that completes when the spill is done.
   *
   * @return a future that is resolved when the spill completes
   */
  ListenableFuture<Void> startMemoryRevoke();

  /**
   * Called after the future from {@link #startMemoryRevoke()} completes. The operator should
   * finalize the spill and free revocable memory.
   */
  void finishMemoryRevoke();
}
