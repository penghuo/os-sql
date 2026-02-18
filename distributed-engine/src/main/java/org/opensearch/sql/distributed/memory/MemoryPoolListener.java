/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.memory;

/** Listener for memory pool events such as reservation and release. */
public interface MemoryPoolListener {

  /** Called when memory is reserved in the pool. */
  void onMemoryReserved(String allocationTag, long bytes);

  /** Called when memory is freed from the pool. */
  void onMemoryFreed(String allocationTag, long bytes);
}
