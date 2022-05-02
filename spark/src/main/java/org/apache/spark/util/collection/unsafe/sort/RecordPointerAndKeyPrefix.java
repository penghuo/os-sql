/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.util.collection.unsafe.sort;

public final class RecordPointerAndKeyPrefix {
  /**
   * A pointer to a record; see {@link org.apache.spark.memory.TaskMemoryManager} for a
   * description of how these addresses are encoded.
   */
  public long recordPointer;

  /**
   * A key prefix, for use in comparisons.
   */
  public long keyPrefix;
}
