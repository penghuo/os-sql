/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.util.collection.unsafe.sort;

/**
 * Compares records for ordering. In cases where the entire sorting key can fit in the 8-byte
 * prefix, this may simply return 0.
 */
public abstract class RecordComparator {

  /**
   * Compare two records for order.
   *
   * @return a negative integer, zero, or a positive integer as the first record is less than,
   *         equal to, or greater than the second.
   */
  public abstract int compare(
    Object leftBaseObject,
    long leftBaseOffset,
    int leftBaseLength,
    Object rightBaseObject,
    long rightBaseOffset,
    int rightBaseLength);
}
