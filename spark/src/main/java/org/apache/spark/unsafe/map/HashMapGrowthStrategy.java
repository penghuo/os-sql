/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.unsafe.map;

import org.apache.spark.unsafe.array.ByteArrayMethods;

/**
 * Interface that defines how we can grow the size of a hash map when it is over a threshold.
 */
public interface HashMapGrowthStrategy {

  int nextCapacity(int currentCapacity);

  /**
   * Double the size of the hash map every time.
   */
  HashMapGrowthStrategy DOUBLING = new Doubling();

  class Doubling implements HashMapGrowthStrategy {

    private static final int ARRAY_MAX = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH;

    @Override
    public int nextCapacity(int currentCapacity) {
      assert (currentCapacity > 0);
      int doubleCapacity = currentCapacity * 2;
      // Guard against overflow
      return (doubleCapacity > 0 && doubleCapacity <= ARRAY_MAX) ? doubleCapacity : ARRAY_MAX;
    }
  }

}
