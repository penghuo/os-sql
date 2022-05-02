/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java;

import org.apache.spark.storage.StorageLevel;

/**
 * Expose some commonly useful storage level constants.
 */
public class StorageLevels {
  public static final StorageLevel NONE = create(false, false, false, false, 1);
  public static final StorageLevel DISK_ONLY = create(true, false, false, false, 1);
  public static final StorageLevel DISK_ONLY_2 = create(true, false, false, false, 2);
  public static final StorageLevel DISK_ONLY_3 = create(true, false, false, false, 3);
  public static final StorageLevel MEMORY_ONLY = create(false, true, false, true, 1);
  public static final StorageLevel MEMORY_ONLY_2 = create(false, true, false, true, 2);
  public static final StorageLevel MEMORY_ONLY_SER = create(false, true, false, false, 1);
  public static final StorageLevel MEMORY_ONLY_SER_2 = create(false, true, false, false, 2);
  public static final StorageLevel MEMORY_AND_DISK = create(true, true, false, true, 1);
  public static final StorageLevel MEMORY_AND_DISK_2 = create(true, true, false, true, 2);
  public static final StorageLevel MEMORY_AND_DISK_SER = create(true, true, false, false, 1);
  public static final StorageLevel MEMORY_AND_DISK_SER_2 = create(true, true, false, false, 2);
  public static final StorageLevel OFF_HEAP = create(true, true, true, false, 1);

  /**
   * Create a new StorageLevel object.
   * @param useDisk saved to disk, if true
   * @param useMemory saved to on-heap memory, if true
   * @param useOffHeap saved to off-heap memory, if true
   * @param deserialized saved as deserialized objects, if true
   * @param replication replication factor
   */
  public static StorageLevel create(
    boolean useDisk,
    boolean useMemory,
    boolean useOffHeap,
    boolean deserialized,
    int replication) {
    return StorageLevel.apply(useDisk, useMemory, useOffHeap, deserialized, replication);
  }
}
