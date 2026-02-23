/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.metadata;

/**
 * Immutable container for basic table-level statistics: row count and index size. Used by the
 * optimizer for cost estimation.
 */
public final class DqeTableStatistics {

  /** Sentinel for unknown statistics (row count = -1, size = -1). */
  public static final DqeTableStatistics UNKNOWN = new DqeTableStatistics(-1, -1, -1);

  private final long rowCount;
  private final long indexSizeBytes;
  private final long generation;

  public DqeTableStatistics(long rowCount, long indexSizeBytes, long generation) {
    this.rowCount = rowCount;
    this.indexSizeBytes = indexSizeBytes;
    this.generation = generation;
  }

  /** Returns the total document count, or -1 if unknown. */
  public long getRowCount() {
    return rowCount;
  }

  /** Returns the total index size in bytes, or -1 if unknown. */
  public long getIndexSizeBytes() {
    return indexSizeBytes;
  }

  /** Returns the generation number used for cache invalidation. */
  public long getGeneration() {
    return generation;
  }

  /** Returns true if this statistics object represents unknown values. */
  public boolean isUnknown() {
    return rowCount < 0;
  }
}
