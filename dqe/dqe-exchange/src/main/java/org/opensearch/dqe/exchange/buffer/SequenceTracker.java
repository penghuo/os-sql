/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.buffer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks the last received sequence number per channel (partitionId) for duplicate detection.
 *
 * <p>Transport-level retries may cause the same chunk to be delivered multiple times. By tracking
 * the highest sequence number seen per channel, duplicates can be silently discarded.
 *
 * <p>Thread-safe. Uses ConcurrentHashMap and atomic operations.
 */
public class SequenceTracker {

  private final ConcurrentMap<Integer, AtomicLong> lastReceivedByPartition;

  /** Create a new sequence tracker. */
  public SequenceTracker() {
    this.lastReceivedByPartition = new ConcurrentHashMap<>();
  }

  /**
   * Check whether a chunk with the given partition and sequence number should be accepted.
   *
   * <p>A chunk is accepted if its sequence number is strictly greater than the last received
   * sequence number for that partition. If accepted, the tracker is updated atomically.
   *
   * @param partitionId the partition (channel) identifier
   * @param sequenceNumber the sequence number of the incoming chunk
   * @return true if the chunk should be accepted, false if it is a duplicate
   */
  public boolean acceptIfNew(int partitionId, long sequenceNumber) {
    AtomicLong lastReceived =
        lastReceivedByPartition.computeIfAbsent(partitionId, k -> new AtomicLong(-1));

    while (true) {
      long current = lastReceived.get();
      if (sequenceNumber <= current) {
        // Duplicate or out-of-order — discard
        return false;
      }
      if (lastReceived.compareAndSet(current, sequenceNumber)) {
        return true;
      }
      // CAS failed — retry
    }
  }

  /**
   * Get the last received sequence number for a partition.
   *
   * @param partitionId the partition identifier
   * @return the last received sequence number, or -1 if no chunks received
   */
  public long getLastReceived(int partitionId) {
    AtomicLong lastReceived = lastReceivedByPartition.get(partitionId);
    return lastReceived == null ? -1 : lastReceived.get();
  }

  /** Reset the tracker, clearing all state. */
  public void reset() {
    lastReceivedByPartition.clear();
  }

  /** Number of partitions currently being tracked. */
  public int getTrackedPartitionCount() {
    return lastReceivedByPartition.size();
  }
}
