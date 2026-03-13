/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

import io.airlift.slice.Slice;

/**
 * Open-addressing hash map from VARCHAR byte ranges (Slice, offset, length) to long counts. Used as
 * Stage 2 of the VARCHAR COUNT(DISTINCT) dedup merge: after the global dedup set is built, this map
 * counts how many distinct entries exist per VARCHAR group key.
 *
 * <p>Entries are stored zero-copy (referencing the original block Slice). Uses linear probing with
 * 65% load factor.
 */
public final class SliceCountMap {

  private static final float LOAD_FACTOR = 0.65f;

  private Slice[] slices;
  private int[] offsets;
  private int[] lengths;
  private long[] hashes;
  private long[] counts;
  private boolean[] occupied;

  private int size;
  private int capacity;
  private int mask;
  private int threshold;

  /** Track occupied slots for fast iteration. */
  private int[] occupiedSlots;

  public SliceCountMap(int expectedElements) {
    int rawCapacity = Math.max(64, (int) (expectedElements / LOAD_FACTOR) + 1);
    this.capacity = Integer.highestOneBit(rawCapacity - 1) << 1;
    this.mask = capacity - 1;
    this.slices = new Slice[capacity];
    this.offsets = new int[capacity];
    this.lengths = new int[capacity];
    this.hashes = new long[capacity];
    this.counts = new long[capacity];
    this.occupied = new boolean[capacity];
    this.occupiedSlots = new int[Math.max(64, expectedElements)];
    this.size = 0;
    this.threshold = (int) (capacity * LOAD_FACTOR);
  }

  /**
   * Increment the count for the given VARCHAR key by 1.
   *
   * @param slice the Slice containing the key bytes
   * @param offset start offset
   * @param length byte length
   * @param sliceHash pre-computed XxHash64 of (slice, offset, length)
   */
  public void increment(Slice slice, int offset, int length, long sliceHash) {
    int hash = (int) (sliceHash ^ (sliceHash >>> 32));
    int slot = hash & mask;

    while (occupied[slot]) {
      if (hashes[slot] == sliceHash
          && lengths[slot] == length
          && sliceEquals(slices[slot], offsets[slot], slice, offset, length)) {
        counts[slot]++;
        return;
      }
      slot = (slot + 1) & mask;
    }

    // New entry
    slices[slot] = slice;
    offsets[slot] = offset;
    lengths[slot] = length;
    hashes[slot] = sliceHash;
    counts[slot] = 1;
    occupied[slot] = true;
    if (size >= occupiedSlots.length) {
      occupiedSlots = java.util.Arrays.copyOf(occupiedSlots, occupiedSlots.length * 2);
    }
    occupiedSlots[size] = slot;
    size++;

    if (size > threshold) {
      resize();
    }
  }

  /** Return the number of unique groups. */
  public int size() {
    return size;
  }

  /** Iterate all entries and invoke the visitor. */
  public void forEach(EntryVisitor visitor) {
    for (int i = 0; i < size; i++) {
      int slot = occupiedSlots[i];
      visitor.visit(slices[slot], offsets[slot], lengths[slot], counts[slot]);
    }
  }

  @FunctionalInterface
  public interface EntryVisitor {
    void visit(Slice slice, int offset, int length, long count);
  }

  private static boolean sliceEquals(Slice a, int aOffset, Slice b, int bOffset, int length) {
    if (length == 0) {
      return true;
    }
    return a.equals(aOffset, length, b, bOffset, length);
  }

  private void resize() {
    int newCapacity = capacity * 2;
    int newMask = newCapacity - 1;
    Slice[] newSlices = new Slice[newCapacity];
    int[] newOffsets = new int[newCapacity];
    int[] newLengths = new int[newCapacity];
    long[] newHashes = new long[newCapacity];
    long[] newCounts = new long[newCapacity];
    boolean[] newOccupied = new boolean[newCapacity];
    int[] newOccupiedSlots = new int[Math.max(occupiedSlots.length, size + (size >> 1))];

    int newIdx = 0;
    for (int i = 0; i < size; i++) {
      int oldSlot = occupiedSlots[i];
      long h = hashes[oldSlot];
      int hash = (int) (h ^ (h >>> 32));
      int slot = hash & newMask;
      while (newOccupied[slot]) {
        slot = (slot + 1) & newMask;
      }
      newSlices[slot] = slices[oldSlot];
      newOffsets[slot] = offsets[oldSlot];
      newLengths[slot] = lengths[oldSlot];
      newHashes[slot] = h;
      newCounts[slot] = counts[oldSlot];
      newOccupied[slot] = true;
      newOccupiedSlots[newIdx++] = slot;
    }

    this.slices = newSlices;
    this.offsets = newOffsets;
    this.lengths = newLengths;
    this.hashes = newHashes;
    this.counts = newCounts;
    this.occupied = newOccupied;
    this.occupiedSlots = newOccupiedSlots;
    this.capacity = newCapacity;
    this.mask = newMask;
    this.threshold = (int) (newCapacity * LOAD_FACTOR);
  }
}
