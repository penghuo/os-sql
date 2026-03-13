/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;

/**
 * Open-addressing hash set for (Slice-range, long) compound keys. Designed for COUNT(DISTINCT)
 * dedup merge where the group-by key is VARCHAR and the distinct column is numeric. Stores entries
 * as (Slice ref, offset, length, longKey) tuples using XxHash64 combined with the long value for
 * hashing, and raw byte + long comparison for equality.
 *
 * <p>After insertion, supports counting the number of distinct entries per VARCHAR group key via
 * {@link #countPerGroup(GroupCounter)}. This two-phase approach (insert all, then count) avoids
 * maintaining per-group counters during insertion, keeping the hot insertion loop simple.
 *
 * <p>Uses linear probing with 65% load factor. Entries reference the original block's raw Slice
 * (zero-copy), so the caller must ensure source pages remain alive for the lifetime of this map.
 */
public final class SliceLongDedupMap {

  private static final float LOAD_FACTOR = 0.65f;

  /** Slice references for each slot (null if unoccupied). */
  private Slice[] slices;

  /** Byte offsets within the Slice for each slot. */
  private int[] offsets;

  /** Byte lengths for each slot. */
  private int[] sliceLengths;

  /** Pre-computed hashes of the Slice range for each slot. */
  private long[] sliceHashes;

  /** The long (numeric) component of the compound key. */
  private long[] longKeys;

  /** Whether each slot is occupied. */
  private boolean[] occupied;

  /** Indices of occupied slots for fast iteration. */
  private int[] occupiedSlots;

  private int size;
  private int capacity;
  private int mask;
  private int threshold;

  public SliceLongDedupMap(int expectedElements) {
    int rawCapacity = Math.max(1024, (int) (expectedElements / LOAD_FACTOR) + 1);
    this.capacity = Integer.highestOneBit(rawCapacity - 1) << 1;
    this.mask = capacity - 1;
    this.slices = new Slice[capacity];
    this.offsets = new int[capacity];
    this.sliceLengths = new int[capacity];
    this.sliceHashes = new long[capacity];
    this.longKeys = new long[capacity];
    this.occupied = new boolean[capacity];
    this.occupiedSlots = new int[Math.max(1024, expectedElements)];
    this.size = 0;
    this.threshold = (int) (capacity * LOAD_FACTOR);
  }

  /**
   * Add a (slice-range, longVal) pair to the set. Returns true if the pair was new.
   *
   * @param slice the Slice containing the VARCHAR bytes
   * @param offset start offset within the Slice
   * @param length number of bytes
   * @param longVal the numeric component (e.g., UserID)
   * @return true if this is a new entry
   */
  public boolean add(Slice slice, int offset, int length, long longVal) {
    long sliceHash = length == 0 ? 0L : XxHash64.hash(slice, offset, length);
    // Combine slice hash with long value for compound hash
    int hash = (int) (sliceHash * 0x9E3779B97F4A7C15L + Long.hashCode(longVal));
    int slot = hash & mask;

    while (occupied[slot]) {
      if (longKeys[slot] == longVal
          && sliceLengths[slot] == length
          && sliceHashes[slot] == sliceHash
          && sliceEquals(slices[slot], offsets[slot], slice, offset, length)) {
        return false; // Already present
      }
      slot = (slot + 1) & mask;
    }

    // Insert new entry
    slices[slot] = slice;
    offsets[slot] = offset;
    sliceLengths[slot] = length;
    sliceHashes[slot] = sliceHash;
    longKeys[slot] = longVal;
    occupied[slot] = true;
    if (size >= occupiedSlots.length) {
      occupiedSlots = java.util.Arrays.copyOf(occupiedSlots, occupiedSlots.length * 2);
    }
    occupiedSlots[size] = slot;
    size++;

    if (size > threshold) {
      resize();
    }
    return true;
  }

  /** Return the number of entries in the set. */
  public int size() {
    return size;
  }

  /**
   * Iterate over all entries and call the counter for each VARCHAR group key. The counter is
   * responsible for counting entries per unique VARCHAR value.
   */
  public void countPerGroup(GroupCounter counter) {
    for (int i = 0; i < size; i++) {
      int slot = occupiedSlots[i];
      counter.count(slices[slot], offsets[slot], sliceLengths[slot], sliceHashes[slot]);
    }
  }

  /** Callback interface for counting entries per VARCHAR group key. */
  @FunctionalInterface
  public interface GroupCounter {
    void count(Slice slice, int offset, int length, long sliceHash);
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
    int[] newSliceLengths = new int[newCapacity];
    long[] newSliceHashes = new long[newCapacity];
    long[] newLongKeys = new long[newCapacity];
    boolean[] newOccupied = new boolean[newCapacity];
    int[] newOccupiedSlots = new int[Math.max(occupiedSlots.length, size + (size >> 1))];

    int newIdx = 0;
    for (int i = 0; i < size; i++) {
      int oldSlot = occupiedSlots[i];
      long sh = sliceHashes[oldSlot];
      long lk = longKeys[oldSlot];
      int hash = (int) (sh * 0x9E3779B97F4A7C15L + Long.hashCode(lk));
      int slot = hash & newMask;
      while (newOccupied[slot]) {
        slot = (slot + 1) & newMask;
      }
      newSlices[slot] = slices[oldSlot];
      newOffsets[slot] = offsets[oldSlot];
      newSliceLengths[slot] = sliceLengths[oldSlot];
      newSliceHashes[slot] = sh;
      newLongKeys[slot] = lk;
      newOccupied[slot] = true;
      newOccupiedSlots[newIdx++] = slot;
    }

    this.slices = newSlices;
    this.offsets = newOffsets;
    this.sliceLengths = newSliceLengths;
    this.sliceHashes = newSliceHashes;
    this.longKeys = newLongKeys;
    this.occupied = newOccupied;
    this.occupiedSlots = newOccupiedSlots;
    this.capacity = newCapacity;
    this.mask = newMask;
    this.threshold = (int) (newCapacity * LOAD_FACTOR);
  }
}
