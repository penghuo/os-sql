/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;

/**
 * Open-addressing hash set for byte ranges within Slice objects. Stores entries as (Slice
 * reference, offset, length) triples, using XxHash64 for hashing and raw byte comparison for
 * equality. Eliminates all per-entry object allocation: no String conversion, no Slice copying, no
 * boxing.
 *
 * <p>Designed for COUNT(DISTINCT) on VARCHAR columns where the coordinator merges pre-deduplicated
 * values from multiple shards. Each shard returns ~18K distinct strings; this set unions them into
 * a single distinct count with zero per-entry heap allocation.
 *
 * <p>Uses linear probing with 65% load factor. Entries reference the original block's raw Slice
 * (zero-copy), so the caller must ensure source pages remain alive for the lifetime of this set.
 */
public final class SliceRangeHashSet {

  private static final int INITIAL_CAPACITY = 1024;
  private static final float LOAD_FACTOR = 0.65f;

  /** Slice references for each slot (null if unoccupied). */
  private Slice[] slices;

  /** Byte offsets within the Slice for each slot. */
  private int[] offsets;

  /** Byte lengths for each slot. */
  private int[] lengths;

  /** Pre-computed hashes for each slot (avoids rehashing on probe). */
  private long[] hashes;

  /** Whether each slot is occupied. */
  private boolean[] occupied;

  private int size;
  private int capacity;
  private int mask;
  private int threshold;

  /** Track whether an empty-string (length 0) has been added. */
  private boolean hasEmpty;

  public SliceRangeHashSet(int expectedElements) {
    int rawCapacity = Math.max(INITIAL_CAPACITY, (int) (expectedElements / LOAD_FACTOR) + 1);
    // Round up to next power of two
    this.capacity = Integer.highestOneBit(rawCapacity - 1) << 1;
    this.mask = capacity - 1;
    this.slices = new Slice[capacity];
    this.offsets = new int[capacity];
    this.lengths = new int[capacity];
    this.hashes = new long[capacity];
    this.occupied = new boolean[capacity];
    this.size = 0;
    this.threshold = (int) (capacity * LOAD_FACTOR);
    this.hasEmpty = false;
  }

  /**
   * Add a byte range to the set. Returns true if the range was not already present.
   *
   * @param slice the Slice containing the bytes
   * @param offset start offset within the Slice
   * @param length number of bytes
   * @return true if this is a new entry
   */
  public boolean add(Slice slice, int offset, int length) {
    // Special-case empty string
    if (length == 0) {
      if (hasEmpty) {
        return false;
      }
      hasEmpty = true;
      size++;
      return true;
    }

    long hash = XxHash64.hash(slice, offset, length);
    int slot = (int) (hash ^ (hash >>> 32)) & mask;

    while (occupied[slot]) {
      if (hashes[slot] == hash
          && lengths[slot] == length
          && slices[slot].equals(offsets[slot], length, slice, offset, length)) {
        return false; // Already present
      }
      slot = (slot + 1) & mask;
    }

    // Insert new entry
    slices[slot] = slice;
    offsets[slot] = offset;
    lengths[slot] = length;
    hashes[slot] = hash;
    occupied[slot] = true;
    size++;

    if (size > threshold) {
      resize();
    }
    return true;
  }

  /** Return the number of distinct byte ranges in the set. */
  public int size() {
    return size;
  }

  private void resize() {
    int newCapacity = capacity * 2;
    int newMask = newCapacity - 1;
    Slice[] newSlices = new Slice[newCapacity];
    int[] newOffsets = new int[newCapacity];
    int[] newLengths = new int[newCapacity];
    long[] newHashes = new long[newCapacity];
    boolean[] newOccupied = new boolean[newCapacity];

    for (int i = 0; i < capacity; i++) {
      if (occupied[i]) {
        long h = hashes[i];
        int slot = (int) (h ^ (h >>> 32)) & newMask;
        while (newOccupied[slot]) {
          slot = (slot + 1) & newMask;
        }
        newSlices[slot] = slices[i];
        newOffsets[slot] = offsets[i];
        newLengths[slot] = lengths[i];
        newHashes[slot] = h;
        newOccupied[slot] = true;
      }
    }

    this.slices = newSlices;
    this.offsets = newOffsets;
    this.lengths = newLengths;
    this.hashes = newHashes;
    this.occupied = newOccupied;
    this.capacity = newCapacity;
    this.mask = newMask;
    this.threshold = (int) (newCapacity * LOAD_FACTOR);
  }
}
