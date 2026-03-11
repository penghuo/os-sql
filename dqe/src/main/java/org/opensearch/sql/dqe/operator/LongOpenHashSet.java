/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

/**
 * Open-addressing hash set for primitive long values. Eliminates the boxing overhead of {@code
 * HashSet<Long>} by storing values directly in a long array with linear probing.
 *
 * <p>Uses a sentinel-based approach: a separate boolean tracks whether the value 0 is in the set,
 * since 0 is also the default value for unoccupied slots. All other values use the occupied[]
 * boolean array to distinguish empty from filled slots.
 *
 * <p>This is designed for COUNT(DISTINCT) on numeric columns where the distinct set can grow large
 * (e.g., ~200K UserIDs in 1M rows). The savings are significant: ~200K Long boxing operations are
 * eliminated per shard, and the hash set uses ~50% less memory than {@code HashSet<Long>}.
 */
public final class LongOpenHashSet {

  private static final int INITIAL_CAPACITY = 1024;
  private static final float LOAD_FACTOR = 0.65f;

  private long[] keys;
  private boolean[] occupied;
  private int size;
  private int capacity;
  private int threshold;

  /** Track whether the value 0 has been added (since 0 is the default value in long[]). */
  private boolean hasZero;

  public LongOpenHashSet() {
    this(INITIAL_CAPACITY);
  }

  /**
   * Create a set with the given initial capacity (rounded up to power of two). Use this when the
   * expected number of elements is known to avoid resizing overhead.
   *
   * @param expectedElements expected number of distinct elements
   */
  public LongOpenHashSet(int expectedElements) {
    int rawCapacity = Math.max(INITIAL_CAPACITY, (int) (expectedElements / LOAD_FACTOR) + 1);
    // Round up to next power of two
    this.capacity = Integer.highestOneBit(rawCapacity - 1) << 1;
    this.keys = new long[capacity];
    this.occupied = new boolean[capacity];
    this.size = 0;
    this.threshold = (int) (capacity * LOAD_FACTOR);
    this.hasZero = false;
  }

  /**
   * Add a value to the set. Returns true if the value was not already present.
   *
   * @param value the long value to add
   * @return true if the set did not already contain this value
   */
  public boolean add(long value) {
    if (value == 0) {
      if (hasZero) {
        return false;
      }
      hasZero = true;
      size++;
      return true;
    }

    int mask = capacity - 1;
    int slot = (int) (value ^ (value >>> 32)) & mask;
    while (occupied[slot]) {
      if (keys[slot] == value) {
        return false; // Already present
      }
      slot = (slot + 1) & mask;
    }

    // Insert new value
    keys[slot] = value;
    occupied[slot] = true;
    size++;

    if (size > threshold) {
      resize();
    }
    return true;
  }

  /** Return the number of distinct values in the set. */
  public int size() {
    return size;
  }

  /** Return the underlying keys array (for iteration). Non-null only at occupied positions. */
  public long[] keys() {
    return keys;
  }

  /** Return the occupied flags array (for iteration). */
  public boolean[] occupied() {
    return occupied;
  }

  /** Return whether the value 0 is in the set. */
  public boolean hasZeroValue() {
    return hasZero;
  }

  /**
   * Add all values from another LongOpenHashSet into this one. Used for merging cross-segment
   * results.
   */
  public void addAll(LongOpenHashSet other) {
    if (other.hasZero) {
      if (!hasZero) {
        hasZero = true;
        size++;
      }
    }
    for (int i = 0; i < other.capacity; i++) {
      if (other.occupied[i]) {
        add(other.keys[i]);
      }
    }
  }

  private void resize() {
    int newCapacity = capacity * 2;
    long[] newKeys = new long[newCapacity];
    boolean[] newOccupied = new boolean[newCapacity];
    int newMask = newCapacity - 1;

    for (int i = 0; i < capacity; i++) {
      if (occupied[i]) {
        long key = keys[i];
        int slot = (int) (key ^ (key >>> 32)) & newMask;
        while (newOccupied[slot]) {
          slot = (slot + 1) & newMask;
        }
        newKeys[slot] = key;
        newOccupied[slot] = true;
      }
    }

    this.keys = newKeys;
    this.occupied = newOccupied;
    this.capacity = newCapacity;
    this.threshold = (int) (newCapacity * LOAD_FACTOR);
  }
}
