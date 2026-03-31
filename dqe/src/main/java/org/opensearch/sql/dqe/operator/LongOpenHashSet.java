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

  private static final int INITIAL_CAPACITY = 8;
  private static final float LOAD_FACTOR = 0.65f;

  /**
   * Sentinel value marking empty slots. Using Long.MIN_VALUE eliminates the need for a separate
   * boolean[] occupied array, halving memory usage and reducing cache misses during probing.
   */
  private static final long EMPTY = Long.MIN_VALUE;

  private long[] keys;
  private int size;
  private int capacity;
  private int threshold;

  /** Track whether the value 0 has been added (since 0 is the default value in long[]). */
  private boolean hasZero;

  /** Track whether EMPTY sentinel has been added as an actual value. */
  private boolean hasSentinel;

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
    java.util.Arrays.fill(this.keys, EMPTY);
    this.size = 0;
    this.threshold = (int) (capacity * LOAD_FACTOR);
    this.hasZero = false;
    this.hasSentinel = false;
  }

  /**
   * Add a value to the set. Returns true if the value was not already present.
   *
   * @param value the long value to add
   * @return true if the set did not already contain this value
   */
  public boolean add(long value) {
    if (value == 0) {
      if (hasZero) return false;
      hasZero = true;
      size++;
      return true;
    }
    if (value == EMPTY) {
      if (hasSentinel) return false;
      hasSentinel = true;
      size++;
      return true;
    }

    int mask = capacity - 1;
    // Murmur3 finalizer: good distribution even for correlated keys
    long h = value;
    h ^= h >>> 33;
    h *= 0xff51afd7ed558ccdL;
    h ^= h >>> 33;
    h *= 0xc4ceb9fe1a85ec53L;
    h ^= h >>> 33;
    int slot = (int) h & mask;
    long k;
    while ((k = keys[slot]) != EMPTY) {
      if (k == value) return false;
      slot = (slot + 1) & mask;
    }

    keys[slot] = value;
    size++;

    if (size > threshold) {
      resize();
    }
    return true;
  }

  /**
   * Check if a value is in the set.
   * @param value the long value to check
   * @return true if the set contains this value
   */
  public boolean contains(long value) {
    if (value == 0) return hasZero;
    if (value == EMPTY) return hasSentinel;
    int mask = capacity - 1;
    long h = value;
    h ^= h >>> 33;
    h *= 0xff51afd7ed558ccdL;
    h ^= h >>> 33;
    h *= 0xc4ceb9fe1a85ec53L;
    h ^= h >>> 33;
    int slot = (int) h & mask;
    long k;
    while ((k = keys[slot]) != EMPTY) {
      if (k == value) return true;
      slot = (slot + 1) & mask;
    }
    return false;
  }

  /** Return the number of distinct values in the set. */
  public int size() {
    return size;
  }

  /** Return the underlying keys array (for iteration). Slots equal to EMPTY are unoccupied. */
  public long[] keys() {
    return keys;
  }

  /**
   * Return an occupied flags view for iteration compatibility. Callers should prefer checking
   * keys[i] != EMPTY directly when possible.
   */
  public boolean[] occupied() {
    boolean[] occ = new boolean[capacity];
    for (int i = 0; i < capacity; i++) {
      occ[i] = keys[i] != EMPTY;
    }
    return occ;
  }

  /** Return whether the value 0 is in the set. */
  public boolean hasZeroValue() {
    return hasZero;
  }

  /** Return whether the sentinel value (Long.MIN_VALUE) is in the set. */
  public boolean hasSentinelValue() {
    return hasSentinel;
  }

  /** Return the sentinel value used for empty slots. */
  public static long emptyMarker() {
    return EMPTY;
  }

  /**
   * Add all values from another LongOpenHashSet into this one. Used for merging cross-segment
   * results.
   */
  public void addAll(LongOpenHashSet other) {
    if (other.hasZero && !hasZero) {
      hasZero = true;
      size++;
    }
    if (other.hasSentinel && !hasSentinel) {
      hasSentinel = true;
      size++;
    }
    long[] otherKeys = other.keys;
    for (int i = 0; i < otherKeys.length; i++) {
      if (otherKeys[i] != EMPTY) {
        add(otherKeys[i]);
      }
    }
  }

  /**
   * Pre-allocate capacity to hold at least {@code minCapacity} elements without resizing.
   * Used before bulk merge operations to avoid incremental resize overhead.
   */
  public void ensureCapacity(int minCapacity) {
    int needed = (int) Math.ceil(minCapacity / LOAD_FACTOR);
    if (needed > capacity) {
      int newCap = Integer.highestOneBit(needed - 1) << 1;
      if (newCap < needed) newCap = needed;
      long[] nk = new long[newCap];
      java.util.Arrays.fill(nk, EMPTY);
      int nm = newCap - 1;
      for (int i = 0; i < capacity; i++) {
        long k = keys[i];
        if (k != EMPTY) {
          long h = k;
          h ^= h >>> 33;
          h *= 0xff51afd7ed558ccdL;
          h ^= h >>> 33;
          h *= 0xc4ceb9fe1a85ec53L;
          h ^= h >>> 33;
          int slot = (int) h & nm;
          while (nk[slot] != EMPTY) slot = (slot + 1) & nm;
          nk[slot] = k;
        }
      }
      this.keys = nk;
      this.capacity = newCap;
      this.threshold = (int) (newCap * LOAD_FACTOR);
    }
  }

  private void resize() {
    int newCapacity = capacity * 2;
    long[] newKeys = new long[newCapacity];
    java.util.Arrays.fill(newKeys, EMPTY);
    int newMask = newCapacity - 1;

    for (int i = 0; i < capacity; i++) {
      long key = keys[i];
      if (key != EMPTY) {
        long h = key;
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        int slot = (int) h & newMask;
        while (newKeys[slot] != EMPTY) {
          slot = (slot + 1) & newMask;
        }
        newKeys[slot] = key;
      }
    }

    this.keys = newKeys;
    this.capacity = newCapacity;
    this.threshold = (int) (newCapacity * LOAD_FACTOR);
  }
}
