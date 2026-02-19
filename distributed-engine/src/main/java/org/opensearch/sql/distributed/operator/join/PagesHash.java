/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.BooleanArrayBlock;
import org.opensearch.sql.distributed.data.ByteArrayBlock;
import org.opensearch.sql.distributed.data.DictionaryBlock;
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.IntArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.RunLengthEncodedBlock;
import org.opensearch.sql.distributed.data.ShortArrayBlock;
import org.opensearch.sql.distributed.data.VariableWidthBlock;

/**
 * Page-based hash lookup table for join operations. Stores build-side pages and provides hash-based
 * lookup by join key columns. Uses a HashMap with composite key (same pattern as GroupByHash).
 *
 * <p>Ported from Trino's io.trino.operator.join.PagesHash (simplified: no bytecode generation, no
 * open addressing).
 */
public class PagesHash {

  private final int[] hashKeyChannels;
  private final List<Page> pages;

  /** Flat position mapping: index i -> pages.get(pageIndices[i]) at positionsInPage[i]. */
  private int[] pageIndices;

  private int[] positionsInPage;
  private int positionCount;

  /** Hash table mapping composite keys to arrays of matching flat position indices. */
  private Map<JoinKey, int[]> hashTable;

  private long estimatedSizeInBytes;

  /**
   * Creates a new PagesHash.
   *
   * @param hashKeyChannels column indices in build-side pages used as join keys
   */
  public PagesHash(int[] hashKeyChannels) {
    this.hashKeyChannels = Objects.requireNonNull(hashKeyChannels, "hashKeyChannels is null");
    this.pages = new ArrayList<>();
    this.positionCount = 0;
    this.estimatedSizeInBytes = 0;
  }

  /** Adds a build-side page. Must be called before {@link #build()}. */
  public void addPage(Page page) {
    Objects.requireNonNull(page, "page is null");
    pages.add(page);
    positionCount += page.getPositionCount();
  }

  /** Builds the hash table from accumulated pages. Must be called after all pages are added. */
  public void build() {
    // Build flat position arrays
    pageIndices = new int[positionCount];
    positionsInPage = new int[positionCount];
    int flatIndex = 0;
    for (int pageIdx = 0; pageIdx < pages.size(); pageIdx++) {
      Page page = pages.get(pageIdx);
      int positions = page.getPositionCount();
      for (int pos = 0; pos < positions; pos++) {
        pageIndices[flatIndex] = pageIdx;
        positionsInPage[flatIndex] = pos;
        flatIndex++;
      }
    }

    // Build hash table
    hashTable = new HashMap<>(positionCount);
    for (int i = 0; i < positionCount; i++) {
      Page page = pages.get(pageIndices[i]);
      int position = positionsInPage[i];
      JoinKey key = extractKey(page, position);

      // Skip null keys — null != null in equi-join
      if (key.hasNull()) {
        continue;
      }

      hashTable.merge(
          key,
          new int[] {i},
          (existing, single) -> {
            int[] merged = Arrays.copyOf(existing, existing.length + 1);
            merged[existing.length] = single[0];
            return merged;
          });
    }

    // Estimate memory
    estimatedSizeInBytes =
        (long) positionCount * (Integer.BYTES * 2) // pageIndices + positionsInPage
            + (long) hashTable.size() * 100L; // rough hash map overhead
  }

  /**
   * Finds all build-side position indices matching the given probe key.
   *
   * @param probePage the probe-side page
   * @param probePosition the position within the probe page
   * @param probeKeyChannels column indices in the probe page corresponding to join keys
   * @return array of matching flat position indices, or empty array if no match
   */
  public int[] getMatchingPositions(Page probePage, int probePosition, int[] probeKeyChannels) {
    JoinKey probeKey = extractKey(probePage, probePosition, probeKeyChannels);
    if (probeKey.hasNull()) {
      return EMPTY_POSITIONS;
    }
    int[] matches = hashTable.get(probeKey);
    return matches != null ? matches : EMPTY_POSITIONS;
  }

  /** Returns the total number of build-side positions. */
  public int getPositionCount() {
    return positionCount;
  }

  /** Returns the page index for a flat position address. */
  public int getPageIndex(int flatPosition) {
    return pageIndices[flatPosition];
  }

  /** Returns the position within the page for a flat position address. */
  public int getPositionInPage(int flatPosition) {
    return positionsInPage[flatPosition];
  }

  /** Returns the build-side page at the given page index. */
  public Page getPage(int pageIndex) {
    return pages.get(pageIndex);
  }

  /** Returns the number of build-side pages. */
  public int getPageCount() {
    return pages.size();
  }

  /** Returns the number of channels (columns) in build-side pages. */
  public int getChannelCount() {
    return pages.isEmpty() ? 0 : pages.get(0).getChannelCount();
  }

  /** Returns the block at the given channel for the given flat position. */
  public Block getBlockForPosition(int flatPosition, int channel) {
    Page page = pages.get(pageIndices[flatPosition]);
    return page.getBlock(channel);
  }

  /** Returns the estimated memory usage in bytes. */
  public long getEstimatedSizeInBytes() {
    long pageBytes = 0;
    for (Page page : pages) {
      pageBytes += page.getRetainedSizeInBytes();
    }
    return estimatedSizeInBytes + pageBytes;
  }

  private JoinKey extractKey(Page page, int position) {
    return extractKey(page, position, hashKeyChannels);
  }

  private static JoinKey extractKey(Page page, int position, int[] keyChannels) {
    Object[] values = new Object[keyChannels.length];
    boolean hasNull = false;
    for (int i = 0; i < keyChannels.length; i++) {
      Block block = page.getBlock(keyChannels[i]);
      if (block.isNull(position)) {
        values[i] = null;
        hasNull = true;
      } else {
        values[i] = extractValue(block, position);
      }
    }
    return new JoinKey(values, hasNull);
  }

  private static Object extractValue(Block block, int position) {
    Block resolved = resolveValueBlock(block);
    int resolvedPos = resolvePosition(block, position);

    if (resolved instanceof LongArrayBlock longBlock) {
      return longBlock.getLong(resolvedPos);
    }
    if (resolved instanceof DoubleArrayBlock doubleBlock) {
      return doubleBlock.getDouble(resolvedPos);
    }
    if (resolved instanceof IntArrayBlock intBlock) {
      return intBlock.getInt(resolvedPos);
    }
    if (resolved instanceof ShortArrayBlock shortBlock) {
      return shortBlock.getShort(resolvedPos);
    }
    if (resolved instanceof ByteArrayBlock byteBlock) {
      return byteBlock.getByte(resolvedPos);
    }
    if (resolved instanceof BooleanArrayBlock boolBlock) {
      return boolBlock.getBoolean(resolvedPos);
    }
    if (resolved instanceof VariableWidthBlock varBlock) {
      return varBlock.getSlice(resolvedPos);
    }
    throw new UnsupportedOperationException(
        "Unsupported block type for join key: " + resolved.getClass().getSimpleName());
  }

  private static Block resolveValueBlock(Block block) {
    if (block instanceof DictionaryBlock dict) {
      return dict.getDictionary();
    }
    if (block instanceof RunLengthEncodedBlock rle) {
      return rle.getValue();
    }
    return block;
  }

  private static int resolvePosition(Block block, int position) {
    if (block instanceof DictionaryBlock dict) {
      return dict.getId(position);
    }
    if (block instanceof RunLengthEncodedBlock) {
      return 0;
    }
    return position;
  }

  private static final int[] EMPTY_POSITIONS = new int[0];

  /** Composite key for join lookups. Uses value-based equality (same pattern as GroupByHash). */
  static final class JoinKey {
    private final Object[] values;
    private final boolean hasNull;
    private final int hash;

    JoinKey(Object[] values, boolean hasNull) {
      this.values = values;
      this.hasNull = hasNull;
      this.hash = computeHash(values);
    }

    boolean hasNull() {
      return hasNull;
    }

    private static int computeHash(Object[] values) {
      int h = 1;
      for (Object value : values) {
        if (value == null) {
          h = 31 * h;
        } else if (value instanceof byte[] bytes) {
          h = 31 * h + Arrays.hashCode(bytes);
        } else {
          h = 31 * h + value.hashCode();
        }
      }
      return h;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (!(other instanceof JoinKey that)) return false;
      if (values.length != that.values.length) return false;
      for (int i = 0; i < values.length; i++) {
        Object a = values[i];
        Object b = that.values[i];
        if (a == null && b == null) continue;
        if (a == null || b == null) return false;
        if (a instanceof byte[] aBytes && b instanceof byte[] bBytes) {
          if (!Arrays.equals(aBytes, bBytes)) return false;
        } else if (!a.equals(b)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return hash;
    }
  }
}
