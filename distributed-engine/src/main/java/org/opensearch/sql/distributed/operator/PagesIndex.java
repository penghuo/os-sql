/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
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
 * Simplified version of Trino's PagesIndex. Stores rows from multiple pages and supports sorting
 * via synthetic addresses. Each address encodes (pageIndex, positionInPage).
 *
 * <p>Used by TopNOperator and OrderByOperator for in-memory sorting.
 */
public class PagesIndex {

  /** Default maximum number of positions per output page. */
  static final int DEFAULT_OUTPUT_PAGE_SIZE = 1024;

  private final List<Page> pages;
  private long[] addresses;
  private int positionCount;

  public PagesIndex(int expectedPositions) {
    this.pages = new ArrayList<>();
    this.addresses = new long[Math.max(expectedPositions, 64)];
    this.positionCount = 0;
  }

  public void addPage(Page page) {
    Objects.requireNonNull(page, "page is null");
    if (page.getPositionCount() == 0) {
      return;
    }
    int pageIndex = pages.size();
    pages.add(page);

    int newCount = positionCount + page.getPositionCount();
    ensureCapacity(newCount);
    for (int pos = 0; pos < page.getPositionCount(); pos++) {
      addresses[positionCount + pos] = encodeSyntheticAddress(pageIndex, pos);
    }
    positionCount = newCount;
  }

  private void ensureCapacity(int requiredCapacity) {
    if (requiredCapacity > addresses.length) {
      int newCapacity = Math.max(addresses.length * 2, requiredCapacity);
      addresses = Arrays.copyOf(addresses, newCapacity);
    }
  }

  public int getPositionCount() {
    return positionCount;
  }

  /** Returns the estimated memory usage in bytes. */
  public long getEstimatedSizeInBytes() {
    long size = (long) Long.BYTES * addresses.length;
    for (Page page : pages) {
      size += page.getRetainedSizeInBytes();
    }
    return size;
  }

  /** Sorts the address index using the given comparator. */
  public void sort(PageWithPositionComparator comparator) {
    sort(comparator, 0, positionCount);
  }

  /** Sorts a range of the address index. */
  public void sort(PageWithPositionComparator comparator, int fromIndex, int toIndex) {
    Objects.requireNonNull(comparator, "comparator is null");
    if (fromIndex < 0 || toIndex > positionCount || fromIndex > toIndex) {
      throw new IndexOutOfBoundsException(
          "fromIndex=" + fromIndex + ", toIndex=" + toIndex + ", size=" + positionCount);
    }
    // TimSort-based sort on address range
    Long[] boxed = new Long[toIndex - fromIndex];
    for (int i = 0; i < boxed.length; i++) {
      boxed[i] = addresses[fromIndex + i];
    }
    Arrays.sort(
        boxed,
        (a, b) -> {
          int pageA = decodePageIndex(a);
          int posA = decodePosition(a);
          int pageB = decodePageIndex(b);
          int posB = decodePosition(b);
          return comparator.compareTo(pages.get(pageA), posA, pages.get(pageB), posB);
        });
    for (int i = 0; i < boxed.length; i++) {
      addresses[fromIndex + i] = boxed[i];
    }
  }

  /** Returns an iterator over sorted pages. */
  public Iterator<Page> getSortedPages() {
    return getSortedPages(DEFAULT_OUTPUT_PAGE_SIZE);
  }

  public Iterator<Page> getSortedPages(int maxPageSize) {
    return getSortedPages(0, positionCount, maxPageSize);
  }

  public Iterator<Page> getSortedPages(int start, int end, int maxPageSize) {
    if (positionCount == 0 || start >= end || pages.isEmpty()) {
      return List.<Page>of().iterator();
    }
    int channelCount = pages.get(0).getChannelCount();
    return new SortedPageIterator(start, end, channelCount, maxPageSize);
  }

  /** Swaps two positions in the address array. */
  public void swap(int a, int b) {
    long temp = addresses[a];
    addresses[a] = addresses[b];
    addresses[b] = temp;
  }

  /** Gets the source page for a given address index. */
  public Page getPageForAddress(int index) {
    return pages.get(decodePageIndex(addresses[index]));
  }

  /** Gets the position within the source page for a given address index. */
  public int getPositionForAddress(int index) {
    return decodePosition(addresses[index]);
  }

  /** Clears all stored pages and resets the index. */
  public void clear() {
    pages.clear();
    positionCount = 0;
  }

  // --- Synthetic address encoding ---

  static long encodeSyntheticAddress(int pageIndex, int position) {
    return ((long) pageIndex << 32) | (position & 0xFFFFFFFFL);
  }

  static int decodePageIndex(long address) {
    return (int) (address >>> 32);
  }

  static int decodePosition(long address) {
    return (int) address;
  }

  // --- Block building utilities ---

  /** Resolves a block to its underlying value block (unwraps Dictionary/RLE). */
  private static Block resolveValueBlock(Block block) {
    if (block instanceof DictionaryBlock dict) {
      return dict.getDictionary();
    }
    if (block instanceof RunLengthEncodedBlock rle) {
      return rle.getValue();
    }
    return block;
  }

  /** Resolves the actual position within the underlying value block. */
  private static int resolvePosition(Block block, int position) {
    if (block instanceof DictionaryBlock dict) {
      return dict.getId(position);
    }
    if (block instanceof RunLengthEncodedBlock) {
      return 0;
    }
    return position;
  }

  /**
   * Builds an output block for the given channel from a set of sorted address indices.
   *
   * @param addressIndices indices into the addresses array
   * @param channel the column to extract
   * @param count number of indices
   */
  private Block buildOutputBlock(int[] addressIndices, int channel, int count) {
    // Determine block type from the first page
    Block sampleBlock = resolveValueBlock(pages.get(0).getBlock(channel));

    if (sampleBlock instanceof LongArrayBlock) {
      return buildLongBlock(addressIndices, channel, count);
    } else if (sampleBlock instanceof DoubleArrayBlock) {
      return buildDoubleBlock(addressIndices, channel, count);
    } else if (sampleBlock instanceof IntArrayBlock) {
      return buildIntBlock(addressIndices, channel, count);
    } else if (sampleBlock instanceof ShortArrayBlock) {
      return buildShortBlock(addressIndices, channel, count);
    } else if (sampleBlock instanceof ByteArrayBlock) {
      return buildByteBlock(addressIndices, channel, count);
    } else if (sampleBlock instanceof BooleanArrayBlock) {
      return buildBooleanBlock(addressIndices, channel, count);
    } else if (sampleBlock instanceof VariableWidthBlock) {
      return buildVariableWidthBlock(addressIndices, channel, count);
    }
    throw new UnsupportedOperationException(
        "Unsupported block type: " + sampleBlock.getClass().getSimpleName());
  }

  private LongArrayBlock buildLongBlock(int[] addressIndices, int channel, int count) {
    long[] values = new long[count];
    boolean[] nulls = null;
    boolean hasNull = false;
    for (int i = 0; i < count; i++) {
      long addr = addresses[addressIndices[i]];
      Page page = pages.get(decodePageIndex(addr));
      int pos = decodePosition(addr);
      Block block = page.getBlock(channel);
      if (block.isNull(pos)) {
        if (nulls == null) {
          nulls = new boolean[count];
        }
        nulls[i] = true;
        hasNull = true;
      } else {
        Block resolved = resolveValueBlock(block);
        int resolvedPos = resolvePosition(block, pos);
        values[i] = ((LongArrayBlock) resolved).getLong(resolvedPos);
      }
    }
    return new LongArrayBlock(count, hasNull ? Optional.of(nulls) : Optional.empty(), values);
  }

  private DoubleArrayBlock buildDoubleBlock(int[] addressIndices, int channel, int count) {
    double[] values = new double[count];
    boolean[] nulls = null;
    boolean hasNull = false;
    for (int i = 0; i < count; i++) {
      long addr = addresses[addressIndices[i]];
      Page page = pages.get(decodePageIndex(addr));
      int pos = decodePosition(addr);
      Block block = page.getBlock(channel);
      if (block.isNull(pos)) {
        if (nulls == null) {
          nulls = new boolean[count];
        }
        nulls[i] = true;
        hasNull = true;
      } else {
        Block resolved = resolveValueBlock(block);
        int resolvedPos = resolvePosition(block, pos);
        values[i] = ((DoubleArrayBlock) resolved).getDouble(resolvedPos);
      }
    }
    return new DoubleArrayBlock(count, hasNull ? Optional.of(nulls) : Optional.empty(), values);
  }

  private IntArrayBlock buildIntBlock(int[] addressIndices, int channel, int count) {
    int[] values = new int[count];
    boolean[] nulls = null;
    boolean hasNull = false;
    for (int i = 0; i < count; i++) {
      long addr = addresses[addressIndices[i]];
      Page page = pages.get(decodePageIndex(addr));
      int pos = decodePosition(addr);
      Block block = page.getBlock(channel);
      if (block.isNull(pos)) {
        if (nulls == null) {
          nulls = new boolean[count];
        }
        nulls[i] = true;
        hasNull = true;
      } else {
        Block resolved = resolveValueBlock(block);
        int resolvedPos = resolvePosition(block, pos);
        values[i] = ((IntArrayBlock) resolved).getInt(resolvedPos);
      }
    }
    return new IntArrayBlock(count, hasNull ? Optional.of(nulls) : Optional.empty(), values);
  }

  private ShortArrayBlock buildShortBlock(int[] addressIndices, int channel, int count) {
    short[] values = new short[count];
    boolean[] nulls = null;
    boolean hasNull = false;
    for (int i = 0; i < count; i++) {
      long addr = addresses[addressIndices[i]];
      Page page = pages.get(decodePageIndex(addr));
      int pos = decodePosition(addr);
      Block block = page.getBlock(channel);
      if (block.isNull(pos)) {
        if (nulls == null) {
          nulls = new boolean[count];
        }
        nulls[i] = true;
        hasNull = true;
      } else {
        Block resolved = resolveValueBlock(block);
        int resolvedPos = resolvePosition(block, pos);
        values[i] = ((ShortArrayBlock) resolved).getShort(resolvedPos);
      }
    }
    return new ShortArrayBlock(count, hasNull ? Optional.of(nulls) : Optional.empty(), values);
  }

  private ByteArrayBlock buildByteBlock(int[] addressIndices, int channel, int count) {
    byte[] values = new byte[count];
    boolean[] nulls = null;
    boolean hasNull = false;
    for (int i = 0; i < count; i++) {
      long addr = addresses[addressIndices[i]];
      Page page = pages.get(decodePageIndex(addr));
      int pos = decodePosition(addr);
      Block block = page.getBlock(channel);
      if (block.isNull(pos)) {
        if (nulls == null) {
          nulls = new boolean[count];
        }
        nulls[i] = true;
        hasNull = true;
      } else {
        Block resolved = resolveValueBlock(block);
        int resolvedPos = resolvePosition(block, pos);
        values[i] = ((ByteArrayBlock) resolved).getByte(resolvedPos);
      }
    }
    return new ByteArrayBlock(count, hasNull ? Optional.of(nulls) : Optional.empty(), values);
  }

  private BooleanArrayBlock buildBooleanBlock(int[] addressIndices, int channel, int count) {
    boolean[] values = new boolean[count];
    boolean[] nulls = null;
    boolean hasNull = false;
    for (int i = 0; i < count; i++) {
      long addr = addresses[addressIndices[i]];
      Page page = pages.get(decodePageIndex(addr));
      int pos = decodePosition(addr);
      Block block = page.getBlock(channel);
      if (block.isNull(pos)) {
        if (nulls == null) {
          nulls = new boolean[count];
        }
        nulls[i] = true;
        hasNull = true;
      } else {
        Block resolved = resolveValueBlock(block);
        int resolvedPos = resolvePosition(block, pos);
        values[i] = ((BooleanArrayBlock) resolved).getBoolean(resolvedPos);
      }
    }
    return new BooleanArrayBlock(count, hasNull ? Optional.of(nulls) : Optional.empty(), values);
  }

  private VariableWidthBlock buildVariableWidthBlock(int[] addressIndices, int channel, int count) {
    // First pass: calculate total bytes
    int totalBytes = 0;
    boolean hasNull = false;
    for (int i = 0; i < count; i++) {
      long addr = addresses[addressIndices[i]];
      Page page = pages.get(decodePageIndex(addr));
      int pos = decodePosition(addr);
      Block block = page.getBlock(channel);
      if (block.isNull(pos)) {
        hasNull = true;
      } else {
        Block resolved = resolveValueBlock(block);
        int resolvedPos = resolvePosition(block, pos);
        totalBytes += ((VariableWidthBlock) resolved).getSliceLength(resolvedPos);
      }
    }

    byte[] slice = new byte[totalBytes];
    int[] offsets = new int[count + 1];
    boolean[] nulls = hasNull ? new boolean[count] : null;
    int byteOffset = 0;
    for (int i = 0; i < count; i++) {
      long addr = addresses[addressIndices[i]];
      Page page = pages.get(decodePageIndex(addr));
      int pos = decodePosition(addr);
      Block block = page.getBlock(channel);
      if (block.isNull(pos)) {
        nulls[i] = true;
        offsets[i + 1] = byteOffset;
      } else {
        Block resolved = resolveValueBlock(block);
        int resolvedPos = resolvePosition(block, pos);
        byte[] data = ((VariableWidthBlock) resolved).getSlice(resolvedPos);
        System.arraycopy(data, 0, slice, byteOffset, data.length);
        byteOffset += data.length;
        offsets[i + 1] = byteOffset;
      }
    }
    return new VariableWidthBlock(
        count, slice, offsets, hasNull ? Optional.of(nulls) : Optional.empty());
  }

  /** Iterator that materializes sorted pages from the address index. */
  private class SortedPageIterator implements Iterator<Page> {
    private int currentPosition;
    private final int endPosition;
    private final int channelCount;
    private final int maxPageSize;

    SortedPageIterator(int start, int end, int channelCount, int maxPageSize) {
      this.currentPosition = start;
      this.endPosition = end;
      this.channelCount = channelCount;
      this.maxPageSize = maxPageSize;
    }

    @Override
    public boolean hasNext() {
      return currentPosition < endPosition;
    }

    @Override
    public Page next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      int remaining = endPosition - currentPosition;
      int pageSize = Math.min(remaining, maxPageSize);
      int[] addressIndices = new int[pageSize];
      for (int i = 0; i < pageSize; i++) {
        addressIndices[i] = currentPosition + i;
      }

      Block[] outputBlocks = new Block[channelCount];
      for (int ch = 0; ch < channelCount; ch++) {
        outputBlocks[ch] = buildOutputBlock(addressIndices, ch, pageSize);
      }
      currentPosition += pageSize;
      return new Page(pageSize, outputBlocks);
    }
  }
}
