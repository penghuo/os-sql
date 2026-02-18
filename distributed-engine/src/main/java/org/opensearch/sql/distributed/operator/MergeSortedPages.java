/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.BooleanArrayBlock;
import org.opensearch.sql.distributed.data.ByteArrayBlock;
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.IntArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.ShortArrayBlock;
import org.opensearch.sql.distributed.data.VariableWidthBlock;

/**
 * K-way merge of pre-sorted Page streams into a single sorted output stream. Ported from Trino's
 * io.trino.operator.MergeSortedPages. Used by the coordinator to merge sorted results from multiple
 * shards.
 */
public final class MergeSortedPages {

  /** Default maximum number of positions per output page. */
  private static final int DEFAULT_MAX_PAGE_SIZE = 1024;

  private MergeSortedPages() {}

  /**
   * Merges multiple sorted page iterators into a single sorted iterator of pages.
   *
   * @param sortedStreams list of iterators, each producing pages in sorted order
   * @param comparator comparator for row ordering across pages
   * @param outputChannels which channels to include in the output (column selection)
   * @return an iterator of merged, sorted pages
   */
  public static Iterator<Page> mergeSortedPages(
      List<Iterator<Page>> sortedStreams,
      PageWithPositionComparator comparator,
      int[] outputChannels) {
    return mergeSortedPages(sortedStreams, comparator, outputChannels, DEFAULT_MAX_PAGE_SIZE);
  }

  /**
   * Merges multiple sorted page iterators into a single sorted iterator of pages.
   *
   * @param sortedStreams list of iterators, each producing pages in sorted order
   * @param comparator comparator for row ordering across pages
   * @param outputChannels which channels to include in the output
   * @param maxPageSize maximum number of positions per output page
   * @return an iterator of merged, sorted pages
   */
  public static Iterator<Page> mergeSortedPages(
      List<Iterator<Page>> sortedStreams,
      PageWithPositionComparator comparator,
      int[] outputChannels,
      int maxPageSize) {
    Objects.requireNonNull(sortedStreams, "sortedStreams is null");
    Objects.requireNonNull(comparator, "comparator is null");
    Objects.requireNonNull(outputChannels, "outputChannels is null");
    if (maxPageSize <= 0) {
      throw new IllegalArgumentException("maxPageSize must be positive");
    }

    if (sortedStreams.isEmpty()) {
      return List.<Page>of().iterator();
    }
    if (sortedStreams.size() == 1) {
      return new SingleStreamProjector(sortedStreams.get(0), outputChannels);
    }

    return new MergingPageIterator(sortedStreams, comparator, outputChannels, maxPageSize);
  }

  /**
   * Internal entry in the priority queue. Tracks a page + current position within that page, plus
   * which stream it came from.
   */
  private static final class PageCursor {
    final int streamIndex;
    Page page;
    int position;

    PageCursor(int streamIndex, Page page) {
      this.streamIndex = streamIndex;
      this.page = page;
      this.position = 0;
    }

    boolean hasMorePositions() {
      return position < page.getPositionCount();
    }
  }

  /** Iterator that performs the K-way merge. */
  private static final class MergingPageIterator implements Iterator<Page> {

    private final List<Iterator<Page>> streams;
    private final PageWithPositionComparator comparator;
    private final int[] outputChannels;
    private final int maxPageSize;
    private final PriorityQueue<PageCursor> heap;
    private boolean initialized;

    MergingPageIterator(
        List<Iterator<Page>> streams,
        PageWithPositionComparator comparator,
        int[] outputChannels,
        int maxPageSize) {
      this.streams = streams;
      this.comparator = comparator;
      this.outputChannels = outputChannels;
      this.maxPageSize = maxPageSize;
      this.heap =
          new PriorityQueue<>(
              Math.max(1, streams.size()),
              (a, b) -> comparator.compareTo(a.page, a.position, b.page, b.position));
      this.initialized = false;
    }

    private void initialize() {
      for (int i = 0; i < streams.size(); i++) {
        advanceStream(i);
      }
      initialized = true;
    }

    /** Advance a stream: skip empty pages, add next non-empty page to heap. */
    private void advanceStream(int streamIndex) {
      Iterator<Page> stream = streams.get(streamIndex);
      while (stream.hasNext()) {
        Page page = stream.next();
        if (page != null && page.getPositionCount() > 0) {
          heap.add(new PageCursor(streamIndex, page));
          return;
        }
      }
    }

    @Override
    public boolean hasNext() {
      if (!initialized) {
        initialize();
      }
      return !heap.isEmpty();
    }

    @Override
    public Page next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return buildNextPage();
    }

    private Page buildNextPage() {
      // Collect positions from the heap in sorted order
      List<PageCursor> selectedCursors = new ArrayList<>();
      List<Integer> selectedPositions = new ArrayList<>();

      int count = 0;
      while (!heap.isEmpty() && count < maxPageSize) {
        PageCursor cursor = heap.poll();
        selectedCursors.add(cursor);
        selectedPositions.add(cursor.position);
        count++;

        cursor.position++;
        if (cursor.hasMorePositions()) {
          heap.add(cursor);
        } else {
          advanceStream(cursor.streamIndex);
        }
      }

      return buildPage(selectedCursors, selectedPositions, outputChannels);
    }
  }

  /**
   * Builds an output page from selected rows. Each (cursor, position) pair identifies a row in a
   * source page.
   */
  private static Page buildPage(
      List<PageCursor> cursors, List<Integer> positions, int[] outputChannels) {
    int positionCount = cursors.size();
    if (positionCount == 0) {
      return new Page(0);
    }

    Block[] outputBlocks = new Block[outputChannels.length];
    for (int col = 0; col < outputChannels.length; col++) {
      outputBlocks[col] = buildOutputBlock(cursors, positions, outputChannels[col]);
    }
    return new Page(positionCount, outputBlocks);
  }

  /**
   * Builds a single output block by copying values from multiple source pages. Auto-detects the
   * block type from the first non-null source.
   */
  private static Block buildOutputBlock(
      List<PageCursor> cursors, List<Integer> positions, int channel) {
    int count = cursors.size();
    // Determine the block type from the first cursor
    Block sampleBlock = resolveValueBlock(cursors.get(0).page.getBlock(channel));

    if (sampleBlock instanceof LongArrayBlock) {
      return buildLongBlock(cursors, positions, channel, count);
    } else if (sampleBlock instanceof DoubleArrayBlock) {
      return buildDoubleBlock(cursors, positions, channel, count);
    } else if (sampleBlock instanceof IntArrayBlock) {
      return buildIntBlock(cursors, positions, channel, count);
    } else if (sampleBlock instanceof ShortArrayBlock) {
      return buildShortBlock(cursors, positions, channel, count);
    } else if (sampleBlock instanceof ByteArrayBlock) {
      return buildByteBlock(cursors, positions, channel, count);
    } else if (sampleBlock instanceof BooleanArrayBlock) {
      return buildBooleanBlock(cursors, positions, channel, count);
    } else if (sampleBlock instanceof VariableWidthBlock) {
      return buildVariableWidthBlock(cursors, positions, channel, count);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported block type: " + sampleBlock.getClass().getSimpleName());
    }
  }

  /** Resolves a block to its underlying value block (unwraps Dictionary/RLE). */
  private static Block resolveValueBlock(Block block) {
    if (block instanceof org.opensearch.sql.distributed.data.DictionaryBlock dict) {
      return dict.getDictionary();
    }
    if (block instanceof org.opensearch.sql.distributed.data.RunLengthEncodedBlock rle) {
      return rle.getValue();
    }
    return block;
  }

  /** Gets a resolved value from a source block accounting for Dictionary/RLE indirection. */
  private static int resolvePosition(Block block, int position) {
    if (block instanceof org.opensearch.sql.distributed.data.DictionaryBlock dict) {
      return dict.getId(position);
    }
    if (block instanceof org.opensearch.sql.distributed.data.RunLengthEncodedBlock) {
      return 0;
    }
    return position;
  }

  private static LongArrayBlock buildLongBlock(
      List<PageCursor> cursors, List<Integer> positions, int channel, int count) {
    long[] values = new long[count];
    boolean[] nulls = null;
    boolean hasNull = false;
    for (int i = 0; i < count; i++) {
      Block srcBlock = cursors.get(i).page.getBlock(channel);
      int srcPos = positions.get(i);
      if (srcBlock.isNull(srcPos)) {
        if (nulls == null) {
          nulls = new boolean[count];
        }
        nulls[i] = true;
        hasNull = true;
      } else {
        Block resolved = resolveValueBlock(srcBlock);
        int resolvedPos = resolvePosition(srcBlock, srcPos);
        values[i] = ((LongArrayBlock) resolved).getLong(resolvedPos);
      }
    }
    return new LongArrayBlock(
        count, hasNull ? java.util.Optional.of(nulls) : java.util.Optional.empty(), values);
  }

  private static DoubleArrayBlock buildDoubleBlock(
      List<PageCursor> cursors, List<Integer> positions, int channel, int count) {
    double[] values = new double[count];
    boolean[] nulls = null;
    boolean hasNull = false;
    for (int i = 0; i < count; i++) {
      Block srcBlock = cursors.get(i).page.getBlock(channel);
      int srcPos = positions.get(i);
      if (srcBlock.isNull(srcPos)) {
        if (nulls == null) {
          nulls = new boolean[count];
        }
        nulls[i] = true;
        hasNull = true;
      } else {
        Block resolved = resolveValueBlock(srcBlock);
        int resolvedPos = resolvePosition(srcBlock, srcPos);
        values[i] = ((DoubleArrayBlock) resolved).getDouble(resolvedPos);
      }
    }
    return new DoubleArrayBlock(
        count, hasNull ? java.util.Optional.of(nulls) : java.util.Optional.empty(), values);
  }

  private static IntArrayBlock buildIntBlock(
      List<PageCursor> cursors, List<Integer> positions, int channel, int count) {
    int[] values = new int[count];
    boolean[] nulls = null;
    boolean hasNull = false;
    for (int i = 0; i < count; i++) {
      Block srcBlock = cursors.get(i).page.getBlock(channel);
      int srcPos = positions.get(i);
      if (srcBlock.isNull(srcPos)) {
        if (nulls == null) {
          nulls = new boolean[count];
        }
        nulls[i] = true;
        hasNull = true;
      } else {
        Block resolved = resolveValueBlock(srcBlock);
        int resolvedPos = resolvePosition(srcBlock, srcPos);
        values[i] = ((IntArrayBlock) resolved).getInt(resolvedPos);
      }
    }
    return new IntArrayBlock(
        count, hasNull ? java.util.Optional.of(nulls) : java.util.Optional.empty(), values);
  }

  private static ShortArrayBlock buildShortBlock(
      List<PageCursor> cursors, List<Integer> positions, int channel, int count) {
    short[] values = new short[count];
    boolean[] nulls = null;
    boolean hasNull = false;
    for (int i = 0; i < count; i++) {
      Block srcBlock = cursors.get(i).page.getBlock(channel);
      int srcPos = positions.get(i);
      if (srcBlock.isNull(srcPos)) {
        if (nulls == null) {
          nulls = new boolean[count];
        }
        nulls[i] = true;
        hasNull = true;
      } else {
        Block resolved = resolveValueBlock(srcBlock);
        int resolvedPos = resolvePosition(srcBlock, srcPos);
        values[i] = ((ShortArrayBlock) resolved).getShort(resolvedPos);
      }
    }
    return new ShortArrayBlock(
        count, hasNull ? java.util.Optional.of(nulls) : java.util.Optional.empty(), values);
  }

  private static ByteArrayBlock buildByteBlock(
      List<PageCursor> cursors, List<Integer> positions, int channel, int count) {
    byte[] values = new byte[count];
    boolean[] nulls = null;
    boolean hasNull = false;
    for (int i = 0; i < count; i++) {
      Block srcBlock = cursors.get(i).page.getBlock(channel);
      int srcPos = positions.get(i);
      if (srcBlock.isNull(srcPos)) {
        if (nulls == null) {
          nulls = new boolean[count];
        }
        nulls[i] = true;
        hasNull = true;
      } else {
        Block resolved = resolveValueBlock(srcBlock);
        int resolvedPos = resolvePosition(srcBlock, srcPos);
        values[i] = ((ByteArrayBlock) resolved).getByte(resolvedPos);
      }
    }
    return new ByteArrayBlock(
        count, hasNull ? java.util.Optional.of(nulls) : java.util.Optional.empty(), values);
  }

  private static BooleanArrayBlock buildBooleanBlock(
      List<PageCursor> cursors, List<Integer> positions, int channel, int count) {
    boolean[] values = new boolean[count];
    boolean[] nulls = null;
    boolean hasNull = false;
    for (int i = 0; i < count; i++) {
      Block srcBlock = cursors.get(i).page.getBlock(channel);
      int srcPos = positions.get(i);
      if (srcBlock.isNull(srcPos)) {
        if (nulls == null) {
          nulls = new boolean[count];
        }
        nulls[i] = true;
        hasNull = true;
      } else {
        Block resolved = resolveValueBlock(srcBlock);
        int resolvedPos = resolvePosition(srcBlock, srcPos);
        values[i] = ((BooleanArrayBlock) resolved).getBoolean(resolvedPos);
      }
    }
    return new BooleanArrayBlock(
        count, hasNull ? java.util.Optional.of(nulls) : java.util.Optional.empty(), values);
  }

  private static VariableWidthBlock buildVariableWidthBlock(
      List<PageCursor> cursors, List<Integer> positions, int channel, int count) {
    // First pass: calculate total bytes
    int totalBytes = 0;
    boolean hasNull = false;
    for (int i = 0; i < count; i++) {
      Block srcBlock = cursors.get(i).page.getBlock(channel);
      int srcPos = positions.get(i);
      if (srcBlock.isNull(srcPos)) {
        hasNull = true;
      } else {
        Block resolved = resolveValueBlock(srcBlock);
        int resolvedPos = resolvePosition(srcBlock, srcPos);
        totalBytes += ((VariableWidthBlock) resolved).getSliceLength(resolvedPos);
      }
    }

    byte[] slice = new byte[totalBytes];
    int[] offsets = new int[count + 1];
    boolean[] nulls = hasNull ? new boolean[count] : null;
    int byteOffset = 0;
    for (int i = 0; i < count; i++) {
      Block srcBlock = cursors.get(i).page.getBlock(channel);
      int srcPos = positions.get(i);
      if (srcBlock.isNull(srcPos)) {
        nulls[i] = true;
        offsets[i + 1] = byteOffset;
      } else {
        Block resolved = resolveValueBlock(srcBlock);
        int resolvedPos = resolvePosition(srcBlock, srcPos);
        byte[] data = ((VariableWidthBlock) resolved).getSlice(resolvedPos);
        System.arraycopy(data, 0, slice, byteOffset, data.length);
        byteOffset += data.length;
        offsets[i + 1] = byteOffset;
      }
    }
    return new VariableWidthBlock(
        count, slice, offsets, hasNull ? java.util.Optional.of(nulls) : java.util.Optional.empty());
  }

  /** Simple projector that applies column selection to a single stream (optimization). */
  private static final class SingleStreamProjector implements Iterator<Page> {
    private final Iterator<Page> source;
    private final int[] outputChannels;

    SingleStreamProjector(Iterator<Page> source, int[] outputChannels) {
      this.source = source;
      this.outputChannels = outputChannels;
    }

    @Override
    public boolean hasNext() {
      return source.hasNext();
    }

    @Override
    public Page next() {
      Page page = source.next();
      return page.getColumns(outputChannels);
    }
  }
}
