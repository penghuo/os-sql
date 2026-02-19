/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.spill;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.operator.Operator;
import org.opensearch.sql.distributed.operator.OperatorFactory;
import org.opensearch.sql.distributed.operator.PageWithPositionComparator;
import org.opensearch.sql.distributed.operator.PagesIndex;
import org.opensearch.sql.distributed.operator.SimplePageWithPositionComparator;
import org.opensearch.sql.distributed.operator.SortOrder;

/**
 * Extends the Phase 1 in-memory {@link org.opensearch.sql.distributed.operator.OrderByOperator}
 * with external sort support (spill-to-disk). When the in-memory PagesIndex exceeds the memory
 * threshold:
 *
 * <ol>
 *   <li>Sorts the current batch in memory
 *   <li>Spills sorted pages to disk via {@link FileSingleStreamSpiller}
 *   <li>Clears the in-memory buffer
 *   <li>Continues accepting input into a fresh buffer
 * </ol>
 *
 * <p>On finish(), performs a k-way merge of all spill files plus any remaining in-memory data,
 * using a priority queue to maintain sorted order across all runs.
 *
 * <p>Output schema is determined by outputChannels (same as OrderByOperator).
 */
public class SpillableOrderByOperator implements Operator, MemoryRevocableOperator {

  private enum State {
    NEEDS_INPUT,
    SPILLING,
    HAS_OUTPUT,
    FINISHED
  }

  private final OperatorContext operatorContext;
  private final PageWithPositionComparator comparator;
  private final int[] outputChannels;
  private final SpillerFactory spillerFactory;
  private final long memoryThreshold;

  /** In-memory buffer for the current sort run. */
  private PagesIndex pagesIndex;

  /** Spill files containing previously sorted runs. */
  private final List<FileSingleStreamSpiller> spillFiles;

  private final List<Integer> sortChannels;
  private final List<SortOrder> sortOrders;

  private State state;
  private Iterator<Page> outputIterator;
  private long currentRevocableMemory;

  public SpillableOrderByOperator(
      OperatorContext operatorContext,
      List<Integer> sortChannels,
      List<SortOrder> sortOrders,
      int[] outputChannels,
      SpillerFactory spillerFactory,
      long memoryThreshold) {
    this.operatorContext = Objects.requireNonNull(operatorContext);
    this.sortChannels = List.copyOf(sortChannels);
    this.sortOrders = List.copyOf(sortOrders);
    this.comparator = new SimplePageWithPositionComparator(sortChannels, sortOrders);
    this.outputChannels = Objects.requireNonNull(outputChannels);
    this.spillerFactory = Objects.requireNonNull(spillerFactory);
    this.memoryThreshold = memoryThreshold;
    this.pagesIndex = new PagesIndex(1024);
    this.spillFiles = new ArrayList<>();
    this.state = State.NEEDS_INPUT;
    this.currentRevocableMemory = 0;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return NOT_BLOCKED;
  }

  @Override
  public boolean needsInput() {
    return state == State.NEEDS_INPUT;
  }

  @Override
  public void addInput(Page page) {
    if (state != State.NEEDS_INPUT) {
      throw new IllegalStateException("Operator does not need input");
    }
    Objects.requireNonNull(page);
    if (page.getPositionCount() > 0) {
      pagesIndex.addPage(page);
      currentRevocableMemory = pagesIndex.getEstimatedSizeInBytes();

      // Check if we need to spill
      if (currentRevocableMemory > memoryThreshold) {
        spillCurrentRun();
      }
    }
  }

  /** Sorts the current in-memory buffer and spills to disk. */
  private void spillCurrentRun() {
    if (pagesIndex.getPositionCount() == 0) {
      return;
    }

    // Sort the current buffer
    pagesIndex.sort(comparator);

    // Spill sorted pages to disk
    FileSingleStreamSpiller spiller = spillerFactory.create();
    spillFiles.add(spiller);

    Iterator<Page> sortedPages = pagesIndex.getSortedPages();
    while (sortedPages.hasNext()) {
      spiller.spill(sortedPages.next());
    }

    // Clear and reset
    pagesIndex.clear();
    pagesIndex = new PagesIndex(1024);
    currentRevocableMemory = 0;
  }

  @Override
  public long getRevocableMemoryUsage() {
    return currentRevocableMemory;
  }

  @Override
  public ListenableFuture<Void> startMemoryRevoke() {
    spillCurrentRun();
    return Futures.immediateVoidFuture();
  }

  @Override
  public void finishMemoryRevoke() {
    currentRevocableMemory = 0;
  }

  @Override
  public Page getOutput() {
    if (state == State.NEEDS_INPUT) {
      return null;
    }
    if (state == State.HAS_OUTPUT) {
      if (outputIterator == null) {
        outputIterator = buildOutputIterator();
      }
      if (outputIterator.hasNext()) {
        Page sortedPage = outputIterator.next();
        if (needsProjection(sortedPage)) {
          return sortedPage.getColumns(outputChannels);
        }
        return sortedPage;
      }
      state = State.FINISHED;
      cleanupSpillFiles();
    }
    return null;
  }

  private boolean needsProjection(Page page) {
    int channelCount = page.getChannelCount();
    if (outputChannels.length != channelCount) {
      return true;
    }
    for (int i = 0; i < outputChannels.length; i++) {
      if (outputChannels[i] != i) {
        return true;
      }
    }
    return false;
  }

  /**
   * Builds the output iterator. If no spilling occurred, just sorts in-memory. If spilling
   * occurred, performs k-way merge of all sorted runs.
   */
  private Iterator<Page> buildOutputIterator() {
    if (spillFiles.isEmpty()) {
      // No spilling — pure in-memory sort
      pagesIndex.sort(comparator);
      return pagesIndex.getSortedPages();
    }

    // Sort remaining in-memory data as the last run
    if (pagesIndex.getPositionCount() > 0) {
      pagesIndex.sort(comparator);
    }

    // K-way merge of all spill files + in-memory data
    return new MergeSortIterator();
  }

  @Override
  public void finish() {
    if (state == State.NEEDS_INPUT) {
      state = State.HAS_OUTPUT;
    }
  }

  @Override
  public boolean isFinished() {
    return state == State.FINISHED;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public void close() {
    pagesIndex.clear();
    outputIterator = null;
    state = State.FINISHED;
    cleanupSpillFiles();
  }

  private void cleanupSpillFiles() {
    for (FileSingleStreamSpiller spiller : spillFiles) {
      spiller.close();
    }
    spillFiles.clear();
  }

  /**
   * K-way merge iterator that merges multiple sorted runs (from spill files and in-memory) using a
   * priority queue. Each entry in the queue represents a single row from a run.
   */
  private class MergeSortIterator implements Iterator<Page> {
    private final List<RunReader> runReaders;
    private final PriorityQueue<RunEntry> heap;
    private boolean initialized;

    MergeSortIterator() {
      this.runReaders = new ArrayList<>();
      this.heap = new PriorityQueue<>(Comparator.comparingInt(a -> 0)); // placeholder
      this.initialized = false;
    }

    private void initialize() {
      if (initialized) return;
      initialized = true;

      // Create a run reader for each spill file
      for (FileSingleStreamSpiller spiller : spillFiles) {
        Iterator<Page> pages = spiller.getSpilledPages();
        RunReader reader = new RunReader(pages);
        if (reader.advance()) {
          runReaders.add(reader);
        }
      }

      // Create a run reader for remaining in-memory data
      if (pagesIndex.getPositionCount() > 0) {
        Iterator<Page> inMemPages = pagesIndex.getSortedPages();
        RunReader reader = new RunReader(inMemPages);
        if (reader.advance()) {
          runReaders.add(reader);
        }
      }

      // Build the priority queue with a comparator that compares current rows
      PriorityQueue<RunEntry> pq =
          new PriorityQueue<>(
              (a, b) ->
                  comparator.compareTo(
                      a.currentPage, a.currentPosition, b.currentPage, b.currentPosition));

      for (int i = 0; i < runReaders.size(); i++) {
        RunReader reader = runReaders.get(i);
        pq.add(new RunEntry(i, reader.getCurrentPage(), reader.getCurrentPosition()));
      }

      // Replace the placeholder heap
      this.heap.clear();
      this.heap.addAll(pq);
    }

    @Override
    public boolean hasNext() {
      initialize();
      return !heap.isEmpty();
    }

    @Override
    public Page next() {
      initialize();
      if (heap.isEmpty()) {
        throw new java.util.NoSuchElementException();
      }

      // Collect up to DEFAULT_OUTPUT_PAGE_SIZE rows from the heap
      int maxPageSize = 1024;
      List<PagePosition> collected = new ArrayList<>(maxPageSize);

      // Re-create heap with proper comparator for extraction
      PriorityQueue<RunEntry> pq =
          new PriorityQueue<>(
              (a, b) ->
                  comparator.compareTo(
                      a.currentPage, a.currentPosition, b.currentPage, b.currentPosition));
      pq.addAll(heap);
      heap.clear();

      while (!pq.isEmpty() && collected.size() < maxPageSize) {
        RunEntry entry = pq.poll();
        collected.add(new PagePosition(entry.currentPage, entry.currentPosition));

        RunReader reader = runReaders.get(entry.runIndex);
        reader.advancePosition();
        if (reader.isValid()) {
          pq.add(
              new RunEntry(entry.runIndex, reader.getCurrentPage(), reader.getCurrentPosition()));
        }
      }

      heap.addAll(pq);

      // Build output page from collected positions
      return buildPageFromPositions(collected);
    }

    private Page buildPageFromPositions(List<PagePosition> positions) {
      if (positions.isEmpty()) {
        return null;
      }
      int count = positions.size();
      Page sample = positions.get(0).page;
      int channelCount = sample.getChannelCount();

      org.opensearch.sql.distributed.data.BlockBuilder[] builders =
          new org.opensearch.sql.distributed.data.BlockBuilder[channelCount];
      // Determine block types from the sample page
      for (int ch = 0; ch < channelCount; ch++) {
        builders[ch] = createBuilderFromBlock(sample.getBlock(ch), count);
      }

      for (PagePosition pp : positions) {
        for (int ch = 0; ch < channelCount; ch++) {
          Block block = pp.page.getBlock(ch);
          appendValue(builders[ch], block, pp.position);
        }
      }

      Block[] blocks = new Block[channelCount];
      for (int ch = 0; ch < channelCount; ch++) {
        blocks[ch] = builders[ch].build();
      }
      return new Page(count, blocks);
    }

    private org.opensearch.sql.distributed.data.BlockBuilder createBuilderFromBlock(
        Block block, int expectedSize) {
      Block resolved = resolveBlock(block);
      if (resolved instanceof org.opensearch.sql.distributed.data.LongArrayBlock) {
        return org.opensearch.sql.distributed.data.BlockBuilder.create(
            org.opensearch.sql.distributed.lucene.ColumnMapping.BlockType.LONG, expectedSize);
      }
      if (resolved instanceof org.opensearch.sql.distributed.data.DoubleArrayBlock) {
        return org.opensearch.sql.distributed.data.BlockBuilder.create(
            org.opensearch.sql.distributed.lucene.ColumnMapping.BlockType.DOUBLE, expectedSize);
      }
      if (resolved instanceof org.opensearch.sql.distributed.data.IntArrayBlock) {
        return org.opensearch.sql.distributed.data.BlockBuilder.create(
            org.opensearch.sql.distributed.lucene.ColumnMapping.BlockType.INT, expectedSize);
      }
      if (resolved instanceof org.opensearch.sql.distributed.data.ShortArrayBlock) {
        return org.opensearch.sql.distributed.data.BlockBuilder.create(
            org.opensearch.sql.distributed.lucene.ColumnMapping.BlockType.SHORT, expectedSize);
      }
      if (resolved instanceof org.opensearch.sql.distributed.data.ByteArrayBlock) {
        return org.opensearch.sql.distributed.data.BlockBuilder.create(
            org.opensearch.sql.distributed.lucene.ColumnMapping.BlockType.BYTE, expectedSize);
      }
      if (resolved instanceof org.opensearch.sql.distributed.data.BooleanArrayBlock) {
        return org.opensearch.sql.distributed.data.BlockBuilder.create(
            org.opensearch.sql.distributed.lucene.ColumnMapping.BlockType.BOOLEAN, expectedSize);
      }
      if (resolved instanceof org.opensearch.sql.distributed.data.VariableWidthBlock) {
        return org.opensearch.sql.distributed.data.BlockBuilder.create(
            org.opensearch.sql.distributed.lucene.ColumnMapping.BlockType.VARIABLE_WIDTH,
            expectedSize);
      }
      throw new UnsupportedOperationException(
          "Unsupported block type: " + resolved.getClass().getSimpleName());
    }

    private void appendValue(
        org.opensearch.sql.distributed.data.BlockBuilder builder, Block block, int position) {
      if (block.isNull(position)) {
        builder.appendNull();
        return;
      }
      Block resolved = resolveBlock(block);
      int resolvedPos = resolvePosition(block, position);

      if (resolved instanceof org.opensearch.sql.distributed.data.LongArrayBlock lb) {
        builder.appendLong(lb.getLong(resolvedPos));
      } else if (resolved instanceof org.opensearch.sql.distributed.data.DoubleArrayBlock db) {
        builder.appendDouble(db.getDouble(resolvedPos));
      } else if (resolved instanceof org.opensearch.sql.distributed.data.IntArrayBlock ib) {
        builder.appendInt(ib.getInt(resolvedPos));
      } else if (resolved instanceof org.opensearch.sql.distributed.data.ShortArrayBlock sb) {
        builder.appendShort(sb.getShort(resolvedPos));
      } else if (resolved instanceof org.opensearch.sql.distributed.data.ByteArrayBlock bb) {
        builder.appendByte(bb.getByte(resolvedPos));
      } else if (resolved instanceof org.opensearch.sql.distributed.data.BooleanArrayBlock bo) {
        builder.appendBoolean(bo.getBoolean(resolvedPos));
      } else if (resolved instanceof org.opensearch.sql.distributed.data.VariableWidthBlock vb) {
        builder.appendBytes(vb.getSlice(resolvedPos));
      } else {
        throw new UnsupportedOperationException(
            "Unsupported block type: " + resolved.getClass().getSimpleName());
      }
    }

    private Block resolveBlock(Block block) {
      if (block instanceof org.opensearch.sql.distributed.data.DictionaryBlock dict) {
        return dict.getDictionary();
      }
      if (block instanceof org.opensearch.sql.distributed.data.RunLengthEncodedBlock rle) {
        return rle.getValue();
      }
      return block;
    }

    private int resolvePosition(Block block, int position) {
      if (block instanceof org.opensearch.sql.distributed.data.DictionaryBlock dict) {
        return dict.getId(position);
      }
      if (block instanceof org.opensearch.sql.distributed.data.RunLengthEncodedBlock) {
        return 0;
      }
      return position;
    }
  }

  /** Tracks a position within a specific page for the merge sort. */
  private static class PagePosition {
    final Page page;
    final int position;

    PagePosition(Page page, int position) {
      this.page = page;
      this.position = position;
    }
  }

  /** Entry in the merge-sort priority queue. */
  private static class RunEntry {
    final int runIndex;
    final Page currentPage;
    final int currentPosition;

    RunEntry(int runIndex, Page currentPage, int currentPosition) {
      this.runIndex = runIndex;
      this.currentPage = currentPage;
      this.currentPosition = currentPosition;
    }
  }

  /**
   * Reads pages from a sorted run (either spill file or in-memory), tracking the current page and
   * position within it.
   */
  private static class RunReader {
    private final Iterator<Page> pageIterator;
    private Page currentPage;
    private int currentPosition;
    private boolean valid;

    RunReader(Iterator<Page> pageIterator) {
      this.pageIterator = pageIterator;
      this.valid = false;
    }

    boolean advance() {
      if (pageIterator.hasNext()) {
        currentPage = pageIterator.next();
        currentPosition = 0;
        valid = currentPage.getPositionCount() > 0;
        return valid;
      }
      valid = false;
      return false;
    }

    void advancePosition() {
      currentPosition++;
      if (currentPosition >= currentPage.getPositionCount()) {
        advance();
      }
    }

    boolean isValid() {
      return valid;
    }

    Page getCurrentPage() {
      return currentPage;
    }

    int getCurrentPosition() {
      return currentPosition;
    }
  }

  /** Factory for creating SpillableOrderByOperator instances. */
  public static class Factory implements OperatorFactory {
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrders;
    private final int[] outputChannels;
    private final SpillerFactory spillerFactory;
    private final long memoryThreshold;
    private boolean closed;

    public Factory(
        List<Integer> sortChannels,
        List<SortOrder> sortOrders,
        int[] outputChannels,
        SpillerFactory spillerFactory,
        long memoryThreshold) {
      this.sortChannels = Objects.requireNonNull(sortChannels);
      this.sortOrders = Objects.requireNonNull(sortOrders);
      this.outputChannels = Objects.requireNonNull(outputChannels);
      this.spillerFactory = Objects.requireNonNull(spillerFactory);
      this.memoryThreshold = memoryThreshold;
    }

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      if (closed) {
        throw new IllegalStateException("Factory is already closed");
      }
      return new SpillableOrderByOperator(
          operatorContext,
          sortChannels,
          sortOrders,
          outputChannels,
          spillerFactory,
          memoryThreshold);
    }

    @Override
    public void noMoreOperators() {
      closed = true;
    }
  }
}
