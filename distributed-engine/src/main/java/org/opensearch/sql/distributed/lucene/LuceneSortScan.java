/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.lucene;

import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldDocs;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.operator.Operator;
import org.opensearch.sql.distributed.operator.OperatorFactory;
import org.opensearch.sql.distributed.operator.SortOrder;
import org.opensearch.sql.distributed.operator.SourceOperator;

/**
 * Source operator that uses {@link IndexSearcher#search(Query, int, Sort)} for efficient top-K
 * retrieval with early termination. For queries where K is much smaller than the total document
 * count, this avoids reading all documents and instead uses Lucene's built-in priority-queue based
 * sorting.
 *
 * <p>This operator:
 *
 * <ul>
 *   <li>Executes a single Lucene sorted search with the specified limit
 *   <li>Converts the resulting ScoreDoc[] to Pages using DocValues
 *   <li>Supports multi-field sort (ASC/DESC) on numeric and keyword fields
 *   <li>Uses early termination when segments are sorted in the requested order
 *   <li>Never blocks (always returns {@link Operator#NOT_BLOCKED})
 * </ul>
 *
 * <p>Performance advantage: for top-10 out of 1M documents, Lucene only needs to maintain a heap of
 * size 10 rather than reading all 1M documents. With index sorting, it can even skip entire
 * segments.
 */
public class LuceneSortScan implements SourceOperator {

  private final OperatorContext operatorContext;
  private final IndexSearcher searcher;
  private final Query query;
  private final List<ColumnMapping> columns;
  private final Sort sort;
  private final int limit;
  private final int batchSize;

  /** State: ScoreDoc results from the sorted search. */
  private ScoreDoc[] scoreDocs;

  private int currentDocIndex;
  private boolean finished;

  public LuceneSortScan(
      OperatorContext operatorContext,
      IndexSearcher searcher,
      Query query,
      List<ColumnMapping> columns,
      Sort sort,
      int limit,
      int batchSize) {
    this.operatorContext = operatorContext;
    this.searcher = searcher;
    this.query = query != null ? query : new MatchAllDocsQuery();
    this.columns = List.copyOf(columns);
    this.sort = sort;
    this.limit = limit;
    this.batchSize = batchSize;
    this.scoreDocs = null;
    this.currentDocIndex = 0;
    this.finished = false;
  }

  public LuceneSortScan(
      OperatorContext operatorContext,
      IndexSearcher searcher,
      Query query,
      List<ColumnMapping> columns,
      Sort sort,
      int limit) {
    this(
        operatorContext,
        searcher,
        query,
        columns,
        sort,
        limit,
        DocValuesToBlockConverter.DEFAULT_BATCH_SIZE);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return NOT_BLOCKED;
  }

  /**
   * Produces the next Page of sorted data. On first call, executes the Lucene sorted search.
   * Subsequent calls produce batches from the result set.
   *
   * @return a Page containing up to batchSize rows, or null if no more data
   */
  @Override
  public Page getOutput() {
    if (finished) {
      return null;
    }

    try {
      if (scoreDocs == null) {
        executeSearch();
      }

      if (currentDocIndex >= scoreDocs.length) {
        finished = true;
        return null;
      }

      int remaining = scoreDocs.length - currentDocIndex;
      int count = Math.min(remaining, batchSize);

      Page page = buildPageFromScoreDocs(currentDocIndex, count);
      currentDocIndex += count;

      if (currentDocIndex >= scoreDocs.length) {
        finished = true;
      }

      return page;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed during sorted Lucene scan", e);
    }
  }

  /**
   * Executes the Lucene sorted search. This is the key performance optimization: Lucene uses a
   * priority queue of size {@code limit} and can skip documents that won't make the top-K.
   */
  private void executeSearch() throws IOException {
    TopFieldDocs topDocs = searcher.search(query, limit, sort);
    this.scoreDocs = topDocs.scoreDocs;
  }

  /**
   * Converts a range of ScoreDoc[] results to a Page by reading DocValues from the corresponding
   * segments. ScoreDocs contain global doc IDs which must be resolved to segment-local doc IDs for
   * DocValues access.
   */
  private Page buildPageFromScoreDocs(int startIndex, int count) throws IOException {
    List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();

    int[] docIds = new int[count];
    LeafReaderContext[] docLeaves = new LeafReaderContext[count];

    // Resolve global doc IDs to segment-local doc IDs
    for (int i = 0; i < count; i++) {
      int globalDocId = scoreDocs[startIndex + i].doc;
      LeafReaderContext leafCtx = findLeaf(leaves, globalDocId);
      docLeaves[i] = leafCtx;
      docIds[i] = globalDocId - leafCtx.docBase;
    }

    // Group consecutive docs from the same segment for efficient batch reading
    // For simplicity, we process each doc individually but grouped by segment
    return buildPageBySegment(docIds, docLeaves, count);
  }

  /**
   * Builds a Page from doc IDs, potentially spanning multiple segments. Groups docs by segment and
   * reads DocValues in batches for each segment, then reassembles in original order.
   */
  private Page buildPageBySegment(int[] localDocIds, LeafReaderContext[] docLeaves, int count)
      throws IOException {
    if (count == 0) {
      return DocValuesToBlockConverter.readDocValues(docLeaves[0], columns, new int[0], 0);
    }

    // Fast path: all docs in same segment
    boolean sameSegment = true;
    for (int i = 1; i < count; i++) {
      if (docLeaves[i] != docLeaves[0]) {
        sameSegment = false;
        break;
      }
    }
    if (sameSegment) {
      return DocValuesToBlockConverter.readDocValues(docLeaves[0], columns, localDocIds, count);
    }

    // Slow path: docs span multiple segments. Read per-segment and merge.
    // We read each doc individually from its segment, accumulating into BlockBuilders.
    org.opensearch.sql.distributed.data.BlockBuilder[] builders =
        new org.opensearch.sql.distributed.data.BlockBuilder[columns.size()];
    for (int col = 0; col < columns.size(); col++) {
      builders[col] =
          org.opensearch.sql.distributed.data.BlockBuilder.create(
              columns.get(col).getBlockType(), count);
    }

    for (int i = 0; i < count; i++) {
      int[] singleDoc = new int[] {localDocIds[i]};
      Page singlePage =
          DocValuesToBlockConverter.readDocValues(docLeaves[i], columns, singleDoc, 1);
      for (int col = 0; col < columns.size(); col++) {
        appendBlockValue(builders[col], singlePage.getBlock(col), 0, columns.get(col));
      }
    }

    org.opensearch.sql.distributed.data.Block[] blocks =
        new org.opensearch.sql.distributed.data.Block[columns.size()];
    for (int col = 0; col < columns.size(); col++) {
      blocks[col] = builders[col].build();
    }
    return new Page(count, blocks);
  }

  private void appendBlockValue(
      org.opensearch.sql.distributed.data.BlockBuilder builder,
      org.opensearch.sql.distributed.data.Block block,
      int position,
      ColumnMapping column) {
    if (block.isNull(position)) {
      builder.appendNull();
      return;
    }
    switch (column.getBlockType()) {
      case LONG ->
          builder.appendLong(
              ((org.opensearch.sql.distributed.data.LongArrayBlock) block).getLong(position));
      case INT ->
          builder.appendInt(
              ((org.opensearch.sql.distributed.data.IntArrayBlock) block).getInt(position));
      case SHORT ->
          builder.appendShort(
              ((org.opensearch.sql.distributed.data.ShortArrayBlock) block).getShort(position));
      case BYTE ->
          builder.appendByte(
              ((org.opensearch.sql.distributed.data.ByteArrayBlock) block).getByte(position));
      case DOUBLE ->
          builder.appendDouble(
              ((org.opensearch.sql.distributed.data.DoubleArrayBlock) block).getDouble(position));
      case FLOAT ->
          builder.appendFloat(
              (float)
                  ((org.opensearch.sql.distributed.data.DoubleArrayBlock) block)
                      .getDouble(position));
      case BOOLEAN ->
          builder.appendBoolean(
              ((org.opensearch.sql.distributed.data.BooleanArrayBlock) block).getBoolean(position));
      case VARIABLE_WIDTH -> {
        byte[] slice =
            ((org.opensearch.sql.distributed.data.VariableWidthBlock) block).getSlice(position);
        builder.appendBytes(slice);
      }
    }
  }

  /** Finds the LeafReaderContext containing the given global doc ID using binary search. */
  private LeafReaderContext findLeaf(List<LeafReaderContext> leaves, int globalDocId) {
    int lo = 0;
    int hi = leaves.size() - 1;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      LeafReaderContext leaf = leaves.get(mid);
      if (globalDocId < leaf.docBase) {
        hi = mid - 1;
      } else if (globalDocId >= leaf.docBase + leaf.reader().maxDoc()) {
        lo = mid + 1;
      } else {
        return leaf;
      }
    }
    throw new IllegalStateException("No leaf found for global doc ID: " + globalDocId);
  }

  @Override
  public void finish() {
    finished = true;
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public void close() throws Exception {
    finished = true;
    scoreDocs = null;
  }

  /** Factory for creating LuceneSortScan operators. */
  public static class Factory implements OperatorFactory {

    private final IndexSearcher searcher;
    private final Query query;
    private final List<ColumnMapping> columns;
    private final List<ColumnMapping> sortColumns;
    private final List<SortOrder> sortOrders;
    private final int limit;
    private final int batchSize;

    public Factory(
        IndexSearcher searcher,
        Query query,
        List<ColumnMapping> columns,
        List<ColumnMapping> sortColumns,
        List<SortOrder> sortOrders,
        int limit) {
      this(
          searcher,
          query,
          columns,
          sortColumns,
          sortOrders,
          limit,
          DocValuesToBlockConverter.DEFAULT_BATCH_SIZE);
    }

    public Factory(
        IndexSearcher searcher,
        Query query,
        List<ColumnMapping> columns,
        List<ColumnMapping> sortColumns,
        List<SortOrder> sortOrders,
        int limit,
        int batchSize) {
      this.searcher = searcher;
      this.query = query;
      this.columns = List.copyOf(columns);
      this.sortColumns = List.copyOf(sortColumns);
      this.sortOrders = List.copyOf(sortOrders);
      this.limit = limit;
      this.batchSize = batchSize;
    }

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      Sort sort = LuceneSortFieldConverter.convert(sortColumns, sortOrders);
      return new LuceneSortScan(operatorContext, searcher, query, columns, sort, limit, batchSize);
    }

    @Override
    public void noMoreOperators() {
      // No cleanup needed
    }
  }
}
