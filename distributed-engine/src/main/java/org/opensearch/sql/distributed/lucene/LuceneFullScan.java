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
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.operator.Operator;
import org.opensearch.sql.distributed.operator.OperatorFactory;
import org.opensearch.sql.distributed.operator.SourceOperator;

/**
 * A source operator that performs a full scan of all documents in a Lucene index by iterating
 * through segments (LeafReaderContexts) and reading DocValues for projected columns.
 *
 * <p>This operator:
 *
 * <ul>
 *   <li>Iterates through all segments of the assigned shard's IndexSearcher
 *   <li>For each segment, reads all documents in batches of {@link
 *       DocValuesToBlockConverter#DEFAULT_BATCH_SIZE}
 *   <li>Converts DocValues to Block arrays forming Pages
 *   <li>Never blocks (always returns {@link Operator#NOT_BLOCKED})
 * </ul>
 *
 * <p>Used when there is no pushed-down filter predicate. For filtered scans, see {@link
 * LuceneFilterScan}.
 */
public class LuceneFullScan implements SourceOperator {

  private final OperatorContext operatorContext;
  private final IndexSearcher searcher;
  private final List<ColumnMapping> columns;
  private final int batchSize;

  /** Segment iteration state. */
  private final List<LeafReaderContext> leaves;

  private int currentLeafIndex;
  private int currentDocInLeaf;
  private boolean finished;

  public LuceneFullScan(
      OperatorContext operatorContext,
      IndexSearcher searcher,
      List<ColumnMapping> columns,
      int batchSize) {
    this.operatorContext = operatorContext;
    this.searcher = searcher;
    this.columns = List.copyOf(columns);
    this.batchSize = batchSize;
    this.leaves = searcher.getIndexReader().leaves();
    this.currentLeafIndex = 0;
    this.currentDocInLeaf = 0;
    this.finished = leaves.isEmpty();
  }

  public LuceneFullScan(
      OperatorContext operatorContext, IndexSearcher searcher, List<ColumnMapping> columns) {
    this(operatorContext, searcher, columns, DocValuesToBlockConverter.DEFAULT_BATCH_SIZE);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  /** A full scan never blocks — data is always available from local Lucene segments. */
  @Override
  public ListenableFuture<?> isBlocked() {
    return NOT_BLOCKED;
  }

  /**
   * Produces the next Page of data by reading a batch of documents from the current segment. When a
   * segment is exhausted, advances to the next one.
   *
   * @return a Page containing up to {@code batchSize} rows, or null if no data is available
   */
  @Override
  public Page getOutput() {
    if (finished) {
      return null;
    }

    try {
      while (currentLeafIndex < leaves.size()) {
        LeafReaderContext leafCtx = leaves.get(currentLeafIndex);
        int maxDoc = leafCtx.reader().maxDoc();

        if (currentDocInLeaf >= maxDoc) {
          currentLeafIndex++;
          currentDocInLeaf = 0;
          continue;
        }

        int remaining = maxDoc - currentDocInLeaf;
        int count = Math.min(remaining, batchSize);

        int[] docIds = new int[count];
        for (int i = 0; i < count; i++) {
          docIds[i] = currentDocInLeaf + i;
        }
        currentDocInLeaf += count;

        Page page = DocValuesToBlockConverter.readDocValues(leafCtx, columns, docIds, count);
        if (page.getPositionCount() > 0) {
          return page;
        }
      }

      finished = true;
      return null;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read DocValues during full scan", e);
    }
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
  }

  /** Factory for creating LuceneFullScan operators. */
  public static class Factory implements OperatorFactory {

    private final IndexSearcher searcher;
    private final List<ColumnMapping> columns;
    private final int batchSize;

    public Factory(IndexSearcher searcher, List<ColumnMapping> columns) {
      this(searcher, columns, DocValuesToBlockConverter.DEFAULT_BATCH_SIZE);
    }

    public Factory(IndexSearcher searcher, List<ColumnMapping> columns, int batchSize) {
      this.searcher = searcher;
      this.columns = List.copyOf(columns);
      this.batchSize = batchSize;
    }

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      return new LuceneFullScan(operatorContext, searcher, columns, batchSize);
    }

    @Override
    public void noMoreOperators() {
      // No cleanup needed — IndexSearcher lifecycle is managed externally
    }
  }
}
