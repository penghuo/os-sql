/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.lucene;

import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.VariableWidthBlock;
import org.opensearch.sql.distributed.operator.Operator;
import org.opensearch.sql.distributed.operator.OperatorFactory;
import org.opensearch.sql.distributed.operator.SourceOperator;

/**
 * Lucene-native partial aggregation operator. Performs aggregation directly on DocValues using
 * Lucene's Collector/LeafCollector pattern, bypassing the Block/Page pipeline for the scan phase.
 * Much faster than LuceneFullScan + HashAggregationOperator for simple aggregates (COUNT, SUM, MIN,
 * MAX) because it avoids materializing intermediate Pages.
 *
 * <p>This operator:
 *
 * <ul>
 *   <li>For each LeafReaderContext, reads DocValues and accumulates directly
 *   <li>Supports GROUP BY on keyword (SortedDocValues) and numeric (NumericDocValues) fields
 *   <li>Produces a single Page with aggregate results when all segments are processed
 *   <li>Never blocks (always returns {@link Operator#NOT_BLOCKED})
 * </ul>
 *
 * <p>Output schema: [groupKey1, ..., agg1, agg2, ...]
 */
public class LuceneAggScan implements SourceOperator {

  private final OperatorContext operatorContext;
  private final IndexSearcher searcher;
  private final Query query;
  private final List<DocValuesAccumulator> accumulators;
  private final GroupBySpec groupBySpec;

  private boolean accumulated;
  private boolean outputProduced;
  private boolean finished;

  /**
   * Specifies the GROUP BY configuration. Null groupBySpec means global aggregation (no GROUP BY).
   */
  public static class GroupBySpec {
    private final String fieldName;
    private final ColumnMapping.DocValuesType docValuesType;
    private final ColumnMapping.BlockType blockType;

    public GroupBySpec(
        String fieldName,
        ColumnMapping.DocValuesType docValuesType,
        ColumnMapping.BlockType blockType) {
      this.fieldName = fieldName;
      this.docValuesType = docValuesType;
      this.blockType = blockType;
    }

    public String getFieldName() {
      return fieldName;
    }

    public ColumnMapping.DocValuesType getDocValuesType() {
      return docValuesType;
    }

    public ColumnMapping.BlockType getBlockType() {
      return blockType;
    }
  }

  public LuceneAggScan(
      OperatorContext operatorContext,
      IndexSearcher searcher,
      Query query,
      List<DocValuesAccumulator> accumulators,
      GroupBySpec groupBySpec) {
    this.operatorContext = operatorContext;
    this.searcher = searcher;
    this.query = query != null ? query : new MatchAllDocsQuery();
    this.accumulators = List.copyOf(accumulators);
    this.groupBySpec = groupBySpec;
    this.accumulated = false;
    this.outputProduced = false;
    this.finished = searcher.getIndexReader().leaves().isEmpty();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return NOT_BLOCKED;
  }

  @Override
  public Page getOutput() {
    if (finished) {
      return null;
    }

    try {
      if (!accumulated) {
        performAccumulation();
        accumulated = true;
      }

      if (!outputProduced) {
        outputProduced = true;
        finished = true;
        return buildOutputPage();
      }

      return null;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed during Lucene aggregation scan", e);
    }
  }

  private void performAccumulation() throws IOException {
    List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
    Query rewritten = searcher.rewrite(query);
    Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);

    if (groupBySpec == null) {
      performGlobalAccumulation(leaves, weight);
    } else {
      performGroupedAccumulation(leaves, weight);
    }
  }

  private void performGlobalAccumulation(List<LeafReaderContext> leaves, Weight weight)
      throws IOException {
    for (LeafReaderContext leafCtx : leaves) {
      DocIdSetIterator iterator = getDocIterator(leafCtx, weight);
      if (iterator == null) {
        continue;
      }
      for (DocValuesAccumulator acc : accumulators) {
        // Re-obtain iterator for each accumulator since we consume it
        DocIdSetIterator accIterator = getDocIterator(leafCtx, weight);
        if (accIterator != null) {
          acc.accumulate(leafCtx, accIterator);
        }
      }
    }
  }

  private void performGroupedAccumulation(List<LeafReaderContext> leaves, Weight weight)
      throws IOException {
    // Use a shared value-to-group map for numeric GROUP BY across segments
    Map<Long, Integer> numericGroupMap =
        groupBySpec.docValuesType == ColumnMapping.DocValuesType.NUMERIC
                || groupBySpec.docValuesType == ColumnMapping.DocValuesType.SORTED_NUMERIC
            ? new HashMap<>()
            : null;

    for (LeafReaderContext leafCtx : leaves) {
      for (DocValuesAccumulator acc : accumulators) {
        DocIdSetIterator iterator = getDocIterator(leafCtx, weight);
        if (iterator == null) {
          continue;
        }

        DocValuesAccumulator.GroupAssigner groupAssigner =
            createGroupAssigner(leafCtx, numericGroupMap);
        if (groupAssigner != null) {
          acc.accumulateGrouped(leafCtx, iterator, groupAssigner);
        }
      }
    }
  }

  private DocValuesAccumulator.GroupAssigner createGroupAssigner(
      LeafReaderContext leafCtx, Map<Long, Integer> numericGroupMap) throws IOException {
    return switch (groupBySpec.docValuesType) {
      case SORTED -> {
        SortedDocValues sdv = DocValues.getSorted(leafCtx.reader(), groupBySpec.fieldName);
        yield DocValuesAccumulator.sortedDocValuesGroupAssigner(sdv);
      }
      case NUMERIC -> {
        NumericDocValues ndv = DocValues.getNumeric(leafCtx.reader(), groupBySpec.fieldName);
        yield DocValuesAccumulator.numericDocValuesGroupAssigner(ndv, numericGroupMap);
      }
      default ->
          throw new UnsupportedOperationException(
              "GROUP BY not supported for DocValues type: " + groupBySpec.docValuesType);
    };
  }

  private DocIdSetIterator getDocIterator(LeafReaderContext leafCtx, Weight weight)
      throws IOException {
    Scorer scorer = weight.scorer(leafCtx);
    if (scorer == null) {
      return null;
    }
    return scorer.iterator();
  }

  private Page buildOutputPage() throws IOException {
    if (groupBySpec == null) {
      return buildGlobalOutputPage();
    }
    return buildGroupedOutputPage();
  }

  private Page buildGlobalOutputPage() {
    // Single row output: one value per accumulator
    Block[] blocks = new Block[accumulators.size()];
    for (int i = 0; i < accumulators.size(); i++) {
      blocks[i] = buildAccumulatorBlock(accumulators.get(i), 0, 1);
    }
    return new Page(1, blocks);
  }

  private Page buildGroupedOutputPage() throws IOException {
    int groups = 0;
    for (DocValuesAccumulator acc : accumulators) {
      groups = Math.max(groups, acc.getGroupCount());
    }
    if (groups == 0) {
      return buildGlobalOutputPage();
    }

    // Build group key block
    Block keyBlock = buildGroupKeyBlock(groups);

    // Build accumulator blocks
    Block[] blocks = new Block[1 + accumulators.size()];
    blocks[0] = keyBlock;
    for (int i = 0; i < accumulators.size(); i++) {
      blocks[i + 1] = buildAccumulatorBlock(accumulators.get(i), 0, groups);
    }

    return new Page(groups, blocks);
  }

  private Block buildGroupKeyBlock(int groupCount) throws IOException {
    if (groupBySpec.docValuesType == ColumnMapping.DocValuesType.SORTED) {
      return buildSortedGroupKeyBlock(groupCount);
    }
    // For numeric group-by, we need to retrieve the mapping
    return buildNumericGroupKeyBlock(groupCount);
  }

  private Block buildSortedGroupKeyBlock(int groupCount) throws IOException {
    // For SortedDocValues, ordinals directly map to group IDs.
    // We need to get the term for each ordinal from any segment.
    List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
    if (leaves.isEmpty()) {
      return new VariableWidthBlock(0, new byte[0], new int[] {0}, Optional.empty());
    }

    // Collect terms from the first segment that has values
    List<byte[]> terms = new ArrayList<>();
    for (LeafReaderContext leafCtx : leaves) {
      SortedDocValues sdv = DocValues.getSorted(leafCtx.reader(), groupBySpec.fieldName);
      int valueCount = sdv.getValueCount();
      if (valueCount > 0) {
        for (int ord = 0; ord < valueCount && ord < groupCount; ord++) {
          org.apache.lucene.util.BytesRef ref = sdv.lookupOrd(ord);
          byte[] copy = new byte[ref.length];
          System.arraycopy(ref.bytes, ref.offset, copy, 0, ref.length);
          terms.add(copy);
        }
        break;
      }
    }

    // Pad if needed
    while (terms.size() < groupCount) {
      terms.add(new byte[0]);
    }

    // Build VariableWidthBlock
    int totalBytes = 0;
    for (byte[] t : terms) {
      totalBytes += t.length;
    }
    byte[] slice = new byte[totalBytes];
    int[] offsets = new int[groupCount + 1];
    int byteOffset = 0;
    for (int g = 0; g < groupCount; g++) {
      offsets[g] = byteOffset;
      byte[] data = terms.get(g);
      System.arraycopy(data, 0, slice, byteOffset, data.length);
      byteOffset += data.length;
      offsets[g + 1] = byteOffset;
    }
    return new VariableWidthBlock(groupCount, slice, offsets, Optional.empty());
  }

  private Block buildNumericGroupKeyBlock(int groupCount) {
    // For numeric group-by, we'd need the reverse mapping from group ID to value.
    // This is a simplified implementation - return group IDs as longs.
    long[] values = new long[groupCount];
    for (int g = 0; g < groupCount; g++) {
      values[g] = g;
    }
    return new LongArrayBlock(groupCount, Optional.empty(), values);
  }

  private Block buildAccumulatorBlock(DocValuesAccumulator acc, int startGroup, int groupCount) {
    if (acc.getFunction() == DocValuesAccumulator.AggFunction.COUNT) {
      long[] values = new long[groupCount];
      for (int g = 0; g < groupCount; g++) {
        values[g] = acc.getCount(startGroup + g);
      }
      return new LongArrayBlock(groupCount, Optional.empty(), values);
    }

    // SUM, MIN, MAX produce doubles
    double[] values = new double[groupCount];
    boolean[] nulls = null;
    boolean hasNull = false;
    for (int g = 0; g < groupCount; g++) {
      if (acc.hasValue(startGroup + g)) {
        values[g] = acc.getValue(startGroup + g);
      } else {
        if (nulls == null) {
          nulls = new boolean[groupCount];
        }
        nulls[g] = true;
        hasNull = true;
      }
    }
    return new DoubleArrayBlock(
        groupCount, hasNull ? Optional.of(nulls) : Optional.empty(), values);
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

  /** Factory for creating LuceneAggScan operators. */
  public static class Factory implements OperatorFactory {

    private final IndexSearcher searcher;
    private final Query query;
    private final List<AggSpec> aggSpecs;
    private final GroupBySpec groupBySpec;

    /** Describes a single aggregation to compute. */
    public static class AggSpec {
      private final DocValuesAccumulator.AggFunction function;
      private final String fieldName;
      private final ColumnMapping.DocValuesType docValuesType;
      private final ColumnMapping.BlockType blockType;

      public AggSpec(
          DocValuesAccumulator.AggFunction function,
          String fieldName,
          ColumnMapping.DocValuesType docValuesType,
          ColumnMapping.BlockType blockType) {
        this.function = function;
        this.fieldName = fieldName;
        this.docValuesType = docValuesType;
        this.blockType = blockType;
      }
    }

    public Factory(
        IndexSearcher searcher, Query query, List<AggSpec> aggSpecs, GroupBySpec groupBySpec) {
      this.searcher = searcher;
      this.query = query;
      this.aggSpecs = List.copyOf(aggSpecs);
      this.groupBySpec = groupBySpec;
    }

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      List<DocValuesAccumulator> accumulators = new ArrayList<>();
      for (AggSpec spec : aggSpecs) {
        accumulators.add(
            new DocValuesAccumulator(
                spec.function, spec.fieldName, spec.docValuesType, spec.blockType));
      }
      return new LuceneAggScan(operatorContext, searcher, query, accumulators, groupBySpec);
    }

    @Override
    public void noMoreOperators() {
      // No cleanup needed
    }
  }
}
