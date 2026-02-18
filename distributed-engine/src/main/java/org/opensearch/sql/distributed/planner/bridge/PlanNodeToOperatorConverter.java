/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner.bridge;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.sql.distributed.operator.FilterAndProjectOperator;
import org.opensearch.sql.distributed.operator.HashAggregationOperator;
import org.opensearch.sql.distributed.operator.OperatorFactory;
import org.opensearch.sql.distributed.operator.OrderByOperator;
import org.opensearch.sql.distributed.operator.PageFilter;
import org.opensearch.sql.distributed.operator.PageProjection;
import org.opensearch.sql.distributed.operator.SortOrder;
import org.opensearch.sql.distributed.operator.TopNOperator;
import org.opensearch.sql.distributed.operator.aggregation.Accumulator;
import org.opensearch.sql.distributed.planner.AggregationNode;
import org.opensearch.sql.distributed.planner.DedupNode;
import org.opensearch.sql.distributed.planner.ExchangeNode;
import org.opensearch.sql.distributed.planner.FilterNode;
import org.opensearch.sql.distributed.planner.LimitNode;
import org.opensearch.sql.distributed.planner.LuceneTableScanNode;
import org.opensearch.sql.distributed.planner.PlanNode;
import org.opensearch.sql.distributed.planner.PlanNodeVisitor;
import org.opensearch.sql.distributed.planner.ProjectNode;
import org.opensearch.sql.distributed.planner.RemoteSourceNode;
import org.opensearch.sql.distributed.planner.SortNode;
import org.opensearch.sql.distributed.planner.TopNNode;
import org.opensearch.sql.distributed.planner.ValuesNode;

/**
 * Converts a PlanNode tree into a list of {@link OperatorFactory} instances that can be passed to a
 * Pipeline for driver creation. This is the critical bridge between Layer 3 (planner) and Layer 2
 * (operators).
 *
 * <p>The visitor walks the PlanNode tree bottom-up (source first), collecting OperatorFactories
 * into an ordered list. The first factory is always a source operator (e.g., LuceneFullScan), and
 * subsequent factories are processing operators (Filter, Project, Aggregate, Sort, TopN).
 *
 * <p>Filter+Project fusion: When a ProjectNode sits above a FilterNode, they are fused into a
 * single {@link FilterAndProjectOperator} to avoid redundant operator overhead.
 *
 * <p>A {@link SourceOperatorFactoryProvider} must be supplied to handle leaf nodes
 * (LuceneTableScanNode, RemoteSourceNode), since these require runtime resources (IndexSearcher,
 * exchange connections) that are not available at planning time.
 */
public class PlanNodeToOperatorConverter extends PlanNodeVisitor<Void, Void> {

  private final List<OperatorFactory> operatorFactories;
  private final SourceOperatorFactoryProvider sourceProvider;

  public PlanNodeToOperatorConverter(SourceOperatorFactoryProvider sourceProvider) {
    this.operatorFactories = new ArrayList<>();
    this.sourceProvider = sourceProvider;
  }

  /**
   * Converts a PlanNode tree to an ordered list of OperatorFactories.
   *
   * @param root the root of the PlanNode tree
   * @param sourceProvider callback for creating source operator factories
   * @return ordered list of OperatorFactories (source first, then processing operators)
   */
  public static List<OperatorFactory> convert(
      PlanNode root, SourceOperatorFactoryProvider sourceProvider) {
    PlanNodeToOperatorConverter converter = new PlanNodeToOperatorConverter(sourceProvider);
    root.accept(converter, null);
    return converter.operatorFactories;
  }

  // --- Leaf nodes ---

  @Override
  public Void visitLuceneTableScan(LuceneTableScanNode node, Void context) {
    operatorFactories.add(sourceProvider.createSourceFactory(node));
    return null;
  }

  @Override
  public Void visitRemoteSource(RemoteSourceNode node, Void context) {
    operatorFactories.add(sourceProvider.createRemoteSourceFactory(node));
    return null;
  }

  @Override
  public Void visitValues(ValuesNode node, Void context) {
    operatorFactories.add(sourceProvider.createValuesFactory(node));
    return null;
  }

  // --- Single-child processing nodes ---

  @Override
  public Void visitProject(ProjectNode node, Void context) {
    PlanNode source = node.getSource();

    // Filter+Project fusion: if source is FilterNode, fuse into single operator
    if (source instanceof FilterNode filterNode) {
      // Visit the filter's source (skip the filter itself)
      filterNode.getSource().accept(this, null);

      PageFilter filter = RexPageFilterInterpreter.toPageFilter(filterNode.getPredicate());
      List<PageProjection> projections =
          RexPageProjectionFactory.createProjections(node.getProjections());

      operatorFactories.add(
          new FilterAndProjectOperator.FilterAndProjectOperatorFactory(filter, projections));
    } else {
      // Visit source first
      source.accept(this, null);

      // Project-only: use ALWAYS_TRUE filter
      List<PageProjection> projections =
          RexPageProjectionFactory.createProjections(node.getProjections());

      operatorFactories.add(
          new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
              PageFilter.ALWAYS_TRUE, projections));
    }
    return null;
  }

  @Override
  public Void visitFilter(FilterNode node, Void context) {
    // Standalone filter (not fused with Project above). This happens when the filter
    // is directly below a non-project node like Aggregate or Sort.
    // We create a FilterAndProjectOperator that passes all input columns through.
    node.getSource().accept(this, null);

    PageFilter filter = RexPageFilterInterpreter.toPageFilter(node.getPredicate());

    // Use a PassThroughFilterFactory that applies the filter and passes all columns
    operatorFactories.add(new PassThroughFilterFactory(filter));
    return null;
  }

  @Override
  public Void visitAggregation(AggregationNode node, Void context) {
    node.getSource().accept(this, null);

    ImmutableBitSet groupSet = node.getGroupSet();
    int[] groupByChannels = groupSet.toArray();

    List<AggregateCall> aggCalls = node.getAggregateCalls();
    int[] aggregateInputChannels = new int[aggCalls.size()];
    List<Supplier<Accumulator>> accumulatorFactories = new ArrayList<>(aggCalls.size());

    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall aggCall = aggCalls.get(i);
      aggregateInputChannels[i] = AccumulatorFactory.resolveInputChannel(aggCall);
      accumulatorFactories.add(AccumulatorFactory.createAccumulatorSupplier(aggCall));
    }

    operatorFactories.add(
        new HashAggregationOperator.HashAggregationOperatorFactory(
            groupByChannels, aggregateInputChannels, accumulatorFactories, 16));
    return null;
  }

  @Override
  public Void visitSort(SortNode node, Void context) {
    node.getSource().accept(this, null);

    List<Integer> sortChannels = SortOrderConverter.extractSortChannels(node.getCollation());
    List<SortOrder> sortOrders = SortOrderConverter.convertSortOrders(node.getCollation());

    // OrderBy outputs all channels. Determine output channel count from the sort channels
    // (at minimum, all referenced channels must be included). The actual channel count
    // is determined at runtime from input pages.
    int[] outputChannels = identityChannels(estimateChannelCount(sortChannels));

    operatorFactories.add(
        new OrderByOperator.OrderByOperatorFactory(sortChannels, sortOrders, outputChannels));
    return null;
  }

  @Override
  public Void visitTopN(TopNNode node, Void context) {
    node.getSource().accept(this, null);

    List<Integer> sortChannels = SortOrderConverter.extractSortChannels(node.getCollation());
    List<SortOrder> sortOrders = SortOrderConverter.convertSortOrders(node.getCollation());
    int limit = Math.toIntExact(node.getLimit());

    operatorFactories.add(new TopNOperator.TopNOperatorFactory(limit, sortChannels, sortOrders));
    return null;
  }

  @Override
  public Void visitLimit(LimitNode node, Void context) {
    node.getSource().accept(this, null);

    int limit = Math.toIntExact(node.getLimit());
    // LIMIT without ORDER BY: use TopN with empty sort spec (preserves input order)
    operatorFactories.add(new TopNOperator.TopNOperatorFactory(limit, List.of(), List.of()));
    return null;
  }

  @Override
  public Void visitExchange(ExchangeNode node, Void context) {
    // In single-node execution, exchange is a pass-through
    node.getSource().accept(this, null);
    return null;
  }

  @Override
  public Void visitDedup(DedupNode node, Void context) {
    throw new UnsupportedOperationException(
        "DedupNode is not yet supported in the distributed engine");
  }

  @Override
  public Void visitPlan(PlanNode node, Void context) {
    throw new UnsupportedOperationException(
        "Unsupported PlanNode type: " + node.getClass().getSimpleName());
  }

  // --- Helper methods ---

  private static int estimateChannelCount(List<Integer> sortChannels) {
    int max = 0;
    for (int ch : sortChannels) {
      max = Math.max(max, ch);
    }
    return max + 1;
  }

  private static int[] identityChannels(int count) {
    int[] channels = new int[count];
    for (int i = 0; i < count; i++) {
      channels[i] = i;
    }
    return channels;
  }

  // --- PassThrough filter (standalone Filter without Project) ---

  /**
   * An OperatorFactory for standalone filters. Applies the filter predicate and passes all input
   * columns through without projection. This works by wrapping the filter evaluation and producing
   * an output page with filtered positions across all channels.
   */
  private static class PassThroughFilterFactory implements OperatorFactory {
    private final PageFilter filter;
    private boolean closed;

    PassThroughFilterFactory(PageFilter filter) {
      this.filter = filter;
    }

    @Override
    public org.opensearch.sql.distributed.operator.Operator createOperator(
        org.opensearch.sql.distributed.context.OperatorContext operatorContext) {
      if (closed) {
        throw new IllegalStateException("Factory is already closed");
      }
      return new PassThroughFilterOperator(operatorContext, filter);
    }

    @Override
    public void noMoreOperators() {
      closed = true;
    }
  }

  /**
   * A filter operator that passes through all columns. Unlike FilterAndProjectOperator which
   * requires explicit projections, this operator applies the filter mask and copies all channels
   * from the input page for selected positions.
   */
  private static class PassThroughFilterOperator
      implements org.opensearch.sql.distributed.operator.Operator {

    private final org.opensearch.sql.distributed.context.OperatorContext operatorContext;
    private final PageFilter filter;
    private boolean finishing;
    private boolean finished;
    private org.opensearch.sql.distributed.data.Page pendingOutput;

    PassThroughFilterOperator(
        org.opensearch.sql.distributed.context.OperatorContext operatorContext, PageFilter filter) {
      this.operatorContext = operatorContext;
      this.filter = filter;
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<?> isBlocked() {
      return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput() {
      return !finishing && pendingOutput == null;
    }

    @Override
    public void addInput(org.opensearch.sql.distributed.data.Page page) {
      if (!needsInput()) {
        throw new IllegalStateException("Operator does not need input");
      }
      if (page.getPositionCount() == 0) {
        return;
      }

      boolean[] selected = filter.filter(page);
      int selectedCount = 0;
      for (int i = 0; i < Math.min(selected.length, page.getPositionCount()); i++) {
        if (selected[i]) selectedCount++;
      }
      if (selectedCount == 0) {
        return;
      }

      // Build selected position indices
      int[] positions = new int[selectedCount];
      int idx = 0;
      for (int i = 0; i < Math.min(selected.length, page.getPositionCount()); i++) {
        if (selected[i]) {
          positions[idx++] = i;
        }
      }

      // Copy all channels for selected positions
      org.opensearch.sql.distributed.data.Block[] outputBlocks =
          new org.opensearch.sql.distributed.data.Block[page.getChannelCount()];
      for (int ch = 0; ch < page.getChannelCount(); ch++) {
        org.opensearch.sql.distributed.data.Block block = page.getBlock(ch);
        if (block instanceof org.opensearch.sql.distributed.data.ValueBlock valueBlock) {
          outputBlocks[ch] = valueBlock.copyPositions(positions, 0, selectedCount);
        } else {
          outputBlocks[ch] = copyBlockPositions(block, positions, selectedCount);
        }
      }

      pendingOutput = new org.opensearch.sql.distributed.data.Page(selectedCount, outputBlocks);
    }

    @Override
    public org.opensearch.sql.distributed.data.Page getOutput() {
      if (pendingOutput != null) {
        org.opensearch.sql.distributed.data.Page output = pendingOutput;
        pendingOutput = null;
        return output;
      }
      return null;
    }

    @Override
    public void finish() {
      finishing = true;
      if (pendingOutput == null) {
        finished = true;
      }
    }

    @Override
    public boolean isFinished() {
      return finished && pendingOutput == null;
    }

    @Override
    public org.opensearch.sql.distributed.context.OperatorContext getOperatorContext() {
      return operatorContext;
    }

    @Override
    public void close() {
      pendingOutput = null;
      finished = true;
      finishing = true;
    }

    private static org.opensearch.sql.distributed.data.Block copyBlockPositions(
        org.opensearch.sql.distributed.data.Block block, int[] positions, int count) {
      if (block instanceof org.opensearch.sql.distributed.data.DictionaryBlock dict) {
        int[] newIds = new int[count];
        for (int i = 0; i < count; i++) {
          newIds[i] = dict.getId(positions[i]);
        }
        return dict.getDictionary().copyPositions(newIds, 0, count);
      }
      if (block instanceof org.opensearch.sql.distributed.data.RunLengthEncodedBlock rle) {
        return new org.opensearch.sql.distributed.data.RunLengthEncodedBlock(rle.getValue(), count);
      }
      throw new UnsupportedOperationException(
          "Cannot copy positions for: " + block.getClass().getSimpleName());
    }
  }

  /**
   * Callback interface for creating source operator factories from leaf PlanNodes. Source operators
   * require runtime resources (IndexSearcher, exchange connections) not available at planning time.
   */
  public interface SourceOperatorFactoryProvider {
    /** Creates a source factory for a Lucene table scan node. */
    OperatorFactory createSourceFactory(LuceneTableScanNode node);

    /** Creates a source factory for a remote exchange source. */
    OperatorFactory createRemoteSourceFactory(RemoteSourceNode node);

    /** Creates a source factory for inline values. */
    default OperatorFactory createValuesFactory(ValuesNode node) {
      throw new UnsupportedOperationException("ValuesNode not supported");
    }
  }
}
