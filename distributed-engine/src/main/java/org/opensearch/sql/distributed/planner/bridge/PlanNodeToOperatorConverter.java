/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner.bridge;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.operator.FilterAndProjectOperator;
import org.opensearch.sql.distributed.operator.HashAggregationOperator;
import org.opensearch.sql.distributed.operator.Operator;
import org.opensearch.sql.distributed.operator.OperatorFactory;
import org.opensearch.sql.distributed.operator.OrderByOperator;
import org.opensearch.sql.distributed.operator.PageFilter;
import org.opensearch.sql.distributed.operator.PageProjection;
import org.opensearch.sql.distributed.operator.SortOrder;
import org.opensearch.sql.distributed.operator.TopNOperator;
import org.opensearch.sql.distributed.operator.aggregation.Accumulator;
import org.opensearch.sql.distributed.operator.join.HashBuilderOperator;
import org.opensearch.sql.distributed.operator.join.JoinHash;
import org.opensearch.sql.distributed.operator.join.JoinType;
import org.opensearch.sql.distributed.operator.join.LookupJoinOperator;
import org.opensearch.sql.distributed.operator.window.RowNumberFunction;
import org.opensearch.sql.distributed.operator.window.WindowFunction;
import org.opensearch.sql.distributed.operator.window.WindowOperator;
import org.opensearch.sql.distributed.planner.AggregationNode;
import org.opensearch.sql.distributed.planner.DedupNode;
import org.opensearch.sql.distributed.planner.ExchangeNode;
import org.opensearch.sql.distributed.planner.FilterNode;
import org.opensearch.sql.distributed.planner.JoinNode;
import org.opensearch.sql.distributed.planner.LimitNode;
import org.opensearch.sql.distributed.planner.LuceneTableScanNode;
import org.opensearch.sql.distributed.planner.PlanNode;
import org.opensearch.sql.distributed.planner.PlanNodeVisitor;
import org.opensearch.sql.distributed.planner.ProjectNode;
import org.opensearch.sql.distributed.planner.RemoteSourceNode;
import org.opensearch.sql.distributed.planner.SortNode;
import org.opensearch.sql.distributed.planner.TopNNode;
import org.opensearch.sql.distributed.planner.ValuesNode;
import org.opensearch.sql.distributed.planner.WindowNode;

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

    // Use the aggregation mode to create the right accumulators:
    // PARTIAL/SINGLE: standard accumulators (COUNT counts rows, SUM sums values)
    // FINAL: merge accumulators (COUNT becomes SUM to merge partial counts)
    AggregationNode.AggregationMode mode = node.getMode();

    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall aggCall = aggCalls.get(i);
      // In FINAL mode, input channel is the position in the partial result (matches group-by
      // column count + aggregate index), which equals the original input channel mapping
      aggregateInputChannels[i] = AccumulatorFactory.resolveInputChannel(aggCall, mode, i,
          groupByChannels.length);
      accumulatorFactories.add(AccumulatorFactory.createAccumulatorSupplier(aggCall, mode));
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

    long offset = node.getOffset();
    int limit = Math.toIntExact(node.getLimit());
    // LIMIT with OFFSET: collect offset+limit rows, then skip the first offset rows
    int totalRows = (int) Math.min((long) limit + offset, Integer.MAX_VALUE);
    operatorFactories.add(
        new TopNOperator.TopNOperatorFactory(totalRows, List.of(), List.of()));
    if (offset > 0) {
      // Add an offset operator that skips the first 'offset' rows
      operatorFactories.add(new OffsetOperatorFactory(Math.toIntExact(offset)));
    }
    return null;
  }

  @Override
  public Void visitExchange(ExchangeNode node, Void context) {
    // After PlanFragmenter.fragment(), all ExchangeNodes are replaced by RemoteSourceNodes.
    // If visitExchange is called, it means we are in one of two scenarios:
    //   1. Pre-fragmentation local execution: the plan was not fragmented, so exchanges
    //      are logical markers only. Pass through to the child subtree.
    //   2. Leaf fragment context: the leaf fragment's plan tree does NOT contain ExchangeNode
    //      at the top (PlanFragmenter extracts only the subtree below the exchange).
    // In both cases, the correct behavior is pass-through: just process the child.
    node.getSource().accept(this, null);
    return null;
  }

  @Override
  public Void visitJoin(JoinNode node, Void context) {
    // Visit left (probe) side — the pipeline's source
    node.getLeft().accept(this, null);

    // Convert build (right) side to operator factories
    List<OperatorFactory> buildFactories = convert(node.getRight(), sourceProvider);

    // Create the join bridge factory that runs the build pipeline and creates the probe operator
    operatorFactories.add(new JoinBridgeFactory(node, buildFactories));
    return null;
  }

  @Override
  public Void visitWindow(WindowNode node, Void context) {
    node.getSource().accept(this, null);

    // Extract partition-by and order-by channels from the WindowNode
    int[] partitionKeyChannels =
        node.getPartitionByColumns().stream().mapToInt(Integer::intValue).toArray();
    int[] orderKeyChannels =
        SortOrderConverter.extractSortChannels(node.getOrderByCollation()).stream()
            .mapToInt(Integer::intValue)
            .toArray();

    // Determine window functions from the output type.
    // WindowNode output has the input columns followed by window function result columns.
    // For now, we default to ROW_NUMBER for each extra output column. The planner should
    // eventually pass explicit window function specifications via WindowNode.
    int inputFieldCount = node.getSource() != null ? estimateInputFieldCount(node) : 0;
    int outputFieldCount =
        node.getOutputType() != null ? node.getOutputType().getFieldCount() : inputFieldCount;
    int windowFunctionCount = Math.max(0, outputFieldCount - inputFieldCount);

    List<WindowFunction> windowFunctions = new ArrayList<>();
    for (int i = 0; i < windowFunctionCount; i++) {
      // Default to ROW_NUMBER — the planner/optimizer should annotate the specific function type
      windowFunctions.add(new RowNumberFunction());
    }
    // Fallback: at least one window function
    if (windowFunctions.isEmpty()) {
      windowFunctions.add(new RowNumberFunction());
    }

    operatorFactories.add(
        new WindowOperator.WindowOperatorFactory(
            partitionKeyChannels, orderKeyChannels, windowFunctions));
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
   * Estimates the input field count for a WindowNode by traversing the source tree to find a node
   * with an explicit row type. Falls back to 0 if unknown.
   */
  private static int estimateInputFieldCount(WindowNode windowNode) {
    // Walk the source chain looking for nodes with known field counts
    PlanNode source = windowNode.getSource();
    if (source instanceof LuceneTableScanNode scanNode) {
      return scanNode.getOutputType().getFieldCount();
    }
    if (source instanceof ProjectNode projectNode) {
      return projectNode.getOutputType().getFieldCount();
    }
    // For other nodes, estimate from the output type minus window functions
    // (conservative: assume output type includes window columns)
    return windowNode.getOutputType() != null ? windowNode.getOutputType().getFieldCount() - 1 : 0;
  }

  /**
   * OperatorFactory that bridges the build-side pipeline with the probe-side join operator. At
   * {@code createOperator()} time, this factory runs the build pipeline to completion, constructs
   * the {@link JoinHash}, and creates a {@link LookupJoinOperator} wired to it.
   */
  public static class JoinBridgeFactory implements OperatorFactory {
    private final JoinNode joinNode;
    private final List<OperatorFactory> buildFactories;
    private boolean closed;
    private JoinHash cachedJoinHash;

    public JoinBridgeFactory(JoinNode joinNode, List<OperatorFactory> buildFactories) {
      this.joinNode = joinNode;
      this.buildFactories = buildFactories;
    }

    public JoinNode getJoinNode() {
      return joinNode;
    }

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      if (closed) {
        throw new IllegalStateException("Factory is already closed");
      }

      // Build the hash table from the build side (runs inline)
      JoinHash joinHash = getOrBuildJoinHash(operatorContext);

      // Convert Calcite JoinRelType to our JoinType
      JoinType joinType = convertJoinType(joinNode.getJoinType());

      // Probe key channels from left side
      int[] probeKeyChannels =
          joinNode.getLeftJoinKeys().stream().mapToInt(Integer::intValue).toArray();

      // Build key channels from right side (used during hash build)
      // Output channels: all probe columns + all build columns (for INNER/LEFT/RIGHT/FULL)
      // For SEMI/ANTI: only probe columns
      int probeChannelCount = estimateProbeChannelCount();
      int buildChannelCount = joinHash.getChannelCount();

      int[] probeOutputChannels = identityArray(probeChannelCount);
      int[] buildOutputChannels =
          joinType.outputBuildColumns() ? identityArray(buildChannelCount) : new int[0];

      return new LookupJoinOperator(
          operatorContext,
          joinHash,
          joinType,
          probeKeyChannels,
          probeOutputChannels,
          buildOutputChannels);
    }

    @Override
    public void noMoreOperators() {
      closed = true;
    }

    /**
     * Builds or returns the cached JoinHash. The build pipeline is run once, and the resulting hash
     * table is reused across multiple operator instances.
     */
    private JoinHash getOrBuildJoinHash(OperatorContext probeContext) {
      if (cachedJoinHash != null) {
        return cachedJoinHash;
      }

      int[] buildKeyChannels =
          joinNode.getRightJoinKeys().stream().mapToInt(Integer::intValue).toArray();

      // Create build-side operators
      List<Operator> buildOperators = new ArrayList<>();
      for (int i = 0; i < buildFactories.size(); i++) {
        OperatorContext buildOpCtx =
            new OperatorContext(
                1000 + i, "BuildSide-" + buildFactories.get(i).getClass().getSimpleName());
        buildOperators.add(buildFactories.get(i).createOperator(buildOpCtx));
      }

      // Create the HashBuilderOperator as the sink
      OperatorContext hashBuilderCtx =
          new OperatorContext(1000 + buildFactories.size(), "HashBuilderOperator");
      HashBuilderOperator hashBuilder = new HashBuilderOperator(hashBuilderCtx, buildKeyChannels);
      buildOperators.add(hashBuilder);

      // Run the build pipeline to completion
      runPipelineToCompletion(buildOperators);

      cachedJoinHash = hashBuilder.getJoinHash();
      return cachedJoinHash;
    }

    /** Runs a pipeline of operators to completion using a simplified driver loop. */
    private static void runPipelineToCompletion(List<Operator> operators) {
      boolean moreWork;
      do {
        moreWork = false;
        for (int i = 0; i < operators.size() - 1; i++) {
          Operator current = operators.get(i);
          Operator next = operators.get(i + 1);

          if (!current.isFinished() && next.needsInput()) {
            Page page = current.getOutput();
            if (page != null && page.getPositionCount() > 0) {
              next.addInput(page);
              moreWork = true;
            }
          }

          if (current.isFinished()) {
            next.finish();
          }
        }
      } while (moreWork);

      // Final finish propagation
      for (int i = 0; i < operators.size() - 1; i++) {
        if (operators.get(i).isFinished()) {
          operators.get(i + 1).finish();
        }
      }
    }

    private int estimateProbeChannelCount() {
      // Estimate probe (left) side channel count from the plan node tree
      PlanNode left = joinNode.getLeft();
      if (left instanceof LuceneTableScanNode scanNode) {
        return scanNode.getOutputType().getFieldCount();
      }
      if (left instanceof ProjectNode projectNode) {
        return projectNode.getOutputType().getFieldCount();
      }
      // Fallback: use the maximum key channel index + 1 as a minimum bound.
      // The actual channel count will be determined from the first input page at runtime.
      int maxKey = 0;
      for (int key : joinNode.getLeftJoinKeys()) {
        maxKey = Math.max(maxKey, key);
      }
      return maxKey + 1;
    }

    private static int[] identityArray(int count) {
      int[] arr = new int[count];
      for (int i = 0; i < count; i++) {
        arr[i] = i;
      }
      return arr;
    }

    private static JoinType convertJoinType(JoinRelType calciteType) {
      switch (calciteType) {
        case INNER:
          return JoinType.INNER;
        case LEFT:
          return JoinType.LEFT;
        case RIGHT:
          return JoinType.RIGHT;
        case FULL:
          return JoinType.FULL;
        case SEMI:
          return JoinType.SEMI;
        case ANTI:
          return JoinType.ANTI;
        default:
          throw new UnsupportedOperationException("Unsupported join type: " + calciteType);
      }
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

  /**
   * An OperatorFactory that skips the first N rows from its input, implementing OFFSET semantics.
   */
  private static class OffsetOperatorFactory implements OperatorFactory {
    private final int offset;
    private boolean closed;

    OffsetOperatorFactory(int offset) {
      this.offset = offset;
    }

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      if (closed) {
        throw new IllegalStateException("Factory is already closed");
      }
      return new OffsetOperator(operatorContext, offset);
    }

    @Override
    public void noMoreOperators() {
      closed = true;
    }
  }

  /** Operator that skips the first N rows from its input, implementing OFFSET semantics. */
  private static class OffsetOperator implements Operator {
    private final OperatorContext operatorContext;
    private final int offset;
    private int skipped;
    private boolean finishing;
    private boolean finished;
    private Page pendingOutput;

    OffsetOperator(OperatorContext operatorContext, int offset) {
      this.operatorContext = operatorContext;
      this.offset = offset;
      this.skipped = 0;
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
    public void addInput(Page page) {
      if (!needsInput()) {
        throw new IllegalStateException("Operator does not need input");
      }
      int positionCount = page.getPositionCount();
      int remaining = offset - skipped;
      if (remaining >= positionCount) {
        // Skip entire page
        skipped += positionCount;
        return;
      }
      if (remaining > 0) {
        // Skip partial page: take positions from 'remaining' to end
        int outputCount = positionCount - remaining;
        int[] positions = new int[outputCount];
        for (int i = 0; i < outputCount; i++) {
          positions[i] = remaining + i;
        }
        org.opensearch.sql.distributed.data.Block[] outputBlocks =
            new org.opensearch.sql.distributed.data.Block[page.getChannelCount()];
        for (int ch = 0; ch < page.getChannelCount(); ch++) {
          org.opensearch.sql.distributed.data.Block block = page.getBlock(ch);
          if (block instanceof org.opensearch.sql.distributed.data.ValueBlock valueBlock) {
            outputBlocks[ch] = valueBlock.copyPositions(positions, 0, outputCount);
          } else {
            outputBlocks[ch] = block;
          }
        }
        pendingOutput = new Page(outputCount, outputBlocks);
        skipped = offset;
      } else {
        // No more skipping needed, pass through
        pendingOutput = page;
      }
    }

    @Override
    public Page getOutput() {
      if (pendingOutput != null) {
        Page output = pendingOutput;
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
    public OperatorContext getOperatorContext() {
      return operatorContext;
    }

    @Override
    public void close() {
      pendingOutput = null;
      finished = true;
      finishing = true;
    }
  }
}
