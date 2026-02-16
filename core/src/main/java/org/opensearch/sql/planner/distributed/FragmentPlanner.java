/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * Plans the distribution of a Calcite {@link RelNode} tree into executable {@link Fragment}s with
 * exchange boundaries.
 *
 * <p>The planner walks the logical plan tree and inserts exchange boundaries at operators that
 * require data redistribution:
 *
 * <ul>
 *   <li>{@link LogicalAggregate} – GATHER exchange: SOURCE fragments perform partial aggregation,
 *       a SINGLE fragment performs final aggregation.
 *   <li>{@link LogicalSort} – Ordered GATHER exchange: preserves sort order when merging results
 *       from multiple shards.
 *   <li>{@link LogicalJoin} – HASH exchange on both sides by the join key(s).
 * </ul>
 *
 * <p>For simple filter/project/scan chains that do not require redistribution, the planner returns
 * {@link Optional#empty()}.
 */
public class FragmentPlanner {

  private final AtomicInteger fragmentIdGenerator = new AtomicInteger(0);

  /**
   * Plans the distribution of the given logical plan into fragments.
   *
   * @param relNode the Calcite logical plan root
   * @param splits the shard splits available for source fragments
   * @return an optional root fragment, or empty if no distribution is needed
   */
  public Optional<Fragment> plan(RelNode relNode, List<ShardSplit> splits) {
    if (!requiresExchange(relNode)) {
      return Optional.empty();
    }
    fragmentIdGenerator.set(0);
    return Optional.of(buildFragments(relNode, splits));
  }

  /**
   * Returns {@code true} if the given plan tree contains any operator that requires an exchange
   * boundary (Aggregate, Sort, or Join).
   *
   * @param relNode the plan tree to inspect
   * @return true if distribution is needed
   */
  public boolean requiresExchange(RelNode relNode) {
    if (relNode instanceof LogicalAggregate
        || relNode instanceof LogicalSort
        || relNode instanceof LogicalJoin) {
      return true;
    }
    for (RelNode input : relNode.getInputs()) {
      if (requiresExchange(input)) {
        return true;
      }
    }
    return false;
  }

  private Fragment buildFragments(RelNode relNode, List<ShardSplit> splits) {
    if (relNode instanceof LogicalAggregate) {
      return planAggregate((LogicalAggregate) relNode, splits);
    } else if (relNode instanceof LogicalSort) {
      return planSort((LogicalSort) relNode, splits);
    } else if (relNode instanceof LogicalJoin) {
      return planJoin((LogicalJoin) relNode, splits);
    }

    // Recurse into children looking for exchange points
    for (RelNode input : relNode.getInputs()) {
      if (requiresExchange(input)) {
        return buildFragments(input, splits);
      }
    }

    // Leaf: source fragment
    return Fragment.source(fragmentIdGenerator.getAndIncrement(), splits, estimateRows(relNode));
  }

  /**
   * Plans an aggregation with a two-phase strategy: SOURCE fragments perform partial aggregation,
   * then a GATHER exchange feeds a SINGLE fragment for final aggregation.
   */
  private Fragment planAggregate(LogicalAggregate aggregate, List<ShardSplit> splits) {
    Fragment sourceFragment =
        Fragment.source(
            fragmentIdGenerator.getAndIncrement(),
            splits,
            estimateRows(aggregate.getInput()));

    ExchangeSpec exchangeSpec = ExchangeSpec.gather();
    List<Fragment> children = Collections.singletonList(sourceFragment);

    return Fragment.single(
        fragmentIdGenerator.getAndIncrement(), children, exchangeSpec, estimateRows(aggregate));
  }

  /**
   * Plans a sort with an ordered GATHER exchange that preserves the sort order from each shard
   * during the merge.
   */
  private Fragment planSort(LogicalSort sort, List<ShardSplit> splits) {
    // Build the source fragment from the sort's input (which may contain further exchanges)
    Fragment sourceFragment;
    RelNode sortInput = sort.getInput();
    if (requiresExchange(sortInput)) {
      sourceFragment = buildFragments(sortInput, splits);
    } else {
      sourceFragment =
          Fragment.source(
              fragmentIdGenerator.getAndIncrement(), splits, estimateRows(sortInput));
    }

    List<String> sortFields = extractSortFields(sort);
    ExchangeSpec exchangeSpec = ExchangeSpec.orderedGather(sortFields);
    List<Fragment> children = Collections.singletonList(sourceFragment);

    return Fragment.single(
        fragmentIdGenerator.getAndIncrement(), children, exchangeSpec, estimateRows(sort));
  }

  /**
   * Plans a join with HASH exchanges on both sides, partitioning by the join key(s). A SINGLE
   * fragment performs the final join.
   */
  private Fragment planJoin(LogicalJoin join, List<ShardSplit> splits) {
    List<String> joinKeys = extractJoinKeys(join);

    // Left child
    Fragment leftFragment;
    RelNode leftInput = join.getLeft();
    if (requiresExchange(leftInput)) {
      leftFragment = buildFragments(leftInput, splits);
    } else {
      leftFragment =
          Fragment.source(
              fragmentIdGenerator.getAndIncrement(), splits, estimateRows(leftInput));
    }

    // Right child
    Fragment rightFragment;
    RelNode rightInput = join.getRight();
    if (requiresExchange(rightInput)) {
      rightFragment = buildFragments(rightInput, splits);
    } else {
      rightFragment =
          Fragment.source(
              fragmentIdGenerator.getAndIncrement(), splits, estimateRows(rightInput));
    }

    ExchangeSpec exchangeSpec = ExchangeSpec.hash(joinKeys);
    List<Fragment> children = new ArrayList<>();
    children.add(leftFragment);
    children.add(rightFragment);

    return Fragment.single(
        fragmentIdGenerator.getAndIncrement(), children, exchangeSpec, estimateRows(join));
  }

  /** Extracts sort field names from a LogicalSort's collation. */
  private List<String> extractSortFields(LogicalSort sort) {
    return sort.getCollation().getFieldCollations().stream()
        .map(
            fieldCollation -> {
              int fieldIndex = fieldCollation.getFieldIndex();
              List<RelDataTypeField> fields = sort.getRowType().getFieldList();
              if (fieldIndex < fields.size()) {
                return fields.get(fieldIndex).getName();
              }
              return String.valueOf(fieldIndex);
            })
        .collect(Collectors.toList());
  }

  /** Extracts join key field names from a LogicalJoin's condition using RexInputRef analysis. */
  private List<String> extractJoinKeys(LogicalJoin join) {
    RexNode condition = join.getCondition();
    List<String> keys = new ArrayList<>();
    extractFieldRefsFromCondition(condition, join, keys);
    if (keys.isEmpty()) {
      // Fallback: use first field from each side
      List<RelDataTypeField> fields = join.getRowType().getFieldList();
      if (!fields.isEmpty()) {
        keys.add(fields.get(0).getName());
      }
    }
    return keys;
  }

  /** Recursively extracts field references from a join condition. */
  private void extractFieldRefsFromCondition(
      RexNode condition, Join join, List<String> keys) {
    if (condition instanceof RexInputRef) {
      int index = ((RexInputRef) condition).getIndex();
      List<RelDataTypeField> fields = join.getRowType().getFieldList();
      if (index < fields.size()) {
        String fieldName = fields.get(index).getName();
        if (!keys.contains(fieldName)) {
          keys.add(fieldName);
        }
      }
    }
    // For call expressions (e.g., =($0, $5)), recurse into operands
    if (condition instanceof org.apache.calcite.rex.RexCall) {
      for (RexNode operand : ((org.apache.calcite.rex.RexCall) condition).getOperands()) {
        extractFieldRefsFromCondition(operand, join, keys);
      }
    }
  }

  /** Estimates row count from RelNode metadata, falling back to a default if unavailable. */
  private long estimateRows(RelNode relNode) {
    try {
      Double rowCount = relNode.estimateRowCount(relNode.getCluster().getMetadataQuery());
      if (rowCount != null && !rowCount.isNaN() && !rowCount.isInfinite()) {
        return rowCount.longValue();
      }
    } catch (Exception e) {
      // Metadata query may not be available in all contexts
    }
    return -1L;
  }
}
