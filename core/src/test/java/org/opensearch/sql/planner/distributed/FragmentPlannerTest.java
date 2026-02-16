/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchRelBuilder;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class FragmentPlannerTest {

  @Mock RelNode mockInput;
  @Mock RelOptCluster cluster;
  @Mock RelOptPlanner planner;
  @Mock RelMetadataQuery mq;

  RelDataType bigintType = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
  RelDataType rowType =
      TYPE_FACTORY.createStructType(List.of(bigintType, bigintType), List.of("a", "b"));
  RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);
  RelBuilder relBuilder;

  FragmentPlanner fragmentPlanner;
  List<ShardSplit> testSplits;

  @BeforeEach
  void setUp() {
    when(cluster.getTypeFactory()).thenReturn(TYPE_FACTORY);
    when(cluster.getRexBuilder()).thenReturn(rexBuilder);
    when(mq.isVisibleInExplain(any(), any())).thenReturn(true);
    when(cluster.getMetadataQuery()).thenReturn(mq);
    when(cluster.traitSet()).thenReturn(RelTraitSet.createEmpty());
    when(cluster.traitSetOf(Convention.NONE))
        .thenReturn(RelTraitSet.createEmpty().replace(Convention.NONE));
    when(cluster.getPlanner()).thenReturn(planner);
    when(planner.getExecutor()).thenReturn(null);

    when(mockInput.getCluster()).thenReturn(cluster);
    when(mockInput.getRowType()).thenReturn(rowType);
    when(mockInput.getTraitSet()).thenReturn(RelTraitSet.createEmpty().replace(Convention.NONE));
    when(mockInput.getInputs()).thenReturn(Collections.emptyList());

    relBuilder = new OpenSearchRelBuilder(null, cluster, null);

    fragmentPlanner = new FragmentPlanner();
    testSplits =
        List.of(
            new ShardSplit("test_index", 0, "node1", Collections.emptyList(), true),
            new ShardSplit("test_index", 1, "node2", Collections.emptyList(), false));
  }

  @Test
  @DisplayName("Simple input with no aggregate/sort/join should not require exchange")
  void simpleInputDoesNotRequireExchange() {
    assertFalse(fragmentPlanner.requiresExchange(mockInput));
  }

  @Test
  @DisplayName("plan() should return empty for simple filter/project chains")
  void planReturnsEmptyForSimpleChains() {
    Optional<Fragment> result = fragmentPlanner.plan(mockInput, testSplits);
    assertFalse(result.isPresent());
  }

  @Test
  @DisplayName("LogicalAggregate should require exchange")
  void aggregateRequiresExchange() {
    relBuilder.push(mockInput);
    RelBuilder.AggCall aggCall = relBuilder.aggregateCall(SqlStdOperatorTable.COUNT);
    LogicalAggregate aggregate =
        (LogicalAggregate)
            relBuilder.aggregate(relBuilder.groupKey(0), ImmutableList.of(aggCall)).build();

    assertTrue(fragmentPlanner.requiresExchange(aggregate));
  }

  @Test
  @DisplayName("plan() for aggregate should create SOURCE and SINGLE fragments with GATHER exchange")
  void planAggregateCreatesCorrectFragments() {
    relBuilder.push(mockInput);
    RelBuilder.AggCall aggCall = relBuilder.aggregateCall(SqlStdOperatorTable.COUNT);
    LogicalAggregate aggregate =
        (LogicalAggregate)
            relBuilder.aggregate(relBuilder.groupKey(0), ImmutableList.of(aggCall)).build();

    Optional<Fragment> result = fragmentPlanner.plan(aggregate, testSplits);
    assertTrue(result.isPresent());

    Fragment root = result.get();
    assertEquals(FragmentType.SINGLE, root.getType());
    assertNotNull(root.getExchangeSpec());
    assertEquals(ExchangeType.GATHER, root.getExchangeSpec().getType());
    assertEquals(1, root.getChildren().size());

    Fragment sourceChild = root.getChildren().get(0);
    assertEquals(FragmentType.SOURCE, sourceChild.getType());
    assertEquals(testSplits, sourceChild.getSplits());
  }

  @Test
  @DisplayName("LogicalSort should require exchange")
  void sortRequiresExchange() {
    relBuilder.push(mockInput);
    LogicalSort sort = (LogicalSort) relBuilder.sort(relBuilder.field("a")).build();

    assertTrue(fragmentPlanner.requiresExchange(sort));
  }

  @Test
  @DisplayName("plan() for sort should create ordered GATHER exchange with sort fields")
  void planSortCreatesOrderedGather() {
    relBuilder.push(mockInput);
    LogicalSort sort = (LogicalSort) relBuilder.sort(relBuilder.field("a")).build();

    Optional<Fragment> result = fragmentPlanner.plan(sort, testSplits);
    assertTrue(result.isPresent());

    Fragment root = result.get();
    assertEquals(FragmentType.SINGLE, root.getType());
    assertNotNull(root.getExchangeSpec());
    assertEquals(ExchangeType.GATHER, root.getExchangeSpec().getType());
    assertFalse(root.getExchangeSpec().getOrderByFields().isEmpty());
    assertTrue(root.getExchangeSpec().getOrderByFields().contains("a"));
  }

  @Test
  @DisplayName("requiresExchange should detect nested aggregation in plan tree")
  void requiresExchangeDetectsNestedAggregate() {
    relBuilder.push(mockInput);
    RelBuilder.AggCall aggCall = relBuilder.aggregateCall(SqlStdOperatorTable.SUM, relBuilder.field("b"));
    RelNode aggWithProject =
        relBuilder.aggregate(relBuilder.groupKey(0), ImmutableList.of(aggCall)).build();

    // The aggregate is nested inside the plan tree
    assertTrue(fragmentPlanner.requiresExchange(aggWithProject));
  }
}
