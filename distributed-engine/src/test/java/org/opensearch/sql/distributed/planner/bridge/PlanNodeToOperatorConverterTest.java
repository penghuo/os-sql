/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner.bridge;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.operator.FilterAndProjectOperator;
import org.opensearch.sql.distributed.operator.HashAggregationOperator;
import org.opensearch.sql.distributed.operator.OperatorFactory;
import org.opensearch.sql.distributed.operator.OrderByOperator;
import org.opensearch.sql.distributed.operator.TopNOperator;
import org.opensearch.sql.distributed.planner.AggregationNode;
import org.opensearch.sql.distributed.planner.FilterNode;
import org.opensearch.sql.distributed.planner.LimitNode;
import org.opensearch.sql.distributed.planner.LuceneTableScanNode;
import org.opensearch.sql.distributed.planner.PlanNodeId;
import org.opensearch.sql.distributed.planner.ProjectNode;
import org.opensearch.sql.distributed.planner.SortNode;
import org.opensearch.sql.distributed.planner.TopNNode;

class PlanNodeToOperatorConverterTest {

  private static RexBuilder rexBuilder;
  private static RelDataTypeFactory typeFactory;
  private static RelDataType bigintType;
  private static RelDataType rowType;

  /** Stub source provider that returns a dummy OperatorFactory for leaf nodes. */
  private static final PlanNodeToOperatorConverter.SourceOperatorFactoryProvider STUB_SOURCE =
      new PlanNodeToOperatorConverter.SourceOperatorFactoryProvider() {
        @Override
        public OperatorFactory createSourceFactory(LuceneTableScanNode node) {
          return new StubOperatorFactory("scan:" + node.getIndexName());
        }

        @Override
        public OperatorFactory createRemoteSourceFactory(
            org.opensearch.sql.distributed.planner.RemoteSourceNode node) {
          return new StubOperatorFactory("remote");
        }
      };

  @BeforeAll
  static void setup() {
    typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);
    bigintType = typeFactory.createSqlType(SqlTypeName.BIGINT);
    rowType =
        typeFactory.createStructType(List.of(bigintType, bigintType), List.of("col0", "col1"));
  }

  @Test
  @DisplayName("Scan → Project produces 2 factories")
  void testScanProject() {
    LuceneTableScanNode scan =
        new LuceneTableScanNode(
            PlanNodeId.next("scan"), "test_index", rowType, List.of("col0", "col1"));
    ProjectNode project =
        new ProjectNode(
            PlanNodeId.next("project"),
            scan,
            List.of(rexBuilder.makeInputRef(bigintType, 0)),
            typeFactory.createStructType(List.of(bigintType), List.of("col0")));

    List<OperatorFactory> factories = PlanNodeToOperatorConverter.convert(project, STUB_SOURCE);

    assertEquals(2, factories.size());
    assertInstanceOf(StubOperatorFactory.class, factories.get(0));
    assertInstanceOf(
        FilterAndProjectOperator.FilterAndProjectOperatorFactory.class, factories.get(1));
  }

  @Test
  @DisplayName("Scan → Filter → Project fuses into 2 factories")
  void testFilterProjectFusion() {
    LuceneTableScanNode scan =
        new LuceneTableScanNode(
            PlanNodeId.next("scan"), "test_index", rowType, List.of("col0", "col1"));

    RexNode predicate =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(bigintType, 0),
            rexBuilder.makeBigintLiteral(new java.math.BigDecimal(10)));

    FilterNode filter = new FilterNode(PlanNodeId.next("filter"), scan, predicate);
    ProjectNode project =
        new ProjectNode(
            PlanNodeId.next("project"),
            filter,
            List.of(rexBuilder.makeInputRef(bigintType, 0)),
            typeFactory.createStructType(List.of(bigintType), List.of("col0")));

    List<OperatorFactory> factories = PlanNodeToOperatorConverter.convert(project, STUB_SOURCE);

    // Filter+Project fused: scan + fused filter-project = 2 factories
    assertEquals(2, factories.size());
    assertInstanceOf(StubOperatorFactory.class, factories.get(0));
    assertInstanceOf(
        FilterAndProjectOperator.FilterAndProjectOperatorFactory.class, factories.get(1));
  }

  @Test
  @DisplayName("Scan → Filter (standalone) produces PassThroughFilterFactory")
  void testStandaloneFilter() {
    LuceneTableScanNode scan =
        new LuceneTableScanNode(
            PlanNodeId.next("scan"), "test_index", rowType, List.of("col0", "col1"));

    RexNode predicate =
        rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(bigintType, 0),
            rexBuilder.makeBigintLiteral(new java.math.BigDecimal(5)));

    FilterNode filter = new FilterNode(PlanNodeId.next("filter"), scan, predicate);

    List<OperatorFactory> factories = PlanNodeToOperatorConverter.convert(filter, STUB_SOURCE);

    assertEquals(2, factories.size());
    assertInstanceOf(StubOperatorFactory.class, factories.get(0));
    // Standalone filter produces PassThroughFilterFactory (private inner class)
    assertEquals("PassThroughFilterFactory", factories.get(1).getClass().getSimpleName());
  }

  @Test
  @DisplayName("Scan → Aggregation produces HashAggregationOperatorFactory")
  void testAggregation() {
    LuceneTableScanNode scan =
        new LuceneTableScanNode(
            PlanNodeId.next("scan"), "test_index", rowType, List.of("col0", "col1"));

    AggregateCall countCall =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            List.of(),
            List.of(),
            -1,
            null,
            RelCollations.EMPTY,
            bigintType,
            "cnt");

    AggregationNode agg =
        new AggregationNode(
            PlanNodeId.next("agg"),
            scan,
            ImmutableBitSet.of(0),
            List.of(countCall),
            AggregationNode.AggregationMode.SINGLE);

    List<OperatorFactory> factories = PlanNodeToOperatorConverter.convert(agg, STUB_SOURCE);

    assertEquals(2, factories.size());
    assertInstanceOf(StubOperatorFactory.class, factories.get(0));
    assertInstanceOf(
        HashAggregationOperator.HashAggregationOperatorFactory.class, factories.get(1));
  }

  @Test
  @DisplayName("Scan → Sort produces OrderByOperatorFactory")
  void testSort() {
    LuceneTableScanNode scan =
        new LuceneTableScanNode(
            PlanNodeId.next("scan"), "test_index", rowType, List.of("col0", "col1"));

    SortNode sort =
        new SortNode(
            PlanNodeId.next("sort"),
            scan,
            RelCollations.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST)));

    List<OperatorFactory> factories = PlanNodeToOperatorConverter.convert(sort, STUB_SOURCE);

    assertEquals(2, factories.size());
    assertInstanceOf(StubOperatorFactory.class, factories.get(0));
    assertInstanceOf(OrderByOperator.OrderByOperatorFactory.class, factories.get(1));
  }

  @Test
  @DisplayName("Scan → TopN produces TopNOperatorFactory")
  void testTopN() {
    LuceneTableScanNode scan =
        new LuceneTableScanNode(
            PlanNodeId.next("scan"), "test_index", rowType, List.of("col0", "col1"));

    TopNNode topN =
        new TopNNode(
            PlanNodeId.next("topn"),
            scan,
            RelCollations.of(new RelFieldCollation(0, Direction.DESCENDING, NullDirection.FIRST)),
            10);

    List<OperatorFactory> factories = PlanNodeToOperatorConverter.convert(topN, STUB_SOURCE);

    assertEquals(2, factories.size());
    assertInstanceOf(StubOperatorFactory.class, factories.get(0));
    assertInstanceOf(TopNOperator.TopNOperatorFactory.class, factories.get(1));
  }

  @Test
  @DisplayName("Scan → Limit produces TopNOperatorFactory with empty sort")
  void testLimit() {
    LuceneTableScanNode scan =
        new LuceneTableScanNode(
            PlanNodeId.next("scan"), "test_index", rowType, List.of("col0", "col1"));

    LimitNode limit = new LimitNode(PlanNodeId.next("limit"), scan, 5, 0);

    List<OperatorFactory> factories = PlanNodeToOperatorConverter.convert(limit, STUB_SOURCE);

    assertEquals(2, factories.size());
    assertInstanceOf(StubOperatorFactory.class, factories.get(0));
    assertInstanceOf(TopNOperator.TopNOperatorFactory.class, factories.get(1));
  }

  @Test
  @DisplayName("Complex pipeline: Scan → Filter → Project → Sort → TopN")
  void testComplexPipeline() {
    LuceneTableScanNode scan =
        new LuceneTableScanNode(
            PlanNodeId.next("scan"), "test_index", rowType, List.of("col0", "col1"));

    RexNode predicate =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(bigintType, 0),
            rexBuilder.makeBigintLiteral(new java.math.BigDecimal(0)));

    FilterNode filter = new FilterNode(PlanNodeId.next("filter"), scan, predicate);
    ProjectNode project =
        new ProjectNode(
            PlanNodeId.next("project"),
            filter,
            List.of(rexBuilder.makeInputRef(bigintType, 0), rexBuilder.makeInputRef(bigintType, 1)),
            rowType);

    TopNNode topN =
        new TopNNode(
            PlanNodeId.next("topn"),
            project,
            RelCollations.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST)),
            100);

    List<OperatorFactory> factories = PlanNodeToOperatorConverter.convert(topN, STUB_SOURCE);

    // scan + fused(filter+project) + topN = 3 factories
    assertEquals(3, factories.size());
    assertInstanceOf(StubOperatorFactory.class, factories.get(0));
    assertInstanceOf(
        FilterAndProjectOperator.FilterAndProjectOperatorFactory.class, factories.get(1));
    assertInstanceOf(TopNOperator.TopNOperatorFactory.class, factories.get(2));
  }

  /** Stub OperatorFactory for tests — just tracks creation. */
  private static class StubOperatorFactory implements OperatorFactory {
    private final String label;
    private boolean closed;

    StubOperatorFactory(String label) {
      this.label = label;
    }

    @Override
    public org.opensearch.sql.distributed.operator.Operator createOperator(
        org.opensearch.sql.distributed.context.OperatorContext operatorContext) {
      throw new UnsupportedOperationException("StubOperatorFactory.createOperator");
    }

    @Override
    public void noMoreOperators() {
      closed = true;
    }

    @Override
    public String toString() {
      return "StubOperatorFactory[" + label + "]";
    }
  }
}
