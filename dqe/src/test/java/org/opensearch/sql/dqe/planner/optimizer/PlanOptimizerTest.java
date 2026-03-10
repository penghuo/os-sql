/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.planner.optimizer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.FilterNode;
import org.opensearch.sql.dqe.planner.plan.LimitNode;
import org.opensearch.sql.dqe.planner.plan.ProjectNode;
import org.opensearch.sql.dqe.planner.plan.SortNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;

@DisplayName("PlanOptimizer")
class PlanOptimizerTest {

  private final PlanOptimizer optimizer = new PlanOptimizer();

  @Nested
  @DisplayName("Predicate Pushdown")
  class PredicatePushdown {

    @Test
    @DisplayName("Pushes integer equality predicate into TableScanNode and removes FilterNode")
    void pushIntegerEquality() {
      // Plan: Filter("status = 200") -> TableScan("logs", [status, category])
      TableScanNode scan = new TableScanNode("logs", List.of("status", "category"));
      FilterNode filter = new FilterNode(scan, "status = 200");

      DqePlanNode result = optimizer.optimize(filter);

      // FilterNode should be removed; result should be a TableScanNode with dslFilter
      assertInstanceOf(TableScanNode.class, result);
      TableScanNode optimizedScan = (TableScanNode) result;
      assertEquals("logs", optimizedScan.getIndexName());
      assertNotNull(optimizedScan.getDslFilter());
      assertEquals("{\"term\":{\"status\":200}}", optimizedScan.getDslFilter());
    }

    @Test
    @DisplayName("Pushes through Project(Filter(TableScan)) tree")
    void pushThroughProject() {
      // Plan: Project([status]) -> Filter("status = 200") -> TableScan
      TableScanNode scan = new TableScanNode("logs", List.of("status", "category"));
      FilterNode filter = new FilterNode(scan, "status = 200");
      ProjectNode project = new ProjectNode(filter, List.of("status"));

      DqePlanNode result = optimizer.optimize(project);

      // Should be Project -> TableScan(with dslFilter), no FilterNode
      assertInstanceOf(ProjectNode.class, result);
      ProjectNode optimizedProject = (ProjectNode) result;
      assertInstanceOf(TableScanNode.class, optimizedProject.getChild());
      TableScanNode optimizedScan = (TableScanNode) optimizedProject.getChild();
      assertEquals("{\"term\":{\"status\":200}}", optimizedScan.getDslFilter());
    }

    @Test
    @DisplayName("Pushes through Limit(Project(Filter(TableScan))) tree")
    void pushThroughLimitProject() {
      TableScanNode scan = new TableScanNode("logs", List.of("status"));
      FilterNode filter = new FilterNode(scan, "status = 404");
      ProjectNode project = new ProjectNode(filter, List.of("status"));
      LimitNode limit = new LimitNode(project, 10);

      DqePlanNode result = optimizer.optimize(limit);

      // Should be Limit -> Project -> TableScan(with dslFilter)
      assertInstanceOf(LimitNode.class, result);
      LimitNode optimizedLimit = (LimitNode) result;
      assertInstanceOf(ProjectNode.class, optimizedLimit.getChild());
      ProjectNode optimizedProject = (ProjectNode) optimizedLimit.getChild();
      assertInstanceOf(TableScanNode.class, optimizedProject.getChild());
      TableScanNode optimizedScan = (TableScanNode) optimizedProject.getChild();
      assertEquals("{\"term\":{\"status\":404}}", optimizedScan.getDslFilter());
    }

    @Test
    @DisplayName("Retains FilterNode when predicate is not a simple equality")
    void retainsNonPushablePredicate() {
      TableScanNode scan = new TableScanNode("logs", List.of("status"));
      FilterNode filter = new FilterNode(scan, "status > 200 AND status < 500");

      DqePlanNode result = optimizer.optimize(filter);

      // FilterNode should remain because predicate is complex
      assertInstanceOf(FilterNode.class, result);
    }

    @Test
    @DisplayName("Pushes string equality predicate as term query")
    void pushStringEquality() {
      TableScanNode scan = new TableScanNode("logs", List.of("category"));
      FilterNode filter = new FilterNode(scan, "category = 'error'");

      DqePlanNode result = optimizer.optimize(filter);

      assertInstanceOf(TableScanNode.class, result);
      TableScanNode optimizedScan = (TableScanNode) result;
      assertEquals("{\"term\":{\"category\":\"error\"}}", optimizedScan.getDslFilter());
    }

    @Test
    @DisplayName("Pushes <> predicate as must_not term query")
    void pushNotEqual() {
      PlanOptimizer typed = new PlanOptimizer(Map.of("AdvEngineID", "integer"));
      TableScanNode scan = new TableScanNode("hits", List.of("AdvEngineID"));
      FilterNode filter = new FilterNode(scan, "AdvEngineID <> 0");

      DqePlanNode result = typed.optimize(filter);

      assertInstanceOf(TableScanNode.class, result);
      TableScanNode optimized = (TableScanNode) result;
      assertNotNull(optimized.getDslFilter());
      assertTrue(optimized.getDslFilter().contains("must_not"));
    }

    @Test
    @DisplayName("Plan without FilterNode passes through unchanged")
    void noFilterPassesThrough() {
      TableScanNode scan = new TableScanNode("logs", List.of("status"));
      ProjectNode project = new ProjectNode(scan, List.of("status"));

      DqePlanNode result = optimizer.optimize(project);

      assertInstanceOf(ProjectNode.class, result);
      assertInstanceOf(TableScanNode.class, ((ProjectNode) result).getChild());
      assertNull(((TableScanNode) ((ProjectNode) result).getChild()).getDslFilter());
    }
  }

  @Nested
  @DisplayName("Projection Pruning")
  class ProjectionPruning {

    @Test
    @DisplayName("Prunes TableScanNode columns to only those needed by ProjectNode")
    void prunesUnusedColumns() {
      // Table has 4 columns, but we only SELECT one
      TableScanNode scan = new TableScanNode("logs", List.of("a", "b", "c", "d"));
      ProjectNode project = new ProjectNode(scan, List.of("b"));

      DqePlanNode result = optimizer.optimize(project);

      assertInstanceOf(ProjectNode.class, result);
      ProjectNode optimizedProject = (ProjectNode) result;
      assertInstanceOf(TableScanNode.class, optimizedProject.getChild());
      TableScanNode optimizedScan = (TableScanNode) optimizedProject.getChild();
      assertEquals(List.of("b"), optimizedScan.getColumns());
    }

    @Test
    @DisplayName("Retains columns referenced by filter predicate even if not in SELECT")
    void retainsFilterColumns() {
      // SELECT b FROM logs WHERE a = 1
      // Both 'a' (filter) and 'b' (projection) must be in TableScanNode
      TableScanNode scan = new TableScanNode("logs", List.of("a", "b", "c"));
      FilterNode filter = new FilterNode(scan, "a = 1");
      ProjectNode project = new ProjectNode(filter, List.of("b"));

      // After pushdown, filter is removed, but we still need to check that columns are pruned
      // correctly. In the case where the filter gets pushed to DSL, we only need 'b'.
      // Since 'a = 1' is pushable, it becomes a DSL filter, then only 'b' is needed.
      DqePlanNode result = optimizer.optimize(project);

      assertInstanceOf(ProjectNode.class, result);
      ProjectNode optimizedProject = (ProjectNode) result;
      assertInstanceOf(TableScanNode.class, optimizedProject.getChild());
      TableScanNode optimizedScan = (TableScanNode) optimizedProject.getChild();
      // After pushdown the filter is gone, so only 'b' is needed
      assertEquals(List.of("b"), optimizedScan.getColumns());
      // DSL filter should be set
      assertNotNull(optimizedScan.getDslFilter());
    }

    @Test
    @DisplayName("Retains sort key columns even if not in SELECT")
    void retainsSortColumns() {
      // SELECT b FROM logs ORDER BY a -> need both 'a' and 'b'
      TableScanNode scan = new TableScanNode("logs", List.of("a", "b", "c"));
      ProjectNode project = new ProjectNode(scan, List.of("b"));
      SortNode sort = new SortNode(project, List.of("b"), List.of(true));

      DqePlanNode result = optimizer.optimize(sort);

      // Sort -> Project -> TableScan
      assertInstanceOf(SortNode.class, result);
    }

    @Test
    @DisplayName("Prunes to aggregation-referenced columns")
    void prunesForAggregation() {
      // SELECT category, COUNT(*) FROM logs GROUP BY category
      // Only 'category' is needed from the scan, not other columns
      TableScanNode scan = new TableScanNode("logs", List.of("category", "status", "message"));
      AggregationNode agg =
          new AggregationNode(
              scan, List.of("category"), List.of("COUNT(*)"), AggregationNode.Step.PARTIAL);

      DqePlanNode result = optimizer.optimize(agg);

      assertInstanceOf(AggregationNode.class, result);
      AggregationNode optimizedAgg = (AggregationNode) result;
      assertInstanceOf(TableScanNode.class, optimizedAgg.getChild());
      TableScanNode optimizedScan = (TableScanNode) optimizedAgg.getChild();
      assertEquals(List.of("category"), optimizedScan.getColumns());
    }

    @Test
    @DisplayName("Prunes to aggregation-referenced columns including aggregate argument")
    void prunesForAggregationWithArgument() {
      // SELECT category, SUM(amount) FROM logs GROUP BY category
      TableScanNode scan =
          new TableScanNode("logs", List.of("category", "amount", "status", "message"));
      AggregationNode agg =
          new AggregationNode(
              scan, List.of("category"), List.of("SUM(amount)"), AggregationNode.Step.PARTIAL);

      DqePlanNode result = optimizer.optimize(agg);

      assertInstanceOf(AggregationNode.class, result);
      AggregationNode optimizedAgg = (AggregationNode) result;
      assertInstanceOf(TableScanNode.class, optimizedAgg.getChild());
      TableScanNode optimizedScan = (TableScanNode) optimizedAgg.getChild();
      // Both 'category' and 'amount' needed
      assertEquals(List.of("category", "amount"), optimizedScan.getColumns());
    }
  }

  @Nested
  @DisplayName("Aggregation Split")
  class AggregationSplit {

    @Test
    @DisplayName("AggregationNode with PARTIAL step remains unchanged")
    void partialRemainsPartial() {
      TableScanNode scan = new TableScanNode("logs", List.of("category"));
      AggregationNode agg =
          new AggregationNode(
              scan, List.of("category"), List.of("COUNT(*)"), AggregationNode.Step.PARTIAL);

      DqePlanNode result = optimizer.optimize(agg);

      assertInstanceOf(AggregationNode.class, result);
      assertEquals(AggregationNode.Step.PARTIAL, ((AggregationNode) result).getStep());
    }

    @Test
    @DisplayName("AggregationNode with FINAL step is converted to PARTIAL")
    void finalConvertedToPartial() {
      TableScanNode scan = new TableScanNode("logs", List.of("category"));
      AggregationNode agg =
          new AggregationNode(
              scan, List.of("category"), List.of("COUNT(*)"), AggregationNode.Step.FINAL);

      DqePlanNode result = optimizer.optimize(agg);

      assertInstanceOf(AggregationNode.class, result);
      assertEquals(AggregationNode.Step.PARTIAL, ((AggregationNode) result).getStep());
    }
  }

  @Nested
  @DisplayName("Full Optimization Chain")
  class FullChain {

    @Test
    @DisplayName("Applies all rules: pushdown + prune + split on complex plan")
    void fullChain() {
      // Plan: Project([category]) -> Filter("status = 200") ->
      //       AggregationNode(FINAL, groupBy=category, aggs=COUNT(*)) -> TableScan(20 columns)
      // Note: This is an unusual plan shape, but tests that all rules fire.
      TableScanNode scan =
          new TableScanNode("logs", List.of("category", "status", "message", "timestamp", "level"));
      FilterNode filter = new FilterNode(scan, "status = 200");
      AggregationNode agg =
          new AggregationNode(
              filter, List.of("category"), List.of("COUNT(*)"), AggregationNode.Step.FINAL);

      DqePlanNode result = optimizer.optimize(agg);

      // After optimization:
      // 1. Predicate pushdown: filter removed, dslFilter set on TableScan
      // 2. Projection pruning: columns narrowed to [category] (only ref'd by agg)
      // 3. Aggregation split: step changed from FINAL to PARTIAL
      assertInstanceOf(AggregationNode.class, result);
      AggregationNode optimizedAgg = (AggregationNode) result;
      assertEquals(AggregationNode.Step.PARTIAL, optimizedAgg.getStep());

      assertInstanceOf(TableScanNode.class, optimizedAgg.getChild());
      TableScanNode optimizedScan = (TableScanNode) optimizedAgg.getChild();
      assertEquals("{\"term\":{\"status\":200}}", optimizedScan.getDslFilter());
      assertEquals(List.of("category"), optimizedScan.getColumns());
    }

    @Test
    @DisplayName("Optimization of simple SELECT only prunes projections")
    void simpleSelectOptimization() {
      // Plan: Project([a]) -> TableScan(logs, [a, b, c, d])
      TableScanNode scan = new TableScanNode("logs", List.of("a", "b", "c", "d"));
      ProjectNode project = new ProjectNode(scan, List.of("a"));

      DqePlanNode result = optimizer.optimize(project);

      assertInstanceOf(ProjectNode.class, result);
      ProjectNode optimizedProject = (ProjectNode) result;
      assertInstanceOf(TableScanNode.class, optimizedProject.getChild());
      TableScanNode optimizedScan = (TableScanNode) optimizedProject.getChild();
      assertEquals(List.of("a"), optimizedScan.getColumns());
      assertNull(optimizedScan.getDslFilter());
    }

    @Test
    @DisplayName("Optimization handles Limit(Project(Filter(TableScan))) correctly")
    void limitProjectFilterScan() {
      TableScanNode scan =
          new TableScanNode("logs", List.of("status", "category", "message", "timestamp"));
      FilterNode filter = new FilterNode(scan, "status = 200");
      ProjectNode project = new ProjectNode(filter, List.of("category"));
      LimitNode limit = new LimitNode(project, 50);

      DqePlanNode result = optimizer.optimize(limit);

      // Limit -> Project -> TableScan (filter pushed, columns pruned)
      assertInstanceOf(LimitNode.class, result);
      LimitNode optimizedLimit = (LimitNode) result;
      assertEquals(50, optimizedLimit.getCount());

      assertInstanceOf(ProjectNode.class, optimizedLimit.getChild());
      ProjectNode optimizedProject = (ProjectNode) optimizedLimit.getChild();
      assertEquals(List.of("category"), optimizedProject.getOutputColumns());

      assertInstanceOf(TableScanNode.class, optimizedProject.getChild());
      TableScanNode optimizedScan = (TableScanNode) optimizedProject.getChild();
      assertEquals("{\"term\":{\"status\":200}}", optimizedScan.getDslFilter());
      assertEquals(List.of("category"), optimizedScan.getColumns());
    }
  }

  @Nested
  @DisplayName("tryConvertToDsl")
  class TryConvertToDsl {

    @Test
    @DisplayName("Converts integer equality to term query")
    void integerEquality() {
      assertEquals("{\"term\":{\"status\":200}}", PlanOptimizer.tryConvertToDsl("status = 200"));
    }

    @Test
    @DisplayName("Converts negative integer equality")
    void negativeInteger() {
      assertEquals("{\"term\":{\"code\":-1}}", PlanOptimizer.tryConvertToDsl("code = -1"));
    }

    @Test
    @DisplayName("Converts string equality to term query")
    void stringEquality() {
      assertEquals(
          "{\"term\":{\"level\":\"error\"}}", PlanOptimizer.tryConvertToDsl("level = 'error'"));
    }

    @Test
    @DisplayName("Returns null for non-equality predicate")
    void nonEquality() {
      assertNull(PlanOptimizer.tryConvertToDsl("status > 200"));
    }

    @Test
    @DisplayName("Returns null for complex predicate")
    void complexPredicate() {
      assertNull(PlanOptimizer.tryConvertToDsl("status = 200 AND level = 'error'"));
    }

    @Test
    @DisplayName("Converts double equality to term query")
    void doubleEquality() {
      assertEquals("{\"term\":{\"price\":19.99}}", PlanOptimizer.tryConvertToDsl("price = 19.99"));
    }
  }
}
