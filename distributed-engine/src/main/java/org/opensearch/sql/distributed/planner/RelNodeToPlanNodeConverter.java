/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.calcite.plan.rel.Dedup;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;

/**
 * Converts an optimized Calcite RelNode tree to a PlanNode tree for the distributed engine.
 *
 * <p>This converter handles all RelNode types produced by the Calcite optimizer in this codebase:
 *
 * <ul>
 *   <li>LogicalFilter -> FilterNode
 *   <li>LogicalProject -> ProjectNode
 *   <li>LogicalAggregate -> AggregationNode (SINGLE mode initially)
 *   <li>LogicalSort -> SortNode/TopNNode/LimitNode depending on collation+fetch
 *   <li>TableScan (CalciteLogicalIndexScan) -> LuceneTableScanNode
 *   <li>LogicalValues -> ValuesNode
 *   <li>LogicalDedup -> DedupNode
 *   <li>LogicalSystemLimit -> LimitNode
 *   <li>LogicalJoin, LogicalCorrelate, LogicalWindow -> throws UnsupportedPatternException
 * </ul>
 */
public class RelNodeToPlanNodeConverter {

  /**
   * Converts an optimized Calcite RelNode tree to a PlanNode tree.
   *
   * @param relNode the root of the optimized Calcite plan
   * @return the root of the PlanNode tree
   * @throws UnsupportedPatternException if the plan contains unsupported patterns
   */
  public PlanNode convert(RelNode relNode) {
    return visitNode(relNode);
  }

  private PlanNode visitNode(RelNode relNode) {
    if (relNode instanceof LogicalFilter) {
      return visitFilter((LogicalFilter) relNode);
    } else if (relNode instanceof LogicalProject) {
      return visitProject((LogicalProject) relNode);
    } else if (relNode instanceof LogicalAggregate) {
      return visitAggregate((LogicalAggregate) relNode);
    } else if (relNode instanceof LogicalSystemLimit) {
      return visitSystemLimit((LogicalSystemLimit) relNode);
    } else if (relNode instanceof LogicalSort) {
      return visitSort((LogicalSort) relNode);
    } else if (relNode instanceof Dedup) {
      return visitDedup((Dedup) relNode);
    } else if (relNode instanceof LogicalValues) {
      return visitValues((LogicalValues) relNode);
    } else if (relNode instanceof TableScan) {
      return visitTableScan((TableScan) relNode);
    } else if (relNode instanceof LogicalJoin) {
      throw new UnsupportedPatternException(
          "LogicalJoin is not supported in Phase 1 distributed engine");
    } else if (relNode instanceof LogicalCorrelate) {
      throw new UnsupportedPatternException(
          "LogicalCorrelate is not supported in Phase 1 distributed engine");
    } else if (relNode instanceof LogicalUnion) {
      throw new UnsupportedPatternException(
          "LogicalUnion is not supported in Phase 1 distributed engine");
    } else {
      // Check for window functions by class name to avoid hard dependency
      String className = relNode.getClass().getSimpleName();
      if (className.contains("Window")) {
        throw new UnsupportedPatternException(
            "Window functions are not supported in Phase 1 distributed engine");
      }
      throw new UnsupportedPatternException(
          "Unsupported RelNode type: " + relNode.getClass().getName());
    }
  }

  private FilterNode visitFilter(LogicalFilter filter) {
    PlanNode source = visitNode(filter.getInput());
    return new FilterNode(PlanNodeId.next("Filter"), source, filter.getCondition());
  }

  private ProjectNode visitProject(LogicalProject project) {
    PlanNode source = visitNode(project.getInput());
    return new ProjectNode(
        PlanNodeId.next("Project"), source, project.getProjects(), project.getRowType());
  }

  private AggregationNode visitAggregate(LogicalAggregate aggregate) {
    PlanNode source = visitNode(aggregate.getInput());
    return new AggregationNode(
        PlanNodeId.next("Aggregation"),
        source,
        aggregate.getGroupSet(),
        aggregate.getAggCallList(),
        AggregationNode.AggregationMode.SINGLE);
  }

  private PlanNode visitSort(LogicalSort sort) {
    PlanNode source = visitNode(sort.getInput());
    RelCollation collation = sort.getCollation();
    boolean hasCollation = collation != null && !collation.getFieldCollations().isEmpty();
    boolean hasFetch = sort.fetch != null;
    boolean hasOffset = sort.offset != null;

    if (hasCollation && hasFetch) {
      // Sort with limit -> TopN
      long limit = extractLongValue(sort.fetch);
      return new TopNNode(PlanNodeId.next("TopN"), source, collation, limit);
    } else if (hasCollation) {
      // Sort only
      return new SortNode(PlanNodeId.next("Sort"), source, collation);
    } else {
      // Limit only (no sort)
      long limit = hasFetch ? extractLongValue(sort.fetch) : Long.MAX_VALUE;
      long offset = hasOffset ? extractLongValue(sort.offset) : 0;
      return new LimitNode(PlanNodeId.next("Limit"), source, limit, offset);
    }
  }

  private LimitNode visitSystemLimit(LogicalSystemLimit systemLimit) {
    PlanNode source = visitNode(systemLimit.getInput());
    long limit = systemLimit.fetch != null ? extractLongValue(systemLimit.fetch) : Long.MAX_VALUE;
    long offset = systemLimit.offset != null ? extractLongValue(systemLimit.offset) : 0;
    return new LimitNode(PlanNodeId.next("SystemLimit"), source, limit, offset);
  }

  private DedupNode visitDedup(Dedup dedup) {
    PlanNode source = visitNode(dedup.getInput());
    return new DedupNode(
        PlanNodeId.next("Dedup"),
        source,
        dedup.getDedupeFields(),
        dedup.getAllowedDuplication(),
        dedup.getKeepEmpty(),
        dedup.getConsecutive());
  }

  private ValuesNode visitValues(LogicalValues values) {
    List<List<RexLiteral>> tuples = new ArrayList<>();
    for (var tuple : values.getTuples()) {
      tuples.add(List.copyOf(tuple));
    }
    return new ValuesNode(PlanNodeId.next("Values"), tuples, values.getRowType());
  }

  private LuceneTableScanNode visitTableScan(TableScan tableScan) {
    String indexName = extractIndexName(tableScan);
    List<String> projectedColumns =
        tableScan.getRowType().getFieldList().stream()
            .map(RelDataTypeField::getName)
            .collect(Collectors.toList());
    return new LuceneTableScanNode(
        PlanNodeId.next("LuceneTableScan"), indexName, tableScan.getRowType(), projectedColumns);
  }

  private String extractIndexName(TableScan tableScan) {
    List<String> qualifiedName = tableScan.getTable().getQualifiedName();
    // The last element is typically the index/table name
    return qualifiedName.get(qualifiedName.size() - 1);
  }

  private long extractLongValue(RexNode rexNode) {
    if (rexNode instanceof RexLiteral) {
      RexLiteral literal = (RexLiteral) rexNode;
      Number value = (Number) literal.getValue();
      return value != null ? value.longValue() : 0;
    }
    throw new IllegalArgumentException("Expected RexLiteral but got: " + rexNode);
  }
}
