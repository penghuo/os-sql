/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.planner.optimizer;

import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanVisitor;
import org.opensearch.sql.dqe.planner.plan.FilterNode;
import org.opensearch.sql.dqe.planner.plan.LimitNode;
import org.opensearch.sql.dqe.planner.plan.ProjectNode;
import org.opensearch.sql.dqe.planner.plan.SortNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;
import org.opensearch.sql.dqe.trino.parser.DqeSqlParser;

/**
 * Optimizes a logical plan by applying rules in sequence: predicate pushdown, projection pruning,
 * and aggregation split.
 *
 * <p>This optimizer sits between {@code LogicalPlanner.plan()} and {@code
 * PlanFragmenter.fragment()} in the DQE pipeline. It transforms the plan tree to push work down to
 * the storage layer (OpenSearch) where possible.
 */
public class PlanOptimizer {

  /** Pattern for simple equality predicates like "column = value". */
  private static final Pattern EQUALITY_PREDICATE =
      Pattern.compile("^\\s*(\\w+)\\s*=\\s*(.+)\\s*$");

  /**
   * Optimize a logical plan by applying rules in sequence.
   *
   * @param plan the logical plan to optimize
   * @return the optimized plan
   */
  public DqePlanNode optimize(DqePlanNode plan) {
    DqePlanNode result = plan;
    result = pushDownPredicates(result);
    result = pruneProjections(result);
    result = splitAggregations(result);
    return result;
  }

  /**
   * Push WHERE predicates into TableScanNode's DSL filter. For simple equality predicates (column =
   * value) where the value is numeric, converts to a DSL term query and removes the FilterNode.
   * Non-pushable predicates are left in place.
   */
  DqePlanNode pushDownPredicates(DqePlanNode plan) {
    return plan.accept(new PredicatePushdownVisitor(), null);
  }

  /**
   * Ensure TableScanNode only requests the columns actually needed by downstream operators. Walks
   * the plan tree to collect all referenced columns and narrows the TableScanNode's column list.
   */
  DqePlanNode pruneProjections(DqePlanNode plan) {
    return plan.accept(new ProjectionPruningVisitor(), null);
  }

  /**
   * Ensure aggregation queries use PARTIAL step so they can be split across shards. If an
   * AggregationNode exists with SINGLE step (or no step), convert it to PARTIAL. The PlanFragmenter
   * will create the corresponding FINAL step at the coordinator.
   */
  DqePlanNode splitAggregations(DqePlanNode plan) {
    return plan.accept(new AggregationSplitVisitor(), null);
  }

  /**
   * Visitor that pushes simple equality predicates from FilterNode down into TableScanNode as DSL
   * filter queries. When the predicate is successfully pushed, the FilterNode is removed from the
   * tree.
   */
  private static class PredicatePushdownVisitor extends DqePlanVisitor<DqePlanNode, Void> {

    @Override
    public DqePlanNode visitFilter(FilterNode node, Void context) {
      // First, recursively optimize the child
      DqePlanNode optimizedChild = node.getChild().accept(this, context);

      // Check if the child (after optimization) is a TableScanNode
      if (optimizedChild instanceof TableScanNode scanNode) {
        String dslFilter = tryConvertToDsl(node.getPredicateString());
        if (dslFilter != null) {
          // Predicate can be pushed down: create a new TableScanNode with the DSL filter
          // and remove the FilterNode
          return new TableScanNode(scanNode.getIndexName(), scanNode.getColumns(), dslFilter);
        }
      }

      // Predicate cannot be pushed down: keep the FilterNode with the optimized child
      return new FilterNode(optimizedChild, node.getPredicateString());
    }

    @Override
    public DqePlanNode visitTableScan(TableScanNode node, Void context) {
      return node;
    }

    @Override
    public DqePlanNode visitProject(ProjectNode node, Void context) {
      DqePlanNode optimizedChild = node.getChild().accept(this, context);
      return new ProjectNode(optimizedChild, node.getOutputColumns());
    }

    @Override
    public DqePlanNode visitAggregation(AggregationNode node, Void context) {
      DqePlanNode optimizedChild = node.getChild().accept(this, context);
      return new AggregationNode(
          optimizedChild, node.getGroupByKeys(), node.getAggregateFunctions(), node.getStep());
    }

    @Override
    public DqePlanNode visitSort(SortNode node, Void context) {
      DqePlanNode optimizedChild = node.getChild().accept(this, context);
      return new SortNode(optimizedChild, node.getSortKeys(), node.getAscending());
    }

    @Override
    public DqePlanNode visitLimit(LimitNode node, Void context) {
      DqePlanNode optimizedChild = node.getChild().accept(this, context);
      return new LimitNode(optimizedChild, node.getCount());
    }
  }

  /**
   * Visitor that prunes TableScanNode columns to only include columns referenced by downstream
   * operators. Collects required columns by walking upward from the scan, then narrows the column
   * list.
   */
  private static class ProjectionPruningVisitor extends DqePlanVisitor<DqePlanNode, Void> {

    @Override
    public DqePlanNode visitProject(ProjectNode node, Void context) {
      // Collect all columns that this ProjectNode and everything above it need
      Set<String> requiredColumns = new HashSet<>(node.getOutputColumns());

      // Recursively collect columns from intermediate nodes between Project and TableScan
      DqePlanNode child = node.getChild();
      collectRequiredColumns(child, requiredColumns);

      // Prune the child subtree
      DqePlanNode prunedChild = pruneSubtree(child, requiredColumns);
      return new ProjectNode(prunedChild, node.getOutputColumns());
    }

    @Override
    public DqePlanNode visitTableScan(TableScanNode node, Void context) {
      // Without an ancestor to determine required columns, pass through unchanged
      return node;
    }

    @Override
    public DqePlanNode visitFilter(FilterNode node, Void context) {
      // When a FilterNode is the root (unlikely), just recurse
      DqePlanNode optimizedChild = node.getChild().accept(this, context);
      return new FilterNode(optimizedChild, node.getPredicateString());
    }

    @Override
    public DqePlanNode visitAggregation(AggregationNode node, Void context) {
      // Aggregation needs its group-by keys and the columns referenced by aggregate functions
      Set<String> requiredColumns = new HashSet<>(node.getGroupByKeys());
      // Extract column references from aggregate functions (e.g., "SUM(amount)" -> "amount")
      for (String func : node.getAggregateFunctions()) {
        extractAggregateColumnRefs(func, requiredColumns);
      }

      // Collect from intermediate nodes
      collectRequiredColumns(node.getChild(), requiredColumns);

      DqePlanNode prunedChild = pruneSubtree(node.getChild(), requiredColumns);
      return new AggregationNode(
          prunedChild, node.getGroupByKeys(), node.getAggregateFunctions(), node.getStep());
    }

    @Override
    public DqePlanNode visitSort(SortNode node, Void context) {
      DqePlanNode optimizedChild = node.getChild().accept(this, context);
      return new SortNode(optimizedChild, node.getSortKeys(), node.getAscending());
    }

    @Override
    public DqePlanNode visitLimit(LimitNode node, Void context) {
      DqePlanNode optimizedChild = node.getChild().accept(this, context);
      return new LimitNode(optimizedChild, node.getCount());
    }

    /**
     * Walk intermediate nodes (Filter, Sort, Limit) between a consumer and the TableScanNode to
     * collect any column references they need (e.g., filter predicates, sort keys).
     */
    private void collectRequiredColumns(DqePlanNode node, Set<String> requiredColumns) {
      if (node instanceof FilterNode filterNode) {
        extractPredicateColumnRefs(filterNode.getPredicateString(), requiredColumns);
        collectRequiredColumns(filterNode.getChild(), requiredColumns);
      } else if (node instanceof SortNode sortNode) {
        requiredColumns.addAll(sortNode.getSortKeys());
        collectRequiredColumns(sortNode.getChild(), requiredColumns);
      } else if (node instanceof LimitNode limitNode) {
        collectRequiredColumns(limitNode.getChild(), requiredColumns);
      } else if (node instanceof AggregationNode aggNode) {
        requiredColumns.addAll(aggNode.getGroupByKeys());
        for (String func : aggNode.getAggregateFunctions()) {
          extractAggregateColumnRefs(func, requiredColumns);
        }
        collectRequiredColumns(aggNode.getChild(), requiredColumns);
      }
      // TableScanNode is the leaf -- nothing to collect
    }

    /**
     * Prune the subtree by narrowing the TableScanNode's columns to only those in the required set.
     */
    private DqePlanNode pruneSubtree(DqePlanNode node, Set<String> requiredColumns) {
      if (node instanceof TableScanNode scanNode) {
        List<String> prunedColumns =
            scanNode.getColumns().stream()
                .filter(requiredColumns::contains)
                .collect(Collectors.toList());
        // Keep at least the columns that exist; if none match, keep original
        if (prunedColumns.isEmpty()) {
          return scanNode;
        }
        return new TableScanNode(scanNode.getIndexName(), prunedColumns, scanNode.getDslFilter());
      } else if (node instanceof FilterNode filterNode) {
        DqePlanNode prunedChild = pruneSubtree(filterNode.getChild(), requiredColumns);
        return new FilterNode(prunedChild, filterNode.getPredicateString());
      } else if (node instanceof SortNode sortNode) {
        DqePlanNode prunedChild = pruneSubtree(sortNode.getChild(), requiredColumns);
        return new SortNode(prunedChild, sortNode.getSortKeys(), sortNode.getAscending());
      } else if (node instanceof LimitNode limitNode) {
        DqePlanNode prunedChild = pruneSubtree(limitNode.getChild(), requiredColumns);
        return new LimitNode(prunedChild, limitNode.getCount());
      } else if (node instanceof AggregationNode aggNode) {
        DqePlanNode prunedChild = pruneSubtree(aggNode.getChild(), requiredColumns);
        return new AggregationNode(
            prunedChild,
            aggNode.getGroupByKeys(),
            aggNode.getAggregateFunctions(),
            aggNode.getStep());
      }
      return node;
    }

    /**
     * Extract column references from a predicate string by parsing it into a Trino Expression AST
     * and collecting all Identifier nodes.
     */
    private void extractPredicateColumnRefs(String predicateString, Set<String> columns) {
      try {
        DqeSqlParser parser = new DqeSqlParser();
        Expression expr = parser.parseExpression(predicateString);
        new DefaultTraversalVisitor<Void>() {
          @Override
          protected Void visitIdentifier(Identifier node, Void context) {
            columns.add(node.getValue());
            return null;
          }
        }.process(expr, null);
      } catch (Exception e) {
        // Fallback: if parsing fails, use the old regex approach
        Matcher matcher = EQUALITY_PREDICATE.matcher(predicateString);
        if (matcher.matches()) {
          columns.add(matcher.group(1));
        }
      }
    }

    /** Extract column references from an aggregate function like "SUM(amount)". */
    private void extractAggregateColumnRefs(String funcStr, Set<String> columns) {
      Pattern aggPattern =
          Pattern.compile("^\\s*(COUNT|SUM|MIN|MAX|AVG)\\((.+?)\\)\\s*$", Pattern.CASE_INSENSITIVE);
      Matcher matcher = aggPattern.matcher(funcStr);
      if (matcher.matches()) {
        String arg = matcher.group(2).trim();
        if (!arg.equals("*")) {
          columns.add(arg);
        }
      }
    }
  }

  /**
   * Visitor that ensures AggregationNode uses PARTIAL step for distributed execution. If an
   * AggregationNode is found with a step other than PARTIAL, it is replaced with PARTIAL. The
   * PlanFragmenter will create the corresponding FINAL step at the coordinator.
   */
  private static class AggregationSplitVisitor extends DqePlanVisitor<DqePlanNode, Void> {

    @Override
    public DqePlanNode visitAggregation(AggregationNode node, Void context) {
      DqePlanNode optimizedChild = node.getChild().accept(this, context);
      // Ensure the aggregation step is PARTIAL for distributed execution
      if (node.getStep() != AggregationNode.Step.PARTIAL) {
        return new AggregationNode(
            optimizedChild,
            node.getGroupByKeys(),
            node.getAggregateFunctions(),
            AggregationNode.Step.PARTIAL);
      }
      return new AggregationNode(
          optimizedChild, node.getGroupByKeys(), node.getAggregateFunctions(), node.getStep());
    }

    @Override
    public DqePlanNode visitTableScan(TableScanNode node, Void context) {
      return node;
    }

    @Override
    public DqePlanNode visitProject(ProjectNode node, Void context) {
      DqePlanNode optimizedChild = node.getChild().accept(this, context);
      return new ProjectNode(optimizedChild, node.getOutputColumns());
    }

    @Override
    public DqePlanNode visitFilter(FilterNode node, Void context) {
      DqePlanNode optimizedChild = node.getChild().accept(this, context);
      return new FilterNode(optimizedChild, node.getPredicateString());
    }

    @Override
    public DqePlanNode visitSort(SortNode node, Void context) {
      DqePlanNode optimizedChild = node.getChild().accept(this, context);
      return new SortNode(optimizedChild, node.getSortKeys(), node.getAscending());
    }

    @Override
    public DqePlanNode visitLimit(LimitNode node, Void context) {
      DqePlanNode optimizedChild = node.getChild().accept(this, context);
      return new LimitNode(optimizedChild, node.getCount());
    }
  }

  /**
   * Attempt to convert a simple predicate string to an OpenSearch DSL term query. Returns null if
   * the predicate cannot be pushed down.
   *
   * <p>Currently supports simple equality predicates of the form "column = value" where value is a
   * numeric literal (integer or decimal) or a single-quoted string.
   *
   * @param predicateString the predicate string from the FilterNode
   * @return a DSL JSON string representing the term query, or null if not pushable
   */
  static String tryConvertToDsl(String predicateString) {
    Matcher matcher = EQUALITY_PREDICATE.matcher(predicateString);
    if (!matcher.matches()) {
      return null;
    }

    String column = matcher.group(1);
    String valueStr = matcher.group(2).trim();

    // Try parsing as a long integer
    try {
      long longValue = Long.parseLong(valueStr);
      return "{\"term\":{\"" + column + "\":" + longValue + "}}";
    } catch (NumberFormatException e) {
      // Not a long, try double
    }

    // Try parsing as a double
    try {
      double doubleValue = Double.parseDouble(valueStr);
      return "{\"term\":{\"" + column + "\":" + doubleValue + "}}";
    } catch (NumberFormatException e) {
      // Not a double, try quoted string
    }

    // Try parsing as a single-quoted string: 'value'
    if (valueStr.startsWith("'") && valueStr.endsWith("'") && valueStr.length() >= 2) {
      String stringValue = valueStr.substring(1, valueStr.length() - 1);
      return "{\"term\":{\"" + column + "\":\"" + escapeJsonString(stringValue) + "\"}}";
    }

    // Cannot push down
    return null;
  }

  /** Escape special characters in a JSON string value. */
  private static String escapeJsonString(String s) {
    return s.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
  }
}
