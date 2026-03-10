/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.planner.optimizer;

import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.StringLiteral;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanVisitor;
import org.opensearch.sql.dqe.planner.plan.EvalNode;
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

  private final Map<String, String> fieldTypes;

  public PlanOptimizer() {
    this.fieldTypes = Map.of();
  }

  public PlanOptimizer(Map<String, String> fieldTypes) {
    this.fieldTypes = fieldTypes;
  }

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
   * Visitor that pushes predicates from FilterNode down into TableScanNode as DSL filter queries.
   * Supports equality, inequality, range, AND/OR, LIKE (keyword only), IN, and BETWEEN. When the
   * predicate is successfully pushed, the FilterNode is removed from the tree.
   */
  private class PredicatePushdownVisitor extends DqePlanVisitor<DqePlanNode, Void> {

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
      return new SortNode(
          optimizedChild, node.getSortKeys(), node.getAscending(), node.getNullsFirst());
    }

    @Override
    public DqePlanNode visitLimit(LimitNode node, Void context) {
      DqePlanNode optimizedChild = node.getChild().accept(this, context);
      return new LimitNode(optimizedChild, node.getCount(), node.getOffset());
    }

    @Override
    public DqePlanNode visitEval(EvalNode node, Void context) {
      DqePlanNode optimizedChild = node.getChild().accept(this, context);
      return new EvalNode(optimizedChild, node.getExpressions(), node.getOutputColumnNames());
    }
  }

  /**
   * Visitor that prunes TableScanNode columns to only include columns referenced by downstream
   * operators. Collects required columns by walking upward from the scan, then narrows the column
   * list.
   */
  private static class ProjectionPruningVisitor extends DqePlanVisitor<DqePlanNode, Void> {

    // Columns required by ancestor nodes (Sort keys, Filter predicates above ProjectNode)
    private final Set<String> ancestorRequiredColumns = new HashSet<>();

    @Override
    public DqePlanNode visitProject(ProjectNode node, Void context) {
      // Collect all columns that this ProjectNode and everything above it need
      Set<String> requiredColumns = new HashSet<>(node.getOutputColumns());
      requiredColumns.addAll(ancestorRequiredColumns);

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
      // Collect filter predicate columns for descendants
      extractPredicateColumnRefs(node.getPredicateString(), ancestorRequiredColumns);
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
      // Sort keys need to be available at the scan level
      ancestorRequiredColumns.addAll(node.getSortKeys());
      DqePlanNode optimizedChild = node.getChild().accept(this, context);
      return new SortNode(
          optimizedChild, node.getSortKeys(), node.getAscending(), node.getNullsFirst());
    }

    @Override
    public DqePlanNode visitLimit(LimitNode node, Void context) {
      DqePlanNode optimizedChild = node.getChild().accept(this, context);
      return new LimitNode(optimizedChild, node.getCount(), node.getOffset());
    }

    @Override
    public DqePlanNode visitEval(EvalNode node, Void context) {
      // EvalNode expressions may reference columns — extract them
      for (String expr : node.getExpressions()) {
        extractPredicateColumnRefs(expr, ancestorRequiredColumns);
      }
      DqePlanNode optimizedChild = node.getChild().accept(this, context);
      return new EvalNode(optimizedChild, node.getExpressions(), node.getOutputColumnNames());
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
      } else if (node instanceof EvalNode evalNode) {
        for (String expr : evalNode.getExpressions()) {
          extractPredicateColumnRefs(expr, requiredColumns);
        }
        collectRequiredColumns(evalNode.getChild(), requiredColumns);
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
        return new SortNode(
            prunedChild, sortNode.getSortKeys(), sortNode.getAscending(), sortNode.getNullsFirst());
      } else if (node instanceof LimitNode limitNode) {
        DqePlanNode prunedChild = pruneSubtree(limitNode.getChild(), requiredColumns);
        return new LimitNode(prunedChild, limitNode.getCount(), limitNode.getOffset());
      } else if (node instanceof EvalNode evalNode) {
        DqePlanNode prunedChild = pruneSubtree(evalNode.getChild(), requiredColumns);
        return new EvalNode(
            prunedChild, evalNode.getExpressions(), evalNode.getOutputColumnNames());
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

    /**
     * Extract column references from an aggregate function like "SUM(amount)" or "COUNT(DISTINCT
     * col)".
     */
    private void extractAggregateColumnRefs(String funcStr, Set<String> columns) {
      Pattern aggPattern =
          Pattern.compile(
              "^\\s*(COUNT|SUM|MIN|MAX|AVG)\\((DISTINCT\\s+)?(.+?)\\)\\s*$",
              Pattern.CASE_INSENSITIVE);
      Matcher matcher = aggPattern.matcher(funcStr);
      if (matcher.matches()) {
        String arg = matcher.group(3).trim();
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
      // For non-decomposable aggregates (COUNT(DISTINCT), AVG without COUNT),
      // force SINGLE step — coordinator will run the full aggregation over raw shard data
      if (hasNonDecomposableAgg(node.getAggregateFunctions())) {
        return new AggregationNode(
            optimizedChild,
            node.getGroupByKeys(),
            node.getAggregateFunctions(),
            AggregationNode.Step.SINGLE);
      }
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

    /**
     * Check if the aggregate function list contains non-decomposable functions that can't be
     * correctly merged with PARTIAL/FINAL: COUNT(DISTINCT) or AVG without a companion COUNT.
     */
    private boolean hasNonDecomposableAgg(List<String> aggFunctions) {
      boolean hasAvg = false;
      boolean hasCount = false;
      boolean hasCountDistinct = false;
      for (String func : aggFunctions) {
        String upper = func.toUpperCase(java.util.Locale.ROOT);
        if (upper.contains("COUNT(DISTINCT")) hasCountDistinct = true;
        else if (upper.startsWith("AVG(")) hasAvg = true;
        else if (upper.startsWith("COUNT(")) hasCount = true;
      }
      if (hasCountDistinct) return true;
      if (hasAvg && !hasCount) return true;
      return false;
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
      return new SortNode(
          optimizedChild, node.getSortKeys(), node.getAscending(), node.getNullsFirst());
    }

    @Override
    public DqePlanNode visitLimit(LimitNode node, Void context) {
      DqePlanNode optimizedChild = node.getChild().accept(this, context);
      return new LimitNode(optimizedChild, node.getCount(), node.getOffset());
    }

    @Override
    public DqePlanNode visitEval(EvalNode node, Void context) {
      DqePlanNode optimizedChild = node.getChild().accept(this, context);
      return new EvalNode(optimizedChild, node.getExpressions(), node.getOutputColumnNames());
    }
  }

  /**
   * Attempt to convert a predicate string to an OpenSearch DSL query. Parses the predicate into a
   * Trino Expression AST and recursively converts each node to DSL JSON. Returns null if any
   * sub-expression is not convertible.
   *
   * <p>Supports: equality, not-equal, comparisons ({@code <, <=, >, >=}), AND/OR, LIKE (keyword
   * fields only), IN, and BETWEEN.
   *
   * @param predicateString the predicate string from the FilterNode
   * @return a DSL JSON string, or null if not pushable
   */
  String tryConvertToDsl(String predicateString) {
    try {
      DqeSqlParser parser = new DqeSqlParser();
      Expression expr = parser.parseExpression(predicateString);
      return convertExprToDsl(expr);
    } catch (Exception e) {
      return null;
    }
  }

  private String convertExprToDsl(Expression expr) {
    if (expr instanceof ComparisonExpression cmp) {
      return convertComparison(cmp);
    } else if (expr instanceof LogicalExpression logical) {
      return convertLogical(logical);
    } else if (expr instanceof NotExpression not) {
      String inner = convertExprToDsl(not.getValue());
      if (inner == null) return null;
      return "{\"bool\":{\"must_not\":[" + inner + "]}}";
    } else if (expr instanceof LikePredicate like) {
      return convertLike(like);
    } else if (expr instanceof InPredicate in) {
      return convertIn(in);
    } else if (expr instanceof BetweenPredicate between) {
      return convertBetween(between);
    }
    return null;
  }

  private String convertComparison(ComparisonExpression cmp) {
    String column = extractColumnName(cmp.getLeft());
    if (column == null) return null;
    String value = extractLiteralValue(cmp.getRight());
    if (value == null) return null;

    // For range operators on date fields with string values, convert to epoch millis
    if (isRangeOperator(cmp.getOperator())
        && cmp.getRight() instanceof StringLiteral stringLit
        && "date".equals(fieldTypes.getOrDefault(column, ""))) {
      try {
        long epochMillis =
            java.time.LocalDate.parse(stringLit.getValue())
                .atStartOfDay(java.time.ZoneOffset.UTC)
                .toInstant()
                .toEpochMilli();
        value = String.valueOf(epochMillis);
      } catch (Exception e) {
        return null;
      }
    }

    switch (cmp.getOperator()) {
      case EQUAL:
        return buildTermDsl(column, value);
      case NOT_EQUAL:
        String term = buildTermDsl(column, value);
        if (term == null) return null;
        return "{\"bool\":{\"must_not\":[" + term + "]}}";
      case GREATER_THAN:
        return "{\"range\":{\"" + column + "\":{\"gt\":" + value + "}}}";
      case GREATER_THAN_OR_EQUAL:
        return "{\"range\":{\"" + column + "\":{\"gte\":" + value + "}}}";
      case LESS_THAN:
        return "{\"range\":{\"" + column + "\":{\"lt\":" + value + "}}}";
      case LESS_THAN_OR_EQUAL:
        return "{\"range\":{\"" + column + "\":{\"lte\":" + value + "}}}";
      default:
        return null;
    }
  }

  private static boolean isRangeOperator(ComparisonExpression.Operator op) {
    return op == ComparisonExpression.Operator.GREATER_THAN
        || op == ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL
        || op == ComparisonExpression.Operator.LESS_THAN
        || op == ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
  }

  private String convertLogical(LogicalExpression logical) {
    List<String> clauses = new ArrayList<>();
    for (Expression term : logical.getTerms()) {
      String dsl = convertExprToDsl(term);
      if (dsl == null) return null;
      clauses.add(dsl);
    }
    String joined = String.join(",", clauses);
    switch (logical.getOperator()) {
      case AND:
        return "{\"bool\":{\"must\":[" + joined + "]}}";
      case OR:
        return "{\"bool\":{\"should\":[" + joined + "]}}";
      default:
        return null;
    }
  }

  private String convertLike(LikePredicate like) {
    String column = extractColumnName(like.getValue());
    if (column == null) return null;
    // Only push LIKE on keyword fields
    String osType = fieldTypes.getOrDefault(column, "keyword");
    if ("text".equals(osType)) return null;
    if (!(like.getPattern() instanceof StringLiteral pattern)) return null;
    // Convert SQL LIKE pattern (% → *, _ → ?) to wildcard
    String wildcardPattern = pattern.getValue().replace('%', '*').replace('_', '?');
    return "{\"wildcard\":{\""
        + column
        + "\":{\"value\":\""
        + escapeJsonString(wildcardPattern)
        + "\"}}}";
  }

  private String convertIn(InPredicate in) {
    String column = extractColumnName(in.getValue());
    if (column == null) return null;
    if (!(in.getValueList() instanceof InListExpression list)) return null;
    List<String> values = new ArrayList<>();
    for (Expression val : list.getValues()) {
      String v = extractLiteralValue(val);
      if (v == null) return null;
      values.add(v);
    }
    return "{\"terms\":{\"" + column + "\":[" + String.join(",", values) + "]}}";
  }

  private String convertBetween(BetweenPredicate between) {
    String column = extractColumnName(between.getValue());
    if (column == null) return null;
    String min = extractLiteralValue(between.getMin());
    String max = extractLiteralValue(between.getMax());
    if (min == null || max == null) return null;
    return "{\"range\":{\"" + column + "\":{\"gte\":" + min + ",\"lte\":" + max + "}}}";
  }

  private static String extractColumnName(Expression expr) {
    if (expr instanceof Identifier id) return id.getValue();
    return null;
  }

  private static String extractLiteralValue(Expression expr) {
    if (expr instanceof LongLiteral lit) return String.valueOf(lit.getParsedValue());
    if (expr instanceof DoubleLiteral lit) return String.valueOf(lit.getValue());
    if (expr instanceof DecimalLiteral lit) return lit.getValue();
    if (expr instanceof StringLiteral lit) return "\"" + escapeJsonString(lit.getValue()) + "\"";
    if (expr instanceof GenericLiteral lit && lit.getType().equalsIgnoreCase("DATE")) {
      long epochMillis =
          java.time.LocalDate.parse(lit.getValue())
              .atStartOfDay(java.time.ZoneOffset.UTC)
              .toInstant()
              .toEpochMilli();
      return String.valueOf(epochMillis);
    }
    return null;
  }

  private static String buildTermDsl(String column, String value) {
    return "{\"term\":{\"" + column + "\":" + value + "}}";
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
