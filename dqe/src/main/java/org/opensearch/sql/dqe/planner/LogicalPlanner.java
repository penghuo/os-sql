/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.planner;

import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Limit;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Offset;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.opensearch.sql.dqe.coordinator.metadata.TableInfo;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.EvalNode;
import org.opensearch.sql.dqe.planner.plan.FilterNode;
import org.opensearch.sql.dqe.planner.plan.LimitNode;
import org.opensearch.sql.dqe.planner.plan.ProjectNode;
import org.opensearch.sql.dqe.planner.plan.SortNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;

/**
 * Converts a parsed Trino AST ({@link Statement}) into a {@link DqePlanNode} tree. Uses instanceof
 * checks on the relevant Trino AST classes to walk the tree and produce plan nodes bottom-up.
 *
 * <p>Plan node ordering (bottom-up):
 *
 * <ol>
 *   <li>{@link TableScanNode} from FROM clause
 *   <li>{@link FilterNode} wrapping scan if WHERE exists
 *   <li>{@link AggregationNode} wrapping filter/scan if GROUP BY exists
 *   <li>{@link ProjectNode} wrapping the above (SELECT columns)
 *   <li>{@link SortNode} wrapping project if ORDER BY exists
 *   <li>{@link LimitNode} wrapping everything if LIMIT exists
 * </ol>
 */
public class LogicalPlanner {

  /**
   * Convert a Trino AST {@link Statement} into a {@link DqePlanNode} tree.
   *
   * @param stmt the parsed Trino statement (must be a {@link Query})
   * @param tableResolver resolves an index name to {@link TableInfo} for schema metadata
   * @return the root of the logical plan tree
   */
  public static DqePlanNode plan(Statement stmt, Function<String, TableInfo> tableResolver) {
    if (!(stmt instanceof Query query)) {
      throw new IllegalArgumentException("Only SELECT queries are supported, got: " + stmt);
    }

    if (!(query.getQueryBody() instanceof QuerySpecification querySpec)) {
      throw new IllegalArgumentException(
          "Only simple query specifications are supported, got: " + query.getQueryBody());
    }

    // 1. Build TableScanNode from FROM clause
    TableScanNode scanNode = buildTableScan(querySpec, tableResolver);
    DqePlanNode current = scanNode;

    // 2. Wrap with FilterNode if WHERE exists
    Optional<Expression> where = querySpec.getWhere();
    if (where.isPresent()) {
      current = new FilterNode(current, where.get().toString());
    }

    // 3. Wrap with AggregationNode if GROUP BY exists
    Optional<GroupBy> groupBy = querySpec.getGroupBy();
    if (groupBy.isPresent()) {
      current = buildAggregation(current, querySpec, groupBy.get());
    }

    // 4. Insert EvalNode for computed columns if needed (outside of GROUP BY)
    if (groupBy.isEmpty()) {
      current = maybeInsertEvalNode(current, querySpec);
    }

    // 5. Wrap with ProjectNode (SELECT columns)
    List<String> outputColumns = extractOutputColumns(querySpec, scanNode.getColumns());
    current = new ProjectNode(current, outputColumns);

    // 6. Wrap with SortNode if ORDER BY exists
    // For simple queries, ORDER BY lives on QuerySpecification; for compound queries, on Query.
    Optional<OrderBy> orderBy = querySpec.getOrderBy().or(query::getOrderBy);
    if (orderBy.isPresent()) {
      current = buildSort(current, orderBy.get());
    }

    // 7. Wrap with LimitNode if LIMIT exists
    // For simple queries, LIMIT lives on QuerySpecification; for compound queries, on Query.
    Optional<Node> limit = querySpec.getLimit().or(query::getLimit);
    Optional<Offset> offset = querySpec.getOffset().or(query::getOffset);
    if (limit.isPresent()) {
      current = buildLimit(current, limit.get(), offset);
    } else if (offset.isPresent()) {
      // OFFSET without LIMIT: use a very large limit
      current = new LimitNode(current, Long.MAX_VALUE, extractOffset(offset.get()));
    }

    return current;
  }

  /**
   * If any SELECT item has a computed expression (not a plain Identifier), insert an EvalNode to
   * compute those expressions. The EvalNode output column names become the input for ProjectNode.
   */
  private static DqePlanNode maybeInsertEvalNode(
      DqePlanNode current, QuerySpecification querySpec) {
    boolean hasComputed = false;
    for (SelectItem item : querySpec.getSelect().getSelectItems()) {
      if (item instanceof SingleColumn singleColumn) {
        if (!(singleColumn.getExpression() instanceof Identifier)) {
          hasComputed = true;
          break;
        }
      }
    }

    if (!hasComputed) {
      return current;
    }

    List<String> expressions = new ArrayList<>();
    List<String> outputColumnNames = new ArrayList<>();
    for (SelectItem item : querySpec.getSelect().getSelectItems()) {
      if (item instanceof SingleColumn singleColumn) {
        Expression expr = singleColumn.getExpression();
        expressions.add(expr.toString());
        String colName =
            singleColumn
                .getAlias()
                .map(Identifier::getValue)
                .orElseGet(() -> expressionToColumnName(expr));
        outputColumnNames.add(colName);
      }
    }
    return new EvalNode(current, expressions, outputColumnNames);
  }

  private static TableScanNode buildTableScan(
      QuerySpecification querySpec, Function<String, TableInfo> tableResolver) {
    Optional<Relation> from = querySpec.getFrom();
    if (from.isEmpty()) {
      throw new IllegalArgumentException("FROM clause is required");
    }

    Relation relation = from.get();
    if (!(relation instanceof Table table)) {
      throw new IllegalArgumentException(
          "Only simple table references are supported, got: " + relation);
    }

    String tableName = table.getName().toString();
    TableInfo tableInfo = tableResolver.apply(tableName);
    List<String> allColumns = tableInfo.columns().stream().map(TableInfo.ColumnInfo::name).toList();

    return new TableScanNode(tableName, allColumns);
  }

  private static List<String> extractOutputColumns(
      QuerySpecification querySpec, List<String> allTableColumns) {
    List<String> columns = new ArrayList<>();
    for (SelectItem item : querySpec.getSelect().getSelectItems()) {
      if (item instanceof SingleColumn singleColumn) {
        // Use alias if present, otherwise use the expression string representation
        String columnName =
            singleColumn
                .getAlias()
                .map(Identifier::getValue)
                .orElseGet(() -> expressionToColumnName(singleColumn.getExpression()));
        columns.add(columnName);
      } else {
        // AllColumns (SELECT *) — expand to all table columns
        columns.addAll(allTableColumns);
      }
    }
    return columns;
  }

  private static String expressionToColumnName(Expression expr) {
    if (expr instanceof Identifier identifier) {
      return identifier.getValue();
    }
    if (expr instanceof FunctionCall functionCall) {
      return functionCall.getName().toString()
          + "("
          + (functionCall.getArguments().isEmpty() ? "*" : functionCall.getArguments().toString())
          + ")";
    }
    return expr.toString();
  }

  private static DqePlanNode buildAggregation(
      DqePlanNode child, QuerySpecification querySpec, GroupBy groupBy) {
    // Extract group-by keys
    List<String> groupByKeys = new ArrayList<>();
    for (var element : groupBy.getGroupingElements()) {
      if (element instanceof SimpleGroupBy simpleGroupBy) {
        for (Expression expr : simpleGroupBy.getExpressions()) {
          groupByKeys.add(expr.toString());
        }
      }
    }

    // Extract aggregate functions from SELECT items
    List<String> aggregateFunctions = new ArrayList<>();
    for (SelectItem item : querySpec.getSelect().getSelectItems()) {
      if (item instanceof SingleColumn singleColumn) {
        Expression expr = singleColumn.getExpression();
        if (expr instanceof FunctionCall functionCall) {
          aggregateFunctions.add(expressionToColumnName(functionCall));
        }
      }
    }

    return new AggregationNode(
        child, groupByKeys, aggregateFunctions, AggregationNode.Step.PARTIAL);
  }

  private static DqePlanNode buildSort(DqePlanNode child, OrderBy orderBy) {
    List<String> sortKeys = new ArrayList<>();
    List<Boolean> ascending = new ArrayList<>();

    for (SortItem sortItem : orderBy.getSortItems()) {
      sortKeys.add(sortItem.getSortKey().toString());
      ascending.add(sortItem.getOrdering() == SortItem.Ordering.ASCENDING);
    }

    return new SortNode(child, sortKeys, ascending);
  }

  private static DqePlanNode buildLimit(
      DqePlanNode child, Node limitNode, Optional<Offset> offset) {
    if (!(limitNode instanceof Limit limit)) {
      throw new IllegalArgumentException("Unsupported limit node type: " + limitNode);
    }

    Expression rowCount = limit.getRowCount();
    if (!(rowCount instanceof LongLiteral longLiteral)) {
      throw new IllegalArgumentException(
          "Only integer literal LIMIT values are supported, got: " + rowCount);
    }

    long offsetValue = offset.map(LogicalPlanner::extractOffset).orElse(0L);
    return new LimitNode(child, longLiteral.getParsedValue(), offsetValue);
  }

  private static long extractOffset(Offset offset) {
    Expression rowCount = offset.getRowCount();
    if (rowCount instanceof LongLiteral longLiteral) {
      return longLiteral.getParsedValue();
    }
    throw new IllegalArgumentException(
        "Only integer literal OFFSET values are supported, got: " + rowCount);
  }
}
