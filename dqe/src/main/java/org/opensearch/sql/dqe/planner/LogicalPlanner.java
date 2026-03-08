/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.planner;

import io.trino.sql.tree.AllColumns;
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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

    // 1. Build TableScanNode from FROM clause (with column pruning)
    TableScanNode scanNode = buildTableScan(querySpec, query, tableResolver);
    DqePlanNode current = scanNode;

    // 2. Wrap with FilterNode if WHERE exists
    Optional<Expression> where = querySpec.getWhere();
    if (where.isPresent()) {
      current = new FilterNode(current, where.get().toString());
    }

    // 3. Wrap with AggregationNode if GROUP BY exists or SELECT has aggregate functions
    Optional<GroupBy> groupBy = querySpec.getGroupBy();
    boolean hasAggregatesInSelect = hasAggregateFunctions(querySpec);
    if (groupBy.isPresent()) {
      current = buildAggregation(current, querySpec, groupBy.get(), scanNode.getColumns());
    } else if (hasAggregatesInSelect) {
      // Global aggregation: no GROUP BY but SELECT has aggregate functions
      current = buildGlobalAggregation(current, querySpec, scanNode.getColumns());
    }

    // 3b. Wrap with HAVING FilterNode if present
    Optional<Expression> having = querySpec.getHaving();
    if (having.isPresent()) {
      current = new FilterNode(current, having.get().toString());
    }

    // 4. Insert EvalNode for computed columns if needed (outside of aggregation)
    if (groupBy.isEmpty() && !hasAggregatesInSelect) {
      current = maybeInsertEvalNode(current, querySpec);
    }

    // Build alias map for resolving ORDER BY references to internal column names.
    // Maps SQL alias (e.g., "c") → internal column name (e.g., "count(*)").
    Map<String, String> aliasToInternal = buildAliasMap(querySpec, scanNode.getColumns());

    // 5. Wrap with SortNode if ORDER BY exists (BEFORE Project, so sort keys are available)
    Optional<OrderBy> orderBy = querySpec.getOrderBy().or(query::getOrderBy);
    if (orderBy.isPresent()) {
      current = buildSort(current, orderBy.get(), aliasToInternal);
    }

    // 6. Wrap with ProjectNode (SELECT columns — after sort so sort keys are still available)
    //    When ORDER BY references columns not in SELECT, include those columns in the
    //    projection so the sort operator (and coordinator merge-sort) can access them.
    //    Extra sort-only columns are appended after the SELECT columns and stripped
    //    by the coordinator before building the response.
    List<String> outputColumns;
    if (current instanceof SortNode && current.getChildren().get(0) instanceof EvalNode evalNode) {
      outputColumns = evalNode.getOutputColumnNames();
    } else if (current instanceof EvalNode evalNode) {
      outputColumns = evalNode.getOutputColumnNames();
    } else {
      outputColumns = extractOutputColumns(querySpec, scanNode.getColumns());
    }
    if (orderBy.isPresent()) {
      List<String> expandedColumns = new ArrayList<>(outputColumns);
      for (SortItem sortItem : orderBy.get().getSortItems()) {
        String resolvedKey = resolveSortKey(sortItem.getSortKey(), aliasToInternal);
        if (!expandedColumns.contains(resolvedKey)) {
          expandedColumns.add(resolvedKey);
        }
      }
      outputColumns = expandedColumns;
    }
    current = new ProjectNode(current, outputColumns);

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
      QuerySpecification querySpec, Query query, Function<String, TableInfo> tableResolver) {
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

    // Column pruning: only scan columns that the query actually references.
    // This avoids fetching all 100+ columns for queries like SELECT COUNT(*).
    boolean needsAllColumns =
        querySpec.getSelect().getSelectItems().stream()
            .anyMatch(item -> item instanceof AllColumns);
    if (needsAllColumns) {
      return new TableScanNode(tableName, allColumns);
    }

    Set<String> referencedColumns = collectReferencedColumns(querySpec, query);
    // Keep only columns that actually exist in the table (filter out aliases, functions, etc.)
    List<String> prunedColumns = allColumns.stream().filter(referencedColumns::contains).toList();
    return new TableScanNode(tableName, prunedColumns);
  }

  /**
   * Collect all column names referenced in the query (SELECT expressions, WHERE, GROUP BY, ORDER
   * BY, HAVING). Only physical column names are collected — aliases, function names, and literals
   * are excluded by filtering against the table's column list at the call site.
   */
  private static Set<String> collectReferencedColumns(QuerySpecification querySpec, Query query) {
    Set<String> refs = new LinkedHashSet<>();

    // SELECT items: walk expressions to find column references
    for (SelectItem item : querySpec.getSelect().getSelectItems()) {
      if (item instanceof SingleColumn sc) {
        Expression expr = sc.getExpression();
        collectColumnReferences(expr, refs);
        // Explicitly extract arguments from aggregate function calls.
        // FunctionCall.getChildren() may not include arguments in all Trino versions.
        if (expr instanceof FunctionCall fc) {
          for (Expression arg : fc.getArguments()) {
            if (arg instanceof Identifier argId) {
              refs.add(argId.getValue());
            } else {
              collectColumnReferences(arg, refs);
            }
          }
        }
      }
    }

    // WHERE clause
    querySpec.getWhere().ifPresent(expr -> collectColumnReferences(expr, refs));

    // GROUP BY
    querySpec
        .getGroupBy()
        .ifPresent(
            gb -> {
              for (var element : gb.getGroupingElements()) {
                if (element instanceof SimpleGroupBy sgb) {
                  for (Expression expr : sgb.getExpressions()) {
                    collectColumnReferences(expr, refs);
                  }
                }
              }
            });

    // ORDER BY (may be on querySpec or query level)
    Optional<OrderBy> orderBy = querySpec.getOrderBy().or(query::getOrderBy);
    orderBy.ifPresent(
        ob -> {
          for (SortItem si : ob.getSortItems()) {
            collectColumnReferences(si.getSortKey(), refs);
          }
        });

    // HAVING
    querySpec.getHaving().ifPresent(expr -> collectColumnReferences(expr, refs));

    return refs;
  }

  /**
   * Recursively collect all {@link Identifier} names from an expression tree. This captures column
   * references but may also include aliases and other identifiers; the caller filters against known
   * table columns. Explicitly handles {@link FunctionCall} arguments which may not be returned by
   * {@link Node#getChildren()} in all Trino versions.
   */
  private static void collectColumnReferences(Expression expr, Set<String> refs) {
    if (expr instanceof Identifier id) {
      refs.add(id.getValue());
    }
    // Explicitly handle FunctionCall arguments (getChildren() may not include them)
    if (expr instanceof FunctionCall functionCall) {
      for (Expression arg : functionCall.getArguments()) {
        collectColumnReferences(arg, refs);
      }
    }
    // Also walk all Node children recursively
    for (Node child : expr.getChildren()) {
      if (child instanceof Expression childExpr) {
        collectColumnReferences(childExpr, refs);
      } else if (child instanceof Node) {
        // Walk non-Expression nodes too (some containers like OrderBy may wrap expressions)
        walkNodeForColumns(child, refs);
      }
    }
  }

  /** Walk any Node tree looking for Expression children that contain column references. */
  private static void walkNodeForColumns(Node node, Set<String> refs) {
    for (Node child : node.getChildren()) {
      if (child instanceof Expression childExpr) {
        collectColumnReferences(childExpr, refs);
      } else {
        walkNodeForColumns(child, refs);
      }
    }
  }

  private static List<String> extractOutputColumns(
      QuerySpecification querySpec, List<String> allTableColumns) {
    List<String> columns = new ArrayList<>();
    for (SelectItem item : querySpec.getSelect().getSelectItems()) {
      if (item instanceof SingleColumn singleColumn) {
        // Use the expression's column name for internal projection mapping.
        // Aliases are handled separately for display names in the response.
        columns.add(expressionToColumnName(singleColumn.getExpression()));
      } else {
        // AllColumns (SELECT *) — expand to all table columns
        columns.addAll(allTableColumns);
      }
    }
    return columns;
  }

  /**
   * Extract display column names for the response schema. Uses aliases when present, otherwise
   * falls back to expression-derived names.
   */
  public static List<String> extractDisplayColumnNames(
      QuerySpecification querySpec, List<String> allTableColumns) {
    List<String> columns = new ArrayList<>();
    for (SelectItem item : querySpec.getSelect().getSelectItems()) {
      if (item instanceof SingleColumn singleColumn) {
        String columnName =
            singleColumn
                .getAlias()
                .map(Identifier::getValue)
                .orElseGet(() -> expressionToColumnName(singleColumn.getExpression()));
        columns.add(columnName);
      } else {
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
      String args;
      if (functionCall.getArguments().isEmpty()) {
        args = "*";
      } else {
        args =
            functionCall.getArguments().stream()
                .map(Object::toString)
                .collect(java.util.stream.Collectors.joining(", "));
      }
      String distinct = functionCall.isDistinct() ? "DISTINCT " : "";
      return functionCall.getName().toString() + "(" + distinct + args + ")";
    }
    return expr.toString();
  }

  /** Known aggregate function names used to detect global aggregation queries. */
  private static final Set<String> AGGREGATE_FUNCTION_NAMES =
      Set.of("count", "sum", "avg", "min", "max", "stddev", "variance", "bool_and", "bool_or");

  /** Check if any SELECT item contains an aggregate function call. */
  private static boolean hasAggregateFunctions(QuerySpecification querySpec) {
    for (SelectItem item : querySpec.getSelect().getSelectItems()) {
      if (item instanceof SingleColumn singleColumn) {
        if (containsAggregateFunction(singleColumn.getExpression())) {
          return true;
        }
      }
    }
    return false;
  }

  /** Recursively check if an expression contains an aggregate function call. */
  private static boolean containsAggregateFunction(Expression expr) {
    if (expr instanceof FunctionCall functionCall) {
      String name = functionCall.getName().toString().toLowerCase();
      if (AGGREGATE_FUNCTION_NAMES.contains(name)) {
        return true;
      }
    }
    // Check child expressions
    for (Node child : expr.getChildren()) {
      if (child instanceof Expression childExpr && containsAggregateFunction(childExpr)) {
        return true;
      }
    }
    return false;
  }

  /** Build a global aggregation (no GROUP BY keys) for queries like SELECT COUNT(*) FROM table. */
  private static DqePlanNode buildGlobalAggregation(
      DqePlanNode child, QuerySpecification querySpec, List<String> scanColumns) {
    // Check if any aggregate function has a computed argument (e.g., SUM(col + 1))
    List<String> evalExpressions = new ArrayList<>();
    List<String> evalOutputNames = new ArrayList<>();
    Set<String> scanColumnSet = new java.util.HashSet<>(scanColumns);

    List<String> aggregateFunctions = new ArrayList<>();
    for (SelectItem item : querySpec.getSelect().getSelectItems()) {
      if (item instanceof SingleColumn singleColumn) {
        Expression expr = singleColumn.getExpression();
        if (expr instanceof FunctionCall functionCall && containsAggregateFunction(functionCall)) {
          // Check if aggregate arguments are computed expressions
          String aggName =
              rewriteAggregateArgs(functionCall, scanColumnSet, evalExpressions, evalOutputNames);
          aggregateFunctions.add(aggName);
        }
      }
    }

    // Insert EvalNode if we have computed expressions
    DqePlanNode current = child;
    if (!evalExpressions.isEmpty()) {
      // Build complete output: all scan columns + computed columns
      List<String> allExprs = new ArrayList<>();
      List<String> allNames = new ArrayList<>();
      for (String col : scanColumns) {
        allExprs.add(col);
        allNames.add(col);
      }
      allExprs.addAll(evalExpressions);
      allNames.addAll(evalOutputNames);
      current = new EvalNode(current, allExprs, allNames);
    }

    return new AggregationNode(
        current, List.of(), aggregateFunctions, AggregationNode.Step.PARTIAL);
  }

  private static DqePlanNode buildAggregation(
      DqePlanNode child, QuerySpecification querySpec, GroupBy groupBy, List<String> scanColumns) {
    Set<String> scanColumnSet = new java.util.HashSet<>(scanColumns);

    // Build SELECT column list for resolving ordinal references (GROUP BY 1 → first SELECT column)
    List<String> selectColumnNames = new ArrayList<>();
    for (SelectItem item : querySpec.getSelect().getSelectItems()) {
      if (item instanceof SingleColumn sc) {
        selectColumnNames.add(expressionToColumnName(sc.getExpression()));
      }
    }

    // Collect computed expressions that need an EvalNode
    List<String> evalExpressions = new ArrayList<>();
    List<String> evalOutputNames = new ArrayList<>();

    // Extract group-by keys, resolving ordinal references and computed expressions
    List<String> groupByKeys = new ArrayList<>();
    for (var element : groupBy.getGroupingElements()) {
      if (element instanceof SimpleGroupBy simpleGroupBy) {
        for (Expression expr : simpleGroupBy.getExpressions()) {
          if (expr instanceof LongLiteral ordinal) {
            int idx = (int) ordinal.getParsedValue() - 1;
            if (idx >= 0 && idx < selectColumnNames.size()) {
              String resolvedCol = selectColumnNames.get(idx);
              try {
                Long.parseLong(resolvedCol);
              } catch (NumberFormatException e) {
                groupByKeys.add(resolvedCol);
              }
            } else {
              groupByKeys.add(expr.toString());
            }
          } else if (expr instanceof Identifier) {
            // Plain column reference — use directly
            groupByKeys.add(expr.toString());
          } else {
            // Computed expression in GROUP BY (e.g., extract(minute FROM EventTime),
            // ClientIP - 1). Create an EvalNode column for it.
            String exprName = expressionToColumnName(expr);
            if (!scanColumnSet.contains(exprName)) {
              evalExpressions.add(expr.toString());
              evalOutputNames.add(exprName);
            }
            groupByKeys.add(exprName);
          }
        }
      }
    }

    // Extract aggregate functions from SELECT items, handling computed arguments
    List<String> aggregateFunctions = new ArrayList<>();
    for (SelectItem item : querySpec.getSelect().getSelectItems()) {
      if (item instanceof SingleColumn singleColumn) {
        Expression expr = singleColumn.getExpression();
        if (containsAggregateFunction(expr)) {
          if (expr instanceof FunctionCall functionCall) {
            String aggName =
                rewriteAggregateArgs(functionCall, scanColumnSet, evalExpressions, evalOutputNames);
            aggregateFunctions.add(aggName);
          }
        }
      }
    }

    // Also handle non-aggregate, non-groupby computed expressions in SELECT
    // (e.g., CASE WHEN ... in a GROUP BY query that isn't an aggregate)
    for (SelectItem item : querySpec.getSelect().getSelectItems()) {
      if (item instanceof SingleColumn singleColumn) {
        Expression expr = singleColumn.getExpression();
        if (!containsAggregateFunction(expr) && !(expr instanceof Identifier)) {
          String exprName = expressionToColumnName(expr);
          if (!groupByKeys.contains(exprName) && !scanColumnSet.contains(exprName)) {
            // This is a computed expression in SELECT that's not a GROUP BY key
            // (e.g., CASE WHEN in SELECT alongside GROUP BY)
            // Add it to the eval expressions so it's computed pre-aggregation
            if (!evalOutputNames.contains(exprName)) {
              evalExpressions.add(expr.toString());
              evalOutputNames.add(exprName);
            }
            // Add to group by keys since it must pass through aggregation
            groupByKeys.add(exprName);
          }
        }
      }
    }

    // Insert EvalNode if we have computed expressions
    DqePlanNode current = child;
    if (!evalExpressions.isEmpty()) {
      // Build complete output: all scan columns + computed columns
      List<String> allExprs = new ArrayList<>();
      List<String> allNames = new ArrayList<>();
      for (String col : scanColumns) {
        allExprs.add(col);
        allNames.add(col);
      }
      // Deduplicate eval expressions
      Set<String> addedExprs = new java.util.HashSet<>();
      for (int i = 0; i < evalExpressions.size(); i++) {
        String name = evalOutputNames.get(i);
        if (!addedExprs.contains(name)) {
          allExprs.add(evalExpressions.get(i));
          allNames.add(name);
          addedExprs.add(name);
        }
      }
      current = new EvalNode(current, allExprs, allNames);
    }

    return new AggregationNode(
        current, groupByKeys, aggregateFunctions, AggregationNode.Step.PARTIAL);
  }

  /**
   * Rewrite aggregate function arguments: if an argument is a computed expression (not a plain
   * column), add it to the eval list and return the rewritten function name using the eval column.
   */
  private static String rewriteAggregateArgs(
      FunctionCall functionCall,
      Set<String> scanColumnSet,
      List<String> evalExpressions,
      List<String> evalOutputNames) {
    String funcName = functionCall.getName().toString();
    List<Expression> args = functionCall.getArguments();
    String distinct = functionCall.isDistinct() ? "DISTINCT " : "";

    if (args.isEmpty()) {
      return funcName + "(*)";
    }

    List<String> rewrittenArgs = new ArrayList<>();
    for (Expression arg : args) {
      String argName = expressionToColumnName(arg);
      if (arg instanceof Identifier || "*".equals(argName) || scanColumnSet.contains(argName)) {
        // Plain column reference or wildcard — use directly
        rewrittenArgs.add(argName);
      } else {
        // Computed argument (e.g., length(URL), col + 1)
        // Add to eval list and use the eval column name
        if (!evalOutputNames.contains(argName)) {
          evalExpressions.add(arg.toString());
          evalOutputNames.add(argName);
        }
        rewrittenArgs.add(argName);
      }
    }
    return funcName + "(" + distinct + String.join(", ", rewrittenArgs) + ")";
  }

  private static DqePlanNode buildSort(
      DqePlanNode child, OrderBy orderBy, Map<String, String> aliasToInternal) {
    List<String> sortKeys = new ArrayList<>();
    List<Boolean> ascending = new ArrayList<>();
    List<Boolean> nullsFirst = new ArrayList<>();

    for (SortItem sortItem : orderBy.getSortItems()) {
      sortKeys.add(resolveSortKey(sortItem.getSortKey(), aliasToInternal));
      boolean asc = sortItem.getOrdering() == SortItem.Ordering.ASCENDING;
      ascending.add(asc);

      // Resolve null ordering: FIRST, LAST, or UNDEFINED.
      // Trino default: NULLS LAST for both ASC and DESC.
      SortItem.NullOrdering nullOrdering = sortItem.getNullOrdering();
      if (nullOrdering == SortItem.NullOrdering.FIRST) {
        nullsFirst.add(true);
      } else {
        // LAST or UNDEFINED: Trino defaults to NULLS LAST
        nullsFirst.add(false);
      }
    }

    return new SortNode(child, sortKeys, ascending, nullsFirst);
  }

  /**
   * Resolve a sort key expression to an internal column name. Handles:
   *
   * <ul>
   *   <li>SQL aliases (e.g., "c" → "count(*)" when SELECT ... AS c)
   *   <li>Function call casing (e.g., "COUNT(*)" → "count(*)")
   *   <li>Plain column references (passed through unchanged)
   * </ul>
   */
  private static String resolveSortKey(Expression sortKey, Map<String, String> aliasToInternal) {
    // 1. Check alias map first (e.g., ORDER BY c → count(*))
    if (sortKey instanceof Identifier id) {
      String aliasName = id.getValue();
      if (aliasToInternal.containsKey(aliasName)) {
        return aliasToInternal.get(aliasName);
      }
      return aliasName;
    }
    // 2. For expressions (e.g., COUNT(*), SUM(col)), use expressionToColumnName
    //    which normalizes function name casing
    return expressionToColumnName(sortKey);
  }

  /**
   * Build a map from SQL aliases to internal column names. For each SELECT item that has an alias
   * (e.g., {@code COUNT(*) AS c}), maps the alias to the internal name (e.g., "c" → "count(*)").
   */
  private static Map<String, String> buildAliasMap(
      QuerySpecification querySpec, List<String> allTableColumns) {
    Map<String, String> aliases = new HashMap<>();
    for (SelectItem item : querySpec.getSelect().getSelectItems()) {
      if (item instanceof SingleColumn singleColumn && singleColumn.getAlias().isPresent()) {
        String alias = singleColumn.getAlias().get().getValue();
        String internalName = expressionToColumnName(singleColumn.getExpression());
        aliases.put(alias, internalName);
      }
    }
    return aliases;
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
