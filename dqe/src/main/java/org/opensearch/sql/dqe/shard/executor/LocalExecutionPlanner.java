/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.executor;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.sql.tree.Expression;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.opensearch.sql.dqe.function.BuiltinFunctions;
import org.opensearch.sql.dqe.function.FunctionRegistry;
import org.opensearch.sql.dqe.function.expression.BlockExpression;
import org.opensearch.sql.dqe.function.expression.ExpressionCompiler;
import org.opensearch.sql.dqe.operator.EvalOperator;
import org.opensearch.sql.dqe.operator.FilterOperator;
import org.opensearch.sql.dqe.operator.HashAggregationOperator;
import org.opensearch.sql.dqe.operator.LimitOperator;
import org.opensearch.sql.dqe.operator.Operator;
import org.opensearch.sql.dqe.operator.ProjectOperator;
import org.opensearch.sql.dqe.operator.SortOperator;
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
 * Converts a {@link DqePlanNode} tree into an {@link Operator} pipeline. Each plan node type maps
 * to its corresponding physical operator.
 *
 * <p>The leaf {@link TableScanNode} is converted via the provided scan factory function, which
 * allows plugging in different data sources (e.g., {@code OpenSearchPageSource} on real shards, or
 * {@code TestPageSource} in unit tests).
 *
 * <p>Column type information is provided via a column-name-to-type mapping that is typically
 * sourced from {@code OpenSearchMetadata} / {@code TableInfo} in production, or constructed
 * directly in tests.
 */
public class LocalExecutionPlanner extends DqePlanVisitor<Operator, Void> {

  /** Pattern for aggregate function expressions like "COUNT(*)", "SUM(column)". */
  private static final Pattern AGG_FUNCTION =
      Pattern.compile("^\\s*(COUNT|SUM|MIN|MAX|AVG)\\((.+?)\\)\\s*$", Pattern.CASE_INSENSITIVE);

  private final Function<TableScanNode, Operator> scanFactory;
  private final Map<String, Type> columnTypeMap;
  private final FunctionRegistry functionRegistry;

  /**
   * Create a LocalExecutionPlanner with a scan factory and column type information.
   *
   * @param scanFactory factory that converts a TableScanNode into a leaf Operator
   * @param columnTypeMap mapping from column name to Trino Type; used by operators that need type
   *     information (sort, aggregation, filter). Pass an empty map if only simple operators (limit,
   *     project) are used.
   */
  public LocalExecutionPlanner(
      Function<TableScanNode, Operator> scanFactory, Map<String, Type> columnTypeMap) {
    this.scanFactory = scanFactory;
    this.columnTypeMap = columnTypeMap;
    this.functionRegistry = BuiltinFunctions.createRegistry();
  }

  /**
   * Convenience constructor that defaults all column types to BIGINT. Suitable for simple plans
   * that only use limit and project operators, or when all data columns are BIGINT.
   *
   * @param scanFactory factory that converts a TableScanNode into a leaf Operator
   */
  public LocalExecutionPlanner(Function<TableScanNode, Operator> scanFactory) {
    this(scanFactory, Collections.emptyMap());
  }

  @Override
  public Operator visitTableScan(TableScanNode node, Void context) {
    return scanFactory.apply(node);
  }

  @Override
  public Operator visitLimit(LimitNode node, Void context) {
    Operator child = node.getChild().accept(this, context);
    // Each shard must return count + offset rows so the coordinator can apply the
    // global offset across all shards and still have enough rows for the final limit.
    return new LimitOperator(child, node.getCount() + node.getOffset());
  }

  @Override
  public Operator visitProject(ProjectNode node, Void context) {
    Operator child = node.getChild().accept(this, context);
    List<String> inputColumns = resolveInputColumns(node.getChild());
    List<Integer> columnIndices =
        node.getOutputColumns().stream()
            .map(col -> resolveColumnIndex(col, inputColumns))
            .collect(Collectors.toList());
    return new ProjectOperator(child, columnIndices);
  }

  @Override
  public Operator visitFilter(FilterNode node, Void context) {
    Operator child = node.getChild().accept(this, context);
    List<String> inputColumns = resolveInputColumns(node.getChild());

    Map<String, Integer> columnIndexMap = new HashMap<>();
    for (int i = 0; i < inputColumns.size(); i++) {
      columnIndexMap.put(inputColumns.get(i), i);
    }

    DqeSqlParser parser = new DqeSqlParser();
    Expression predicate = parser.parseExpression(node.getPredicateString());
    ExpressionCompiler compiler =
        new ExpressionCompiler(functionRegistry, columnIndexMap, columnTypeMap);
    BlockExpression blockPredicate = compiler.compile(predicate);
    return new FilterOperator(child, blockPredicate);
  }

  @Override
  public Operator visitEval(EvalNode node, Void context) {
    Operator child = node.getChild().accept(this, context);
    List<String> inputColumns = resolveInputColumns(node.getChild());

    Map<String, Integer> columnIndexMap = new HashMap<>();
    for (int i = 0; i < inputColumns.size(); i++) {
      columnIndexMap.put(inputColumns.get(i), i);
    }

    DqeSqlParser parser = new DqeSqlParser();
    ExpressionCompiler compiler =
        new ExpressionCompiler(functionRegistry, columnIndexMap, columnTypeMap);
    List<BlockExpression> outputExprs =
        node.getExpressions().stream()
            .map(exprStr -> compiler.compile(parser.parseExpression(exprStr)))
            .collect(Collectors.toList());
    return new EvalOperator(child, outputExprs);
  }

  @Override
  public Operator visitSort(SortNode node, Void context) {
    Operator child = node.getChild().accept(this, context);
    List<String> inputColumns = resolveInputColumns(node.getChild());
    List<Integer> sortColumnIndices =
        node.getSortKeys().stream()
            .map(key -> resolveColumnIndex(key, inputColumns))
            .collect(Collectors.toList());

    List<Type> columnTypes = resolveColumnTypes(inputColumns);
    return new SortOperator(
        child, sortColumnIndices, node.getAscending(), node.getNullsFirst(), columnTypes);
  }

  @Override
  public Operator visitAggregation(AggregationNode node, Void context) {
    Operator child = node.getChild().accept(this, context);
    List<String> inputColumns = resolveInputColumns(node.getChild());

    List<Integer> groupByIndices =
        node.getGroupByKeys().stream()
            .map(key -> resolveColumnIndex(key, inputColumns))
            .collect(Collectors.toList());

    List<Type> columnTypes = resolveColumnTypes(inputColumns);
    List<HashAggregationOperator.AggregateFunction> aggFunctions =
        node.getAggregateFunctions().stream()
            .map(funcStr -> parseAggregateFunction(funcStr, inputColumns, columnTypes))
            .collect(Collectors.toList());

    return new HashAggregationOperator(child, groupByIndices, aggFunctions, columnTypes);
  }

  /**
   * Resolves the output column list for a given plan node. This determines the column order at a
   * given point in the plan tree by walking the node structure.
   */
  private List<String> resolveInputColumns(DqePlanNode node) {
    if (node instanceof TableScanNode) {
      return ((TableScanNode) node).getColumns();
    } else if (node instanceof ProjectNode) {
      return ((ProjectNode) node).getOutputColumns();
    } else if (node instanceof FilterNode) {
      return resolveInputColumns(((FilterNode) node).getChild());
    } else if (node instanceof LimitNode) {
      return resolveInputColumns(((LimitNode) node).getChild());
    } else if (node instanceof SortNode) {
      return resolveInputColumns(((SortNode) node).getChild());
    } else if (node instanceof AggregationNode) {
      AggregationNode aggNode = (AggregationNode) node;
      List<String> output = new ArrayList<>(aggNode.getGroupByKeys());
      output.addAll(aggNode.getAggregateFunctions());
      return output;
    } else if (node instanceof EvalNode) {
      return ((EvalNode) node).getOutputColumnNames();
    }
    throw new IllegalArgumentException("Unknown plan node type: " + node.getClass().getName());
  }

  /**
   * Parse an aggregate function string like "COUNT(*)", "SUM(amount)" into an {@link
   * HashAggregationOperator.AggregateFunction}.
   */
  private HashAggregationOperator.AggregateFunction parseAggregateFunction(
      String funcStr, List<String> columns, List<Type> columnTypes) {
    Matcher matcher = AGG_FUNCTION.matcher(funcStr);
    if (!matcher.matches()) {
      throw new UnsupportedOperationException("Unsupported aggregate function: " + funcStr);
    }
    String funcName = matcher.group(1).toUpperCase();
    String argument = matcher.group(2).trim();

    switch (funcName) {
      case "COUNT":
        return HashAggregationOperator.count();
      case "SUM":
        {
          int colIdx = resolveColumnIndex(argument, columns);
          return HashAggregationOperator.sum(colIdx, columnTypes.get(colIdx));
        }
      case "MIN":
        {
          int colIdx = resolveColumnIndex(argument, columns);
          return HashAggregationOperator.min(colIdx, columnTypes.get(colIdx));
        }
      case "MAX":
        {
          int colIdx = resolveColumnIndex(argument, columns);
          return HashAggregationOperator.max(colIdx, columnTypes.get(colIdx));
        }
      case "AVG":
        {
          int colIdx = resolveColumnIndex(argument, columns);
          return HashAggregationOperator.avg(colIdx, columnTypes.get(colIdx));
        }
      default:
        throw new UnsupportedOperationException("Unsupported aggregate function: " + funcName);
    }
  }

  /**
   * Resolve the index of a column name within a list of column names.
   *
   * @throws IllegalArgumentException if the column is not found
   */
  private int resolveColumnIndex(String columnName, List<String> columns) {
    int idx = columns.indexOf(columnName);
    if (idx < 0) {
      throw new IllegalArgumentException(
          "Column '" + columnName + "' not found in columns: " + columns);
    }
    return idx;
  }

  /**
   * Resolve Trino types for the given column names using the column type map. Falls back to BIGINT
   * if a column is not found in the type map.
   */
  private List<Type> resolveColumnTypes(List<String> columns) {
    List<Type> types = new ArrayList<>(columns.size());
    for (String col : columns) {
      types.add(columnTypeMap.getOrDefault(col, BigintType.BIGINT));
    }
    return types;
  }
}
