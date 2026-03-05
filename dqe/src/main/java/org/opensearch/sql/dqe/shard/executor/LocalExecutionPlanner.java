/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.executor;

import io.trino.spi.Page;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.opensearch.sql.dqe.operator.FilterOperator;
import org.opensearch.sql.dqe.operator.HashAggregationOperator;
import org.opensearch.sql.dqe.operator.LimitOperator;
import org.opensearch.sql.dqe.operator.Operator;
import org.opensearch.sql.dqe.operator.ProjectOperator;
import org.opensearch.sql.dqe.operator.SortOperator;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanVisitor;
import org.opensearch.sql.dqe.planner.plan.FilterNode;
import org.opensearch.sql.dqe.planner.plan.LimitNode;
import org.opensearch.sql.dqe.planner.plan.ProjectNode;
import org.opensearch.sql.dqe.planner.plan.SortNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;

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

  /** Pattern for simple equality predicates like "column = value". */
  private static final Pattern EQUALITY_PREDICATE =
      Pattern.compile("^\\s*(\\w+)\\s*=\\s*(.+)\\s*$");

  /** Pattern for aggregate function expressions like "COUNT(*)", "SUM(column)". */
  private static final Pattern AGG_FUNCTION =
      Pattern.compile("^\\s*(COUNT|SUM|MIN|MAX|AVG)\\((.+?)\\)\\s*$", Pattern.CASE_INSENSITIVE);

  private final Function<TableScanNode, Operator> scanFactory;
  private final Map<String, Type> columnTypeMap;

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
    return new LimitOperator(child, node.getCount());
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
    BiFunction<Page, Integer, Boolean> predicate =
        buildPredicate(node.getPredicateString(), inputColumns);
    return new FilterOperator(child, predicate);
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
    return new SortOperator(child, sortColumnIndices, node.getAscending(), columnTypes);
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
    }
    throw new IllegalArgumentException("Unknown plan node type: " + node.getClass().getName());
  }

  /**
   * Build a predicate function from a simple predicate string. Currently supports equality
   * predicates of the form "column = value" where the value is a long integer. Handles
   * surrounding parentheses that the Trino AST toString() may produce.
   */
  private BiFunction<Page, Integer, Boolean> buildPredicate(
      String predicateString, List<String> columns) {
    // Strip surrounding parentheses produced by Trino's Expression.toString()
    String normalized = predicateString.trim();
    while (normalized.startsWith("(") && normalized.endsWith(")")) {
      normalized = normalized.substring(1, normalized.length() - 1).trim();
    }
    Matcher matcher = EQUALITY_PREDICATE.matcher(normalized);
    if (!matcher.matches()) {
      throw new UnsupportedOperationException(
          "Unsupported predicate expression: " + predicateString);
    }
    String columnName = matcher.group(1);
    String valueStr = matcher.group(2).trim();

    int colIdx = resolveColumnIndex(columnName, columns);

    // MVP: parse as long equality
    long targetValue = Long.parseLong(valueStr);
    return (page, pos) -> {
      if (page.getBlock(colIdx).isNull(pos)) {
        return false;
      }
      return BigintType.BIGINT.getLong(page.getBlock(colIdx), pos) == targetValue;
    };
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
