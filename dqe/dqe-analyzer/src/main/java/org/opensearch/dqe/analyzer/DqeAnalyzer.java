/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer;

import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Limit;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Offset;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.opensearch.dqe.analyzer.predicate.PredicateAnalysisResult;
import org.opensearch.dqe.analyzer.predicate.PredicateAnalyzer;
import org.opensearch.dqe.analyzer.projection.ProjectionAnalyzer;
import org.opensearch.dqe.analyzer.projection.RequiredColumns;
import org.opensearch.dqe.analyzer.scope.Scope;
import org.opensearch.dqe.analyzer.security.SecurityContext;
import org.opensearch.dqe.analyzer.sort.OperatorSelectionRule;
import org.opensearch.dqe.analyzer.sort.OrderByAnalyzer;
import org.opensearch.dqe.analyzer.sort.PipelineDecision;
import org.opensearch.dqe.analyzer.sort.SortSpecification;
import org.opensearch.dqe.analyzer.type.ExpressionTypeChecker;
import org.opensearch.dqe.analyzer.type.TypedExpression;
import org.opensearch.dqe.analyzer.validation.UnsupportedConstructValidator;
import org.opensearch.dqe.metadata.DqeColumnHandle;
import org.opensearch.dqe.metadata.DqeMetadata;
import org.opensearch.dqe.metadata.DqeTableHandle;
import org.opensearch.dqe.parser.DqeAccessDeniedException;
import org.opensearch.dqe.types.DqeType;
import org.opensearch.dqe.types.DqeTypes;

/**
 * Entry point for semantic analysis. Accepts a Trino Statement AST (from DqeSqlParser) and
 * DqeMetadata, performs semantic analysis, and produces an {@link AnalyzedQuery} with resolved
 * tables, columns, types, predicates, projections, and sort specifications.
 */
public class DqeAnalyzer {

  public DqeAnalyzer() {}

  /**
   * Analyzes a parsed Trino Statement against metadata, producing a fully resolved AnalyzedQuery.
   *
   * @param statement the parsed Trino Statement AST
   * @param metadata the metadata service for table/column resolution
   * @param securityContext security context for permission checking
   * @return the analyzed query
   * @throws org.opensearch.dqe.parser.DqeUnsupportedOperationException if unsupported constructs
   * @throws org.opensearch.dqe.parser.DqeTypeMismatchException on type errors
   * @throws DqeAccessDeniedException if user lacks index-level read access
   * @throws DqeAnalysisException for general semantic errors
   */
  public AnalyzedQuery analyze(
      Statement statement, DqeMetadata metadata, SecurityContext securityContext) {
    // Step 1: Validate for unsupported constructs
    UnsupportedConstructValidator validator = new UnsupportedConstructValidator();
    validator.validate(statement);

    // Step 2: Extract the QuerySpecification
    QuerySpecification qs = extractQuerySpecification(statement);

    // Step 3: Resolve the FROM table
    TableInfo tableInfo = resolveTable(qs, metadata);

    // Step 4: Check security permissions (A-8)
    checkPermissions(tableInfo.tableHandle, securityContext);

    // Step 5: Get columns from metadata
    List<DqeColumnHandle> columns = metadata.getColumnHandles(tableInfo.tableHandle);

    // Step 6: Build scope
    Scope scope = new Scope(tableInfo.tableHandle, columns, tableInfo.alias);

    // Step 7: Create type checker
    ExpressionTypeChecker typeChecker = new ExpressionTypeChecker();

    // Step 8: Analyze SELECT clause
    SelectAnalysisResult selectResult = analyzeSelect(qs, scope, typeChecker);

    // Register column aliases for ORDER BY
    for (int i = 0; i < selectResult.columnNames.size(); i++) {
      String name = selectResult.columnNames.get(i);
      DqeType type = selectResult.columnTypes.get(i);
      scope.addColumnAlias(name, type);
    }

    // Step 9: Analyze WHERE clause (A-5)
    PredicateAnalysisResult predicateResult = null;
    TypedExpression whereClause = null;
    if (qs.getWhere().isPresent()) {
      whereClause = typeChecker.check(qs.getWhere().get(), scope);
      if (!whereClause.getType().equals(DqeTypes.BOOLEAN)) {
        throw new DqeAnalysisException(
            "WHERE clause must be a boolean expression, got " + whereClause.getType());
      }
      PredicateAnalyzer predicateAnalyzer = new PredicateAnalyzer();
      predicateResult = predicateAnalyzer.analyze(whereClause, scope);
    }

    // Step 10: Analyze ORDER BY (A-7)
    OrderByAnalyzer orderByAnalyzer = new OrderByAnalyzer(typeChecker);
    List<SortSpecification> sortSpecs = List.of();
    if (qs.getOrderBy().isPresent()) {
      sortSpecs = orderByAnalyzer.analyze(qs.getOrderBy().get().getSortItems(), scope);
    }

    // Step 11: Analyze LIMIT and OFFSET
    OptionalLong limit = OptionalLong.empty();
    OptionalLong offset = OptionalLong.empty();

    if (qs.getLimit().isPresent()) {
      limit = OptionalLong.of(extractLimit(qs.getLimit().get()));
    }
    if (qs.getOffset().isPresent()) {
      offset = OptionalLong.of(extractOffset(qs.getOffset().get()));
    }

    // Step 12: Operator selection rule (A-7)
    OperatorSelectionRule selectionRule = new OperatorSelectionRule();
    PipelineDecision pipelineDecision = selectionRule.decide(sortSpecs, limit, offset);

    // Step 13: Compute required columns (A-6)
    ProjectionAnalyzer projectionAnalyzer = new ProjectionAnalyzer();
    List<TypedExpression> orderByExpressions = new ArrayList<>();
    for (SortSpecification spec : sortSpecs) {
      orderByExpressions.add(
          new TypedExpression(
              new Identifier(spec.getColumn().getFieldName()), spec.getType()));
    }

    RequiredColumns requiredColumns;
    if (selectResult.isSelectAll) {
      requiredColumns = RequiredColumns.allColumns(columns);
    } else {
      requiredColumns =
          projectionAnalyzer.computeRequiredColumns(
              selectResult.outputExpressions, whereClause, orderByExpressions, scope);
    }

    // Step 14: Build AnalyzedQuery
    AnalyzedQuery.Builder builder =
        AnalyzedQuery.builder()
            .table(tableInfo.tableHandle)
            .outputColumnNames(selectResult.columnNames)
            .outputColumnTypes(selectResult.columnTypes)
            .outputExpressions(selectResult.outputExpressions)
            .requiredColumns(requiredColumns)
            .sortSpecifications(sortSpecs)
            .pipelineDecision(pipelineDecision)
            .selectAll(selectResult.isSelectAll);

    if (predicateResult != null) {
      builder.predicateAnalysis(predicateResult);
    }
    if (limit.isPresent()) {
      builder.limit(limit.getAsLong());
    }
    if (offset.isPresent()) {
      builder.offset(offset.getAsLong());
    }

    return builder.build();
  }

  private QuerySpecification extractQuerySpecification(Statement statement) {
    if (statement instanceof Query query) {
      if (query.getQueryBody() instanceof QuerySpecification qs) {
        return qs;
      }
      throw new DqeAnalysisException(
          "Expected QuerySpecification, got " + query.getQueryBody().getClass().getSimpleName());
    }
    throw new DqeAnalysisException(
        "Expected Query statement, got " + statement.getClass().getSimpleName());
  }

  private TableInfo resolveTable(QuerySpecification qs, DqeMetadata metadata) {
    if (qs.getFrom().isEmpty()) {
      throw new DqeAnalysisException("FROM clause is required in Phase 1");
    }

    Relation from = qs.getFrom().get();
    Optional<String> alias = Optional.empty();

    if (from instanceof AliasedRelation aliased) {
      alias = Optional.of(aliased.getAlias().getValue());
      from = aliased.getRelation();
    }

    if (!(from instanceof Table table)) {
      throw new DqeAnalysisException(
          "Phase 1 only supports single-table FROM clauses, got "
              + from.getClass().getSimpleName());
    }

    String tableName = table.getName().toString();
    DqeTableHandle tableHandle = metadata.getTableHandle("default", tableName);
    return new TableInfo(tableHandle, alias);
  }

  private void checkPermissions(DqeTableHandle table, SecurityContext securityContext) {
    for (String index : table.getResolvedIndices()) {
      if (!securityContext.hasIndexReadPermission(index)) {
        throw new DqeAccessDeniedException(index, "user does not have read permission");
      }
    }
  }

  private SelectAnalysisResult analyzeSelect(
      QuerySpecification qs, Scope scope, ExpressionTypeChecker typeChecker) {
    List<String> columnNames = new ArrayList<>();
    List<DqeType> columnTypes = new ArrayList<>();
    List<TypedExpression> outputExpressions = new ArrayList<>();
    boolean isSelectAll = false;

    for (SelectItem item : qs.getSelect().getSelectItems()) {
      if (item instanceof AllColumns) {
        isSelectAll = true;
        for (DqeColumnHandle col : scope.getColumns()) {
          columnNames.add(col.getFieldName());
          columnTypes.add(col.getType());
          outputExpressions.add(
              new TypedExpression(new Identifier(col.getFieldName()), col.getType()));
        }
      } else if (item instanceof SingleColumn sc) {
        TypedExpression typed = typeChecker.check(sc.getExpression(), scope);
        outputExpressions.add(typed);
        columnTypes.add(typed.getType());

        // Column name: use alias if present, else derive from expression
        if (sc.getAlias().isPresent()) {
          columnNames.add(sc.getAlias().get().getValue());
        } else if (sc.getExpression() instanceof Identifier id) {
          columnNames.add(id.getValue());
        } else {
          columnNames.add(sc.getExpression().toString());
        }
      }
    }

    return new SelectAnalysisResult(columnNames, columnTypes, outputExpressions, isSelectAll);
  }

  private long extractLimit(io.trino.sql.tree.Node limitNode) {
    if (limitNode instanceof Limit limit) {
      io.trino.sql.tree.Expression rowCount = limit.getRowCount();
      if (rowCount instanceof LongLiteral longLit) {
        long value = longLit.getParsedValue();
        if (value < 0) {
          throw new DqeAnalysisException("LIMIT must be a non-negative integer, got " + value);
        }
        return value;
      }
      throw new DqeAnalysisException("LIMIT must be an integer literal");
    }
    // Fallback: try to parse as string (e.g., "ALL")
    throw new DqeAnalysisException(
        "Unsupported LIMIT type: " + limitNode.getClass().getSimpleName());
  }

  private long extractOffset(Offset offset) {
    if (offset.getRowCount() instanceof LongLiteral longLit) {
      long value = longLit.getParsedValue();
      if (value < 0) {
        throw new DqeAnalysisException("OFFSET must be a non-negative integer, got " + value);
      }
      return value;
    }
    throw new DqeAnalysisException("OFFSET must be an integer literal");
  }

  private record TableInfo(DqeTableHandle tableHandle, Optional<String> alias) {}

  private record SelectAnalysisResult(
      List<String> columnNames,
      List<DqeType> columnTypes,
      List<TypedExpression> outputExpressions,
      boolean isSelectAll) {}
}
