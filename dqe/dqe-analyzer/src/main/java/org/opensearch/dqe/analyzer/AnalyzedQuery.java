/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import org.opensearch.dqe.analyzer.predicate.PredicateAnalysisResult;
import org.opensearch.dqe.analyzer.projection.RequiredColumns;
import org.opensearch.dqe.analyzer.sort.PipelineDecision;
import org.opensearch.dqe.analyzer.sort.SortSpecification;
import org.opensearch.dqe.analyzer.type.TypedExpression;
import org.opensearch.dqe.metadata.DqeTableHandle;
import org.opensearch.dqe.types.DqeType;

/**
 * The fully analyzed query -- the primary output of the analyzer module. Consumed by dqe-execution
 * (operators, driver) and dqe-plugin (plan construction). Immutable after construction.
 */
public class AnalyzedQuery {

  private final DqeTableHandle table;
  private final List<String> outputColumnNames;
  private final List<DqeType> outputColumnTypes;
  private final List<TypedExpression> outputExpressions;
  private final PredicateAnalysisResult predicateAnalysis;
  private final RequiredColumns requiredColumns;
  private final List<SortSpecification> sortSpecifications;
  private final OptionalLong limit;
  private final OptionalLong offset;
  private final PipelineDecision pipelineDecision;
  private final boolean selectAll;

  private AnalyzedQuery(Builder builder) {
    this.table = Objects.requireNonNull(builder.table, "table must not be null");
    this.outputColumnNames = List.copyOf(builder.outputColumnNames);
    this.outputColumnTypes = List.copyOf(builder.outputColumnTypes);
    this.outputExpressions = List.copyOf(builder.outputExpressions);
    this.predicateAnalysis = builder.predicateAnalysis;
    this.requiredColumns =
        Objects.requireNonNull(builder.requiredColumns, "requiredColumns must not be null");
    this.sortSpecifications =
        builder.sortSpecifications != null ? List.copyOf(builder.sortSpecifications) : List.of();
    this.limit = builder.limit;
    this.offset = builder.offset;
    this.pipelineDecision =
        Objects.requireNonNull(builder.pipelineDecision, "pipelineDecision must not be null");
    this.selectAll = builder.selectAll;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** The resolved source table. */
  public DqeTableHandle getTable() {
    return table;
  }

  /** Ordered list of output column names (as they appear in SELECT). */
  public List<String> getOutputColumnNames() {
    return outputColumnNames;
  }

  /** Ordered list of output column types. */
  public List<DqeType> getOutputColumnTypes() {
    return outputColumnTypes;
  }

  /** Ordered list of output expressions (typed, from SELECT clause). */
  public List<TypedExpression> getOutputExpressions() {
    return outputExpressions;
  }

  /** Predicate analysis result: pushdown predicates + residual predicates. */
  public Optional<PredicateAnalysisResult> getPredicateAnalysis() {
    return Optional.ofNullable(predicateAnalysis);
  }

  /** True if the query has a WHERE clause. */
  public boolean hasWhereClause() {
    return predicateAnalysis != null;
  }

  /** Minimal columns required from the table scan. */
  public RequiredColumns getRequiredColumns() {
    return requiredColumns;
  }

  /** Sort specifications from ORDER BY (empty list if no ORDER BY). */
  public List<SortSpecification> getSortSpecifications() {
    return sortSpecifications;
  }

  /** LIMIT value if present. */
  public OptionalLong getLimit() {
    return limit;
  }

  /** OFFSET value if present. */
  public OptionalLong getOffset() {
    return offset;
  }

  /** The operator pipeline decision (TopN vs Sort vs Limit vs plain scan). */
  public PipelineDecision getPipelineDecision() {
    return pipelineDecision;
  }

  /** True if the query is SELECT * (all columns, no expressions). */
  public boolean isSelectAll() {
    return selectAll;
  }

  /** Builder for constructing an immutable AnalyzedQuery. */
  public static class Builder {

    private DqeTableHandle table;
    private List<String> outputColumnNames = new ArrayList<>();
    private List<DqeType> outputColumnTypes = new ArrayList<>();
    private List<TypedExpression> outputExpressions = new ArrayList<>();
    private PredicateAnalysisResult predicateAnalysis;
    private RequiredColumns requiredColumns;
    private List<SortSpecification> sortSpecifications;
    private OptionalLong limit = OptionalLong.empty();
    private OptionalLong offset = OptionalLong.empty();
    private PipelineDecision pipelineDecision;
    private boolean selectAll;

    public Builder table(DqeTableHandle table) {
      this.table = table;
      return this;
    }

    public Builder outputColumnNames(List<String> names) {
      this.outputColumnNames = names;
      return this;
    }

    public Builder outputColumnTypes(List<DqeType> types) {
      this.outputColumnTypes = types;
      return this;
    }

    public Builder outputExpressions(List<TypedExpression> expressions) {
      this.outputExpressions = expressions;
      return this;
    }

    public Builder predicateAnalysis(PredicateAnalysisResult result) {
      this.predicateAnalysis = result;
      return this;
    }

    public Builder requiredColumns(RequiredColumns columns) {
      this.requiredColumns = columns;
      return this;
    }

    public Builder sortSpecifications(List<SortSpecification> specs) {
      this.sortSpecifications = specs;
      return this;
    }

    public Builder limit(long limit) {
      this.limit = OptionalLong.of(limit);
      return this;
    }

    public Builder offset(long offset) {
      this.offset = OptionalLong.of(offset);
      return this;
    }

    public Builder pipelineDecision(PipelineDecision decision) {
      this.pipelineDecision = decision;
      return this;
    }

    public Builder selectAll(boolean selectAll) {
      this.selectAll = selectAll;
      return this;
    }

    public AnalyzedQuery build() {
      return new AnalyzedQuery(this);
    }
  }
}
