/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.ast.tree.HighlightConfig;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.planner.PlanContext;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/** The low level interface of core engine. */
@RequiredArgsConstructor
@AllArgsConstructor
@Log4j2
public class QueryService {
  private final Analyzer analyzer;
  private final ExecutionEngine executionEngine;
  private final Planner planner;
  private DataSourceService dataSourceService;
  private Settings settings;


  /** Execute the {@link UnresolvedPlan}, using {@link ResponseListener} to get response.<br> */
  public void execute(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    execute(plan, queryType, null, listener);
  }

  /** Execute with optional highlight config. */
  public void execute(
      UnresolvedPlan plan,
      QueryType queryType,
      HighlightConfig highlightConfig,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    executeWithLegacy(plan, queryType, listener, Optional.empty());
  }

  /** Explain the {@link UnresolvedPlan}, using {@link ResponseListener} to get response.<br> */
  public void explain(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      ExplainMode mode) {
    explain(plan, queryType, null, listener, mode);
  }

  /** Explain with optional highlight config. */
  public void explain(
      UnresolvedPlan plan,
      QueryType queryType,
      HighlightConfig highlightConfig,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      ExplainMode mode) {
    explainWithLegacy(plan, queryType, listener, mode, Optional.empty());
  }

  public void executeWithLegacy(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.QueryResponse> listener,
      Optional<Throwable> calciteFailure) {
    try {
      executePlan(analyze(plan, queryType), PlanContext.emptyPlanContext(), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Explain the query in {@link UnresolvedPlan} using {@link ResponseListener} to get and format
   * explain response.
   *
   * @param plan {@link UnresolvedPlan}
   * @param queryType {@link QueryType}
   * @param listener {@link ResponseListener} for explain response
   * @param calciteFailure Optional failure thrown from calcite
   */
  public void explainWithLegacy(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      ExplainMode mode,
      Optional<Throwable> calciteFailure) {
    try {
      if (mode != null && (mode != ExplainMode.STANDARD)) {
        throw new UnsupportedOperationException(
            "Explain mode " + mode.name() + " is not supported in v2 engine");
      }
      executionEngine.explain(plan(analyze(plan, queryType)), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Execute the {@link LogicalPlan}, with {@link PlanContext} and using {@link ResponseListener} to
   * get response.<br>
   * Todo. Pass split from PlanContext to ExecutionEngine in following PR.
   *
   * @param plan {@link LogicalPlan}
   * @param planContext {@link PlanContext}
   * @param listener {@link ResponseListener}
   */
  public void executePlan(
      LogicalPlan plan,
      PlanContext planContext,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    try {
      planContext
          .getSplit()
          .ifPresentOrElse(
              split -> executionEngine.execute(plan(plan), new ExecutionContext(split), listener),
              () ->
                  executionEngine.execute(
                      plan(plan),
                      ExecutionContext.querySizeLimit(
                          // For pagination, querySizeLimit shouldn't take effect.
                          // See {@link PaginationWindowIT::testQuerySizeLimitDoesNotEffectPageSize}
                          plan instanceof LogicalPaginate
                              ? null
                              : SysLimit.fromSettings(settings).querySizeLimit()),
                      listener));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /** Analyze {@link UnresolvedPlan}. */
  public LogicalPlan analyze(UnresolvedPlan plan, QueryType queryType) {
    return analyzer.analyze(plan, new AnalysisContext(queryType));
  }

  /** Translate {@link LogicalPlan} to {@link PhysicalPlan}. */
  public PhysicalPlan plan(LogicalPlan plan) {
    return planner.plan(plan);
  }
}
