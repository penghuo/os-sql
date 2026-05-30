/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.ast.tree.HighlightConfig;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.OpenSearchSchema;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit.SystemLimitType;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.common.error.QueryProcessingStage;
import org.opensearch.sql.common.error.StageErrorHandler;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.exception.CalciteUnsupportedException;
import org.opensearch.sql.exception.NonFallbackCalciteException;
import org.opensearch.sql.monitor.profile.MetricName;
import org.opensearch.sql.monitor.profile.ProfileContext;
import org.opensearch.sql.monitor.profile.ProfileMetric;
import org.opensearch.sql.monitor.profile.QueryProfiling;
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

  /** Helper: depending on the type of error, either re-raise or propagate to the listener. */
  private void propagateCalciteError(Throwable t, ResponseListener<?> listener)
      throws VirtualMachineError {
    if (t instanceof VirtualMachineError) {
      // throw and fast fail the VM errors such as OOM (same with v2).
      throw (VirtualMachineError) t;
    }
    if (t instanceof Exception) {
      listener.onFailure((Exception) t);
    } else if (t instanceof ExceptionInInitializerError
        && ((ExceptionInInitializerError) t).getException() instanceof Exception) {
      listener.onFailure((Exception) ((ExceptionInInitializerError) t).getException());
    } else {
      // Calcite may throw AssertError during query execution.
      listener.onFailure(new CalciteUnsupportedException(t.getMessage(), t));
    }
  }

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
    if (shouldUseCalcite(queryType)) {
      executeWithCalcite(plan, queryType, highlightConfig, listener);
    } else {
      executeWithLegacy(plan, queryType, listener, Optional.empty());
    }
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
    if (shouldUseCalcite(queryType)) {
      explainWithCalcite(plan, queryType, highlightConfig, listener, mode);
    } else {
      explainWithLegacy(plan, queryType, listener, mode, Optional.empty());
    }
  }

  public void executeWithCalcite(
      UnresolvedPlan plan,
      QueryType queryType,
      HighlightConfig highlightConfig,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    CalcitePlanContext.run(
        () -> {
          try {
            ProfileContext profileContext =
                QueryProfiling.activate(QueryContext.isProfileEnabled());
            ProfileMetric analyzeMetric = profileContext.getOrCreateMetric(MetricName.ANALYZE);
            long analyzeStart = System.nanoTime();
            CalcitePlanContext context =
                CalcitePlanContext.create(
                    buildFrameworkConfig(), SysLimit.fromSettings(settings), queryType);

            context.setHighlightConfig(highlightConfig);

            // Wrap analyze with ANALYZING stage tracking
            RelNode relNode =
                StageErrorHandler.executeStage(
                    QueryProcessingStage.ANALYZING,
                    () -> analyze(plan, context),
                    "while preparing and validating the query plan");

            // Wrap plan conversion with PLAN_CONVERSION stage tracking
            RelNode calcitePlan =
                StageErrorHandler.executeStage(
                    QueryProcessingStage.PLAN_CONVERSION,
                    () -> convertToCalcitePlan(relNode, context),
                    "while converting the query to an executable plan");

            analyzeMetric.set(System.nanoTime() - analyzeStart);

            // Wrap execution with EXECUTING stage tracking
            StageErrorHandler.executeStageVoid(
                QueryProcessingStage.EXECUTING,
                () -> executionEngine.execute(calcitePlan, context, listener),
                "while running the query");
          } catch (Throwable t) {
            if (isCalciteFallbackAllowed(t) && !(t instanceof NonFallbackCalciteException)) {
              log.warn("Fallback to V2 query engine since got exception", t);
              executeWithLegacy(plan, queryType, listener, Optional.of(t));
            } else {
              propagateCalciteError(t, listener);
            }
          }
        },
        settings);
  }

  public void explainWithCalcite(
      UnresolvedPlan plan,
      QueryType queryType,
      HighlightConfig highlightConfig,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      ExplainMode mode) {
    CalcitePlanContext.run(
        () -> {
          try {
            QueryProfiling.noop();
            CalcitePlanContext context =
                CalcitePlanContext.create(
                    buildFrameworkConfig(), SysLimit.fromSettings(settings), queryType);
            context.setHighlightConfig(highlightConfig);
            context.run(
                () -> {
                  RelNode relNode = analyze(plan, context);
                  RelNode calcitePlan = convertToCalcitePlan(relNode, context);
                  executionEngine.explain(calcitePlan, mode, context, listener);
                },
                settings);
          } catch (Throwable t) {
            if (isCalciteFallbackAllowed(t)) {
              log.warn("Fallback to V2 query engine since got exception", t);
              explainWithLegacy(plan, queryType, listener, mode, Optional.of(t));
            } else {
              propagateCalciteError(t, listener);
            }
          }
        },
        settings);
  }

  public void executeWithLegacy(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.QueryResponse> listener,
      Optional<Throwable> calciteFailure) {
    try {
      executePlan(analyze(plan, queryType), PlanContext.emptyPlanContext(), listener);
    } catch (Exception e) {
      if (calciteFailure.isPresent()) {
        // This happens if Calcite fell back to V2 due to some issue, and then V2 also failed.
        // Prefer the Calcite error.
        // https://github.com/opensearch-project/sql/issues/5060
        propagateCalciteError(calciteFailure.get(), listener);
      } else {
        listener.onFailure(e);
      }
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
      if (calciteFailure.isPresent()) {
        // This happens if Calcite fell back to V2 due to some issue, and then V2 also failed.
        // Prefer the Calcite error.
        // https://github.com/opensearch-project/sql/issues/5060
        propagateCalciteError(calciteFailure.get(), listener);
      } else {
        listener.onFailure(e);
      }
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

  public RelNode analyze(UnresolvedPlan plan, CalcitePlanContext context) {
    // Every PPL query goes through PPL→SqlNode→SqlValidator→SqlToRelConverter.
    org.opensearch.sql.calcite.sqlnode.SqlNodePlanner planner =
        new org.opensearch.sql.calcite.sqlnode.SqlNodePlanner(context.config, context);
    // SysLimit fields are boxed Integers and can be null when settings haven't been wired
    // (e.g. standalone test contexts). Treat null as 0 (no cap).
    int subsearchLimit = 0;
    int joinSubsearchLimit = 0;
    if (context.sysLimit != null) {
      Integer sl = context.sysLimit.subsearchLimit();
      if (sl != null) subsearchLimit = sl;
      Integer jsl = context.sysLimit.joinSubsearchLimit();
      if (jsl != null) joinSubsearchLimit = jsl;
    }
    org.apache.calcite.sql.SqlNode sqlNode =
        new org.opensearch.sql.calcite.sqlnode.PPLToSqlNodeVisitor(
                planner.tableFields(), planner.tableRowType())
            .translate(plan);
    RelNode rel = planner.plan(sqlNode);
    // PPL setting plugins.ppl.join.subsearch_maxout caps the right side of every JOIN to N rows
    // total. The SqlNode visitor doesn't model this directly because wrapping a bare relation
    // would hide the table identifier from the JOIN's outer scope, breaking ON clauses written
    // as `<table>.col`. Apply post-RelNode by injecting a LogicalSystemLimit on top of each
    // LogicalJoin's right input.
    if (joinSubsearchLimit > 0) {
      rel = applyJoinSubsearchMaxOut(rel, joinSubsearchLimit);
    }
    // OpenSearch tables expose metadata fields (`_id`, `_index`, `_score`, ...) in their row
    // type, but PPL hides them from user-facing output. The PPL→SqlNode visitor strips them at
    // every explicit projection; queries with no outer projection (e.g. bare `source=X`) reach
    // here with metadata still present. Add a top-level Project that drops them.
    rel = stripMetadataFields(rel);
    // Highlight is pushed into the storage scan, not modeled in the SqlNode tree (it comes
    // from the request body's `highlight` parameter, parallel to the PPL pipe). Rewrite each
    // LogicalTableScan to its pushDownHighlight form and add an outer Project that surfaces
    // the `_highlight` column to the caller.
    if (context.getHighlightConfig() != null) {
      rel = injectHighlight(rel, context);
      context.setHighlightConfig(null);
    }
    return rel;
  }

  /**
   * Drop OpenSearch metadata fields ({@code _id}, {@code _index}, {@code _score}, ...) from the
   * top-level row type. PPL hides these from user-facing output; only explicit references (e.g.
   * {@code | fields _id, name}) keep them.
   */
  private static RelNode stripMetadataFields(RelNode rel) {
    java.util.List<org.apache.calcite.rel.type.RelDataTypeField> fields =
        rel.getRowType().getFieldList();
    boolean anyMeta =
        fields.stream()
            .anyMatch(
                f ->
                    org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                        .containsKey(f.getName()));
    if (!anyMeta) {
      return rel;
    }
    org.apache.calcite.rex.RexBuilder rb = rel.getCluster().getRexBuilder();
    java.util.List<org.apache.calcite.rex.RexNode> projects = new java.util.ArrayList<>();
    java.util.List<String> names = new java.util.ArrayList<>();
    for (org.apache.calcite.rel.type.RelDataTypeField f : fields) {
      if (org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(
          f.getName())) {
        continue;
      }
      projects.add(rb.makeInputRef(rel, f.getIndex()));
      names.add(f.getName());
    }
    if (projects.isEmpty()) {
      return rel;
    }
    return org.apache.calcite.rel.logical.LogicalProject.create(
        rel, java.util.Collections.emptyList(), projects, names, java.util.Set.of());
  }

  private static RelNode injectHighlight(
      RelNode rel, org.opensearch.sql.calcite.CalcitePlanContext context) {
    org.opensearch.sql.ast.tree.HighlightConfig hl = context.getHighlightConfig();
    org.apache.calcite.rel.RelShuttle shuttle =
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(org.apache.calcite.rel.core.TableScan scan) {
            if (scan instanceof org.opensearch.sql.calcite.plan.HighlightPushDown hp) {
              return hp.pushDownHighlight(hl);
            }
            return super.visit(scan);
          }
        };
    RelNode rewritten = rel.accept(shuttle);
    // If the rewritten relation's row-type now includes _highlight, surface it through an
    // outer Project. The SqlNode→RelNode pipeline produced an explicit projection from the
    // pipe's terminal `fields` (or implicit `fields *`); the column count there is fixed and
    // doesn't pick up the appended _highlight. Add it explicitly only when the RelNode's
    // current row type doesn't already include it (which it won't, because the project list
    // was frozen at SqlNode time).
    if (!rewritten
        .getRowType()
        .getFieldNames()
        .contains(org.opensearch.sql.expression.HighlightExpression.HIGHLIGHT_FIELD)) {
      // Walk down to find a node whose input has _highlight and re-project.
      rewritten = surfaceHighlight(rewritten);
    }
    return rewritten;
  }

  /**
   * Walk down to the deepest input that carries _highlight and re-build the project chain on top of
   * it preserving the original column order, with _highlight appended.
   */
  private static RelNode surfaceHighlight(RelNode rel) {
    String hlField = org.opensearch.sql.expression.HighlightExpression.HIGHLIGHT_FIELD;
    if (rel.getInputs().isEmpty()) {
      return rel;
    }
    java.util.List<RelNode> newInputs = new java.util.ArrayList<>();
    for (RelNode input : rel.getInputs()) {
      newInputs.add(surfaceHighlight(input));
    }
    RelNode replaced = rel.copy(rel.getTraitSet(), newInputs);
    if (rel instanceof org.apache.calcite.rel.core.Project proj) {
      RelNode input = newInputs.get(0);
      int hlIdx = input.getRowType().getFieldNames().indexOf(hlField);
      if (hlIdx >= 0 && !replaced.getRowType().getFieldNames().contains(hlField)) {
        java.util.List<org.apache.calcite.rex.RexNode> projects =
            new java.util.ArrayList<>(proj.getProjects());
        java.util.List<String> names = new java.util.ArrayList<>(proj.getRowType().getFieldNames());
        org.apache.calcite.rex.RexBuilder rb = proj.getCluster().getRexBuilder();
        projects.add(rb.makeInputRef(input, hlIdx));
        names.add(hlField);
        return org.apache.calcite.rel.logical.LogicalProject.create(
            input, java.util.Collections.emptyList(), projects, names, proj.getVariablesSet());
      }
    }
    return replaced;
  }

  /** Analyze {@link UnresolvedPlan}. */
  public LogicalPlan analyze(UnresolvedPlan plan, QueryType queryType) {
    return analyzer.analyze(plan, new AnalysisContext(queryType));
  }

  /**
   * Cap the right side of every {@link org.apache.calcite.rel.logical.LogicalJoin} with a {@link
   * LogicalSystemLimit} of type {@code JOIN_SUBSEARCH_MAXOUT}. PPL setting {@code
   * plugins.ppl.join.subsearch_maxout} requires the cluster-wide cap on every join's subsearch
   * input. Done post-RelNode rather than at SqlNode time because wrapping a bare relation as {@code
   * (SELECT * FROM X) FETCH N} hides {@code X} from the JOIN's outer scope and breaks ON clauses
   * written as {@code X.col}.
   */
  private static RelNode applyJoinSubsearchMaxOut(RelNode rel, int limit) {
    org.apache.calcite.rex.RexLiteral capLit =
        rel.getCluster().getRexBuilder().makeExactLiteral(java.math.BigDecimal.valueOf(limit));
    org.apache.calcite.rel.RelShuttle shuttle =
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            if (visited instanceof org.apache.calcite.rel.logical.LogicalJoin join) {
              RelNode right = join.getRight();
              // Don't double-cap if the right side is already a JOIN_SUBSEARCH_MAXOUT.
              if (right instanceof LogicalSystemLimit lsl
                  && lsl.getType() == SystemLimitType.JOIN_SUBSEARCH_MAXOUT) {
                return visited;
              }
              RelNode capped =
                  LogicalSystemLimit.create(SystemLimitType.JOIN_SUBSEARCH_MAXOUT, right, capLit);
              return join.copy(
                  join.getTraitSet(),
                  join.getCondition(),
                  join.getLeft(),
                  capped,
                  join.getJoinType(),
                  join.isSemiJoinDone());
            }
            return visited;
          }
        };
    return rel.accept(shuttle);
  }

  /** Translate {@link LogicalPlan} to {@link PhysicalPlan}. */
  public PhysicalPlan plan(LogicalPlan plan) {
    return planner.plan(plan);
  }

  private boolean isCalciteUnsupportedError(@Nullable Throwable t) {
    return switch (t) {
      case null -> false;
      case CalciteUnsupportedException calciteUnsupportedException -> true;
      case ErrorReport errorReport when t.getCause() instanceof CalciteUnsupportedException -> true;
      default -> false;
    };
  }

  private boolean isCalciteFallbackAllowed(@Nullable Throwable t) {
    // We always allow fallback the query failed with CalciteUnsupportedException.
    // This is for avoiding breaking changes when enable Calcite by default.
    if (isCalciteUnsupportedError(t)) {
      return true;
    }

    if (settings != null) {
      Boolean fallback_allowed = settings.getSettingValue(Settings.Key.CALCITE_FALLBACK_ALLOWED);
      if (fallback_allowed == null) {
        return false;
      }
      return fallback_allowed;
    }

    return true;
  }

  private boolean isCalciteEnabled(Settings settings) {
    if (settings != null) {
      return settings.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED);
    } else {
      return false;
    }
  }

  // TODO https://github.com/opensearch-project/sql/issues/3457
  // Calcite is not available for SQL query now. Maybe release in 3.1.0?
  private boolean shouldUseCalcite(QueryType queryType) {
    return isCalciteEnabled(settings) && queryType == QueryType.PPL;
  }

  private FrameworkConfig buildFrameworkConfig() {
    // Use simple calcite schema since we don't compute tables in advance of the query.
    final SchemaPlus rootSchema = CalciteSchema.createRootSchema(true, false).plus();
    final SchemaPlus opensearchSchema =
        rootSchema.add(
            OpenSearchSchema.OPEN_SEARCH_SCHEMA_NAME, new OpenSearchSchema(dataSourceService));
    Frameworks.ConfigBuilder configBuilder =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT) // TODO check
            .defaultSchema(opensearchSchema)
            .traitDefs((List<RelTraitDef>) null)
            .programs(Programs.standard())
            .typeSystem(OpenSearchTypeSystem.INSTANCE);
    return configBuilder.build();
  }

  /**
   * Convert OpenSearch Plan to Calcite Plan. Although both plans consist of Calcite RelNodes, there
   * are some differences in the topological structures or semantics between them.
   *
   * @param osPlan Logical Plan derived from OpenSearch PPL
   * @param context Calcite context
   */
  private static RelNode convertToCalcitePlan(RelNode osPlan, CalcitePlanContext context) {
    // Explicitly add a limit operator to enforce query size limit
    RelNode calcitePlan =
        LogicalSystemLimit.create(
            SystemLimitType.QUERY_SIZE_LIMIT,
            osPlan,
            context.relBuilder.literal(context.sysLimit.querySizeLimit()));
    /* Calcite only ensures collation of the final result produced from the root sort operator.
     * While we expect that the collation can be preserved through the pipes over PPL, we need to
     * explicitly add a sort operator on top of the original plan
     * to ensure the correct collation of the final result.
     * See logic in ${@link CalcitePrepareImpl}
     * For the redundant sort, we rely on Calcite optimizer to eliminate
     */
    RelCollation collation = calcitePlan.getTraitSet().getCollation();
    if (!(calcitePlan instanceof Sort) && collation != RelCollations.EMPTY) {
      calcitePlan = LogicalSort.create(calcitePlan, collation, null, null);
    }
    return calcitePlan;
  }
}
