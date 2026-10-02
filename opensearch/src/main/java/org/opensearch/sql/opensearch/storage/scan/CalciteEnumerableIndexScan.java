/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.plan.Scannable;
import org.opensearch.sql.calcite.plan.rule.OpenSearchRules;
import org.opensearch.sql.opensearch.executor.progress.ProgressiveQueryContext;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownContext;
import org.opensearch.sql.opensearch.util.OpenSearchRelOptUtil;

/** The physical relational operator representing a scan of an OpenSearchIndex type. */
public class CalciteEnumerableIndexScan extends AbstractCalciteIndexScan
    implements Scannable, EnumerableRel {
  private static final Logger LOG = LogManager.getLogger(CalciteEnumerableIndexScan.class);

  /**
   * Creates an CalciteOpenSearchIndexScan.
   *
   * @param cluster Cluster
   * @param table Table
   * @param osIndex OpenSearch index
   */
  public CalciteEnumerableIndexScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchIndex osIndex,
      RelDataType schema,
      PushDownContext pushDownContext) {
    super(cluster, traitSet, hints, table, osIndex, schema, pushDownContext);
  }

  @Override
  protected AbstractCalciteIndexScan buildScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchIndex osIndex,
      RelDataType schema,
      PushDownContext pushDownContext) {
    return new CalciteEnumerableIndexScan(
        cluster, traitSet, hints, table, osIndex, schema, pushDownContext);
  }

  @Override
  public AbstractCalciteIndexScan copy() {
    return new CalciteEnumerableIndexScan(
        getCluster(), traitSet, hints, table, osIndex, schema, pushDownContext.clone());
  }

  @Override
  public void register(RelOptPlanner planner) {
    for (RelOptRule rule : OpenSearchRules.OPEN_SEARCH_OPT_RULES) {
      planner.addRule(rule);
    }

    // remove this rule otherwise opensearch can't correctly interpret approx_count_distinct()
    // it is converted to cardinality aggregation in OpenSearch
    planner.removeRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES);
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    /* In Calcite enumerable operators, row of single column will be optimized to a scalar value.
     * See {@link PhysTypeImpl}.
     * Since we need to combine this operator with their original ones,
     * let's follow this convention to apply the optimization here and ensure `scan` method
     * returns the correct data format for single column rows.
     * See {@link OpenSearchIndexEnumerator}
     * Besides, we replace all dots in fields to avoid the Calcite codegen bug.
     * https://github.com/opensearch-project/sql/issues/4619
     */
    PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            OpenSearchRelOptUtil.replaceDot(getCluster().getTypeFactory(), getRowType()),
            pref.preferArray());

    Expression scanOperator = implementor.stash(this, CalciteEnumerableIndexScan.class);
    // Claim this position's progress source now, while the position is being code-generated, and
    // bake its id
    // into this call site. Resolving it at scan() time instead would merge the two positions of a
    // self-join:
    // Calcite canonicalizes equal plan nodes, so both of them are this same object, and only the
    // code generator
    // ever sees them as distinct.
    long progressSourceId = ProgressiveQueryContext.claimPosition(this);
    // An unobserved query generates exactly the code it generated before progress existed, down to
    // the method
    // signature. Extended explain publishes this generated source, so emitting the overload
    // unconditionally would
    // change synchronous explain output for a feature synchronous queries do not even have.
    Expression scanCall =
        progressSourceId == NO_PROGRESS_SOURCE
            ? Expressions.call(scanOperator, "scan")
            : Expressions.call(
                scanOperator, "scan", Expressions.constant(progressSourceId, long.class));
    return implementor.result(physType, Blocks.toBlock(scanCall));
  }

  /**
   * Scans without progress reporting; used by the direct-scan path that bypasses code generation.
   */
  @Override
  public Enumerable<@Nullable Object> scan() {
    return scan(NO_PROGRESS_SOURCE);
  }

  /**
   * This Enumerator may be iterated for multiple times, so we need to create opensearch request for
   * each time to avoid reusing source builder. That's because the source builder has stats like PIT
   * or SearchAfter recorded during previous search.
   */
  @Override
  public Enumerable<@Nullable Object> scan(long progressSourceId) {
    // One binding per physical position, resolved once here and captured by the returned
    // enumerable, so every
    // enumerator this position creates — a nested-loop join re-drives its inner side per outer row
    // — reports into
    // the same source.
    ProgressiveQueryContext.Binding progressBinding =
        ProgressiveQueryContext.bindingFor(progressSourceId);
    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<Object> enumerator() {
        OpenSearchRequestBuilder requestBuilder = pushDownContext.createRequestBuilder();
        return new OpenSearchIndexEnumerator(
            osIndex.getClient(),
            getRowType().getFieldNames(),
            requestBuilder.getMaxResponseSize(),
            requestBuilder.getMaxResultWindow(),
            osIndex.getQueryBucketSize(),
            osIndex.buildRequest(requestBuilder),
            osIndex.createOpenSearchResourceMonitor(),
            progressBinding);
      }
    };
  }
}
