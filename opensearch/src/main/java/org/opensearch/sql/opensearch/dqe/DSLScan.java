/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe;

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
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexEnumerator;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownContext;
import org.opensearch.sql.opensearch.util.OpenSearchRelOptUtil;

/**
 * A shard-local physical scan operator that builds a DSL query from retained pushdown rules and
 * executes it against OpenSearch. This is the DQE (Distributed Query Execution) replacement for
 * {@link org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan}.
 */
public class DSLScan extends AbstractCalciteIndexScan implements Scannable, EnumerableRel {
    private static final Logger LOG = LogManager.getLogger(DSLScan.class);

    /**
     * Creates a DSLScan.
     *
     * @param cluster Cluster
     * @param traits Trait set (must include EnumerableConvention)
     * @param hints Rel hints
     * @param table Table
     * @param index OpenSearch index
     * @param schema Row type schema
     * @param pushDownContext Retained pushdown state (filter, project, limit, sort)
     */
    public DSLScan(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints,
            RelOptTable table,
            OpenSearchIndex index,
            RelDataType schema,
            PushDownContext pushDownContext) {
        super(cluster, traits, hints, table, index, schema, pushDownContext);
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
        return new DSLScan(cluster, traitSet, hints, table, osIndex, schema, pushDownContext);
    }

    @Override
    public AbstractCalciteIndexScan copy() {
        return new DSLScan(
                getCluster(), traitSet, hints, table, osIndex, schema, pushDownContext.clone());
    }

    /**
     * Register optimization rules when this node is added to the planner. Matches the behavior of
     * CalciteEnumerableIndexScan: add aggregate optimization rules and remove the distinct
     * aggregate expansion rule (which interferes with approx_count_distinct).
     */
    @Override
    public void register(RelOptPlanner planner) {
        for (RelOptRule rule : OpenSearchRules.OPEN_SEARCH_OPT_RULES) {
            planner.addRule(rule);
        }
        planner.removeRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        /*
         * Follow the same pattern as CalciteEnumerableIndexScan.implement():
         * - Single column rows are optimized to scalar values by PhysTypeImpl
         * - Replace dots in field names to avoid Calcite codegen bug
         *   https://github.com/opensearch-project/sql/issues/4619
         */
        PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        OpenSearchRelOptUtil.replaceDot(
                                getCluster().getTypeFactory(), getRowType()),
                        pref.preferArray());

        Expression scanOperator = implementor.stash(this, DSLScan.class);
        return implementor.result(
                physType, Blocks.toBlock(Expressions.call(scanOperator, "scan")));
    }

    /**
     * Executes the search against OpenSearch. Creates a fresh request builder from PushDownContext
     * each time to avoid reusing source builder state (PIT, SearchAfter) from previous iterations.
     */
    @Override
    public Enumerable<@Nullable Object> scan() {
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
                        osIndex.createOpenSearchResourceMonitor());
            }
        };
    }
}
