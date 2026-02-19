/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.merge;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.plan.Scannable;

/**
 * Abstract base class for Exchange operators that represent the boundary between shard-level
 * execution and coordinator-level merge in distributed query execution.
 *
 * <p>The child of an Exchange is the "shard fragment" — the portion of the query plan that executes
 * on each shard independently. The Exchange itself runs on the coordinator and merges results from
 * all shards according to a strategy defined by each concrete subclass.
 */
public abstract class Exchange extends SingleRel implements EnumerableRel, Scannable {

    /** Container for shard execution results, decoupled from transport layer. */
    public static class ShardResult {
        private final List<Object[]> rows;
        private final List<String> columnNames;
        private final int shardId;
        private final Map<String, byte[]> binaryFields;

        public ShardResult(List<Object[]> rows, List<String> columnNames, int shardId) {
            this(rows, columnNames, shardId, Collections.emptyMap());
        }

        public ShardResult(
                List<Object[]> rows,
                List<String> columnNames,
                int shardId,
                Map<String, byte[]> binaryFields) {
            this.rows = rows;
            this.columnNames = columnNames;
            this.shardId = shardId;
            this.binaryFields = binaryFields != null ? binaryFields : Collections.emptyMap();
        }

        public List<Object[]> getRows() {
            return rows;
        }

        public List<String> getColumnNames() {
            return columnNames;
        }

        public int getShardId() {
            return shardId;
        }

        public Map<String, byte[]> getBinaryFields() {
            return binaryFields;
        }
    }

    /** Shard results injected by DistributedExecutor after collecting from all shards. */
    protected List<ShardResult> shardResults;

    protected Exchange(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
    }

    /**
     * Sets the shard results collected by the coordinator. Called by DistributedExecutor after all
     * shard responses are received.
     */
    public void setShardResults(List<ShardResult> results) {
        this.shardResults = results;
    }

    /**
     * Returns a human-readable name for this exchange type, used in EXPLAIN output.
     *
     * @return the exchange type name (e.g., "CONCAT", "MERGE_SORT", "MERGE_AGGREGATE")
     */
    public abstract String getExchangeType();

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("exchangeType", getExchangeType());
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(), getRowType(), pref.preferArray());
        Expression scanOperator = implementor.stash(this, Exchange.class);
        return implementor.result(
                physType, Blocks.toBlock(Expressions.call(scanOperator, "scan")));
    }

    @Override
    public abstract Enumerable<@Nullable Object> scan();

    /**
     * Creates a new Exchange of the same type with the given traitSet and inputs. Subclasses must
     * override to preserve their additional fields.
     */
    @Override
    public abstract RelNode copy(RelTraitSet traitSet, List<RelNode> inputs);

    /**
     * Factory helper to build a trait set with {@link EnumerableConvention}.
     *
     * @param cluster the cluster
     * @return a trait set with ENUMERABLE convention
     */
    protected static RelTraitSet enumerableTraitSet(RelOptCluster cluster) {
        return cluster.traitSetOf(EnumerableConvention.INSTANCE);
    }
}
