/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.calcite.plan.Scannable;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.opensearch.storage.serde.RelNodeSerializer;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Runtime for executing Calcite plan fragments on a data node shard.
 *
 * <p>Execution flow:
 *
 * <ol>
 *   <li>Receive serialized plan fragment + shard context
 *   <li>Create RelOptCluster with OpenSearchTypeFactory
 *   <li>Deserialize plan fragment → RelNode tree (via RelNodeSerializer)
 *   <li>Find scan placeholders, replace with CalciteLocalShardScan bound to local shard
 *   <li>Execute Calcite Enumerable pipeline
 *   <li>Collect result rows as Object[]
 *   <li>Return results
 * </ol>
 *
 * <p>Phase 2 adds support for shuffle fragments: plan fragments that receive pre-shuffled data
 * from other shards (via HashExchange) and execute local operators like hash joins and window
 * functions.
 */
public class ShardCalciteRuntime {
    private static final Logger LOG = LogManager.getLogger(ShardCalciteRuntime.class);

    private final NodeClient client;
    private final RelNodeSerializer serializer;

    @Getter private List<String> lastColumnNames = List.of();

    public ShardCalciteRuntime(NodeClient client) {
        this.client = client;
        this.serializer = new RelNodeSerializer();
    }

    /**
     * Executes a serialized Calcite plan fragment on the specified shard.
     *
     * <p>Deserializes the plan, replaces scan placeholders with actual shard-local scans, and
     * executes the Calcite Enumerable pipeline.
     *
     * @param serializedPlan the JSON-serialized RelNode plan fragment
     * @param indexName the index to scan
     * @param shardId the target shard ordinal
     * @return list of result rows as Object arrays
     */
    public List<Object[]> execute(String serializedPlan, String indexName, int shardId) {
        RelOptCluster cluster = createCluster();

        try {
            // Deserialize the plan fragment
            RelNode plan = serializer.deserialize(serializedPlan, cluster);

            // Replace scan placeholders with actual shard-local scans
            RelNode boundPlan = bindScanPlaceholders(plan, indexName, shardId, cluster);

            // Execute the plan
            return executeEnumerable(boundPlan);
        } catch (IOException e) {
            LOG.error("Failed to deserialize plan fragment for shard {}", shardId, e);
            throw new RuntimeException("Failed to deserialize plan fragment", e);
        }
    }

    /**
     * Executes a plan fragment that receives pre-shuffled data during a shuffle phase.
     *
     * <p>In Phase 2 shuffle execution, data has already been partitioned by hash key. This method
     * receives the pre-materialized rows and executes the remaining plan fragment (e.g., local
     * hash join, local window function) on them.
     *
     * @param serializedPlan the JSON-serialized plan fragment to execute on shuffled data
     * @param shuffledRows the pre-shuffled rows for this partition
     * @param columnNames the column names for the shuffled data
     * @param partitionId the partition this data belongs to
     * @return list of result rows as Object arrays
     */
    public List<Object[]> executeShuffleFragment(
            String serializedPlan,
            List<Object[]> shuffledRows,
            List<String> columnNames,
            int partitionId) {
        RelOptCluster cluster = createCluster();

        try {
            // Deserialize the plan fragment
            RelNode plan = serializer.deserialize(serializedPlan, cluster);

            // TODO: Replace scan placeholders with in-memory data sources backed by shuffledRows
            // For now, return the shuffled rows directly (pass-through)
            LOG.debug(
                    "Executing shuffle fragment for partition {} with {} rows",
                    partitionId,
                    shuffledRows.size());

            lastColumnNames = columnNames;
            return shuffledRows;
        } catch (IOException e) {
            LOG.error(
                    "Failed to deserialize shuffle plan fragment for partition {}",
                    partitionId,
                    e);
            throw new RuntimeException("Failed to deserialize shuffle plan fragment", e);
        }
    }

    /**
     * Creates a RelOptCluster for deserialization and execution.
     */
    private RelOptCluster createCluster() {
        OpenSearchTypeFactory typeFactory = OpenSearchTypeFactory.TYPE_FACTORY;
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }

    /**
     * Replaces scan placeholders in the deserialized plan with actual CalciteLocalShardScan nodes
     * bound to the local shard.
     */
    private RelNode bindScanPlaceholders(
            RelNode plan, String indexName, int shardId, RelOptCluster cluster) {
        return plan.accept(
                new RelShuttleImpl() {
                    @Override
                    public RelNode visit(RelNode other) {
                        RelNode visited = super.visit(other);
                        if (visited instanceof RelNodeSerializer.ShardScanPlaceholder placeholder) {
                            RelTraitSet traitSet =
                                    cluster.traitSetOf(EnumerableConvention.INSTANCE);
                            return new CalciteLocalShardScan(
                                    cluster,
                                    traitSet,
                                    placeholder.getRowType(),
                                    shardId,
                                    indexName,
                                    null, // Lucene query will be extracted from filter above
                                    client);
                        }
                        return visited;
                    }
                });
    }

    /**
     * Executes an EnumerableRel node and collects the results.
     */
    private List<Object[]> executeEnumerable(RelNode node) {
        if (!(node instanceof Scannable)) {
            throw new UnsupportedOperationException(
                    "Node does not implement Scannable: " + node.getClass().getName());
        }

        Scannable scannable = (Scannable) node;
        Enumerable<?> enumerable = scannable.scan();
        RelDataType rowType = node.getRowType();
        lastColumnNames = rowType.getFieldNames();
        int colCount = rowType.getFieldCount();

        List<Object[]> rows = new ArrayList<>();
        try (Enumerator<?> enumerator = enumerable.enumerator()) {
            while (enumerator.moveNext()) {
                Object current = enumerator.current();
                Object[] row;
                if (colCount == 1) {
                    // Single column: Calcite optimizes to scalar
                    row = new Object[] {current};
                } else if (current instanceof Object[]) {
                    row = (Object[]) current;
                } else {
                    row = new Object[] {current};
                }
                rows.add(row);
            }
        }
        return rows;
    }
}
