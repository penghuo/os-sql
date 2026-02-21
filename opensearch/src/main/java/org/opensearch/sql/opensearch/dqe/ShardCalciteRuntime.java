/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.calcite.plan.Scannable;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.opensearch.dqe.serde.RelNodeSerializer;

/**
 * Executes a serialized Calcite plan fragment on a local shard. This is the shard-side runtime
 * component of the Distributed Query Execution (DQE) engine.
 *
 * <p>Steps:
 *
 * <ol>
 *   <li>Create a RelOptCluster with OpenSearchTypeFactory
 *   <li>Deserialize the plan JSON via RelNodeSerializer
 *   <li>Locate the Scannable root in the deserialized plan
 *   <li>Execute the Calcite Enumerable pipeline via Scannable.scan()
 *   <li>Collect typed Object[] rows
 *   <li>Return as ShardCalciteRuntime.Result
 * </ol>
 *
 * <p>All exceptions are caught and returned in the result — this class never throws.
 */
public class ShardCalciteRuntime {

    private static final Logger LOG = LogManager.getLogger(ShardCalciteRuntime.class);

    /**
     * Result of shard-level plan execution, containing typed rows or an error. This is a
     * module-local alternative to CalciteShardResponse (which lives in the plugin module).
     */
    @Getter
    public static class Result {
        private final List<Object[]> rows;
        private final List<String> columnNames;
        private final List<SqlTypeName> columnTypes;
        private final Exception error;

        /** Construct a successful result. */
        public Result(
                List<Object[]> rows, List<String> columnNames, List<SqlTypeName> columnTypes) {
            this.rows = rows;
            this.columnNames = columnNames;
            this.columnTypes = columnTypes;
            this.error = null;
        }

        /** Construct an error result. */
        public Result(Exception error) {
            this.rows = List.of();
            this.columnNames = List.of();
            this.columnTypes = List.of();
            this.error = error;
        }

        public boolean hasError() {
            return error != null;
        }
    }

    /**
     * Execute a serialized plan fragment on a local shard.
     *
     * @param planJson serialized RelNode plan JSON from RelNodeSerializer
     * @param indexName the OpenSearch index name (for binding context)
     * @param shardId the shard ID on this node (for binding context)
     * @return Result containing typed rows or an error
     */
    public Result execute(String planJson, String indexName, int shardId) {
        try {
            // Step 1: Create RelOptCluster with OpenSearchTypeFactory
            RelOptCluster cluster = createCluster();

            // Step 2: Deserialize the plan JSON
            RelNode plan = RelNodeSerializer.deserialize(planJson, cluster, null);

            // Step 3: Verify the deserialized plan root implements Scannable
            if (!(plan instanceof Scannable)) {
                return new Result(
                        new IllegalStateException(
                                "Deserialized plan root is not Scannable: "
                                        + plan.getClass().getName()
                                        + ". The shard plan fragment must be rooted at a DSLScan "
                                        + "or other Scannable operator."));
            }

            // Step 4: Execute the Scannable pipeline
            Scannable scannable = (Scannable) plan;
            Enumerable<?> enumerable = scannable.scan();

            // Step 5: Extract column metadata from the plan's row type
            RelDataType rowType = plan.getRowType();
            List<String> columnNames = new ArrayList<>();
            List<SqlTypeName> columnTypes = new ArrayList<>();
            for (RelDataTypeField field : rowType.getFieldList()) {
                columnNames.add(field.getName());
                columnTypes.add(field.getType().getSqlTypeName());
            }

            // Step 6: Collect rows
            List<Object[]> rows = collectRows(enumerable, rowType.getFieldCount());

            LOG.debug(
                    "Shard execution completed: index={}, shardId={}, rows={}",
                    indexName,
                    shardId,
                    rows.size());

            return new Result(rows, columnNames, columnTypes);

        } catch (Exception e) {
            LOG.error(
                    "Shard execution failed: index={}, shardId={}", indexName, shardId, e);
            return new Result(e);
        }
    }

    /**
     * Creates a RelOptCluster using the OpenSearchTypeFactory. This provides the type system needed
     * for plan deserialization and execution.
     */
    static RelOptCluster createCluster() {
        RexBuilder rexBuilder = new RexBuilder(OpenSearchTypeFactory.TYPE_FACTORY);
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, rexBuilder);
    }

    /**
     * Collect rows from an Enumerable result. For single-column results, Calcite optimizes the
     * representation to scalar values (not Object[]), so we wrap them.
     */
    private List<Object[]> collectRows(Enumerable<?> enumerable, int columnCount) {
        List<Object[]> rows = new ArrayList<>();
        try (Enumerator<?> enumerator = enumerable.enumerator()) {
            while (enumerator.moveNext()) {
                Object current = enumerator.current();
                if (current == null) {
                    rows.add(new Object[columnCount]);
                } else if (columnCount == 1) {
                    // Single-column rows are optimized to scalar values by Calcite
                    rows.add(new Object[] {current});
                } else if (current instanceof Object[]) {
                    rows.add((Object[]) current);
                } else {
                    // Unexpected type — wrap in single-element array
                    rows.add(new Object[] {current});
                }
            }
        }
        return rows;
    }
}
