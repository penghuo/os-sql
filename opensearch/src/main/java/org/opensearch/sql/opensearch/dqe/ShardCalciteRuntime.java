/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.calcite.plan.Scannable;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.opensearch.dqe.serde.RelNodeSerializer;
import org.opensearch.sql.opensearch.dqe.serde.ShardDeserializationContext;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexEnumerator;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownContext;

/**
 * Executes a serialized Calcite plan fragment on a local shard. This is the shard-side runtime
 * component of the Distributed Query Execution (DQE) engine.
 *
 * <p>Two execution paths:
 *
 * <ul>
 *   <li><b>Fast path</b>: If the deserialized plan root implements {@link Scannable} (e.g.,
 *       DSLScan, CalciteLogicalIndexScan), execute directly via {@code scan()}.
 *   <li><b>Interpreter path</b>: If the plan root is a non-Scannable operator (e.g., LogicalSort,
 *       LogicalAggregate, LogicalProject, LogicalFilter, LogicalSystemLimit), use Calcite's
 *       {@link Interpreter} to walk the plan tree. The Interpreter handles standard Calcite
 *       built-in functions but cannot execute {@code SqlUserDefinedFunction} operators (which
 *       require code generation).
 * </ul>
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
     * @param osIndex the OpenSearch index for shard-side scan construction
     * @return Result containing typed rows or an error
     */
    public Result execute(String planJson, String indexName, int shardId,
            OpenSearchIndex osIndex) {
        try {
            // Step 1: Create RelOptCluster with OpenSearchTypeFactory
            RelOptCluster cluster = createCluster();

            // Step 2: Set up deserialization context with OpenSearchIndex
            // so DSLScan(RelInput) can retrieve it during construction.
            ShardDeserializationContext.set(new ShardDeserializationContext(osIndex));
            RelNode plan;
            try {
                // Create a RelOptSchema that can resolve the table name during deserialization.
                RelOptSchema relOptSchema = createShardSchema(osIndex, cluster);
                plan = RelNodeSerializer.deserialize(planJson, cluster, relOptSchema);
            } finally {
                ShardDeserializationContext.clear();
            }

            // Step 3: Execute the plan
            Enumerable<?> enumerable;
            if (plan instanceof Scannable) {
                // Fast path: plan root is Scannable (DSLScan, CalciteLogicalIndexScan)
                enumerable = ((Scannable) plan).scan();
            } else {
                // Interpreter path: plan root is a logical operator
                // (LogicalSort, LogicalAggregate, LogicalProject, LogicalFilter, etc.)
                enumerable = executeWithInterpreter(plan);
            }

            // Step 4: Extract column metadata from the plan's row type
            RelDataType rowType = plan.getRowType();
            List<String> columnNames = new ArrayList<>();
            List<SqlTypeName> columnTypes = new ArrayList<>();
            for (RelDataTypeField field : rowType.getFieldList()) {
                columnNames.add(field.getName());
                columnTypes.add(field.getType().getSqlTypeName());
            }

            // Step 5: Collect rows
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
     * Creates a minimal {@link RelOptSchema} for shard-side deserialization. The schema resolves
     * any table name to a {@link RelOptTable} backed by a {@link ShardScannableTable} wrapper
     * around the given {@link OpenSearchIndex}. This allows Calcite's {@code RelJsonReader} to
     * resolve table references when constructing {@code DSLScan} nodes from JSON, and ensures
     * that Calcite's code generation can unwrap the table as a {@link ScannableTable} during
     * execution.
     */
    private static RelOptSchema createShardSchema(OpenSearchIndex osIndex,
            RelOptCluster cluster) {
        return new RelOptSchema() {
            @Override
            public RelOptTable getTableForMember(java.util.List<String> qualifiedName) {
                RelDataType rowType = osIndex.getRowType(cluster.getTypeFactory());
                ScannableTable scannableWrapper = new ShardScannableTable(osIndex, rowType);
                return RelOptTableImpl.create(
                        this, rowType, scannableWrapper,
                        com.google.common.collect.ImmutableList.copyOf(qualifiedName));
            }

            @Override
            public org.apache.calcite.rel.type.RelDataTypeFactory getTypeFactory() {
                return cluster.getTypeFactory();
            }

            @Override
            public void registerRules(org.apache.calcite.plan.RelOptPlanner planner) {
                // No rules to register for shard-side deserialization
            }
        };
    }

    /**
     * Executes a non-Scannable plan tree using Calcite's {@link Interpreter}. The Interpreter
     * walks the plan tree and evaluates operators one row at a time. It handles standard Calcite
     * built-in functions but cannot execute {@code SqlUserDefinedFunction} operators (which
     * require code generation via the EnumerableRel path).
     *
     * @param plan the deserialized plan tree with a non-Scannable root
     * @return an Enumerable of rows
     */
    private Enumerable<Object[]> executeWithInterpreter(RelNode plan) {
        DataContext dataContext = createInterpreterDataContext(Frameworks.createRootSchema(false));
        return new Interpreter(dataContext, plan);
    }

    /**
     * Creates a minimal {@link DataContext} for the {@link Interpreter}. The DataContext provides
     * the root schema and type factory needed by the Interpreter to resolve types and schemas
     * during plan execution.
     */
    private static DataContext createInterpreterDataContext(SchemaPlus rootSchema) {
        return new DataContext() {
            @Override
            public SchemaPlus getRootSchema() {
                return rootSchema;
            }

            @Override
            public org.apache.calcite.adapter.java.JavaTypeFactory getTypeFactory() {
                return OpenSearchTypeFactory.TYPE_FACTORY;
            }

            @Override
            public QueryProvider getQueryProvider() {
                return null;
            }

            @Override
            public Object get(String name) {
                return null;
            }
        };
    }

    /**
     * Collect rows from an Enumerable result produced by the fast path ({@link Scannable#scan()}).
     * Returns {@code Enumerable<Object>} where single-column rows are scalar values and
     * multi-column rows are {@code Object[]}.
     *
     * <p>The {@code instanceof Object[]} check must come BEFORE the scalar wrap to avoid
     * double-wrapping multi-column rows.
     */
    private List<Object[]> collectRows(Enumerable<?> enumerable, int columnCount) {
        List<Object[]> rows = new ArrayList<>();
        try (Enumerator<?> enumerator = enumerable.enumerator()) {
            while (enumerator.moveNext()) {
                Object current = enumerator.current();
                if (current == null) {
                    rows.add(new Object[columnCount]);
                } else if (current instanceof Object[]) {
                    // Interpreter always returns Object[] (even for single-column).
                    // Fast path returns Object[] for multi-column rows.
                    rows.add((Object[]) current);
                } else {
                    // Fast path single-column scalar, or unexpected type — wrap in array
                    rows.add(new Object[] {current});
                }
            }
        }
        return rows;
    }

    /**
     * A {@link ScannableTable} implementation that wraps an {@link OpenSearchIndex} for shard-side
     * execution. This is used during plan deserialization so that the {@link RelOptTableImpl}
     * created by {@link #createShardSchema} is backed by a {@link ScannableTable}. Calcite
     * calls {@code relOptTable.unwrap(ScannableTable.class)} to obtain the table, so this
     * wrapper ensures that unwrap succeeds.
     *
     * <p>The scan implementation creates a fresh {@link PushDownContext} and
     * {@link OpenSearchRequestBuilder} from the {@link OpenSearchIndex}, then returns rows via
     * {@link OpenSearchIndexEnumerator}. Single-column rows from the enumerator (which are
     * returned as scalars) are wrapped in {@code Object[]} since Calcite expects
     * {@code Enumerable<Object[]>}.
     */
    static class ShardScannableTable extends AbstractTable implements ScannableTable {
        private final OpenSearchIndex osIndex;
        private final RelDataType rowType;

        ShardScannableTable(OpenSearchIndex osIndex, RelDataType rowType) {
            this.osIndex = osIndex;
            this.rowType = rowType;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return rowType;
        }

        @Override
        public Enumerable<Object[]> scan(DataContext root) {
            PushDownContext ctx = new PushDownContext(osIndex);
            OpenSearchRequestBuilder requestBuilder = ctx.createRequestBuilder();
            List<String> fieldNames = rowType.getFieldNames();
            int columnCount = fieldNames.size();

            return new AbstractEnumerable<>() {
                @Override
                public Enumerator<Object[]> enumerator() {
                    Enumerator<Object> inner = new OpenSearchIndexEnumerator(
                            osIndex.getClient(),
                            fieldNames,
                            requestBuilder.getMaxResponseSize(),
                            requestBuilder.getMaxResultWindow(),
                            osIndex.getQueryBucketSize(),
                            osIndex.buildRequest(requestBuilder),
                            osIndex.createOpenSearchResourceMonitor());
                    return new Enumerator<>() {
                        @Override
                        public Object[] current() {
                            Object current = inner.current();
                            if (current == null) {
                                return new Object[columnCount];
                            } else if (current instanceof Object[]) {
                                return (Object[]) current;
                            } else {
                                // Single-column rows are returned as scalars by
                                // OpenSearchIndexEnumerator; wrap in Object[]
                                return new Object[] {current};
                            }
                        }

                        @Override
                        public boolean moveNext() {
                            return inner.moveNext();
                        }

                        @Override
                        public void reset() {
                            inner.reset();
                        }

                        @Override
                        public void close() {
                            inner.close();
                        }
                    };
                }
            };
        }
    }
}
