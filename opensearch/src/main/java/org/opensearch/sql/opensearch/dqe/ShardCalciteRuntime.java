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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.calcite.plan.Scannable;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.opensearch.dqe.serde.RelNodeSerializer;
import org.opensearch.sql.opensearch.dqe.serde.ShardDeserializationContext;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

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
 *       {@link Interpreter} to execute the plan tree. The Interpreter handles standard relational
 *       operators natively. The scan leaf is wrapped in a {@link ScannableTable} so the Interpreter
 *       can read data from it.
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
     * any table name to a {@link RelOptTable} backed by the given {@link OpenSearchIndex}. This
     * allows Calcite's {@code RelJsonReader} to resolve table references when constructing
     * {@code DSLScan} nodes from JSON.
     */
    private static RelOptSchema createShardSchema(OpenSearchIndex osIndex,
            RelOptCluster cluster) {
        return new RelOptSchema() {
            @Override
            public RelOptTable getTableForMember(java.util.List<String> qualifiedName) {
                RelDataType rowType = osIndex.getRowType(cluster.getTypeFactory());
                return RelOptTableImpl.create(
                        this, rowType, osIndex,
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
     * Executes a non-Scannable plan tree using Calcite's {@link Interpreter}. The Interpreter can
     * handle standard logical operators (Filter, Project, Sort, Aggregate, etc.) natively.
     *
     * <p>The scan leaf (CalciteLogicalIndexScan or DSLScan) is located in the plan tree and wrapped
     * in a {@link ScannableTable} so that the Interpreter's {@code TableScanNode} can read data
     * from it. A {@link DataContext} is created with a root schema containing this table.
     *
     * @param plan the deserialized plan tree with a non-Scannable root
     * @return an Enumerable of Object[] rows
     */
    private Enumerable<Object[]> executeWithInterpreter(RelNode plan) {
        // Find the Scannable leaf in the plan tree
        Scannable scannableLeaf = findScannableLeaf(plan);
        if (scannableLeaf == null) {
            throw new IllegalStateException(
                    "No Scannable leaf found in plan tree rooted at "
                            + plan.getClass().getName()
                            + ". The shard plan must contain a DSLScan or "
                            + "CalciteLogicalIndexScan leaf.");
        }

        // Get the table name from the scan leaf's RelOptTable
        AbstractCalciteIndexScan scanNode = (AbstractCalciteIndexScan) scannableLeaf;
        List<String> qualifiedName = scanNode.getTable().getQualifiedName();

        // Create a ScannableTable wrapper that delegates to the leaf's scan()
        ScannableTable scannableTable = new ScannableTableAdapter(scannableLeaf, scanNode);

        // Build a SchemaPlus with the ScannableTable registered under the table name
        SchemaPlus rootSchema = Frameworks.createRootSchema(false);
        // Register the table under each component of the qualified name path
        // For a single-component name like ["myindex"], register directly in root
        // For multi-component like ["opensearch", "myindex"], create intermediate schemas
        SchemaPlus targetSchema = rootSchema;
        for (int i = 0; i < qualifiedName.size() - 1; i++) {
            SchemaPlus child = targetSchema.getSubSchema(qualifiedName.get(i));
            if (child == null) {
                child = targetSchema.add(qualifiedName.get(i), new AbstractSchema());
            }
            targetSchema = child;
        }
        String tableName = qualifiedName.get(qualifiedName.size() - 1);
        targetSchema.add(tableName, scannableTable);

        // Create a DataContext with the root schema
        DataContext dataContext = createInterpreterDataContext(rootSchema);

        // Execute via Interpreter
        return new Interpreter(dataContext, plan);
    }

    /**
     * Walks the plan tree to find the first {@link Scannable} leaf node. Returns null if none
     * found.
     */
    private static Scannable findScannableLeaf(RelNode node) {
        if (node instanceof Scannable) {
            return (Scannable) node;
        }
        for (RelNode child : node.getInputs()) {
            Scannable found = findScannableLeaf(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /**
     * Creates a minimal {@link DataContext} for the Calcite {@link Interpreter}. The DataContext
     * provides the root schema (containing the ScannableTable wrapper) and the type factory.
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

    /**
     * A {@link ScannableTable} adapter that wraps a {@link Scannable} leaf node's output for use
     * by Calcite's {@link Interpreter}. The Interpreter's {@code TableScanNode} requires a
     * {@link ScannableTable} to read data from. This adapter bridges between our custom
     * {@link Scannable} interface (which returns {@code Enumerable<Object>} with scalars for
     * single-column rows) and Calcite's {@link ScannableTable} (which returns
     * {@code Enumerable<Object[]>}).
     */
    private static class ScannableTableAdapter extends AbstractTable implements ScannableTable {
        private final Scannable scannableLeaf;
        private final AbstractCalciteIndexScan scanNode;

        ScannableTableAdapter(Scannable scannableLeaf, AbstractCalciteIndexScan scanNode) {
            this.scannableLeaf = scannableLeaf;
            this.scanNode = scanNode;
        }

        @Override
        public RelDataType getRowType(
                org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
            return scanNode.getRowType();
        }

        @Override
        public Enumerable<Object[]> scan(DataContext root) {
            Enumerable<?> raw = scannableLeaf.scan();
            int columnCount = scanNode.getRowType().getFieldCount();
            return new org.apache.calcite.linq4j.AbstractEnumerable<Object[]>() {
                @Override
                public Enumerator<Object[]> enumerator() {
                    Enumerator<?> inner = raw.enumerator();
                    return new Enumerator<>() {
                        @Override
                        public Object[] current() {
                            Object current = inner.current();
                            if (current == null) {
                                return new Object[columnCount];
                            } else if (columnCount == 1) {
                                return new Object[] {current};
                            } else if (current instanceof Object[]) {
                                return (Object[]) current;
                            } else {
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
