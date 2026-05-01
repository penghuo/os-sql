/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.ppl;

import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableMetadata;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Adapts Trino's Metadata service to Calcite's Schema interface.
 * Structure: CalciteSchemaAdapter (catalog) → sub-schema per Trino schema → table per Trino table.
 */
public class CalciteSchemaAdapter extends AbstractSchema {

    private static final Logger log = LogManager.getLogger(CalciteSchemaAdapter.class);
    private final Metadata metadata;
    private final Session session;
    private final String catalogName;

    public CalciteSchemaAdapter(Metadata metadata, Session session, String catalogName) {
        this.metadata = metadata;
        this.session = session;
        this.catalogName = catalogName;
    }

    @Override
    protected Map<String, Schema> getSubSchemaMap() {
        List<String> schemas = metadata.listSchemaNames(session, catalogName);
        log.info("CalciteSchemaAdapter.getSubSchemaMap() catalog={} schemas={}", catalogName, schemas);
        Map<String, Schema> result = new LinkedHashMap<>();
        for (String schema : schemas) {
            result.put(schema, new TrinoSchemaAdapter(schema));
        }
        return result;
    }

    private class TrinoSchemaAdapter extends AbstractSchema {
        private final String schemaName;
        private final Map<String, Table> tableMap = new LinkedHashMap<String, Table>() {
            @Override
            public Table get(Object key) {
                String name = (String) key;
                // First: try the static cache
                if (super.containsKey(name)) {
                    return super.get(name);
                }
                // Fallback: try resolving dynamically via metadata.getTableHandle.
                // This lets wildcard patterns ("logs-*") work. Comma-separated patterns ("a,b")
                // require additional connector support that may be added in future phases.
                try {
                    Optional<TableHandle> handle = metadata.getTableHandle(
                            session,
                            new QualifiedObjectName(catalogName, schemaName, name));
                    if (handle.isPresent()) {
                        log.debug("Resolved table pattern {} dynamically", name);
                        Table table = new TrinoTableAdapter(schemaName, name);
                        super.put(name, table);
                        return table;
                    }
                } catch (Exception e) {
                    log.debug("Dynamic table resolution failed for {}.{}.{}: {}",
                            catalogName, schemaName, name, e.getMessage());
                }
                return null;
            }
        };

        TrinoSchemaAdapter(String schemaName) {
            this.schemaName = schemaName;
            // Pre-populate the table map with statically-listed tables
            List<QualifiedObjectName> tables = metadata.listTables(
                    session, new QualifiedTablePrefix(catalogName, schemaName));
            log.info("TrinoSchemaAdapter() catalog={} schema={} tables={}", catalogName, schemaName, tables);
            for (QualifiedObjectName table : tables) {
                tableMap.put(table.getObjectName(), new TrinoTableAdapter(schemaName, table.getObjectName()));
            }
        }

        @Override
        protected Map<String, Table> getTableMap() {
            return tableMap;
        }
    }

    private class TrinoTableAdapter extends AbstractTable {
        private final String schemaName;
        private final String tableName;

        TrinoTableAdapter(String schemaName, String tableName) {
            this.schemaName = schemaName;
            this.tableName = tableName;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            Optional<TableHandle> handle = metadata.getTableHandle(
                    session, new QualifiedObjectName(catalogName, schemaName, tableName));
            if (handle.isEmpty()) {
                return typeFactory.builder().build();
            }
            // Pull column handles via the metadata service — OpenSearchMetadata preserves
            // the original OpenSearch field case on OpenSearchColumnHandle.name, whereas
            // Trino's ColumnMetadata lowercases it. PPL needs the original case so users can
            // reference "severityText" rather than "severitytext" and verifySchema matches.
            java.util.Map<String, io.trino.spi.connector.ColumnHandle> columnHandles =
                    metadata.getColumnHandles(session, handle.get());
            RelDataTypeFactory.Builder builder = typeFactory.builder();
            for (var entry : columnHandles.entrySet()) {
                io.trino.spi.connector.ColumnHandle ch = entry.getValue();
                if (ch instanceof org.opensearch.plugin.omni.connector.opensearch.OpenSearchColumnHandle osch) {
                    if (osch.getName().startsWith("_")) continue; // hidden built-ins
                    flattenColumn(osch.getName(), osch.getType(), typeFactory, builder);
                } else {
                    // Non-OpenSearch handle: fall back to Trino's ColumnMetadata (lowercased).
                    io.trino.spi.connector.ColumnMetadata cm =
                            metadata.getColumnMetadata(session, handle.get(), ch);
                    if (cm.isHidden()) continue;
                    flattenColumn(cm.getName(), cm.getType(), typeFactory, builder);
                }
            }
            return builder.build();
        }
    }

    /**
     * Flattens RowType columns into dotted field names matching os-sql/PPL convention.
     * e.g., column "cloud" ROW(region VARCHAR) → "cloud" MAP, "cloud.region" VARCHAR
     */
    private static void flattenColumn(String prefix, Type trinoType,
            RelDataTypeFactory typeFactory, RelDataTypeFactory.Builder builder) {
        if (trinoType instanceof RowType rt) {
            // Add the object itself as MAP(VARCHAR, ANY) matching os-sql convention
            builder.add(prefix, typeFactory.createMapType(
                    typeFactory.createSqlType(SqlTypeName.VARCHAR),
                    typeFactory.createSqlType(SqlTypeName.ANY))).nullable(true);
            // Recursively flatten sub-fields with dotted names
            for (RowType.Field field : rt.getFields()) {
                String fieldName = field.getName().orElse("field");
                flattenColumn(prefix + "." + fieldName, field.getType(), typeFactory, builder);
            }
        } else {
            builder.add(prefix, mapType(trinoType, typeFactory)).nullable(true);
        }
    }

    static RelDataType mapType(Type trinoType, RelDataTypeFactory typeFactory) {
        if (trinoType instanceof BigintType) return typeFactory.createSqlType(SqlTypeName.BIGINT);
        if (trinoType instanceof IntegerType) return typeFactory.createSqlType(SqlTypeName.INTEGER);
        if (trinoType instanceof SmallintType) return typeFactory.createSqlType(SqlTypeName.SMALLINT);
        if (trinoType instanceof TinyintType) return typeFactory.createSqlType(SqlTypeName.TINYINT);
        if (trinoType instanceof DoubleType) return typeFactory.createSqlType(SqlTypeName.DOUBLE);
        if (trinoType instanceof RealType) return typeFactory.createSqlType(SqlTypeName.REAL);
        if (trinoType instanceof BooleanType) return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        if (trinoType instanceof DateType) return typeFactory.createSqlType(SqlTypeName.DATE);
        if (trinoType instanceof TimestampType) return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        if (trinoType instanceof TimestampWithTimeZoneType) return typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        if (trinoType instanceof VarbinaryType) return typeFactory.createSqlType(SqlTypeName.VARBINARY);
        if (trinoType instanceof VarcharType) {
            VarcharType vt = (VarcharType) trinoType;
            return vt.isUnbounded()
                    ? typeFactory.createSqlType(SqlTypeName.VARCHAR)
                    : typeFactory.createSqlType(SqlTypeName.VARCHAR, vt.getBoundedLength());
        }
        if (trinoType instanceof CharType) {
            return typeFactory.createSqlType(SqlTypeName.CHAR, ((CharType) trinoType).getLength());
        }
        if (trinoType instanceof DecimalType) {
            DecimalType dt = (DecimalType) trinoType;
            return typeFactory.createSqlType(SqlTypeName.DECIMAL, dt.getPrecision(), dt.getScale());
        }
        if (trinoType instanceof ArrayType) {
            RelDataType elementType = mapType(((ArrayType) trinoType).getElementType(), typeFactory);
            return typeFactory.createArrayType(elementType, -1);
        }
        if (trinoType instanceof MapType) {
            MapType mt = (MapType) trinoType;
            return typeFactory.createMapType(
                    mapType(mt.getKeyType(), typeFactory),
                    mapType(mt.getValueType(), typeFactory));
        }
        if (trinoType instanceof RowType) {
            RowType rt = (RowType) trinoType;
            RelDataTypeFactory.Builder b = typeFactory.builder();
            for (RowType.Field field : rt.getFields()) {
                b.add(field.getName().orElse("field"), mapType(field.getType(), typeFactory)).nullable(true);
            }
            return b.build();
        }
        // Fallback for unknown types
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }
}
