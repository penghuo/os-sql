/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.adapter;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/**
 * Maps Calcite {@link RelDataType} to OpenSearch SQL {@link ExprType}.
 * Provides the inverse of {@link org.opensearch.sql.planner.opensearch.client.storage.data.calcite.CalciteSchemaAdapter#mapType}.
 *
 * <p>This mapper is used to restore PPL's inferred types (from Calcite's type system)
 * when building QueryResponse schemas, ensuring INTEGER stays INTEGER (not BIGINT).
 */
public final class CalciteToExprTypeMapper {
    private CalciteToExprTypeMapper() {}

    /**
     * Convert a Calcite RelDataType to an OpenSearch SQL ExprType.
     *
     * @param type the Calcite type
     * @return the corresponding ExprType, or UNDEFINED if unmapped
     */
    public static ExprType toExprType(RelDataType type) {
        SqlTypeName sqlType = type.getSqlTypeName();
        return switch (sqlType) {
            case BOOLEAN -> ExprCoreType.BOOLEAN;
            case TINYINT -> ExprCoreType.BYTE;
            case SMALLINT -> ExprCoreType.SHORT;
            case INTEGER -> ExprCoreType.INTEGER;
            case BIGINT -> ExprCoreType.LONG;
            case FLOAT, REAL -> ExprCoreType.FLOAT;
            case DOUBLE, DECIMAL -> ExprCoreType.DOUBLE;
            case CHAR, VARCHAR -> ExprCoreType.STRING;
            case DATE -> ExprCoreType.DATE;
            case TIME, TIME_WITH_LOCAL_TIME_ZONE -> ExprCoreType.TIME;
            case TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> ExprCoreType.TIMESTAMP;
            case BINARY, VARBINARY -> ExprCoreType.BINARY;
            case ARRAY -> ExprCoreType.ARRAY;
            case MAP, ROW -> ExprCoreType.STRUCT;
            case NULL -> ExprCoreType.UNDEFINED;
            default -> ExprCoreType.UNDEFINED;
        };
    }
}
