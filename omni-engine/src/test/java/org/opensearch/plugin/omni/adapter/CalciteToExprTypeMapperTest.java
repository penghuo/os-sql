/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.adapter;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CalciteToExprTypeMapperTest {
    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

    @Test
    void testMapIntegerToInteger() {
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        ExprType result = CalciteToExprTypeMapper.toExprType(intType);
        assertEquals(ExprCoreType.INTEGER, result);
    }

    @Test
    void testMapBigintToLong() {
        RelDataType bigintType = typeFactory.createSqlType(SqlTypeName.BIGINT);
        ExprType result = CalciteToExprTypeMapper.toExprType(bigintType);
        assertEquals(ExprCoreType.LONG, result);
    }

    @Test
    void testMapSmallintToShort() {
        RelDataType smallintType = typeFactory.createSqlType(SqlTypeName.SMALLINT);
        ExprType result = CalciteToExprTypeMapper.toExprType(smallintType);
        assertEquals(ExprCoreType.SHORT, result);
    }

    @Test
    void testMapTinyintToByte() {
        RelDataType tinyintType = typeFactory.createSqlType(SqlTypeName.TINYINT);
        ExprType result = CalciteToExprTypeMapper.toExprType(tinyintType);
        assertEquals(ExprCoreType.BYTE, result);
    }

    @Test
    void testMapDateToDate() {
        RelDataType dateType = typeFactory.createSqlType(SqlTypeName.DATE);
        ExprType result = CalciteToExprTypeMapper.toExprType(dateType);
        assertEquals(ExprCoreType.DATE, result);
    }

    @Test
    void testMapTimestampToTimestamp() {
        RelDataType timestampType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        ExprType result = CalciteToExprTypeMapper.toExprType(timestampType);
        assertEquals(ExprCoreType.TIMESTAMP, result);
    }

    @Test
    void testMapVarcharToString() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        ExprType result = CalciteToExprTypeMapper.toExprType(varcharType);
        assertEquals(ExprCoreType.STRING, result);
    }

    @Test
    void testMapDoubleToDouble() {
        RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        ExprType result = CalciteToExprTypeMapper.toExprType(doubleType);
        assertEquals(ExprCoreType.DOUBLE, result);
    }

    @Test
    void testMapFloatToFloat() {
        RelDataType floatType = typeFactory.createSqlType(SqlTypeName.FLOAT);
        ExprType result = CalciteToExprTypeMapper.toExprType(floatType);
        assertEquals(ExprCoreType.FLOAT, result);
    }

    @Test
    void testMapBooleanToBoolean() {
        RelDataType booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        ExprType result = CalciteToExprTypeMapper.toExprType(booleanType);
        assertEquals(ExprCoreType.BOOLEAN, result);
    }

    @Test
    void testMapArrayToArray() {
        RelDataType arrayType = typeFactory.createArrayType(
                typeFactory.createSqlType(SqlTypeName.INTEGER), -1);
        ExprType result = CalciteToExprTypeMapper.toExprType(arrayType);
        assertEquals(ExprCoreType.ARRAY, result);
    }

    @Test
    void testMapMapToStruct() {
        RelDataType mapType = typeFactory.createMapType(
                typeFactory.createSqlType(SqlTypeName.VARCHAR),
                typeFactory.createSqlType(SqlTypeName.INTEGER));
        ExprType result = CalciteToExprTypeMapper.toExprType(mapType);
        assertEquals(ExprCoreType.STRUCT, result);
    }

    @Test
    void testMapNullToUndefined() {
        RelDataType nullType = typeFactory.createSqlType(SqlTypeName.NULL);
        ExprType result = CalciteToExprTypeMapper.toExprType(nullType);
        assertEquals(ExprCoreType.UNDEFINED, result);
    }
}
