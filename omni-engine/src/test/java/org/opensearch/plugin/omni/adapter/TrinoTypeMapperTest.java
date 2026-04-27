/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.omni.adapter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprCoreType;

public class TrinoTypeMapperTest {

  @Test
  void mapsPrimitives() {
    assertEquals(ExprCoreType.LONG, TrinoTypeMapper.toExprType("bigint"));
    assertEquals(ExprCoreType.INTEGER, TrinoTypeMapper.toExprType("integer"));
    assertEquals(ExprCoreType.INTEGER, TrinoTypeMapper.toExprType("int"));
    assertEquals(ExprCoreType.SHORT, TrinoTypeMapper.toExprType("smallint"));
    assertEquals(ExprCoreType.BYTE, TrinoTypeMapper.toExprType("tinyint"));
    assertEquals(ExprCoreType.DOUBLE, TrinoTypeMapper.toExprType("double"));
    assertEquals(ExprCoreType.FLOAT, TrinoTypeMapper.toExprType("real"));
    assertEquals(ExprCoreType.BOOLEAN, TrinoTypeMapper.toExprType("boolean"));
  }

  @Test
  void mapsStringTypes() {
    assertEquals(ExprCoreType.STRING, TrinoTypeMapper.toExprType("varchar"));
    assertEquals(ExprCoreType.STRING, TrinoTypeMapper.toExprType("varchar(100)"));
    assertEquals(ExprCoreType.STRING, TrinoTypeMapper.toExprType("char(10)"));
  }

  @Test
  void mapsTemporalTypes() {
    assertEquals(ExprCoreType.DATE, TrinoTypeMapper.toExprType("date"));
    assertEquals(ExprCoreType.TIMESTAMP, TrinoTypeMapper.toExprType("timestamp"));
    assertEquals(ExprCoreType.TIMESTAMP, TrinoTypeMapper.toExprType("timestamp(3)"));
    assertEquals(ExprCoreType.TIMESTAMP, TrinoTypeMapper.toExprType("timestamp(6) with time zone"));
  }

  @Test
  void mapsDecimalToDouble() {
    assertEquals(ExprCoreType.DOUBLE, TrinoTypeMapper.toExprType("decimal(10,2)"));
    assertEquals(ExprCoreType.DOUBLE, TrinoTypeMapper.toExprType("decimal(38,0)"));
  }

  @Test
  void mapsCollectionTypes() {
    assertEquals(ExprCoreType.ARRAY, TrinoTypeMapper.toExprType("array(varchar)"));
    assertEquals(ExprCoreType.ARRAY, TrinoTypeMapper.toExprType("array(bigint)"));
    assertEquals(ExprCoreType.STRUCT, TrinoTypeMapper.toExprType("row(a bigint, b varchar)"));
    assertEquals(ExprCoreType.STRUCT, TrinoTypeMapper.toExprType("map(varchar, bigint)"));
  }

  @Test
  void mapsBinary() {
    assertEquals(ExprCoreType.BINARY, TrinoTypeMapper.toExprType("varbinary"));
  }

  @Test
  void unknownMapsToUndefined() {
    assertEquals(ExprCoreType.UNDEFINED, TrinoTypeMapper.toExprType("uuid"));
    assertEquals(ExprCoreType.UNDEFINED, TrinoTypeMapper.toExprType("ipaddress"));
    assertEquals(ExprCoreType.UNDEFINED, TrinoTypeMapper.toExprType("hyperloglog"));
  }

  @Test
  void caseInsensitive() {
    assertEquals(ExprCoreType.LONG, TrinoTypeMapper.toExprType("BIGINT"));
    assertEquals(ExprCoreType.STRING, TrinoTypeMapper.toExprType("VARCHAR(50)"));
  }
}
