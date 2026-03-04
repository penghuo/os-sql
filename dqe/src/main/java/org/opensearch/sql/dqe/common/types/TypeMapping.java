/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.common.types;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import java.util.Map;

/** Maps OpenSearch field types to Trino types. */
public final class TypeMapping {

  private static final Map<String, Type> OS_TO_TRINO =
      Map.ofEntries(
          Map.entry("keyword", VarcharType.VARCHAR),
          Map.entry("text", VarcharType.VARCHAR),
          Map.entry("long", BigintType.BIGINT),
          Map.entry("integer", IntegerType.INTEGER),
          Map.entry("short", SmallintType.SMALLINT),
          Map.entry("byte", TinyintType.TINYINT),
          Map.entry("double", DoubleType.DOUBLE),
          Map.entry("float", RealType.REAL),
          Map.entry("half_float", RealType.REAL),
          Map.entry("scaled_float", DoubleType.DOUBLE),
          Map.entry("boolean", BooleanType.BOOLEAN),
          Map.entry("date", TimestampType.TIMESTAMP_MILLIS),
          Map.entry("ip", VarcharType.VARCHAR),
          Map.entry("binary", VarbinaryType.VARBINARY));

  private TypeMapping() {}

  public static Type toTrinoType(String openSearchType) {
    Type result = OS_TO_TRINO.get(openSearchType);
    if (result == null) {
      throw new IllegalArgumentException("Unsupported OpenSearch type: " + openSearchType);
    }
    return result;
  }
}
