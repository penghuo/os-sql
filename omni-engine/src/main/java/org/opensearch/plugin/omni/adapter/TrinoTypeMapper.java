/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.omni.adapter;

import java.util.Locale;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/** Maps a Trino client-protocol type string to an OpenSearch SQL {@link ExprType}. */
public final class TrinoTypeMapper {

  private TrinoTypeMapper() {}

  public static ExprType toExprType(String trinoType) {
    String lower = trinoType.toLowerCase(Locale.ROOT);
    String base = lower.replaceAll("\\([^)]*\\)", "").trim();

    return switch (base) {
      case "bigint" -> ExprCoreType.LONG;
      case "integer", "int" -> ExprCoreType.INTEGER;
      case "smallint" -> ExprCoreType.SHORT;
      case "tinyint" -> ExprCoreType.BYTE;
      case "double" -> ExprCoreType.DOUBLE;
      case "real" -> ExprCoreType.FLOAT;
      case "boolean" -> ExprCoreType.BOOLEAN;
      case "varchar", "char" -> ExprCoreType.STRING;
      case "date" -> ExprCoreType.DATE;
      case "timestamp", "timestamp with time zone" -> ExprCoreType.TIMESTAMP;
      case "decimal" -> ExprCoreType.DOUBLE;
      case "array" -> ExprCoreType.ARRAY;
      case "row", "map" -> ExprCoreType.STRUCT;
      case "varbinary" -> ExprCoreType.BINARY;
      default -> ExprCoreType.UNDEFINED;
    };
  }
}
