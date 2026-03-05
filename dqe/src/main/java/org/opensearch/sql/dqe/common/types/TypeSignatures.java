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

/**
 * Maps Trino type signature strings back to their {@link Type} singleton instances. Used during
 * deserialization to reconstruct types from their display names.
 */
public final class TypeSignatures {

  private static final Map<String, Type> SIGNATURE_TO_TYPE =
      Map.ofEntries(
          Map.entry("bigint", BigintType.BIGINT),
          Map.entry("integer", IntegerType.INTEGER),
          Map.entry("smallint", SmallintType.SMALLINT),
          Map.entry("tinyint", TinyintType.TINYINT),
          Map.entry("double", DoubleType.DOUBLE),
          Map.entry("real", RealType.REAL),
          Map.entry("boolean", BooleanType.BOOLEAN),
          Map.entry("varchar", VarcharType.VARCHAR),
          Map.entry("varbinary", VarbinaryType.VARBINARY),
          Map.entry("timestamp(3)", TimestampType.TIMESTAMP_MILLIS));

  private TypeSignatures() {}

  /** Convert a type signature string to a Trino Type instance. */
  public static Type fromSignature(String signature) {
    Type result = SIGNATURE_TO_TYPE.get(signature);
    if (result == null) {
      throw new IllegalArgumentException("Unknown type signature: " + signature);
    }
    return result;
  }

  /** Convert a Trino Type to its signature string. */
  public static String toSignature(Type type) {
    return type.getDisplayName();
  }
}
