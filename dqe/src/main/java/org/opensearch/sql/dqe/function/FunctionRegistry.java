/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Registry of all available functions. Supports overload resolution with implicit type coercion.
 * Functions are registered by name (case-insensitive) and resolved by name + argument types.
 */
public class FunctionRegistry {

  private final Map<String, List<FunctionMetadata>> functions = new HashMap<>();

  /** Register a function metadata entry. */
  public void register(FunctionMetadata metadata) {
    functions
        .computeIfAbsent(metadata.getName().toLowerCase(Locale.ROOT), k -> new ArrayList<>())
        .add(metadata);
  }

  /**
   * Resolve a function by name and argument types. Picks the overload requiring the fewest
   * coercions (0 = exact match).
   *
   * @throws IllegalArgumentException if no matching function is found
   */
  public ResolvedFunction resolve(String name, List<Type> argumentTypes) {
    String key = name.toLowerCase(Locale.ROOT);
    List<FunctionMetadata> overloads = functions.get(key);
    if (overloads == null || overloads.isEmpty()) {
      throw new IllegalArgumentException("Unknown function: " + name);
    }

    FunctionMetadata bestMatch = null;
    int bestCoercions = Integer.MAX_VALUE;

    for (FunctionMetadata metadata : overloads) {
      int coercions = matchArguments(metadata.getArgumentTypes(), argumentTypes);
      if (coercions >= 0 && coercions < bestCoercions) {
        bestMatch = metadata;
        bestCoercions = coercions;
      }
    }

    if (bestMatch == null) {
      throw new IllegalArgumentException(
          "No matching overload for " + name + " with argument types " + argumentTypes);
    }

    return new ResolvedFunction(
        bestMatch.getName(),
        bestMatch.getArgumentTypes(),
        bestMatch.getReturnType(),
        bestMatch.getKind());
  }

  /**
   * Retrieve the full metadata for a previously resolved function reference.
   *
   * @throws IllegalArgumentException if the function cannot be found
   */
  public FunctionMetadata getMetadata(ResolvedFunction resolved) {
    String key = resolved.getName().toLowerCase(Locale.ROOT);
    List<FunctionMetadata> overloads = functions.get(key);
    if (overloads == null) {
      throw new IllegalArgumentException("Unknown function: " + resolved.getName());
    }
    for (FunctionMetadata metadata : overloads) {
      if (metadata.getArgumentTypes().equals(resolved.getArgumentTypes())
          && metadata.getKind() == resolved.getKind()) {
        return metadata;
      }
    }
    throw new IllegalArgumentException("No metadata found for resolved function: " + resolved);
  }

  /**
   * Returns the number of coercions needed to match actualTypes to parameterTypes, or -1 if no
   * match is possible.
   */
  private int matchArguments(List<Type> parameterTypes, List<Type> actualTypes) {
    if (parameterTypes.size() != actualTypes.size()) {
      return -1;
    }
    int coercions = 0;
    for (int i = 0; i < parameterTypes.size(); i++) {
      Type param = parameterTypes.get(i);
      Type actual = actualTypes.get(i);
      if (param.equals(actual)) {
        continue;
      }
      if (canCoerce(actual, param)) {
        coercions++;
      } else {
        return -1;
      }
    }
    return coercions;
  }

  /** Check if an implicit coercion from source to target is supported. */
  private boolean canCoerce(Type source, Type target) {
    // Integer family -> BIGINT
    if (target instanceof BigintType
        && (source instanceof IntegerType
            || source instanceof SmallintType
            || source instanceof TinyintType)) {
      return true;
    }
    // BIGINT -> DOUBLE
    if (target instanceof DoubleType && source instanceof BigintType) {
      return true;
    }
    // Integer family -> DOUBLE
    if (target instanceof DoubleType
        && (source instanceof IntegerType
            || source instanceof SmallintType
            || source instanceof TinyintType)) {
      return true;
    }
    // REAL -> DOUBLE
    if (target instanceof DoubleType && source instanceof RealType) {
      return true;
    }
    // TIMESTAMP(higher precision) -> TIMESTAMP(lower precision), e.g. TIMESTAMP(6) -> TIMESTAMP(3)
    if (source instanceof TimestampType && target instanceof TimestampType) {
      return ((TimestampType) source).getPrecision() > ((TimestampType) target).getPrecision();
    }
    if (source instanceof TimestampWithTimeZoneType && target instanceof TimestampType) {
      return true;
    }
    return false;
  }
}
