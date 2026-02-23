/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Resolves type conflicts across indices in an index pattern.
 *
 * <p>When a field exists in multiple indices with different types (e.g. {@code integer} in one
 * index and {@code long} in another), this class widens to the most general compatible type (e.g.
 * {@code BIGINT}). If the types are incompatible (e.g. {@code keyword} vs {@code long}), the result
 * is empty and the field should be excluded with a warning.
 */
public final class TypeWidening {

  /**
   * Widens a set of types for the same field across indices to a single compatible type.
   *
   * @param fieldName for error reporting
   * @param types set of DqeTypes found across indices for this field
   * @return the widened type, or empty if types are incompatible
   */
  public static Optional<DqeType> widen(String fieldName, Set<DqeType> types) {
    Objects.requireNonNull(fieldName, "fieldName must not be null");
    Objects.requireNonNull(types, "types must not be null");

    if (types.isEmpty()) {
      return Optional.empty();
    }

    if (types.size() == 1) {
      return Optional.of(types.iterator().next());
    }

    // Try to find a common supertype by folding all types through getCommonSuperType
    DqeType result = null;
    for (DqeType type : types) {
      if (result == null) {
        result = type;
      } else {
        Optional<DqeType> common = DqeTypeCoercion.getCommonSuperType(result, type);
        if (common.isEmpty()) {
          // Incompatible types — cannot widen
          return Optional.empty();
        }
        result = common.get();
      }
    }

    return Optional.ofNullable(result);
  }

  private TypeWidening() {
    // no instantiation
  }
}
