/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.api.spec.LanguageSpec.LanguageExtension;
import org.opensearch.sql.calcite.type.ExprDateType;
import org.opensearch.sql.calcite.type.ExprTimeStampType;
import org.opensearch.sql.calcite.type.ExprTimeType;

/** Datetime language extension that normalizes UDT types and casts output for wire-format. */
public class DatetimeExtension implements LanguageExtension {

  @Override
  public List<RelShuttle> postAnalysisRules() {
    return List.of(DatetimeUdtNormalizeRule.INSTANCE, DatetimeOutputCastRule.INSTANCE);
  }

  /** Maps datetime UDT types to their standard Calcite equivalents. */
  @Getter
  @RequiredArgsConstructor
  enum UdtMapping {
    DATE(ExprDateType.class, SqlTypeName.DATE),
    TIME(ExprTimeType.class, SqlTypeName.TIME),
    TIMESTAMP(ExprTimeStampType.class, SqlTypeName.TIMESTAMP);

    private final Class<?> udtClass;
    private final SqlTypeName stdType;

    /** Matches a UDT RelDataType to its mapping, or empty if not a datetime UDT. */
    static Optional<UdtMapping> fromUdtType(RelDataType type) {
      return Arrays.stream(values()).filter(u -> u.udtClass.isInstance(type)).findFirst();
    }

    /** Returns true if the given SqlTypeName is a standard datetime type. */
    static boolean isDatetimeType(SqlTypeName typeName) {
      return Arrays.stream(values()).anyMatch(u -> u.stdType == typeName);
    }
  }
}
