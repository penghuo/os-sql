/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner.bridge;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.opensearch.sql.distributed.operator.SortOrder;

/**
 * Converts Calcite's {@link RelCollation} to distributed engine's {@link SortOrder} enum and sort
 * channel indices.
 */
public final class SortOrderConverter {

  private SortOrderConverter() {}

  /** Extracts sort channel indices from a Calcite collation. */
  public static List<Integer> extractSortChannels(RelCollation collation) {
    List<Integer> channels = new ArrayList<>(collation.getFieldCollations().size());
    for (RelFieldCollation fc : collation.getFieldCollations()) {
      channels.add(fc.getFieldIndex());
    }
    return channels;
  }

  /** Converts a Calcite collation to a list of SortOrder enums. */
  public static List<SortOrder> convertSortOrders(RelCollation collation) {
    List<SortOrder> orders = new ArrayList<>(collation.getFieldCollations().size());
    for (RelFieldCollation fc : collation.getFieldCollations()) {
      orders.add(convertFieldCollation(fc));
    }
    return orders;
  }

  /** Converts a single Calcite field collation to a SortOrder. */
  public static SortOrder convertFieldCollation(RelFieldCollation fc) {
    boolean ascending =
        fc.getDirection() == RelFieldCollation.Direction.ASCENDING
            || fc.getDirection() == RelFieldCollation.Direction.STRICTLY_ASCENDING;

    // Calcite NullDirection: FIRST, LAST, UNSPECIFIED
    // UNSPECIFIED defaults: ASC → NULLS LAST, DESC → NULLS FIRST (SQL standard)
    boolean nullsFirst;
    if (fc.nullDirection == RelFieldCollation.NullDirection.FIRST) {
      nullsFirst = true;
    } else if (fc.nullDirection == RelFieldCollation.NullDirection.LAST) {
      nullsFirst = false;
    } else {
      // UNSPECIFIED: SQL standard defaults
      nullsFirst = !ascending;
    }

    if (ascending && nullsFirst) {
      return SortOrder.ASC_NULLS_FIRST;
    } else if (ascending) {
      return SortOrder.ASC_NULLS_LAST;
    } else if (nullsFirst) {
      return SortOrder.DESC_NULLS_FIRST;
    } else {
      return SortOrder.DESC_NULLS_LAST;
    }
  }
}
