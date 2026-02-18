/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

/** Sort ordering specification. Ported from Trino's io.trino.spi.connector.SortOrder. */
public enum SortOrder {
  ASC_NULLS_FIRST(true, true),
  ASC_NULLS_LAST(true, false),
  DESC_NULLS_FIRST(false, true),
  DESC_NULLS_LAST(false, false);

  private final boolean ascending;
  private final boolean nullsFirst;

  SortOrder(boolean ascending, boolean nullsFirst) {
    this.ascending = ascending;
    this.nullsFirst = nullsFirst;
  }

  public boolean isAscending() {
    return ascending;
  }

  public boolean isNullsFirst() {
    return nullsFirst;
  }
}
