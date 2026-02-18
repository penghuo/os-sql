/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

import org.opensearch.sql.distributed.data.Page;

/**
 * Comparator for comparing rows across pages by position. Ported from Trino's
 * io.trino.operator.PageWithPositionComparator.
 */
@FunctionalInterface
public interface PageWithPositionComparator {

  /**
   * Compares the row at {@code leftPosition} in {@code left} with the row at {@code rightPosition}
   * in {@code right}.
   *
   * @return negative if left < right, 0 if equal, positive if left > right
   */
  int compareTo(Page left, int leftPosition, Page right, int rightPosition);
}
