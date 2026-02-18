/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

import org.opensearch.sql.distributed.data.Page;

/**
 * Evaluates a filter predicate against each position in a Page, producing a boolean array
 * indicating which positions pass the filter.
 */
@FunctionalInterface
public interface PageFilter {

  /**
   * Evaluates the filter against all positions in the given page.
   *
   * @param page the input page
   * @return a boolean array of length page.getPositionCount(), where true means the position passes
   */
  boolean[] filter(Page page);

  /** A filter that accepts all positions. */
  PageFilter ALWAYS_TRUE =
      page -> {
        boolean[] result = new boolean[page.getPositionCount()];
        java.util.Arrays.fill(result, true);
        return result;
      };
}
