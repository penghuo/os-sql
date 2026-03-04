/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

import io.trino.spi.Page;

/**
 * Physical operator that limits the number of output rows. Passes through pages from the child
 * operator until the specified row limit is reached, trimming the final page if necessary.
 */
public class LimitOperator implements Operator {

  private final Operator source;
  private final long limit;
  private long emitted;

  public LimitOperator(Operator source, long limit) {
    this.source = source;
    this.limit = limit;
    this.emitted = 0;
  }

  @Override
  public Page processNextBatch() {
    if (emitted >= limit) {
      return null;
    }

    Page page = source.processNextBatch();
    if (page == null) {
      return null;
    }

    long remaining = limit - emitted;
    if (page.getPositionCount() <= remaining) {
      emitted += page.getPositionCount();
      return page;
    }

    // Trim the page to only include the remaining rows
    Page trimmed = page.getRegion(0, (int) remaining);
    emitted += remaining;
    return trimmed;
  }

  @Override
  public void close() {
    source.close();
  }
}
