/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

import io.trino.spi.Page;
import java.io.Closeable;

/**
 * Base interface for physical operators that process data in Trino's Page/Block columnar format.
 * Each operator pulls pages from its input (child operator) and produces output pages. Returns null
 * when no more data is available.
 */
public interface Operator extends Closeable {

  /**
   * Process and return the next batch of data as a {@link Page}.
   *
   * @return the next page of data, or {@code null} if no more data is available
   */
  Page processNextBatch();

  /** Release any resources held by this operator. */
  @Override
  void close();
}
