/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progressive;

import java.util.List;
import org.opensearch.sql.data.model.ExprValue;

/** Concurrent job-scoped storage for rows published by the primary query. */
interface QueryStateStore extends AutoCloseable {

  /** Returns a stable copy of the rows visible when this method is called. */
  List<ExprValue> rows();

  @Override
  void close();
}
