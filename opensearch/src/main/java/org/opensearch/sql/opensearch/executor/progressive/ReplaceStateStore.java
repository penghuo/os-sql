/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progressive;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.opensearch.sql.data.model.ExprValue;

/** State store for aggregation leaf rows where each reduce supersedes the previous reduce. */
final class ReplaceStateStore implements QueryStateStore {
  private final AtomicReference<List<ExprValue>> rows = new AtomicReference<>(List.of());

  void replace(List<ExprValue> replacement) {
    rows.set(List.copyOf(replacement));
  }

  @Override
  public List<ExprValue> rows() {
    return rows.get();
  }

  @Override
  public void close() {
    rows.set(List.of());
  }
}
