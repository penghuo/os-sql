/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.sql.data.model.ExprValue;

/** Batches finalized Calcite root rows into an APPEND state store. */
final class StateStoreRootCollector implements CalciteResultSetMaterializer.RowObserver {
  private static final int BATCH_SIZE = 200;
  private final StateStore store;
  private List<ExprValue> pending = new ArrayList<>();

  StateStoreRootCollector(StateStore store) {
    this.store = store;
  }

  @Override
  public void onRow(ExprValue row) {
    pending.add(row);
    if (store.snapshot().rowCount() == 0 || pending.size() >= BATCH_SIZE) {
      flush();
    }
  }

  @Override
  public void finish() {
    flush();
  }

  private void flush() {
    if (!pending.isEmpty()) {
      store.publish(pending);
      pending = new ArrayList<>();
    }
  }
}
