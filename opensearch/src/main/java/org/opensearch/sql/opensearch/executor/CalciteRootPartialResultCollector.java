/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.PartialResultResponseListener;
import org.opensearch.sql.executor.PartialResultResponseListener.UpdateMode;

/** Publishes semantically valid rows observed at the Calcite execution root. */
final class CalciteRootPartialResultCollector {
  private static final int BATCH_SIZE = 200;

  private final PartialResultResponseListener listener;
  private final UpdateMode updateMode;
  private final CalciteRootResultMaterializer materializer;
  private List<ExprValue> appendBatch = new ArrayList<>();
  private int nextPublishSize = 1;

  CalciteRootPartialResultCollector(
      PartialResultResponseListener listener,
      UpdateMode updateMode,
      CalciteRootResultMaterializer materializer) {
    this.listener = listener;
    this.updateMode = updateMode;
    this.materializer = materializer;
  }

  void onRow(ExprValue row, List<ExprValue> allRows) {
    if (updateMode == UpdateMode.APPEND) {
      appendBatch.add(row);
    }
    if (allRows.size() < nextPublishSize) {
      return;
    }

    publish(allRows);
    nextPublishSize =
        allRows.size() == 1
            ? BATCH_SIZE
            : Math.min(
                Integer.MAX_VALUE, Math.max(allRows.size() + BATCH_SIZE, nextPublishSize * 2));
  }

  void finish() {
    if (updateMode == UpdateMode.APPEND && !appendBatch.isEmpty()) {
      listener.onPartial(materializer.response(appendBatch));
      appendBatch = new ArrayList<>();
    }
  }

  private void publish(List<ExprValue> allRows) {
    if (updateMode == UpdateMode.APPEND) {
      listener.onPartial(materializer.response(appendBatch));
      appendBatch = new ArrayList<>();
    } else {
      listener.onPartial(materializer.response(allRows));
    }
  }
}
