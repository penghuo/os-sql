/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;

class PartialResultContextTest {

  @Test
  void captured_context_can_publish_from_another_thread() throws Exception {
    AtomicReference<List<ExprValue>> observed = new AtomicReference<>();
    PartialResultContext.Captured captured;
    try (PartialResultContext.Scope ignored = PartialResultContext.open(observed::set)) {
      captured = PartialResultContext.capture();
    }
    List<ExprValue> rows =
        List.of(
            ExprTupleValue.fromExprValueMap(java.util.Map.of("value", new ExprIntegerValue(1))));

    Thread worker =
        new Thread(
            () ->
                PartialResultContext.withContext(
                    captured,
                    () -> {
                      PartialResultContext.publishAggregationSnapshot(rows);
                      return null;
                    }));
    worker.start();
    worker.join();

    assertEquals(rows, observed.get());
  }
}
