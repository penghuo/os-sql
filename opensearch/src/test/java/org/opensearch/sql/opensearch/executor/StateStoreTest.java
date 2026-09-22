/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;

class StateStoreTest {

  @Test
  void append_store_retains_old_snapshot_and_adds_segments() {
    StateStore store = new StateStore(StateStore.UpdateMode.APPEND);
    store.publish(List.of(row(1)));
    StateStore.Snapshot first = store.snapshot();

    store.publish(List.of(row(2), row(3)));
    StateStore.Snapshot second = store.snapshot();

    assertEquals(1, first.generation());
    assertEquals(1, first.rowCount());
    assertEquals(List.of(1), values(first));
    assertEquals(2, second.generation());
    assertEquals(3, second.rowCount());
    assertEquals(List.of(1, 2, 3), values(second));
  }

  @Test
  void replace_store_atomically_replaces_the_visible_snapshot() {
    StateStore store = new StateStore(StateStore.UpdateMode.REPLACE);
    store.publish(List.of(row(1), row(2)));
    StateStore.Snapshot first = store.snapshot();

    store.publish(List.of(row(7)));
    StateStore.Snapshot second = store.snapshot();

    assertEquals(List.of(1, 2), values(first));
    assertEquals(List.of(7), values(second));
    assertEquals(2, second.generation());
  }

  @Test
  void root_collector_publishes_first_row_immediately_then_batches() {
    StateStore store = new StateStore(StateStore.UpdateMode.APPEND);
    StateStoreRootCollector collector = new StateStoreRootCollector(store);

    collector.onRow(row(1));
    assertEquals(List.of(1), values(store.snapshot()));

    for (int value = 2; value <= 200; value++) {
      collector.onRow(row(value));
    }
    assertEquals(1, store.snapshot().rowCount());

    collector.onRow(row(201));
    assertEquals(201, store.snapshot().rowCount());
  }

  private static ExprValue row(int value) {
    return ExprValueUtils.tupleValue(Map.of("value", value));
  }

  private static List<Integer> values(StateStore.Snapshot snapshot) {
    return snapshot.segments().stream()
        .flatMap(List::stream)
        .map(row -> row.tupleValue().get("value").integerValue())
        .toList();
  }
}
