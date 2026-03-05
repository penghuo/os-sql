/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.plan;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.parser.DqeException;

class PlanFragmentTests {

  @Test
  @DisplayName("round-trip serialize/deserialize preserves all fields")
  void roundTripPreservesAllFields() {
    PlanFragment original =
        new PlanFragment("SELECT * FROM accounts WHERE age > 30", "query-123", 300L, 1000);

    byte[] serialized = original.serialize();
    PlanFragment deserialized = PlanFragment.deserialize(serialized);

    assertEquals(original.getQueryText(), deserialized.getQueryText());
    assertEquals(original.getQueryId(), deserialized.getQueryId());
    assertEquals(original.getPitKeepAliveSeconds(), deserialized.getPitKeepAliveSeconds());
    assertEquals(original.getBatchSize(), deserialized.getBatchSize());
  }

  @Test
  @DisplayName("round-trip with unicode query text")
  void roundTripWithUnicode() {
    PlanFragment original = new PlanFragment("SELECT * FROM tëst WHERE name = '日本語'", "q-1", 60L, 500);

    byte[] serialized = original.serialize();
    PlanFragment deserialized = PlanFragment.deserialize(serialized);

    assertEquals(original.getQueryText(), deserialized.getQueryText());
  }

  @Test
  @DisplayName("round-trip with empty query text")
  void roundTripWithEmptyQuery() {
    PlanFragment original = new PlanFragment("", "q-empty", 0L, 0);

    byte[] serialized = original.serialize();
    PlanFragment deserialized = PlanFragment.deserialize(serialized);

    assertEquals("", deserialized.getQueryText());
    assertEquals("q-empty", deserialized.getQueryId());
    assertEquals(0L, deserialized.getPitKeepAliveSeconds());
    assertEquals(0, deserialized.getBatchSize());
  }

  @Test
  @DisplayName("deserialize rejects corrupted data")
  void deserializeRejectsCorruptedData() {
    byte[] corrupted = new byte[] {0, 0, 0, 99}; // version 99
    assertThrows(DqeException.class, () -> PlanFragment.deserialize(corrupted));
  }

  @Test
  @DisplayName("deserialize rejects null data")
  void deserializeRejectsNull() {
    assertThrows(NullPointerException.class, () -> PlanFragment.deserialize(null));
  }

  @Test
  @DisplayName("constructor rejects null queryText")
  void constructorRejectsNullQueryText() {
    assertThrows(NullPointerException.class, () -> new PlanFragment(null, "q1", 300L, 1000));
  }

  @Test
  @DisplayName("constructor rejects null queryId")
  void constructorRejectsNullQueryId() {
    assertThrows(NullPointerException.class, () -> new PlanFragment("SELECT 1", null, 300L, 1000));
  }

  @Test
  @DisplayName("serialize produces deterministic output")
  void serializeIsDeterministic() {
    PlanFragment fragment = new PlanFragment("SELECT 1", "q-det", 60L, 100);
    byte[] first = fragment.serialize();
    byte[] second = fragment.serialize();
    assertArrayEquals(first, second);
  }
}
