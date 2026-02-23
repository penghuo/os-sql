/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.pit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.unit.TimeValue;

class PitHandleTests {

  @Test
  @DisplayName("getters return constructor values")
  void gettersReturnConstructorValues() {
    TimeValue keepAlive = TimeValue.timeValueMinutes(5);
    PitHandle handle = new PitHandle("pit-123", "my-index", keepAlive);

    assertEquals("pit-123", handle.getPitId());
    assertEquals("my-index", handle.getIndexName());
    assertEquals(keepAlive, handle.getKeepAlive());
  }

  @Test
  @DisplayName("isReleased defaults to false")
  void isReleasedDefaultsFalse() {
    PitHandle handle = new PitHandle("pit-123", "my-index", TimeValue.timeValueMinutes(5));
    assertFalse(handle.isReleased());
  }

  @Test
  @DisplayName("markReleased sets released flag")
  void markReleasedSetsFlag() {
    PitHandle handle = new PitHandle("pit-123", "my-index", TimeValue.timeValueMinutes(5));
    handle.markReleased();
    assertTrue(handle.isReleased());
  }

  @Test
  @DisplayName("markReleased is idempotent")
  void markReleasedIdempotent() {
    PitHandle handle = new PitHandle("pit-123", "my-index", TimeValue.timeValueMinutes(5));
    handle.markReleased();
    handle.markReleased(); // second call should not fail
    assertTrue(handle.isReleased());
  }

  @Test
  @DisplayName("constructor rejects null pitId")
  void rejectsNullPitId() {
    assertThrows(
        NullPointerException.class,
        () -> new PitHandle(null, "my-index", TimeValue.timeValueMinutes(5)));
  }

  @Test
  @DisplayName("constructor rejects null indexName")
  void rejectsNullIndexName() {
    assertThrows(
        NullPointerException.class,
        () -> new PitHandle("pit-123", null, TimeValue.timeValueMinutes(5)));
  }

  @Test
  @DisplayName("constructor rejects null keepAlive")
  void rejectsNullKeepAlive() {
    assertThrows(NullPointerException.class, () -> new PitHandle("pit-123", "my-index", null));
  }

  @Test
  @DisplayName("toString contains index name and pit id")
  void toStringContainsInfo() {
    PitHandle handle = new PitHandle("pit-123", "my-index", TimeValue.timeValueMinutes(5));
    String str = handle.toString();
    assertTrue(str.contains("my-index"));
    assertTrue(str.contains("pit-123"));
  }
}
