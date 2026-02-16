/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FragmentTypeTest {

  @Test
  @DisplayName("FragmentType enum should have exactly three values")
  void enumHasThreeValues() {
    assertEquals(3, FragmentType.values().length);
  }

  @Test
  @DisplayName("FragmentType values should be SOURCE, HASH, SINGLE")
  void enumValuesAreCorrect() {
    assertNotNull(FragmentType.SOURCE);
    assertNotNull(FragmentType.HASH);
    assertNotNull(FragmentType.SINGLE);
  }

  @Test
  @DisplayName("FragmentType valueOf should return correct enum")
  void valueOfReturnsCorrectEnum() {
    assertEquals(FragmentType.SOURCE, FragmentType.valueOf("SOURCE"));
    assertEquals(FragmentType.HASH, FragmentType.valueOf("HASH"));
    assertEquals(FragmentType.SINGLE, FragmentType.valueOf("SINGLE"));
  }
}
