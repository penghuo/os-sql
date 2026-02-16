/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ExchangeTypeTest {

  @Test
  @DisplayName("ExchangeType enum should have exactly four values")
  void enumHasFourValues() {
    assertEquals(4, ExchangeType.values().length);
  }

  @Test
  @DisplayName("ExchangeType values should be GATHER, HASH, BROADCAST, ROUND_ROBIN")
  void enumValuesAreCorrect() {
    assertNotNull(ExchangeType.GATHER);
    assertNotNull(ExchangeType.HASH);
    assertNotNull(ExchangeType.BROADCAST);
    assertNotNull(ExchangeType.ROUND_ROBIN);
  }

  @Test
  @DisplayName("ExchangeType valueOf should return correct enum")
  void valueOfReturnsCorrectEnum() {
    assertEquals(ExchangeType.GATHER, ExchangeType.valueOf("GATHER"));
    assertEquals(ExchangeType.HASH, ExchangeType.valueOf("HASH"));
    assertEquals(ExchangeType.BROADCAST, ExchangeType.valueOf("BROADCAST"));
    assertEquals(ExchangeType.ROUND_ROBIN, ExchangeType.valueOf("ROUND_ROBIN"));
  }
}
