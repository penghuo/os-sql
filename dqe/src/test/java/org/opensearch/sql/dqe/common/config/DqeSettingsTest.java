/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.common.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

@DisplayName("DqeSettings")
class DqeSettingsTest {

  @Test
  @DisplayName("DQE_ENABLED defaults to false")
  void enabledDefaultsFalse() {
    assertEquals(false, DqeSettings.DQE_ENABLED.getDefault(Settings.EMPTY));
  }

  @Test
  @DisplayName("QUERY_TIMEOUT defaults to 30 seconds")
  void timeoutDefaults30s() {
    assertEquals(
        TimeValue.timeValueSeconds(30), DqeSettings.QUERY_TIMEOUT.getDefault(Settings.EMPTY));
  }

  @Test
  @DisplayName("PAGE_BATCH_SIZE defaults to 1024")
  void batchSizeDefaults1024() {
    assertEquals(1024, (int) DqeSettings.PAGE_BATCH_SIZE.getDefault(Settings.EMPTY));
  }

  @Test
  @DisplayName("settings() returns all three settings")
  void settingsListComplete() {
    assertEquals(3, DqeSettings.settings().size());
  }
}
