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
  @DisplayName("QUERY_TIMEOUT defaults to 30 seconds")
  void timeoutDefaults30s() {
    assertEquals(
        TimeValue.timeValueSeconds(30), DqeSettings.QUERY_TIMEOUT.getDefault(Settings.EMPTY));
  }

  @Test
  @DisplayName("PAGE_BATCH_SIZE defaults to 1000000")
  void batchSizeDefaults1000000() {
    assertEquals(1000000, (int) DqeSettings.PAGE_BATCH_SIZE.getDefault(Settings.EMPTY));
  }

  @Test
  @DisplayName("settings() returns both settings")
  void settingsListComplete() {
    assertEquals(2, DqeSettings.settings().size());
  }
}
