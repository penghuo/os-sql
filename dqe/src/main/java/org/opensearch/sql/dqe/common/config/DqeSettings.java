/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.common.config;

import java.util.List;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;

/** Cluster settings for the DQE module. */
public final class DqeSettings {

  public static final Setting<TimeValue> QUERY_TIMEOUT =
      Setting.timeSetting(
          "plugins.dqe.query.timeout",
          TimeValue.timeValueSeconds(30),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<Integer> PAGE_BATCH_SIZE =
      Setting.intSetting(
          "plugins.dqe.page.batch_size",
          10000,
          1,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  private DqeSettings() {}

  public static List<Setting<?>> settings() {
    return List.of(QUERY_TIMEOUT, PAGE_BATCH_SIZE);
  }
}
