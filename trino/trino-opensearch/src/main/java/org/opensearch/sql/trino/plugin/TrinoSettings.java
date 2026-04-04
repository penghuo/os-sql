/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.plugin;

import java.util.List;
import org.opensearch.common.settings.Setting;

public class TrinoSettings {
  public static final Setting<Boolean> TRINO_ENABLED =
      Setting.boolSetting(
          "plugins.trino.enabled",
          false,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  public static final Setting<String> TRINO_CATALOG_WAREHOUSE =
      Setting.simpleString(
          "plugins.trino.catalog.iceberg.warehouse", "", Setting.Property.NodeScope);

  public static List<Setting<?>> settings() {
    return List.of(TRINO_ENABLED, TRINO_CATALOG_WAREHOUSE);
  }
}
