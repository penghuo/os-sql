/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.conf;

import java.util.List;
import java.util.Map;
import lombok.Builder;
import org.opensearch.sql.datasource.model.DataSourceType;


public class DataSourceProperty {

  private Map<String, String> properties;

  private DataSourceProperty(Map<String, String> properties) {
    this.properties = properties;
  }

  public static DataSourceProperty load(Map<String, String> properties) {
    return new DataSourceProperty(properties);
  }

  public boolean validate(List<PropertyEntry> propertyEntryList) {
    return true;
  }

  public String get(PropertyEntry propertyEntry) {
    return properties.getOrDefault(propertyEntry.propertyKey(), propertyEntry.defaultValue);
  }

  @Builder(builderClassName = "Builder")
  static public class PropertyEntry {
    private DataSourceType dataSourceType;

    private String postfix;

    private String defaultValue;

    private Boolean isMandatory;

    public String propertyKey() {
      return String.join(".", dataSourceType.getText(), postfix);
    }

    public String getDefaultValue() {
      return defaultValue;
    }
  }
}
