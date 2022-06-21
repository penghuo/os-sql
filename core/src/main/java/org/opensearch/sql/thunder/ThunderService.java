/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.thunder;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ThunderService {

  private final Map<String, String> cache;

  public ThunderService() {
    cache = new HashMap<>();
  }

  synchronized public void put(String tableName, String indexName) {
    cache.put(tableName, indexName);
  }

  synchronized public Optional<String> get(String tableName) {
    return Optional.ofNullable(cache.get(tableName));
  }
}
