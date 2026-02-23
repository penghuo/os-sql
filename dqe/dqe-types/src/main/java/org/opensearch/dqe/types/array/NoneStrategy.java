/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types.array;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Fallback strategy that treats all fields as scalar (no arrays detected).
 *
 * <p>When this strategy is active, any multi-valued field access at runtime will produce a runtime
 * error rather than being wrapped in an ARRAY type.
 */
public class NoneStrategy implements ArrayDetectionStrategy {

  @Override
  public Set<String> detectArrayFields(
      String indexName, Set<String> fieldPaths, Map<String, Object> indexMapping) {
    return Collections.emptySet();
  }
}
