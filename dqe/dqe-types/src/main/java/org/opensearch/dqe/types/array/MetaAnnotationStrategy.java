/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types.array;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Detects array fields from the {@code _meta.dqe.arrays} annotation in the index mapping.
 *
 * <p>This is the preferred detection mode. Users annotate their index mapping with an explicit list
 * of array field paths:
 *
 * <pre>{@code
 * PUT my-index
 * {
 *   "mappings": {
 *     "_meta": {
 *       "dqe": {
 *         "arrays": ["tags", "nested.values"]
 *       }
 *     },
 *     "properties": { ... }
 *   }
 * }
 * }</pre>
 */
public class MetaAnnotationStrategy implements ArrayDetectionStrategy {

  /** The path within the index mapping to the array annotation: _meta.dqe.arrays */
  static final String META_KEY = "_meta";

  static final String DQE_KEY = "dqe";
  static final String ARRAYS_KEY = "arrays";

  @Override
  @SuppressWarnings("unchecked")
  public Set<String> detectArrayFields(
      String indexName, Set<String> fieldPaths, Map<String, Object> indexMapping) {
    // Navigate _meta.dqe.arrays
    Object metaObj = indexMapping.get(META_KEY);
    if (!(metaObj instanceof Map)) {
      return Collections.emptySet();
    }

    Map<String, Object> meta = (Map<String, Object>) metaObj;
    Object dqeObj = meta.get(DQE_KEY);
    if (!(dqeObj instanceof Map)) {
      return Collections.emptySet();
    }

    Map<String, Object> dqe = (Map<String, Object>) dqeObj;
    Object arraysObj = dqe.get(ARRAYS_KEY);
    if (!(arraysObj instanceof Collection)) {
      return Collections.emptySet();
    }

    Collection<String> annotatedArrays = (Collection<String>) arraysObj;
    Set<String> result = new HashSet<>();
    for (String annotated : annotatedArrays) {
      if (fieldPaths.contains(annotated)) {
        result.add(annotated);
      }
    }
    return Collections.unmodifiableSet(result);
  }
}
