/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types.array;

import java.util.Map;
import java.util.Set;

/**
 * Strategy interface for detecting which fields in an OpenSearch index are multi-valued (arrays).
 *
 * <p>OpenSearch does not distinguish arrays in mappings — any field can hold an array of values.
 * Implementations of this interface provide different mechanisms for detecting array fields.
 *
 * <p><b>R-note (signature deviation):</b> The spec defines a 2-parameter method {@code
 * detectArrayFields(String indexName, Set<String> fieldPaths)}, but this implementation uses 3
 * parameters — an additional {@code Map<String, Object> indexMapping} is required for the
 * MetaAnnotationStrategy to inspect {@code _meta.dqe.arrays} without an OpenSearch Client
 * dependency. The dqe-types module must remain free of OpenSearch Client classes; the caller
 * (dqe-metadata) provides the mapping as a pre-fetched Map. The SamplingStrategy similarly receives
 * a {@code BiFunction} document sampler instead of a direct Client reference.
 */
public interface ArrayDetectionStrategy {

  /**
   * Detects which field paths in the given index are multi-valued.
   *
   * @param indexName the OpenSearch index name
   * @param fieldPaths the set of field paths to check
   * @param indexMapping the full index mapping (may be used for _meta inspection)
   * @return the subset of fieldPaths that are multi-valued (arrays)
   */
  Set<String> detectArrayFields(
      String indexName, Set<String> fieldPaths, Map<String, Object> indexMapping);
}
