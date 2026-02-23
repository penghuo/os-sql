/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types.array;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Detects array fields by sampling documents from the index and checking for multi-valued fields.
 *
 * <p>This strategy samples the first N documents (configurable, default 100) and checks which
 * fields contain arrays (lists). The sampling function is injected to allow the caller (typically
 * dqe-metadata or dqe-execution) to provide the actual OpenSearch client call.
 *
 * <p>A field is considered multi-valued if any sampled document contains a list (array) value for
 * that field.
 */
public class SamplingStrategy implements ArrayDetectionStrategy {

  /** Default number of documents to sample for array detection. */
  public static final int DEFAULT_SAMPLE_SIZE = 100;

  private final int sampleSize;
  private final BiFunction<String, Integer, List<Map<String, Object>>> documentSampler;

  /**
   * Creates a sampling strategy.
   *
   * @param sampleSize number of documents to sample (must be > 0)
   * @param documentSampler function that fetches sample documents given (indexName, count). Each
   *     document is a Map of field path to value. The caller is responsible for providing the
   *     source maps from SearchHits.
   */
  public SamplingStrategy(
      int sampleSize, BiFunction<String, Integer, List<Map<String, Object>>> documentSampler) {
    if (sampleSize <= 0) {
      throw new IllegalArgumentException("sampleSize must be positive, got: " + sampleSize);
    }
    this.sampleSize = sampleSize;
    this.documentSampler =
        Objects.requireNonNull(documentSampler, "documentSampler must not be null");
  }

  @Override
  public Set<String> detectArrayFields(
      String indexName, Set<String> fieldPaths, Map<String, Object> indexMapping) {
    List<Map<String, Object>> docs = documentSampler.apply(indexName, sampleSize);
    if (docs == null || docs.isEmpty()) {
      return Collections.emptySet();
    }

    Set<String> arrayFields = new HashSet<>();
    for (Map<String, Object> doc : docs) {
      for (String fieldPath : fieldPaths) {
        if (arrayFields.contains(fieldPath)) {
          continue; // Already detected
        }
        Object value = extractNestedValue(doc, fieldPath);
        if (value instanceof List) {
          arrayFields.add(fieldPath);
        }
      }
    }
    return Collections.unmodifiableSet(arrayFields);
  }

  /** Returns the configured sample size. */
  public int getSampleSize() {
    return sampleSize;
  }

  /**
   * Extracts a value from a nested map using dot-notation path.
   *
   * @param doc the document map
   * @param fieldPath dot-separated field path (e.g., "address.city")
   * @return the value at the path, or null if not found
   */
  @SuppressWarnings("unchecked")
  static Object extractNestedValue(Map<String, Object> doc, String fieldPath) {
    String[] parts = fieldPath.split("\\.");
    Object current = doc;
    for (String part : parts) {
      if (!(current instanceof Map)) {
        return null;
      }
      current = ((Map<String, Object>) current).get(part);
      if (current == null) {
        return null;
      }
    }
    return current;
  }
}
