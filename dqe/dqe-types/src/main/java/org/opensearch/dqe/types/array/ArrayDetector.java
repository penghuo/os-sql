/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types.array;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Orchestrator for array field detection. Selects the appropriate {@link ArrayDetectionStrategy}
 * based on the configured mode.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * ArrayDetector detector = new ArrayDetector(ArrayDetectionMode.META_ANNOTATION, 100, null);
 * Set<String> arrays = detector.detect("my-index", fieldPaths, indexMapping);
 * }</pre>
 */
public class ArrayDetector {

  private final ArrayDetectionStrategy strategy;
  private final ArrayDetectionMode mode;

  /**
   * Creates an ArrayDetector with the given mode and configuration.
   *
   * @param mode the detection mode
   * @param sampleSize sample size for SAMPLING mode (ignored for other modes)
   * @param documentSampler function to fetch sample documents for SAMPLING mode (null ok for other
   *     modes)
   */
  public ArrayDetector(
      ArrayDetectionMode mode,
      int sampleSize,
      BiFunction<String, Integer, List<Map<String, Object>>> documentSampler) {
    this.mode = Objects.requireNonNull(mode, "mode must not be null");
    this.strategy =
        switch (mode) {
          case META_ANNOTATION -> new MetaAnnotationStrategy();
          case SAMPLING -> {
            Objects.requireNonNull(documentSampler, "documentSampler required for SAMPLING mode");
            yield new SamplingStrategy(sampleSize, documentSampler);
          }
          case NONE -> new NoneStrategy();
        };
  }

  /**
   * Creates an ArrayDetector with a pre-built strategy (for testing).
   *
   * @param mode the mode label
   * @param strategy the strategy implementation
   */
  ArrayDetector(ArrayDetectionMode mode, ArrayDetectionStrategy strategy) {
    this.mode = Objects.requireNonNull(mode, "mode must not be null");
    this.strategy = Objects.requireNonNull(strategy, "strategy must not be null");
  }

  /**
   * Detects which fields are multi-valued arrays.
   *
   * @param indexName the index to inspect
   * @param fieldPaths set of field paths to check
   * @param indexMapping the full index mapping (including _meta if present)
   * @return the subset of fieldPaths that are multi-valued
   */
  public Set<String> detect(
      String indexName, Set<String> fieldPaths, Map<String, Object> indexMapping) {
    return strategy.detectArrayFields(indexName, fieldPaths, indexMapping);
  }

  /** Returns the configured detection mode. */
  public ArrayDetectionMode getMode() {
    return mode;
  }

  /** Returns the underlying strategy (for testing). */
  ArrayDetectionStrategy getStrategy() {
    return strategy;
  }

  /** Detection modes for array field discovery. */
  public enum ArrayDetectionMode {
    /** Use {@code _meta.dqe.arrays} annotation on the index mapping (preferred). */
    META_ANNOTATION,
    /** Sample first N documents to detect multi-valued fields. */
    SAMPLING,
    /** Treat all fields as scalar. Multi-value access causes runtime error. */
    NONE
  }
}
