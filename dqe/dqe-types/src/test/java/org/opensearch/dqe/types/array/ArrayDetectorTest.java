/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types.array;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("ArrayDetector (T-6)")
class ArrayDetectorTest {

  @Nested
  @DisplayName("MetaAnnotationStrategy")
  class MetaAnnotation {

    @Test
    @DisplayName("Detects array fields from _meta.dqe.arrays")
    void detectsFromMeta() {
      Map<String, Object> mapping = new HashMap<>();
      mapping.put(
          "_meta",
          Map.of("dqe", Map.of("arrays", List.of("tags", "nested.values", "unknown_field"))));
      mapping.put("properties", Map.of("tags", Map.of("type", "keyword")));

      ArrayDetector detector =
          new ArrayDetector(ArrayDetector.ArrayDetectionMode.META_ANNOTATION, 0, null);
      Set<String> arrays =
          detector.detect("my-index", Set.of("tags", "nested.values", "name"), mapping);

      assertEquals(Set.of("tags", "nested.values"), arrays);
    }

    @Test
    @DisplayName("Returns empty when no _meta key")
    void noMetaKey() {
      ArrayDetector detector =
          new ArrayDetector(ArrayDetector.ArrayDetectionMode.META_ANNOTATION, 0, null);
      Set<String> arrays = detector.detect("my-index", Set.of("tags"), Map.of());
      assertTrue(arrays.isEmpty());
    }

    @Test
    @DisplayName("Returns empty when _meta.dqe.arrays is missing")
    void metaWithoutArrays() {
      Map<String, Object> mapping = Map.of("_meta", Map.of("dqe", Map.of("version", "1.0")));
      ArrayDetector detector =
          new ArrayDetector(ArrayDetector.ArrayDetectionMode.META_ANNOTATION, 0, null);
      Set<String> arrays = detector.detect("my-index", Set.of("tags"), mapping);
      assertTrue(arrays.isEmpty());
    }

    @Test
    @DisplayName("Only includes fields that are in the fieldPaths set")
    void filtersToFieldPaths() {
      Map<String, Object> mapping =
          Map.of("_meta", Map.of("dqe", Map.of("arrays", List.of("tags", "extra"))));
      ArrayDetector detector =
          new ArrayDetector(ArrayDetector.ArrayDetectionMode.META_ANNOTATION, 0, null);
      Set<String> arrays = detector.detect("my-index", Set.of("tags"), mapping);
      assertEquals(Set.of("tags"), arrays);
    }
  }

  @Nested
  @DisplayName("SamplingStrategy")
  class Sampling {

    @Test
    @DisplayName("Detects multi-valued fields from sampled documents")
    void detectsFromSamples() {
      List<Map<String, Object>> docs =
          List.of(
              Map.of("name", "Alice", "tags", List.of("a", "b")),
              Map.of("name", "Bob", "tags", "single"));

      ArrayDetector detector =
          new ArrayDetector(ArrayDetector.ArrayDetectionMode.SAMPLING, 10, (index, size) -> docs);

      Set<String> arrays = detector.detect("my-index", Set.of("name", "tags"), Map.of());
      assertEquals(Set.of("tags"), arrays);
    }

    @Test
    @DisplayName("Returns empty when sampled docs have no arrays")
    void noArraysInSamples() {
      List<Map<String, Object>> docs =
          List.of(Map.of("name", "Alice", "age", 30), Map.of("name", "Bob", "age", 25));

      ArrayDetector detector =
          new ArrayDetector(ArrayDetector.ArrayDetectionMode.SAMPLING, 10, (index, size) -> docs);

      Set<String> arrays = detector.detect("my-index", Set.of("name", "age"), Map.of());
      assertTrue(arrays.isEmpty());
    }

    @Test
    @DisplayName("Handles nested field paths in sampled documents")
    void nestedFieldPaths() {
      List<Map<String, Object>> docs =
          List.of(
              Map.of("address", Map.of("city", "NYC", "tags", List.of("work", "home"))),
              Map.of("address", Map.of("city", "LA", "tags", "home")));

      ArrayDetector detector =
          new ArrayDetector(ArrayDetector.ArrayDetectionMode.SAMPLING, 10, (index, size) -> docs);

      Set<String> arrays =
          detector.detect("my-index", Set.of("address.city", "address.tags"), Map.of());
      assertEquals(Set.of("address.tags"), arrays);
    }

    @Test
    @DisplayName("Returns empty when sampler returns empty list")
    void emptySamples() {
      ArrayDetector detector =
          new ArrayDetector(
              ArrayDetector.ArrayDetectionMode.SAMPLING, 10, (index, size) -> List.of());

      Set<String> arrays = detector.detect("my-index", Set.of("name"), Map.of());
      assertTrue(arrays.isEmpty());
    }

    @Test
    @DisplayName("Sample size is configurable")
    void sampleSizeConfigurable() {
      var sampler = new SamplingStrategy(50, (index, size) -> List.of());
      assertEquals(50, sampler.getSampleSize());
    }

    @Test
    @DisplayName("Invalid sample size throws")
    void invalidSampleSize() {
      assertThrows(
          IllegalArgumentException.class,
          () -> new SamplingStrategy(0, (index, size) -> List.of()));
      assertThrows(
          IllegalArgumentException.class,
          () -> new SamplingStrategy(-1, (index, size) -> List.of()));
    }
  }

  @Nested
  @DisplayName("NoneStrategy")
  class None {

    @Test
    @DisplayName("Always returns empty set")
    void alwaysEmpty() {
      ArrayDetector detector = new ArrayDetector(ArrayDetector.ArrayDetectionMode.NONE, 0, null);
      Set<String> arrays = detector.detect("my-index", Set.of("tags", "name"), Map.of());
      assertTrue(arrays.isEmpty());
    }
  }

  @Nested
  @DisplayName("ArrayDetector configuration")
  class Configuration {

    @Test
    @DisplayName("Reports configured mode")
    void reportsMode() {
      ArrayDetector detector = new ArrayDetector(ArrayDetector.ArrayDetectionMode.NONE, 0, null);
      assertEquals(ArrayDetector.ArrayDetectionMode.NONE, detector.getMode());
    }

    @Test
    @DisplayName("SAMPLING mode requires documentSampler")
    void samplingRequiresSampler() {
      assertThrows(
          NullPointerException.class,
          () -> new ArrayDetector(ArrayDetector.ArrayDetectionMode.SAMPLING, 10, null));
    }

    @Test
    @DisplayName("All three modes are available")
    void allModesAvailable() {
      assertEquals(3, ArrayDetector.ArrayDetectionMode.values().length);
    }
  }
}
