/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types.mapping;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("MultiFieldResolver")
class MultiFieldResolverTest {

  private final MultiFieldResolver resolver = new MultiFieldResolver();

  @Test
  @DisplayName("No fields property returns empty")
  void noFields() {
    Optional<MultiFieldResolver.MultiFieldInfo> result =
        resolver.resolve("title", Map.of("type", "text"));
    assertTrue(result.isEmpty());
  }

  @Test
  @DisplayName("Empty fields map returns empty")
  void emptyFields() {
    Map<String, Object> mapping = new HashMap<>();
    mapping.put("type", "text");
    mapping.put("fields", Map.of());
    assertTrue(resolver.resolve("title", mapping).isEmpty());
  }

  @Test
  @DisplayName("Keyword sub-field detected")
  void keywordSubField() {
    Map<String, Object> mapping = new HashMap<>();
    Map<String, Object> fields = new HashMap<>();
    fields.put("keyword", Map.of("type", "keyword"));
    mapping.put("type", "text");
    mapping.put("fields", fields);

    Optional<MultiFieldResolver.MultiFieldInfo> result = resolver.resolve("title", mapping);
    assertTrue(result.isPresent());
    assertEquals("title.keyword", result.get().getKeywordSubFieldPath());
    assertFalse(result.get().parentHasFielddata());
  }

  @Test
  @DisplayName("Keyword sub-field with custom name")
  void customKeywordName() {
    Map<String, Object> mapping = new HashMap<>();
    Map<String, Object> fields = new HashMap<>();
    fields.put("raw", Map.of("type", "keyword"));
    mapping.put("type", "text");
    mapping.put("fields", fields);

    Optional<MultiFieldResolver.MultiFieldInfo> result = resolver.resolve("description", mapping);
    assertTrue(result.isPresent());
    assertEquals("description.raw", result.get().getKeywordSubFieldPath());
  }

  @Test
  @DisplayName("Text field with fielddata=true")
  void fielddataTrue() {
    Map<String, Object> mapping = new HashMap<>();
    Map<String, Object> fields = new HashMap<>();
    fields.put("keyword", Map.of("type", "keyword"));
    mapping.put("type", "text");
    mapping.put("fields", fields);
    mapping.put("fielddata", true);

    Optional<MultiFieldResolver.MultiFieldInfo> result = resolver.resolve("tags", mapping);
    assertTrue(result.isPresent());
    assertTrue(result.get().parentHasFielddata());
  }

  @Test
  @DisplayName("Text field with fielddata as string 'true'")
  void fielddataString() {
    Map<String, Object> mapping = new HashMap<>();
    Map<String, Object> fields = new HashMap<>();
    fields.put("keyword", Map.of("type", "keyword"));
    mapping.put("type", "text");
    mapping.put("fields", fields);
    mapping.put("fielddata", "true");

    Optional<MultiFieldResolver.MultiFieldInfo> result = resolver.resolve("tags", mapping);
    assertTrue(result.isPresent());
    assertTrue(result.get().parentHasFielddata());
  }

  @Test
  @DisplayName("Non-keyword sub-field types are ignored")
  void nonKeywordSubField() {
    Map<String, Object> mapping = new HashMap<>();
    Map<String, Object> fields = new HashMap<>();
    fields.put("analyzed", Map.of("type", "text", "analyzer", "english"));
    mapping.put("type", "text");
    mapping.put("fields", fields);

    assertTrue(resolver.resolve("content", mapping).isEmpty());
  }

  @Test
  @DisplayName("MultiFieldInfo equality")
  void multiFieldInfoEquality() {
    var info1 = new MultiFieldResolver.MultiFieldInfo("title.keyword", false);
    var info2 = new MultiFieldResolver.MultiFieldInfo("title.keyword", false);
    assertEquals(info1, info2);
    assertEquals(info1.hashCode(), info2.hashCode());
  }
}
