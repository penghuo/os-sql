/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types.mapping;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Detects text+keyword sub-fields in OpenSearch field mappings.
 *
 * <p>For text fields with a keyword sub-field, records the sub-field path so that equality ({@code
 * =}), {@code IN}, and sorting operations can transparently use the {@code .keyword} sub-field for
 * pushdown, while full-text predicates use the text field.
 */
public class MultiFieldResolver {

  /**
   * Inspects a field mapping for multi-field (fields) definitions.
   *
   * @param fieldName the parent field name
   * @param fieldMapping the mapping node for this field (may contain a "fields" key)
   * @return multi-field info with sub-field details, or empty if no keyword sub-field
   */
  @SuppressWarnings("unchecked")
  public Optional<MultiFieldInfo> resolve(String fieldName, Map<String, Object> fieldMapping) {
    Objects.requireNonNull(fieldName, "fieldName must not be null");
    Objects.requireNonNull(fieldMapping, "fieldMapping must not be null");

    Object fieldsObj = fieldMapping.get("fields");
    if (!(fieldsObj instanceof Map)) {
      return Optional.empty();
    }

    Map<String, Object> fields = (Map<String, Object>) fieldsObj;

    // Look for a keyword sub-field
    for (Map.Entry<String, Object> entry : fields.entrySet()) {
      String subFieldName = entry.getKey();
      if (!(entry.getValue() instanceof Map)) {
        continue;
      }
      Map<String, Object> subFieldMapping = (Map<String, Object>) entry.getValue();
      String subFieldType = (String) subFieldMapping.get("type");
      if ("keyword".equals(subFieldType)) {
        String keywordSubFieldPath = fieldName + "." + subFieldName;
        boolean parentHasFielddata = parseFielddata(fieldMapping);
        return Optional.of(new MultiFieldInfo(keywordSubFieldPath, parentHasFielddata));
      }
    }

    return Optional.empty();
  }

  private static boolean parseFielddata(Map<String, Object> fieldMapping) {
    Object fielddataObj = fieldMapping.get("fielddata");
    if (fielddataObj instanceof Boolean) {
      return (Boolean) fielddataObj;
    }
    if (fielddataObj instanceof String) {
      return "true".equalsIgnoreCase((String) fielddataObj);
    }
    return false;
  }

  /** Information about a multi-field (text + keyword sub-field) configuration. */
  public static final class MultiFieldInfo {

    private final String keywordSubFieldPath;
    private final boolean parentHasFielddata;

    public MultiFieldInfo(String keywordSubFieldPath, boolean parentHasFielddata) {
      this.keywordSubFieldPath =
          Objects.requireNonNull(keywordSubFieldPath, "keywordSubFieldPath must not be null");
      this.parentHasFielddata = parentHasFielddata;
    }

    /** Returns the full path to the keyword sub-field (e.g. "title.keyword"). */
    public String getKeywordSubFieldPath() {
      return keywordSubFieldPath;
    }

    /** Returns true if the parent text field has fielddata enabled. */
    public boolean parentHasFielddata() {
      return parentHasFielddata;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof MultiFieldInfo other)) {
        return false;
      }
      return parentHasFielddata == other.parentHasFielddata
          && keywordSubFieldPath.equals(other.keywordSubFieldPath);
    }

    @Override
    public int hashCode() {
      return Objects.hash(keywordSubFieldPath, parentHasFielddata);
    }

    @Override
    public String toString() {
      return "MultiFieldInfo{keyword="
          + keywordSubFieldPath
          + ", fielddata="
          + parentHasFielddata
          + "}";
    }
  }
}
