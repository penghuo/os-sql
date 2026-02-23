/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types.mapping;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.opensearch.dqe.types.DqeType;
import org.opensearch.dqe.types.DqeTypes;

/**
 * Converts OpenSearch field mapping types to {@link DqeType}. Handles all 22 OpenSearch types as
 * defined in design doc Section 9.1.
 *
 * <p>The mapping table:
 *
 * <ul>
 *   <li>{@code keyword} -> VARCHAR (sortable)
 *   <li>{@code text} -> VARCHAR (sortable only if fielddata:true)
 *   <li>{@code long} -> BIGINT
 *   <li>{@code integer} -> INTEGER
 *   <li>{@code short} -> SMALLINT
 *   <li>{@code byte} -> TINYINT
 *   <li>{@code double} -> DOUBLE
 *   <li>{@code float}, {@code half_float} -> REAL
 *   <li>{@code scaled_float} -> DECIMAL(p,s) derived from scaling_factor
 *   <li>{@code boolean} -> BOOLEAN
 *   <li>{@code date} -> TIMESTAMP(3)
 *   <li>{@code date_nanos} -> TIMESTAMP(9)
 *   <li>{@code ip} -> VARCHAR (lexicographic sort)
 *   <li>{@code geo_point} -> ROW(lat DOUBLE, lon DOUBLE)
 *   <li>{@code geo_shape} -> VARCHAR (GeoJSON)
 *   <li>{@code nested} -> ARRAY(ROW(...))
 *   <li>{@code object} -> ROW(...)
 *   <li>{@code flattened} -> MAP(VARCHAR, VARCHAR)
 *   <li>{@code dense_vector} -> ARRAY(REAL)
 *   <li>{@code binary} -> VARBINARY
 *   <li>{@code unsigned_long} -> DECIMAL(20,0)
 * </ul>
 */
public class OpenSearchTypeMappingResolver {

  private final MultiFieldResolver multiFieldResolver;
  private final DateFormatResolver dateFormatResolver;

  public OpenSearchTypeMappingResolver(
      MultiFieldResolver multiFieldResolver, DateFormatResolver dateFormatResolver) {
    this.multiFieldResolver =
        Objects.requireNonNull(multiFieldResolver, "multiFieldResolver must not be null");
    this.dateFormatResolver =
        Objects.requireNonNull(dateFormatResolver, "dateFormatResolver must not be null");
  }

  /**
   * Resolves a single field's OpenSearch type to a DqeType.
   *
   * @param fieldName the field name (leaf name, not full path)
   * @param osTypeName OpenSearch type name (e.g. "keyword", "long", "scaled_float")
   * @param mappingProperties the full properties map for this field
   * @return resolved field with type and metadata
   * @throws IllegalArgumentException if the OpenSearch type is unrecognized
   */
  public ResolvedField resolve(
      String fieldName, String osTypeName, Map<String, Object> mappingProperties) {
    Objects.requireNonNull(fieldName, "fieldName must not be null");
    Objects.requireNonNull(osTypeName, "osTypeName must not be null");
    Objects.requireNonNull(mappingProperties, "mappingProperties must not be null");

    return switch (osTypeName) {
      case "keyword" -> resolveKeyword(fieldName);
      case "text" -> resolveText(fieldName, mappingProperties);
      case "long" -> simpleField(fieldName, DqeTypes.BIGINT, true);
      case "integer" -> simpleField(fieldName, DqeTypes.INTEGER, true);
      case "short" -> simpleField(fieldName, DqeTypes.SMALLINT, true);
      case "byte" -> simpleField(fieldName, DqeTypes.TINYINT, true);
      case "double" -> simpleField(fieldName, DqeTypes.DOUBLE, true);
      case "float", "half_float" -> simpleField(fieldName, DqeTypes.REAL, true);
      case "scaled_float" -> resolveScaledFloat(fieldName, mappingProperties);
      case "boolean" -> simpleField(fieldName, DqeTypes.BOOLEAN, true);
      case "date" -> resolveDate(fieldName, mappingProperties);
      case "date_nanos" -> simpleField(fieldName, DqeTypes.TIMESTAMP_NANOS, true);
      case "ip" -> simpleField(fieldName, DqeTypes.VARCHAR, true);
      case "geo_point" -> resolveGeoPoint(fieldName);
      case "geo_shape" -> simpleField(fieldName, DqeTypes.VARCHAR, false);
      case "nested" -> resolveNested(fieldName, mappingProperties);
      case "object" -> resolveObject(fieldName, mappingProperties);
      case "flattened" -> resolveFlattened(fieldName);
      case "dense_vector" -> resolveDenseVector(fieldName);
      case "binary" -> simpleField(fieldName, DqeTypes.VARBINARY, false);
      case "unsigned_long" -> simpleField(fieldName, DqeTypes.decimal(20, 0), true);
      default -> throw new IllegalArgumentException("Unsupported OpenSearch type: " + osTypeName);
    };
  }

  /**
   * Resolves all fields from a complete index mapping.
   *
   * @param indexMapping the full mapping "properties" map from ClusterState
   * @return map of dot-notation field path to resolved field
   */
  public Map<String, ResolvedField> resolveAll(Map<String, Object> indexMapping) {
    Objects.requireNonNull(indexMapping, "indexMapping must not be null");
    Map<String, ResolvedField> result = new LinkedHashMap<>();
    resolveProperties("", indexMapping, result);
    return result;
  }

  @SuppressWarnings("unchecked")
  private void resolveProperties(
      String prefix, Map<String, Object> properties, Map<String, ResolvedField> result) {
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      String fieldName = entry.getKey();
      String fullPath = prefix.isEmpty() ? fieldName : prefix + "." + fieldName;

      if (!(entry.getValue() instanceof Map)) {
        continue;
      }

      Map<String, Object> fieldMapping = (Map<String, Object>) entry.getValue();
      String type = (String) fieldMapping.get("type");

      if (type != null) {
        ResolvedField resolved = resolve(fullPath, type, fieldMapping);
        result.put(fullPath, resolved);

        // For object/nested types, also resolve sub-properties
        if ("object".equals(type) || "nested".equals(type)) {
          Object subProperties = fieldMapping.get("properties");
          if (subProperties instanceof Map) {
            resolveProperties(fullPath, (Map<String, Object>) subProperties, result);
          }
        }
      } else {
        // No explicit type means object type (implicit)
        Object subProperties = fieldMapping.get("properties");
        if (subProperties instanceof Map) {
          // Resolve the implicit object itself
          ResolvedField objectField = resolveObject(fullPath, fieldMapping);
          result.put(fullPath, objectField);
          resolveProperties(fullPath, (Map<String, Object>) subProperties, result);
        }
      }
    }
  }

  private ResolvedField resolveKeyword(String fieldPath) {
    return new ResolvedField(fieldPath, DqeTypes.VARCHAR, true, null, false, false);
  }

  private ResolvedField resolveText(String fieldPath, Map<String, Object> mappingProperties) {
    Optional<MultiFieldResolver.MultiFieldInfo> multiField =
        multiFieldResolver.resolve(fieldPath, mappingProperties);

    String keywordSubField =
        multiField.map(MultiFieldResolver.MultiFieldInfo::getKeywordSubFieldPath).orElse(null);
    boolean hasFielddata =
        multiField.map(MultiFieldResolver.MultiFieldInfo::parentHasFielddata).orElse(false);

    if (!hasFielddata) {
      // Also check directly on the field mapping
      hasFielddata = parseFielddata(mappingProperties);
    }

    // Text fields are only sortable if fielddata is enabled
    boolean sortable = hasFielddata;

    return new ResolvedField(
        fieldPath, DqeTypes.VARCHAR, sortable, keywordSubField, hasFielddata, false);
  }

  private ResolvedField resolveScaledFloat(
      String fieldPath, Map<String, Object> mappingProperties) {
    Object scalingFactorObj = mappingProperties.get("scaling_factor");
    if (scalingFactorObj == null) {
      // scaling_factor is required for scaled_float in OpenSearch. Its absence indicates
      // a malformed mapping. Throw so callers can detect the problem early rather than
      // silently degrading to an imprecise DOUBLE representation.
      throw new IllegalArgumentException(
          "scaled_float field '" + fieldPath + "' is missing required 'scaling_factor' property");
    }

    double scalingFactor;
    if (scalingFactorObj instanceof Number) {
      scalingFactor = ((Number) scalingFactorObj).doubleValue();
    } else {
      scalingFactor = Double.parseDouble(scalingFactorObj.toString());
    }

    // Derive precision and scale from scaling_factor.
    // scaling_factor of 100 means 2 decimal places, 1000 means 3, etc.
    int scale = Math.max(0, (int) Math.ceil(Math.log10(scalingFactor)));
    // Total precision: enough for the integer part + scale digits.
    // OpenSearch scaled_float is stored as long internally, max ~18 digits.
    int precision = Math.min(38, 18 + scale);

    DqeType decimalType = DqeTypes.decimal(precision, scale);
    return new ResolvedField(fieldPath, decimalType, true, null, false, false);
  }

  private ResolvedField resolveDate(String fieldPath, Map<String, Object> mappingProperties) {
    String format = (String) mappingProperties.get("format");
    DateFormatResolver.DateFormatInfo dateInfo = dateFormatResolver.resolve(format);

    DqeType timestampType = DqeTypes.timestamp(dateInfo.getTimestampPrecision());
    return new ResolvedField(fieldPath, timestampType, true, null, false, false);
  }

  private ResolvedField resolveGeoPoint(String fieldPath) {
    // geo_point -> ROW(lat DOUBLE, lon DOUBLE)
    List<DqeType.RowField> fields =
        List.of(
            new DqeType.RowField("lat", DqeTypes.DOUBLE),
            new DqeType.RowField("lon", DqeTypes.DOUBLE));
    DqeType rowType = DqeTypes.row(fields);
    return new ResolvedField(fieldPath, rowType, false, null, false, false);
  }

  @SuppressWarnings("unchecked")
  private ResolvedField resolveNested(String fieldPath, Map<String, Object> mappingProperties) {
    // nested -> ARRAY(ROW(...))
    Object subProperties = mappingProperties.get("properties");
    DqeType rowType;
    if (subProperties instanceof Map) {
      List<DqeType.RowField> rowFields = resolveRowFields((Map<String, Object>) subProperties);
      if (rowFields.isEmpty()) {
        // Empty nested object: treat as ARRAY(ROW(dummy VARCHAR))
        rowFields = List.of(new DqeType.RowField("_empty", DqeTypes.VARCHAR));
      }
      rowType = DqeTypes.row(rowFields);
    } else {
      rowType = DqeTypes.row(List.of(new DqeType.RowField("_empty", DqeTypes.VARCHAR)));
    }
    DqeType arrayType = DqeTypes.array(rowType);
    return new ResolvedField(fieldPath, arrayType, false, null, false, true);
  }

  @SuppressWarnings("unchecked")
  private ResolvedField resolveObject(String fieldPath, Map<String, Object> mappingProperties) {
    // object -> ROW(...)
    Object subProperties = mappingProperties.get("properties");
    if (subProperties instanceof Map) {
      List<DqeType.RowField> rowFields = resolveRowFields((Map<String, Object>) subProperties);
      if (!rowFields.isEmpty()) {
        DqeType rowType = DqeTypes.row(rowFields);
        return new ResolvedField(fieldPath, rowType, false, null, false, false);
      }
    }
    // Empty or no sub-properties: treat as VARCHAR (serialized JSON)
    return new ResolvedField(fieldPath, DqeTypes.VARCHAR, false, null, false, false);
  }

  @SuppressWarnings("unchecked")
  private List<DqeType.RowField> resolveRowFields(Map<String, Object> properties) {
    List<DqeType.RowField> fields = new ArrayList<>();
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      String subFieldName = entry.getKey();
      if (!(entry.getValue() instanceof Map)) {
        continue;
      }
      Map<String, Object> subFieldMapping = (Map<String, Object>) entry.getValue();
      String subFieldType = (String) subFieldMapping.get("type");
      if (subFieldType != null) {
        ResolvedField resolved = resolve(subFieldName, subFieldType, subFieldMapping);
        fields.add(new DqeType.RowField(subFieldName, resolved.getType()));
      } else {
        // Implicit object sub-field
        Object innerProps = subFieldMapping.get("properties");
        if (innerProps instanceof Map) {
          List<DqeType.RowField> innerFields = resolveRowFields((Map<String, Object>) innerProps);
          if (!innerFields.isEmpty()) {
            fields.add(new DqeType.RowField(subFieldName, DqeTypes.row(innerFields)));
          }
        }
      }
    }
    return fields;
  }

  private ResolvedField resolveFlattened(String fieldPath) {
    // flattened -> MAP(VARCHAR, VARCHAR)
    DqeType mapType = DqeTypes.map(DqeTypes.VARCHAR, DqeTypes.VARCHAR);
    return new ResolvedField(fieldPath, mapType, false, null, false, false);
  }

  private ResolvedField resolveDenseVector(String fieldPath) {
    // dense_vector -> ARRAY(REAL)
    DqeType arrayType = DqeTypes.array(DqeTypes.REAL);
    return new ResolvedField(fieldPath, arrayType, false, null, false, false);
  }

  private static ResolvedField simpleField(String fieldPath, DqeType type, boolean sortable) {
    return new ResolvedField(fieldPath, type, sortable, null, false, false);
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
}
