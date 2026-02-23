/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types.mapping;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.types.DqeTypes;

@DisplayName("OpenSearchTypeMappingResolver")
class OpenSearchTypeMappingResolverTest {

  private OpenSearchTypeMappingResolver resolver;

  @BeforeEach
  void setUp() {
    resolver =
        new OpenSearchTypeMappingResolver(new MultiFieldResolver(), new DateFormatResolver());
  }

  @Nested
  @DisplayName("Simple type mappings")
  class SimpleTypes {

    @Test
    @DisplayName("keyword -> VARCHAR, sortable")
    void keyword() {
      ResolvedField result = resolver.resolve("name", "keyword", Map.of());
      assertEquals(DqeTypes.VARCHAR, result.getType());
      assertTrue(result.isSortable());
      assertNull(result.getKeywordSubField());
    }

    @Test
    @DisplayName("long -> BIGINT")
    void longType() {
      ResolvedField result = resolver.resolve("count", "long", Map.of());
      assertEquals(DqeTypes.BIGINT, result.getType());
      assertTrue(result.isSortable());
    }

    @Test
    @DisplayName("integer -> INTEGER")
    void integerType() {
      ResolvedField result = resolver.resolve("age", "integer", Map.of());
      assertEquals(DqeTypes.INTEGER, result.getType());
      assertTrue(result.isSortable());
    }

    @Test
    @DisplayName("short -> SMALLINT")
    void shortType() {
      ResolvedField result = resolver.resolve("val", "short", Map.of());
      assertEquals(DqeTypes.SMALLINT, result.getType());
      assertTrue(result.isSortable());
    }

    @Test
    @DisplayName("byte -> TINYINT")
    void byteType() {
      ResolvedField result = resolver.resolve("flag", "byte", Map.of());
      assertEquals(DqeTypes.TINYINT, result.getType());
      assertTrue(result.isSortable());
    }

    @Test
    @DisplayName("double -> DOUBLE")
    void doubleType() {
      ResolvedField result = resolver.resolve("score", "double", Map.of());
      assertEquals(DqeTypes.DOUBLE, result.getType());
      assertTrue(result.isSortable());
    }

    @Test
    @DisplayName("float -> REAL")
    void floatType() {
      ResolvedField result = resolver.resolve("temp", "float", Map.of());
      assertEquals(DqeTypes.REAL, result.getType());
      assertTrue(result.isSortable());
    }

    @Test
    @DisplayName("half_float -> REAL")
    void halfFloat() {
      ResolvedField result = resolver.resolve("val", "half_float", Map.of());
      assertEquals(DqeTypes.REAL, result.getType());
      assertTrue(result.isSortable());
    }

    @Test
    @DisplayName("boolean -> BOOLEAN")
    void booleanType() {
      ResolvedField result = resolver.resolve("active", "boolean", Map.of());
      assertEquals(DqeTypes.BOOLEAN, result.getType());
      assertTrue(result.isSortable());
    }

    @Test
    @DisplayName("ip -> VARCHAR, sortable")
    void ipType() {
      ResolvedField result = resolver.resolve("addr", "ip", Map.of());
      assertEquals(DqeTypes.VARCHAR, result.getType());
      assertTrue(result.isSortable());
    }

    @Test
    @DisplayName("geo_shape -> VARCHAR, not sortable")
    void geoShape() {
      ResolvedField result = resolver.resolve("boundary", "geo_shape", Map.of());
      assertEquals(DqeTypes.VARCHAR, result.getType());
      assertFalse(result.isSortable());
    }

    @Test
    @DisplayName("binary -> VARBINARY, not sortable")
    void binaryType() {
      ResolvedField result = resolver.resolve("data", "binary", Map.of());
      assertEquals(DqeTypes.VARBINARY, result.getType());
      assertFalse(result.isSortable());
    }
  }

  @Nested
  @DisplayName("Parameterized type mappings")
  class ParameterizedTypes {

    @Test
    @DisplayName("scaled_float with scaling_factor=100 -> DECIMAL(20,2)")
    void scaledFloat() {
      Map<String, Object> props = new HashMap<>();
      props.put("scaling_factor", 100);
      ResolvedField result = resolver.resolve("price", "scaled_float", props);
      assertTrue(result.getType().isNumeric());
      assertEquals(2, result.getType().getScale());
      assertEquals(20, result.getType().getPrecision());
      assertTrue(result.isSortable());
    }

    @Test
    @DisplayName("scaled_float with scaling_factor=1000 -> DECIMAL(21,3)")
    void scaledFloat1000() {
      Map<String, Object> props = new HashMap<>();
      props.put("scaling_factor", 1000);
      ResolvedField result = resolver.resolve("val", "scaled_float", props);
      assertEquals(3, result.getType().getScale());
      assertEquals(21, result.getType().getPrecision());
    }

    @Test
    @DisplayName("scaled_float without scaling_factor -> throws IllegalArgumentException")
    void scaledFloatNoFactor() {
      assertThrows(
          IllegalArgumentException.class, () -> resolver.resolve("val", "scaled_float", Map.of()));
    }

    @Test
    @DisplayName("unsigned_long -> DECIMAL(20,0)")
    void unsignedLong() {
      ResolvedField result = resolver.resolve("id", "unsigned_long", Map.of());
      assertEquals(DqeTypes.decimal(20, 0), result.getType());
      assertTrue(result.isSortable());
    }

    @Test
    @DisplayName("date default -> TIMESTAMP(3)")
    void dateDefault() {
      ResolvedField result = resolver.resolve("created", "date", Map.of());
      assertEquals(DqeTypes.TIMESTAMP_MILLIS, result.getType());
      assertTrue(result.isSortable());
    }

    @Test
    @DisplayName("date with epoch_millis format -> TIMESTAMP(3)")
    void dateEpochMillis() {
      Map<String, Object> props = new HashMap<>();
      props.put("format", "epoch_millis");
      ResolvedField result = resolver.resolve("ts", "date", props);
      assertEquals(DqeTypes.TIMESTAMP_MILLIS, result.getType());
    }

    @Test
    @DisplayName("date_nanos -> TIMESTAMP(9)")
    void dateNanos() {
      ResolvedField result = resolver.resolve("precise_ts", "date_nanos", Map.of());
      assertEquals(DqeTypes.TIMESTAMP_NANOS, result.getType());
      assertTrue(result.isSortable());
    }
  }

  @Nested
  @DisplayName("Complex type mappings")
  class ComplexTypes {

    @Test
    @DisplayName("geo_point -> ROW(lat DOUBLE, lon DOUBLE)")
    void geoPoint() {
      ResolvedField result = resolver.resolve("location", "geo_point", Map.of());
      assertTrue(result.getType().isRowType());
      assertFalse(result.isSortable());
      var fields = result.getType().getRowFields();
      assertEquals(2, fields.size());
      assertEquals("lat", fields.get(0).getName());
      assertEquals(DqeTypes.DOUBLE, fields.get(0).getType());
      assertEquals("lon", fields.get(1).getName());
    }

    @Test
    @DisplayName("flattened -> MAP(VARCHAR, VARCHAR)")
    void flattened() {
      ResolvedField result = resolver.resolve("labels", "flattened", Map.of());
      assertTrue(result.getType().isMapType());
      assertFalse(result.isSortable());
    }

    @Test
    @DisplayName("dense_vector -> ARRAY(REAL)")
    void denseVector() {
      ResolvedField result = resolver.resolve("embedding", "dense_vector", Map.of());
      assertTrue(result.getType().isArrayType());
      assertEquals(DqeTypes.REAL, result.getType().getElementType());
      assertFalse(result.isSortable());
    }

    @Test
    @DisplayName("nested with properties -> ARRAY(ROW(...))")
    void nested() {
      Map<String, Object> props = new HashMap<>();
      Map<String, Object> subProps = new LinkedHashMap<>();
      subProps.put("name", Map.of("type", "keyword"));
      subProps.put("age", Map.of("type", "integer"));
      props.put("properties", subProps);

      ResolvedField result = resolver.resolve("people", "nested", props);
      assertTrue(result.getType().isArrayType());
      assertTrue(result.isArray());
      var rowType = result.getType().getElementType();
      assertTrue(rowType.isRowType());
      var rowFields = rowType.getRowFields();
      assertEquals(2, rowFields.size());
    }

    @Test
    @DisplayName("object with properties -> ROW(...)")
    void objectType() {
      Map<String, Object> props = new HashMap<>();
      Map<String, Object> subProps = new LinkedHashMap<>();
      subProps.put("street", Map.of("type", "keyword"));
      subProps.put("city", Map.of("type", "keyword"));
      props.put("properties", subProps);

      ResolvedField result = resolver.resolve("address", "object", props);
      assertTrue(result.getType().isRowType());
      assertFalse(result.isSortable());
    }
  }

  @Nested
  @DisplayName("Text field with multi-fields")
  class TextField {

    @Test
    @DisplayName("text without keyword sub-field -> VARCHAR, not sortable")
    void textNoKeyword() {
      ResolvedField result = resolver.resolve("description", "text", Map.of());
      assertEquals(DqeTypes.VARCHAR, result.getType());
      assertFalse(result.isSortable());
      assertNull(result.getKeywordSubField());
    }

    @Test
    @DisplayName("text with keyword sub-field -> VARCHAR, with keyword path")
    void textWithKeyword() {
      Map<String, Object> props = new HashMap<>();
      Map<String, Object> fields = new HashMap<>();
      fields.put("keyword", Map.of("type", "keyword"));
      props.put("fields", fields);

      ResolvedField result = resolver.resolve("title", "text", props);
      assertEquals(DqeTypes.VARCHAR, result.getType());
      assertFalse(result.isSortable()); // text not sortable even with keyword sub-field
      assertEquals("title.keyword", result.getKeywordSubField());
    }

    @Test
    @DisplayName("text with fielddata=true -> sortable")
    void textWithFielddata() {
      Map<String, Object> props = new HashMap<>();
      props.put("fielddata", true);

      ResolvedField result = resolver.resolve("tags", "text", props);
      assertTrue(result.isSortable());
      assertTrue(result.hasFielddata());
    }
  }

  @Nested
  @DisplayName("resolveAll")
  class ResolveAll {

    @Test
    @DisplayName("Resolves flat mapping properties")
    void flatMapping() {
      Map<String, Object> mapping = new LinkedHashMap<>();
      mapping.put("name", Map.of("type", "keyword"));
      mapping.put("age", Map.of("type", "integer"));
      mapping.put("score", Map.of("type", "double"));

      Map<String, ResolvedField> result = resolver.resolveAll(mapping);
      assertEquals(3, result.size());
      assertEquals(DqeTypes.VARCHAR, result.get("name").getType());
      assertEquals(DqeTypes.INTEGER, result.get("age").getType());
      assertEquals(DqeTypes.DOUBLE, result.get("score").getType());
    }

    @Test
    @DisplayName("Resolves nested object fields with dot notation")
    void nestedObjects() {
      Map<String, Object> mapping = new LinkedHashMap<>();
      Map<String, Object> addressMapping = new HashMap<>();
      Map<String, Object> addressProps = new LinkedHashMap<>();
      addressProps.put("street", Map.of("type", "keyword"));
      addressProps.put("city", Map.of("type", "keyword"));
      addressMapping.put("type", "object");
      addressMapping.put("properties", addressProps);
      mapping.put("address", addressMapping);

      Map<String, ResolvedField> result = resolver.resolveAll(mapping);
      // Should have: address (ROW), address.street, address.city
      assertTrue(result.containsKey("address"));
      assertTrue(result.containsKey("address.street"));
      assertTrue(result.containsKey("address.city"));
      assertEquals(DqeTypes.VARCHAR, result.get("address.street").getType());
    }

    @Test
    @DisplayName("Resolves implicit object (no type field)")
    void implicitObject() {
      Map<String, Object> mapping = new LinkedHashMap<>();
      Map<String, Object> addressMapping = new HashMap<>();
      Map<String, Object> addressProps = new LinkedHashMap<>();
      addressProps.put("zip", Map.of("type", "keyword"));
      addressMapping.put("properties", addressProps);
      mapping.put("location", addressMapping);

      Map<String, ResolvedField> result = resolver.resolveAll(mapping);
      assertTrue(result.containsKey("location"));
      assertTrue(result.containsKey("location.zip"));
    }
  }

  @Test
  @DisplayName("Unsupported type throws IllegalArgumentException")
  void unsupportedType() {
    assertThrows(
        IllegalArgumentException.class, () -> resolver.resolve("field", "unknown_type", Map.of()));
  }

  @Test
  @DisplayName("Null arguments rejected")
  void nullRejected() {
    assertThrows(NullPointerException.class, () -> resolver.resolve(null, "keyword", Map.of()));
    assertThrows(NullPointerException.class, () -> resolver.resolve("f", null, Map.of()));
    assertThrows(NullPointerException.class, () -> resolver.resolve("f", "keyword", null));
    assertThrows(NullPointerException.class, () -> resolver.resolveAll(null));
  }
}
