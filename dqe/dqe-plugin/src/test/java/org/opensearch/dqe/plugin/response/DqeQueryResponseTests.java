/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.response;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("DqeQueryResponse")
class DqeQueryResponseTests {

  @Nested
  @DisplayName("Builder defaults")
  class BuilderDefaults {

    @Test
    @DisplayName("default engine is dqe")
    void defaultEngine() {
      DqeQueryResponse response = DqeQueryResponse.builder().build();
      assertEquals("dqe", response.getEngine());
    }

    @Test
    @DisplayName("default schema is empty list")
    void defaultSchema() {
      DqeQueryResponse response = DqeQueryResponse.builder().build();
      assertTrue(response.getSchema().isEmpty());
    }

    @Test
    @DisplayName("default data is empty list")
    void defaultData() {
      DqeQueryResponse response = DqeQueryResponse.builder().build();
      assertTrue(response.getData().isEmpty());
    }

    @Test
    @DisplayName("default stats is null")
    void defaultStats() {
      DqeQueryResponse response = DqeQueryResponse.builder().build();
      assertNull(response.getStats());
    }
  }

  @Nested
  @DisplayName("Immutability")
  class Immutability {

    @Test
    @DisplayName("schema list is immutable")
    void schemaImmutable() {
      DqeQueryResponse response =
          DqeQueryResponse.builder()
              .schema(List.of(new DqeQueryResponse.ColumnSchema("a", "text")))
              .build();
      assertThrows(UnsupportedOperationException.class, () -> response.getSchema().clear());
    }

    @Test
    @DisplayName("data list is immutable")
    void dataImmutable() {
      DqeQueryResponse response =
          DqeQueryResponse.builder().data(List.of(List.of((Object) "val"))).build();
      assertThrows(UnsupportedOperationException.class, () -> response.getData().clear());
    }
  }

  @Nested
  @DisplayName("ColumnSchema")
  class ColumnSchemaTests {

    @Test
    @DisplayName("stores name and type")
    void storesNameAndType() {
      DqeQueryResponse.ColumnSchema col = new DqeQueryResponse.ColumnSchema("age", "integer");
      assertEquals("age", col.getName());
      assertEquals("integer", col.getType());
    }
  }

  @Nested
  @DisplayName("QueryStats")
  class QueryStatsTests {

    @Test
    @DisplayName("builder sets all fields")
    void builderSetsAllFields() {
      DqeQueryResponse.QueryStats stats =
          DqeQueryResponse.QueryStats.builder()
              .state("RUNNING")
              .queryId("qid")
              .elapsedMs(100)
              .rowsProcessed(500)
              .bytesProcessed(2048)
              .stages(3)
              .shardsQueried(10)
              .build();

      assertEquals("RUNNING", stats.getState());
      assertEquals("qid", stats.getQueryId());
      assertEquals(100, stats.getElapsedMs());
      assertEquals(500, stats.getRowsProcessed());
      assertEquals(2048, stats.getBytesProcessed());
      assertEquals(3, stats.getStages());
      assertEquals(10, stats.getShardsQueried());
    }

    @Test
    @DisplayName("default state is COMPLETED")
    void defaultState() {
      DqeQueryResponse.QueryStats stats = DqeQueryResponse.QueryStats.builder().build();
      assertEquals("COMPLETED", stats.getState());
    }
  }
}
