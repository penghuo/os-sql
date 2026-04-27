/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.omni.adapter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.trino.client.ClientTypeSignature;
import io.trino.client.Column;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine;

public class QueryResponseAdapterTest {

  @Test
  void adaptsSchemaAndRows() {
    List<Column> cols = List.of(
        new Column("id", "bigint", new ClientTypeSignature("bigint")),
        new Column("name", "varchar", new ClientTypeSignature("varchar")));
    List<List<Object>> rows = List.of(
        List.of(1L, "alice"),
        List.of(2L, "bob"));

    ExecutionEngine.QueryResponse response = QueryResponseAdapter.adapt(cols, rows);

    assertEquals(2, response.getSchema().getColumns().size());
    assertEquals("id", response.getSchema().getColumns().get(0).getName());
    assertEquals(ExprCoreType.LONG, response.getSchema().getColumns().get(0).getExprType());
    assertEquals("name", response.getSchema().getColumns().get(1).getName());
    assertEquals(ExprCoreType.STRING, response.getSchema().getColumns().get(1).getExprType());
    assertEquals(2, response.getResults().size());
    assertEquals(1L, response.getResults().get(0).tupleValue().get("id").longValue());
    assertEquals("alice", response.getResults().get(0).tupleValue().get("name").stringValue());
  }

  @Test
  void emptyRowsProducesEmptyResults() {
    List<Column> cols = List.of(new Column("x", "integer", new ClientTypeSignature("integer")));
    ExecutionEngine.QueryResponse response = QueryResponseAdapter.adapt(cols, List.of());
    assertEquals(1, response.getSchema().getColumns().size());
    assertTrue(response.getResults().isEmpty());
  }

  @Test
  void nullCellIsPreserved() {
    List<Column> cols = List.of(
        new Column("id", "bigint", new ClientTypeSignature("bigint")),
        new Column("name", "varchar", new ClientTypeSignature("varchar")));
    java.util.List<Object> row = new java.util.ArrayList<>();
    row.add(42L);
    row.add(null);
    List<List<Object>> rows = List.of(row);

    ExecutionEngine.QueryResponse response = QueryResponseAdapter.adapt(cols, rows);

    assertEquals(1, response.getResults().size());
    assertTrue(response.getResults().get(0).tupleValue().get("name").isNull());
  }
}
