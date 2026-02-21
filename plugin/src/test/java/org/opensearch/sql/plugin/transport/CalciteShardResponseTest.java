/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;

public class CalciteShardResponseTest {

  @Test
  public void testSuccessResponseRoundTrip() throws IOException {
    List<Object[]> rows =
        Arrays.asList(new Object[] {"Alice", 30, true}, new Object[] {"Bob", 25, false});
    List<String> columnNames = Arrays.asList("name", "age", "active");
    List<SqlTypeName> columnTypes =
        Arrays.asList(SqlTypeName.VARCHAR, SqlTypeName.INTEGER, SqlTypeName.BOOLEAN);

    CalciteShardResponse original = new CalciteShardResponse(rows, columnNames, columnTypes);

    CalciteShardResponse deserialized = roundTrip(original);

    assertNull(deserialized.getError());
    assertEquals(columnNames, deserialized.getColumnNames());
    assertEquals(columnTypes, deserialized.getColumnTypes());
    assertEquals(2, deserialized.getRows().size());
    assertArrayEquals(new Object[] {"Alice", 30, true}, deserialized.getRows().get(0));
    assertArrayEquals(new Object[] {"Bob", 25, false}, deserialized.getRows().get(1));
  }

  @Test
  public void testEmptyResultRoundTrip() throws IOException {
    List<Object[]> rows = List.of();
    List<String> columnNames = Arrays.asList("col1", "col2");
    List<SqlTypeName> columnTypes = Arrays.asList(SqlTypeName.VARCHAR, SqlTypeName.DOUBLE);

    CalciteShardResponse original = new CalciteShardResponse(rows, columnNames, columnTypes);

    CalciteShardResponse deserialized = roundTrip(original);

    assertNull(deserialized.getError());
    assertEquals(columnNames, deserialized.getColumnNames());
    assertEquals(columnTypes, deserialized.getColumnTypes());
    assertTrue(deserialized.getRows().isEmpty());
  }

  @Test
  public void testErrorResponseRoundTrip() throws IOException {
    Exception error = new RuntimeException("Shard execution failed: out of memory");

    CalciteShardResponse original = new CalciteShardResponse(error);

    CalciteShardResponse deserialized = roundTrip(original);

    assertNotNull(deserialized.getError());
    assertEquals("Shard execution failed: out of memory", deserialized.getError().getMessage());
    assertTrue(deserialized.getRows().isEmpty());
    assertTrue(deserialized.getColumnNames().isEmpty());
    assertTrue(deserialized.getColumnTypes().isEmpty());
  }

  @Test
  public void testErrorResponseWithNullMessage() throws IOException {
    Exception error = new RuntimeException((String) null);

    CalciteShardResponse original = new CalciteShardResponse(error);

    CalciteShardResponse deserialized = roundTrip(original);

    assertNotNull(deserialized.getError());
    assertEquals("Unknown error", deserialized.getError().getMessage());
  }

  @Test
  public void testResponseWithNullValues() throws IOException {
    List<Object[]> rows = Collections.singletonList(new Object[] {null, 42, null});
    List<String> columnNames = Arrays.asList("a", "b", "c");
    List<SqlTypeName> columnTypes =
        Arrays.asList(SqlTypeName.VARCHAR, SqlTypeName.INTEGER, SqlTypeName.BIGINT);

    CalciteShardResponse original = new CalciteShardResponse(rows, columnNames, columnTypes);

    CalciteShardResponse deserialized = roundTrip(original);

    assertNull(deserialized.getError());
    assertEquals(1, deserialized.getRows().size());
    assertArrayEquals(new Object[] {null, 42, null}, deserialized.getRows().get(0));
  }

  private CalciteShardResponse roundTrip(CalciteShardResponse response) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStreamStreamOutput out = new OutputStreamStreamOutput(baos);
    response.writeTo(out);
    out.flush();

    StreamInput in = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()));
    return new CalciteShardResponse(in);
  }
}
