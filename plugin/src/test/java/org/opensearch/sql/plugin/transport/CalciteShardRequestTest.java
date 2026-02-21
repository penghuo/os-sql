/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Test;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;

public class CalciteShardRequestTest {

  @Test
  public void testSerializationRoundTrip() throws IOException {
    String planJson = "{\"type\":\"LogicalProject\",\"fields\":[\"col1\"]}";
    String indexName = "test_index";
    int shardId = 3;

    CalciteShardRequest original = new CalciteShardRequest(planJson, indexName, shardId);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStreamStreamOutput out = new OutputStreamStreamOutput(baos);
    original.writeTo(out);
    out.flush();

    StreamInput in = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()));
    CalciteShardRequest deserialized = new CalciteShardRequest(in);

    assertEquals(planJson, deserialized.getPlanJson());
    assertEquals(indexName, deserialized.getIndexName());
    assertEquals(shardId, deserialized.getShardId());
  }

  @Test
  public void testSerializationWithEmptyPlan() throws IOException {
    CalciteShardRequest original = new CalciteShardRequest("", "my_index", 0);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStreamStreamOutput out = new OutputStreamStreamOutput(baos);
    original.writeTo(out);
    out.flush();

    StreamInput in = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()));
    CalciteShardRequest deserialized = new CalciteShardRequest(in);

    assertEquals("", deserialized.getPlanJson());
    assertEquals("my_index", deserialized.getIndexName());
    assertEquals(0, deserialized.getShardId());
  }

  @Test
  public void testValidateReturnsNull() {
    CalciteShardRequest request = new CalciteShardRequest("{}", "idx", 1);
    assertNull(request.validate());
  }
}
