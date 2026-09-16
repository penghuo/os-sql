/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.Test;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;

public class TransportPPLQueryActionTest {

  @Test
  public void async_row_formatting_preserves_integral_double_values() {
    LinkedHashMap<String, ExprValue> row = new LinkedHashMap<>();
    row.put("average", new ExprDoubleValue(45_000D));
    QueryResponse response =
        new QueryResponse(
            new Schema(List.of(new Column("average", null, ExprCoreType.DOUBLE))),
            List.of(ExprTupleValue.fromExprValueMap(row)),
            null);

    JsonObject json = TransportPPLQueryAction.formatRows(response, 0, 10, true);
    String serialized = new Gson().toJson(json);

    assertEquals(
        45_000D, json.getAsJsonArray("datarows").get(0).getAsJsonArray().get(0).getAsDouble(), 0D);
    assertTrue(serialized.contains("45000.0"));
  }
}
