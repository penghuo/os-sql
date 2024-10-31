/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.pagination.Cursor;

public class LivySession {

  private static final Logger LOG = LogManager.getLogger(LivySession.class);

  private static final String LIVY_URL = "http://localhost:8998";
  private static final CloseableHttpClient httpClient = HttpClients.createDefault();

  public final int sessionId;

  public LivySession(int sessionId) {
    this.sessionId = sessionId;
  }

  public static LivySession session() throws Exception {
    int sessionId = createSession();
    waitForSessionToBeReady(sessionId);
    LOG.info("created session with id {}", sessionId);

    return new LivySession(sessionId);
  }

  private static int createSession() throws IOException {
    HttpPost request = new HttpPost(LIVY_URL + "/sessions");
    String jsonPayload = "{\n" +
        "  \"kind\": \"sql\",\n" +
        "  \"conf\": {\n" +
        "    \"spark.sql.catalog.dev\": \"org.apache.spark.opensearch.catalog.OpenSearchCatalog\",\n" +
        "    \"spark.sql.catalog.dev.opensearch.port\": \"9200\",\n" +
        "    \"spark.sql.catalog.dev.opensearch.scheme\": \"http\",\n" +
        "    \"spark.sql.catalog.dev.opensearch.auth\": \"noauth\",\n" +
        "    \"spark.master\": \"local[*]\"\n" +
        "  }\n" +
        "}";
    request.setEntity(new StringEntity(jsonPayload, ContentType.APPLICATION_JSON));
    try (CloseableHttpResponse response = httpClient.execute(request)) {
      String jsonResponse = EntityUtils.toString(response.getEntity());
      JsonObject jsonObject = JsonParser.parseString(jsonResponse).getAsJsonObject();
      return jsonObject.get("id").getAsInt();
    }
  }

  private static void waitForSessionToBeReady(int sessionId) throws IOException, InterruptedException {
    String state;
    do {
      TimeUnit.SECONDS.sleep(5);  // Poll every 5 seconds
      HttpGet request = new HttpGet(LIVY_URL + "/sessions/" + sessionId);
      try (CloseableHttpResponse response = httpClient.execute(request)) {
        String jsonResponse = EntityUtils.toString(response.getEntity());
        JsonObject jsonObject = JsonParser.parseString(jsonResponse).getAsJsonObject();
        state = jsonObject.get("state").getAsString();
      }
    } while (!"idle".equalsIgnoreCase(state) && !"error".equalsIgnoreCase(state));

    if ("error".equalsIgnoreCase(state)) {
      throw new IllegalStateException("Session " + sessionId + " failed to start.");
    }
  }

  public int submitQuery(String query) throws IOException {
    HttpPost request = new HttpPost(LIVY_URL + "/sessions/" + sessionId + "/statements");
    request.setEntity(new StringEntity("{\"code\": \"" + query + "\"}", ContentType.APPLICATION_JSON));

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      String jsonResponse = EntityUtils.toString(response.getEntity());
      JsonObject jsonObject = JsonParser.parseString(jsonResponse).getAsJsonObject();
      return jsonObject.get("id").getAsInt();
    }
  }

  public ExecutionEngine.QueryResponse getQueryResult(int statementId) throws IOException, InterruptedException {
    String state;
    ExecutionEngine.QueryResponse output = null;

    do {
      TimeUnit.MILLISECONDS.sleep(100);

      HttpGet request = new HttpGet(LIVY_URL + "/sessions/" + sessionId + "/statements/" + statementId);
      try (CloseableHttpResponse response = httpClient.execute(request)) {
        String jsonResponse = EntityUtils.toString(response.getEntity());
        JsonObject jsonObject = JsonParser.parseString(jsonResponse).getAsJsonObject();
        state = jsonObject.get("state").getAsString();

        if ("available".equalsIgnoreCase(state)) {
          JsonObject outputObject = jsonObject.get("output").getAsJsonObject();
          String status = outputObject.get("status").getAsString();

          if ("ok".equalsIgnoreCase(status)) {
            output =
                parseQueryResponse( outputObject.getAsJsonObject("data").getAsJsonObject(
                    "application/json"));
          } else {
            String errorMessage = outputObject.get("evalue").getAsString();
            throw new IllegalStateException("Statement " + statementId + " failed with error: " + errorMessage);
          }
        }
      }
    } while (!"available".equalsIgnoreCase(state) && !"error".equalsIgnoreCase(state));

    if ("error".equalsIgnoreCase(state)) {
      throw new IllegalStateException("Statement " + statementId + " failed.");
    }

    return output;
  }

  public static void closeSession(int sessionId) throws IOException {
    HttpDelete request = new HttpDelete(LIVY_URL + "/sessions/" + sessionId);
    try (CloseableHttpResponse response = httpClient.execute(request)) {
      EntityUtils.consume(response.getEntity());
    }
  }

  public static ExecutionEngine.QueryResponse parseQueryResponse(JsonObject result) {
    JsonObject schema = result.get("schema").getAsJsonObject();
    JsonArray data = result.get("data").getAsJsonArray();

    List<ExecutionEngine.Schema.Column> columns = new ArrayList<>();
    JsonArray fields = schema.getAsJsonArray("fields");
    for (int i = 0; i < fields.size(); i++) {
      JsonObject field = fields.get(i).getAsJsonObject();

      String name = field.get("name").getAsString();
      String type = field.get("type").getAsString();
      ExecutionEngine.Schema.Column column = new ExecutionEngine.Schema.Column(name, null,
          new SparkDataType(type));
      columns.add(column);
    }
    List<ExprValue> results = new LinkedList<>();
    for (JsonElement element : data) {
      results.add(new ExprTupleValue(extractRow(element.getAsJsonArray(), columns)));
    }

    return new ExecutionEngine.QueryResponse(new ExecutionEngine.Schema(columns), results, Cursor.None);
  }

  private static LinkedHashMap<String, ExprValue> extractRow(
      JsonArray row, List<ExecutionEngine.Schema.Column> columnList) {
    LinkedHashMap<String, ExprValue> linkedHashMap = new LinkedHashMap<>();
    for(int i = 0; i < row.size(); i++) {
      JsonElement element = row.get(i);
      ExecutionEngine.Schema.Column column = columnList.get(i);
      SparkDataType dataType = (SparkDataType) column.getExprType();
      if (element.isJsonNull()) {
        linkedHashMap.put(column.getName(), ExprNullValue.of());
      } else {
        linkedHashMap.put(column.getName(), new SparkExprValue(dataType, element));
      }
    }

    return linkedHashMap;
  }
}
