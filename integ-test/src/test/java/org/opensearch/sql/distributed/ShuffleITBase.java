/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import static org.opensearch.sql.legacy.TestUtils.isIndexExist;

import java.io.IOException;
import java.util.Locale;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;

/**
 * Base class for IC-4 shuffle integration tests. Extends ScatterGatherITBase with additional test
 * indices for join and multi-stage query testing.
 *
 * <p>Additional indices:
 *
 * <ul>
 *   <li>{@code shuffle_dept} - Department dimension table (5 rows) for join tests
 *   <li>{@code shuffle_orders} - Orders fact table (500 rows, 5 shards) for join + window tests
 * </ul>
 */
public abstract class ShuffleITBase extends ScatterGatherITBase {

  protected static final String DEPT_INDEX = "shuffle_dept";
  protected static final String ORDERS_INDEX = "shuffle_orders";
  protected static final int ORDERS_DOC_COUNT = 500;

  @Override
  protected void init() throws Exception {
    super.init();
    supportAllJoinTypes();
    ensureDeptIndex();
    ensureOrdersIndex();
  }

  /** Creates a small department dimension table for join tests. */
  private void ensureDeptIndex() throws IOException {
    if (isIndexExist(client(), DEPT_INDEX)) {
      return;
    }

    String settings =
        "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0"
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"dept\": {\"type\": \"keyword\"},"
            + "    \"dept_head\": {\"type\": \"keyword\"},"
            + "    \"budget\": {\"type\": \"long\"},"
            + "    \"floor\": {\"type\": \"integer\"}"
            + "  }"
            + "}"
            + "}";

    Request createRequest = new Request("PUT", "/" + DEPT_INDEX);
    createRequest.setJsonEntity(settings);
    client().performRequest(createRequest);

    // Insert 5 department rows matching the departments in scatter_gather_test
    String[] depts = {"Engineering", "Sales", "Marketing", "Support", "HR"};
    String[] heads = {"Alice", "Bob", "Carol", "Dave", "Eve"};
    long[] budgets = {5000000, 3000000, 2000000, 1500000, 1000000};
    int[] floors = {3, 2, 2, 1, 1};

    StringBuilder bulk = new StringBuilder();
    for (int i = 0; i < depts.length; i++) {
      bulk.append(String.format(Locale.ROOT, "{\"index\":{\"_id\":\"%d\"}}\n", i));
      bulk.append(
          String.format(
              Locale.ROOT,
              "{\"dept\":\"%s\",\"dept_head\":\"%s\",\"budget\":%d,\"floor\":%d}\n",
              depts[i],
              heads[i],
              budgets[i],
              floors[i]));
    }

    Request bulkRequest = new Request("POST", "/" + DEPT_INDEX + "/_bulk");
    bulkRequest.setJsonEntity(bulk.toString());
    RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
    options.addHeader("Content-Type", "application/x-ndjson");
    bulkRequest.setOptions(options);
    Response response = client().performRequest(bulkRequest);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Request refreshRequest = new Request("POST", "/" + DEPT_INDEX + "/_refresh");
    client().performRequest(refreshRequest);
  }

  /** Creates an orders fact table (500 rows, 5 shards) for join and window tests. */
  private void ensureOrdersIndex() throws IOException {
    if (isIndexExist(client(), ORDERS_INDEX)) {
      return;
    }

    String settings =
        String.format(
            Locale.ROOT,
            "{"
                + "\"settings\": {"
                + "  \"number_of_shards\": %d,"
                + "  \"number_of_replicas\": 0"
                + "},"
                + "\"mappings\": {"
                + "  \"properties\": {"
                + "    \"order_id\": {\"type\": \"long\"},"
                + "    \"customer_id\": {\"type\": \"long\"},"
                + "    \"dept\": {\"type\": \"keyword\"},"
                + "    \"amount\": {\"type\": \"double\"},"
                + "    \"quantity\": {\"type\": \"integer\"},"
                + "    \"order_date\": {\"type\": \"date\"}"
                + "  }"
                + "}"
                + "}",
            SHARD_COUNT);

    Request createRequest = new Request("PUT", "/" + ORDERS_INDEX);
    createRequest.setJsonEntity(settings);
    client().performRequest(createRequest);

    String[] depts = {"Engineering", "Sales", "Marketing", "Support", "HR"};
    long baseTimestamp = 1700000000000L;

    StringBuilder bulk = new StringBuilder();
    for (int i = 0; i < ORDERS_DOC_COUNT; i++) {
      bulk.append(String.format(Locale.ROOT, "{\"index\":{\"_id\":\"%d\"}}\n", i));
      bulk.append(
          String.format(
              Locale.ROOT,
              "{\"order_id\":%d,\"customer_id\":%d,\"dept\":\"%s\","
                  + "\"amount\":%.2f,\"quantity\":%d,\"order_date\":%d}\n",
              i,
              i % 50, // 50 unique customers
              depts[i % depts.length],
              100.0 + (i * 10.0), // amounts 100-5090
              1 + (i % 10), // quantities 1-10
              baseTimestamp + (i * 3600000L))); // 1 hour apart
    }

    Request bulkRequest = new Request("POST", "/" + ORDERS_INDEX + "/_bulk");
    bulkRequest.setJsonEntity(bulk.toString());
    RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
    options.addHeader("Content-Type", "application/x-ndjson");
    bulkRequest.setOptions(options);
    Response response = client().performRequest(bulkRequest);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Request refreshRequest = new Request("POST", "/" + ORDERS_INDEX + "/_refresh");
    client().performRequest(refreshRequest);
  }
}
