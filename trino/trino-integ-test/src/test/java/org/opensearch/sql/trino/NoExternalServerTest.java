/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;

/**
 * Integration test verifying that the Trino plugin integration does NOT start any external server
 * or open extra ports. All Trino functionality is accessed via the OpenSearch REST API.
 */
public class NoExternalServerTest extends OpenSearchRestTestCase {

  private static final Logger LOG = LogManager.getLogger(NoExternalServerTest.class);

  /**
   * Verify that Trino's default port 8080 is NOT in use. If this test fails, it means the plugin
   * has started a Trino HTTP server, which violates the integration design.
   */
  public void testTrinoDefaultPortNotInUse() {
    try (ServerSocket socket = new ServerSocket()) {
      socket.setReuseAddress(true);
      socket.bind(new InetSocketAddress("127.0.0.1", 8080));
      LOG.info("Port 8080 is available (not bound by Trino) -- good");
    } catch (IOException e) {
      fail("Port 8080 is in use. This indicates a Trino server was started, which is not allowed.");
    }
  }

  /**
   * Verify that POST to /_plugins/_trino_sql/v1/statement returns HTTP 200 with a valid Trino
   * protocol response.
   */
  public void testTrinoStatementEndpointReturns200() throws IOException {
    Request request = new Request("POST", "/_plugins/_trino_sql/v1/statement");
    request.setJsonEntity("SELECT 1");
    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());

    String body =
        new String(
            response.getEntity().getContent().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    LOG.info("Trino statement response: {}", body);

    // Verify the response contains expected Trino protocol fields
    assertTrue("Response should contain 'id' field", body.contains("\"id\""));
    assertTrue("Response should contain 'stats' field", body.contains("\"stats\""));
    assertTrue(
        "Response should contain FINISHED state", body.contains("\"state\":\"FINISHED\""));
  }

  /**
   * Verify that the trino_query, trino_exchange, and trino_scheduler thread pools are registered
   * and visible via /_cat/thread_pool.
   */
  public void testTrinoThreadPoolsRegistered() throws IOException {
    Request request = new Request("GET", "/_cat/thread_pool?v&h=name");
    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());

    String body =
        new String(
            response.getEntity().getContent().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    LOG.info("Thread pools: {}", body);

    assertTrue(
        "Thread pool 'trino_query' should be registered", body.contains("trino_query"));
    assertTrue(
        "Thread pool 'trino_exchange' should be registered",
        body.contains("trino_exchange"));
    assertTrue(
        "Thread pool 'trino_scheduler' should be registered",
        body.contains("trino_scheduler"));
  }
}
