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
   * Verify that SELECT 1 returns the correct result through the REST endpoint using the real
   * TrinoEngine (not a stub).
   */
  public void testSelectOneReturnsCorrectResult() throws IOException {
    Request request = new Request("POST", "/_plugins/_trino_sql/v1/statement");
    request.setJsonEntity("{\"query\": \"SELECT 1\"}");
    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());

    String body =
        new String(
            response.getEntity().getContent().readAllBytes(),
            java.nio.charset.StandardCharsets.UTF_8);
    LOG.info("SELECT 1 response: {}", body);

    // Verify columns are present
    assertTrue("Response should contain 'columns' field", body.contains("\"columns\""));
    assertTrue(
        "Response should contain column name '_col0'", body.contains("\"name\":\"_col0\""));
    assertTrue(
        "Response should contain column type 'integer'", body.contains("\"type\":\"integer\""));

    // Verify data is present with value 1
    assertTrue("Response should contain 'data' field", body.contains("\"data\""));
    assertTrue("Response should contain data [[1]]", body.contains("[1]"));

    // Verify state is FINISHED
    assertTrue(
        "Response should contain FINISHED state", body.contains("\"state\":\"FINISHED\""));
  }

  /**
   * Verify that SELECT with multiple columns returns correct results.
   */
  public void testSelectMultipleConstants() throws IOException {
    Request request = new Request("POST", "/_plugins/_trino_sql/v1/statement");
    request.setJsonEntity("{\"query\": \"SELECT 1, 'hello', true\"}");
    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());

    String body =
        new String(
            response.getEntity().getContent().readAllBytes(),
            java.nio.charset.StandardCharsets.UTF_8);
    LOG.info("SELECT multiple constants response: {}", body);

    assertTrue("Response should contain 'columns' field", body.contains("\"columns\""));
    assertTrue("Response should contain 'data' field", body.contains("\"data\""));
    assertTrue(
        "Response should contain FINISHED state", body.contains("\"state\":\"FINISHED\""));
    // Verify data contains the integer 1
    assertTrue("Response should contain integer value 1", body.contains("1"));
    // Verify data contains the string "hello"
    assertTrue("Response should contain string 'hello'", body.contains("\"hello\""));
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
