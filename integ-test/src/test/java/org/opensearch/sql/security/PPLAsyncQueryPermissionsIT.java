/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.plugin.transport.PPLAsyncDeleteAction.NAME;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.plugin.transport.PPLAsyncGetResultAction;
import org.opensearch.sql.util.ClusterPlugins;

/** FGAC integration tests for asynchronous PPL submit, get, delete, and owner isolation. */
public class PPLAsyncQueryPermissionsIT extends SecurityTestBase {

  private static final String FULL_USER = "ppl_async_full_user";
  private static final String FULL_ROLE = "ppl_async_full_role";
  private static final String OTHER_USER = "ppl_async_other_user";
  private static final String OTHER_ROLE = "ppl_async_other_role";
  private static final String SUBMIT_ONLY_USER = "ppl_async_submit_user";
  private static final String SUBMIT_ONLY_ROLE = "ppl_async_submit_role";
  private static final String GET_USER = "ppl_async_get_user";
  private static final String GET_ROLE = "ppl_async_get_role";
  private static final String DELETE_USER = "ppl_async_delete_user";
  private static final String DELETE_ROLE = "ppl_async_delete_role";

  private static final String SUBMIT_ACTION = "cluster:admin/opensearch/ppl";
  private static final String GET_ACTION = PPLAsyncGetResultAction.NAME;
  private static final String DELETE_ACTION = NAME;
  private static final String[] INDEX_PERMISSIONS = {
    "indices:data/read/search*",
    "indices:admin/mappings/get",
    "indices:monitor/settings/get",
    "indices:data/read/point_in_time/create",
    "indices:data/read/point_in_time/delete",
    "indices:admin/resolve/index",
    "indices:data/read/field_caps*"
  };

  private boolean initialized;

  @Override
  protected void init() throws Exception {
    ClusterPlugins.requirePluginOrAssume(
        client(),
        ClusterPlugins.SECURITY_PLUGIN,
        "opensearch-security plugin not installed on test cluster; skipping FGAC tests");
    super.init();
    enableCalcite();
    loadIndex(Index.BANK);
    if (!initialized) {
      createRoleAndUser(FULL_ROLE, FULL_USER, SUBMIT_ACTION, GET_ACTION, DELETE_ACTION);
      createRoleAndUser(OTHER_ROLE, OTHER_USER, SUBMIT_ACTION, GET_ACTION, DELETE_ACTION);
      createRoleAndUser(SUBMIT_ONLY_ROLE, SUBMIT_ONLY_USER, SUBMIT_ACTION);
      createRoleAndUser(GET_ROLE, GET_USER, SUBMIT_ACTION, GET_ACTION);
      createRoleAndUser(DELETE_ROLE, DELETE_USER, SUBMIT_ACTION, DELETE_ACTION);
      initialized = true;
    }
  }

  @Test
  public void submitGetAndDeleteAreIndependentlyAuthorized() throws Exception {
    String submitOnlyId = submitForId(SUBMIT_ONLY_USER);
    assertForbidden(() -> request("GET", jobEndpoint(submitOnlyId), null, SUBMIT_ONLY_USER));
    assertForbidden(() -> request("DELETE", jobEndpoint(submitOnlyId), null, SUBMIT_ONLY_USER));

    String getId = submitForId(GET_USER);
    assertEquals(
        200, request("GET", jobEndpoint(getId), null, GET_USER).getStatusLine().getStatusCode());
    assertForbidden(() -> request("DELETE", jobEndpoint(getId), null, GET_USER));

    String deleteId = submitForId(DELETE_USER);
    assertForbidden(() -> request("GET", jobEndpoint(deleteId), null, DELETE_USER));
    JSONObject deleted =
        new JSONObject(
            getResponseBody(request("DELETE", jobEndpoint(deleteId), null, DELETE_USER), true));
    assertTrue(
        deleted.getString("status").equals("CANCELLED")
            || deleted.getString("status").equals("SUCCEEDED"));
  }

  @Test
  public void callerWithAllActionsCannotReadOrDeleteAnotherUsersJob() throws Exception {
    String id = submitForId(FULL_USER);

    assertForbidden(() -> request("GET", jobEndpoint(id), null, OTHER_USER));
    assertForbidden(() -> request("DELETE", jobEndpoint(id), null, OTHER_USER));

    Response ownerPoll = request("GET", jobEndpoint(id), null, FULL_USER);
    assertEquals(200, ownerPoll.getStatusLine().getStatusCode());
    Response ownerDelete = request("DELETE", jobEndpoint(id), null, FULL_USER);
    assertEquals(200, ownerDelete.getStatusLine().getStatusCode());
  }

  private void createRoleAndUser(String role, String user, String... clusterPermissions)
      throws IOException {
    createRoleWithPermissions(role, TEST_INDEX_BANK, clusterPermissions, INDEX_PERMISSIONS);
    createUser(user, role);
  }

  private String submitForId(String user) throws Exception {
    String query =
        String.format(
            Locale.ROOT,
            "source=%s | sort account_number | fields account_number, firstname",
            TEST_INDEX_BANK);
    for (int attempt = 0; attempt < 20; attempt++) {
      JSONObject response =
          new JSONObject(
              getResponseBody(
                  request(
                      "POST",
                      "/_plugins/_ppl",
                      new JSONObject()
                          .put("query", query)
                          .put("wait_for_completion_timeout", "0s")
                          .put("keep_alive", "1m")
                          .toString(),
                      user),
                  true));
      if (response.has("id")) {
        assertEquals("RUNNING", response.getString("status"));
        assertFalse(response.has("progress"));
        return response.getString("id");
      }
    }
    throw new AssertionError("Asynchronous query always completed before returning a job ID");
  }

  private Response request(String method, String endpoint, String body, String user)
      throws IOException {
    Request request = new Request(method, endpoint);
    if (body != null) {
      request.setJsonEntity(body);
    }
    RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
    options.addHeader("Content-Type", "application/json");
    options.addHeader("Authorization", createBasicAuthHeader(user, STRONG_PASSWORD));
    request.setOptions(options);
    return client().performRequest(request);
  }

  private static String jobEndpoint(String id) {
    return "/_plugins/_ppl/jobs/" + id;
  }

  private static void assertForbidden(ThrowingRequest request) {
    ResponseException exception = assertThrows(ResponseException.class, request::perform);
    assertEquals(403, exception.getResponse().getStatusLine().getStatusCode());
  }

  @FunctionalInterface
  private interface ThrowingRequest {
    void perform() throws IOException;
  }
}
