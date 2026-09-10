/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;

/**
 * Pins the v2 engine's behaviour for an object mapped with {@code disable_objects: true}, over both
 * the PPL and the SQL endpoint. The duplicate field of <a
 * href="https://github.com/opensearch-project/sql/issues/5746">issue 5746</a> is fixed on the
 * Calcite engine only (see {@code CalciteDisableObjectsIT}); v2 serves SQL always and PPL when
 * Calcite is off, and is deliberately left unchanged. This test exists so that the split is a
 * recorded decision rather than an accident.
 */
public class DisableObjectsIT extends PPLIntegTestCase {

  private static final String INDEX = "test_disable_objects_v2";

  @Override
  public void init() throws Exception {
    super.init(); // leaves Calcite disabled

    Request delete = new Request("DELETE", "/" + INDEX);
    delete.addParameter("ignore_unavailable", "true");
    client().performRequest(delete);

    Request create = new Request("PUT", "/" + INDEX);
    create.setJsonEntity(
        "{\"mappings\":{\"properties\":{\"attributes\":{\"disable_objects\":true,\"type\":\"object\"}}}}");
    client().performRequest(create);

    Request doc = new Request("PUT", "/" + INDEX + "/_doc/1?refresh=true");
    doc.setJsonEntity("{\"attributes\":{\"log.file.path\":\"/var/log/app.log\",\"logtag\":\"F\"}}");
    client().performRequest(doc);
  }

  /** PPL on v2 still surfaces the multi-level child next to its parent struct. */
  @Test
  public void ppl_on_v2_still_duplicates_the_multi_level_child() throws IOException {
    verifySchema(
        executeQuery("source=" + INDEX),
        schema("attributes", "struct"),
        schema("attributes.log.file.path", "string"));
  }

  /** SQL always runs on v2, so it duplicates too. */
  @Test
  public void sql_still_duplicates_the_multi_level_child() throws IOException {
    JSONObject result = executeJdbcRequest("SELECT * FROM " + INDEX);
    verifySchema(
        result, schema("attributes", "object"), schema("attributes.log.file.path", "text"));
  }

  /** Single-level children are hidden by v2's immediate-parent rule, as before. */
  @Test
  public void v2_hides_single_level_children() throws IOException {
    assertFalse(
        executeQuery("source=" + INDEX).getJSONArray("schema").toString().contains("logtag"));
  }
}
