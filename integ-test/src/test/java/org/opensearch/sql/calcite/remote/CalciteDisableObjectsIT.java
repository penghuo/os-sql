/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * An object mapped with {@code disable_objects: true} declares children whose names keep their dots
 * instead of being expanded into intermediate object mappers, so {@code attributes} can declare one
 * child literally named {@code log.file.path} and there is no {@code attributes.log} level in
 * between. A bare {@code *} must still hide that child behind its parent struct. See <a
 * href="https://github.com/opensearch-project/sql/issues/5746">issue 5746</a>.
 */
public class CalciteDisableObjectsIT extends PPLIntegTestCase {

  /** disable_objects: true */
  private static final String FLAT = "test-disable-objects";

  /** control: same document, no disable_objects */
  private static final String NESTED = "test-normal-objects";

  /** control: same document, disable_objects explicitly false */
  private static final String FALSE = "test-disable-objects-false";

  /** disable_objects with a scalar child whose name is a prefix of another child */
  private static final String PREFIX = "test-disable-objects-prefix";

  /** object with dynamic mapping off, so its contents are a MAP the mapping does not declare */
  private static final String UNMAPPED = "test-unmapped-object";

  /** two scalar roots beside the object, so an expression can reference two distinct columns */
  private static final String TWO_ROOTS = "test-disable-objects-two-roots";

  /** join probe: declares a scalar {@code attributes}, unrelated to the other index's object */
  private static final String JOIN_LEFT = "test-disable-objects-join-left";

  /** join probe: declares the object, joined on id */
  private static final String JOIN_RIGHT = "test-disable-objects-join-right";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    create(
        FLAT,
        "{\"mappings\":{\"properties\":{\"attributes\":{\"disable_objects\":true,\"type\":\"object\"}}}}",
        "{\"attributes\":{\"log.file.path\":\"/var/log/app.log\",\"logtag\":\"F\"}}");
    create(
        NESTED,
        "{\"mappings\":{\"properties\":{\"attributes\":{\"type\":\"object\"}}}}",
        "{\"attributes\":{\"log.file.path\":\"/var/log/app.log\",\"logtag\":\"F\"}}");
    create(
        FALSE,
        "{\"mappings\":{\"properties\":{\"attributes\":{\"disable_objects\":false,\"type\":\"object\"}}}}",
        "{\"attributes\":{\"log.file.path\":\"/var/log/app.log\",\"logtag\":\"F\"}}");
    create(
        PREFIX,
        "{\"mappings\":{\"properties\":{\"attributes\":{\"disable_objects\":true,\"type\":\"object\","
            + "\"properties\":{\"log\":{\"type\":\"keyword\"},"
            + "\"log.file.path\":{\"type\":\"keyword\"}}}}}}",
        "{\"attributes\":{\"log\":\"app\",\"log.file.path\":\"/var/log/app.log\"}}");
    create(
        UNMAPPED,
        "{\"mappings\":{\"properties\":{\"doc\":{\"type\":\"object\",\"dynamic\":false}}}}",
        "{\"doc\":{\"user\":{\"name\":\"alice\"}}}");
    create(
        TWO_ROOTS,
        "{\"mappings\":{\"properties\":{\"id\":{\"type\":\"keyword\"},"
            + "\"tag\":{\"type\":\"keyword\"},"
            + "\"attributes\":{\"disable_objects\":true,\"type\":\"object\"}}}}",
        "{\"id\":\"a\",\"tag\":\"b\",\"attributes\":{\"log.file.path\":\"/var/log/app.log\"}}");
    create(
        JOIN_LEFT,
        "{\"mappings\":{\"properties\":{\"id\":{\"type\":\"keyword\"},"
            + "\"attributes\":{\"type\":\"keyword\"}}}}",
        "{\"id\":\"1\",\"attributes\":\"left-scalar\"}");
    create(
        JOIN_RIGHT,
        "{\"mappings\":{\"properties\":{\"id\":{\"type\":\"keyword\"},"
            + "\"attributes\":{\"disable_objects\":true,\"type\":\"object\"}}}}",
        "{\"id\":\"1\",\"attributes\":{\"log.file.path\":\"/var/log/app.log\"}}");
  }

  private void create(String index, String mapping, String document) throws IOException {
    Request delete = new Request("DELETE", "/" + index);
    delete.addParameter("ignore_unavailable", "true");
    client().performRequest(delete);

    Request create = new Request("PUT", "/" + index);
    create.setJsonEntity(mapping);
    client().performRequest(create);

    Request doc = new Request("PUT", "/" + index + "/_doc/1?refresh=true");
    doc.setJsonEntity(document);
    client().performRequest(doc);
  }

  /** Guards the mapping shape the fix relies on: the dotted key stays a single property. */
  @Test
  public void disable_objects_keeps_dotted_property_name() throws IOException {
    assertTrue(
        "expected a single flat property, got " + properties(FLAT),
        properties(FLAT).has("log.file.path") && !properties(FLAT).has("log"));
    assertTrue(
        "expected expanded object properties, got " + properties(NESTED),
        properties(NESTED).has("log") && !properties(NESTED).has("log.file.path"));
  }

  private JSONObject properties(String index) throws IOException {
    return new JSONObject(executeRequest(new Request("GET", "/" + index + "/_mapping")))
        .getJSONObject(index)
        .getJSONObject("mappings")
        .getJSONObject("properties")
        .getJSONObject("attributes")
        .getJSONObject("properties");
  }

  @Test
  public void no_duplicate_fields_in_schema() throws IOException {
    verifySchema(executeQuery("source=" + FLAT), schema("attributes", "struct"));
  }

  @Test
  public void control_no_disable_objects_has_single_struct_column() throws IOException {
    verifySchema(executeQuery("source=" + NESTED), schema("attributes", "struct"));
  }

  @Test
  public void control_disable_objects_false_has_single_struct_column() throws IOException {
    verifySchema(executeQuery("source=" + FALSE), schema("attributes", "struct"));
  }

  @Test
  public void flat_leaf_is_still_selectable() throws IOException {
    JSONObject result = executeQuery("source=" + FLAT + " | fields attributes.log.file.path");
    verifySchema(result, schema("attributes.log.file.path", "string"));
    verifyDataRows(result, rows("/var/log/app.log"));
  }

  /**
   * The parent struct is gone from the row type by the time the implicit trailing {@code *} runs,
   * so the group key must survive. Guards against deciding removal from a table-level snapshot
   * taken at scan time rather than from the row type in hand.
   */
  @Test
  public void group_key_survives_when_parent_struct_is_not_in_the_output() throws IOException {
    JSONObject result =
        executeQuery("source=" + FLAT + " | stats count() by attributes.log.file.path");
    verifySchema(result, schema("count()", "bigint"), schema("attributes.log.file.path", "string"));
    verifyDataRows(result, rows(1, "/var/log/app.log"));
  }

  @Test
  public void sort_keeps_parent_struct_only() throws IOException {
    verifySchema(
        executeQuery("source=" + FLAT + " | sort attributes.log.file.path"),
        schema("attributes", "struct"));
  }

  @Test
  public void filter_on_flat_leaf_keeps_parent_struct_only() throws IOException {
    JSONObject result =
        executeQuery("source=" + FLAT + " | where attributes.log.file.path = '/var/log/app.log'");
    verifySchema(result, schema("attributes", "struct"));
    verifyDataRows(
        result,
        rows(Map.of("log", Map.of("file", Map.of("path", "/var/log/app.log")), "logtag", "F")));
  }

  @Test
  public void flatten_expands_the_declared_child() throws IOException {
    verifyDataRows(
        executeQuery("source=" + FLAT + " | flatten attributes | fields `log.file.path`"),
        rows("/var/log/app.log"));
  }

  /**
   * A scalar child whose name is a prefix of another declared child. Both are declared by {@code
   * attributes} and so both are carried by it.
   */
  @Test
  public void scalar_child_sharing_a_prefix_is_also_hidden() throws IOException {
    verifySchema(executeQuery("source=" + PREFIX), schema("attributes", "struct"));
  }

  /**
   * A computed column that reuses a mapped name is not the mapped leaf and must survive, per the PR
   * #5351 semantics. The mapped leaf is gone from the row type, so {@code eval} creates a fresh
   * column rather than overriding one, and no struct-parent pruning happens on its behalf.
   */
  @Test
  public void computed_column_reusing_a_mapped_name_survives() throws IOException {
    JSONObject result =
        executeQuery(
            "source=" + FLAT + " | fields attributes | eval `attributes.log.file.path` = 'edited'");
    verifySchema(
        result, schema("attributes", "struct"), schema("attributes.log.file.path", "string"));
    assertEquals("edited", result.getJSONArray("datarows").getJSONArray(0).getString(1));
  }

  /**
   * Same conflation, reached through an exclusion projection, which does not mark a project as
   * visited and so still runs the removal pass over the implicit trailing {@code *}.
   */
  @Test
  public void computed_column_reusing_a_mapped_name_survives_after_exclusion() throws IOException {
    JSONObject result =
        executeQuery(
            "source="
                + FLAT
                + " | fields - `attributes.log.file.path` | eval `attributes.log.file.path` ="
                + " 'edited'");
    verifySchema(
        result, schema("attributes", "struct"), schema("attributes.log.file.path", "string"));
    assertEquals("edited", result.getJSONArray("datarows").getJSONArray(0).getString(1));
  }

  /**
   * A declared ancestor name must be matched to the scan that actually declares it, not to any
   * column that happens to share the name. Here the right index contributes the object's child
   * while its own {@code attributes} is projected away inside the subsearch, and the only {@code
   * attributes} column in the output is the left index's unrelated keyword. The child belongs to
   * the right object, so it must survive.
   *
   * <p>End-to-end guard only: join lowering marks a project as visited, so this query does not
   * currently reach {@code tryToRemoveNestedFields}. It pins the user-visible contract in case that
   * changes; the same-table requirement in {@code carriesChild} is what makes the helper safe.
   */
  @Test
  public void ancestor_name_from_another_scan_does_not_hide_the_child() throws IOException {
    JSONObject result =
        executeQuery(
            "source="
                + JOIN_LEFT
                + " | inner join left=l, right=r ON l.id = r.id [ source="
                + JOIN_RIGHT
                + " | fields id, `attributes.log.file.path` ] | fields attributes,"
                + " `attributes.log.file.path`");
    verifySchema(
        result, schema("attributes", "string"), schema("attributes.log.file.path", "string"));
    verifyDataRows(result, rows("left-scalar", "/var/log/app.log"));
  }

  /**
   * The same join without the trailing projection. Also an end-to-end guard rather than a unit of
   * this pass, for the same reason.
   */
  @Test
  public void ancestor_name_from_another_scan_does_not_hide_the_child_without_projection()
      throws IOException {
    JSONObject result =
        executeQuery(
            "source="
                + JOIN_LEFT
                + " | inner join left=l, right=r ON l.id = r.id [ source="
                + JOIN_RIGHT
                + " | fields id, `attributes.log.file.path` ]");
    assertTrue(
        "expected the right index's child to survive, got " + result.getJSONArray("schema"),
        result.getJSONArray("schema").toString().contains("attributes.log.file.path"));
  }

  /**
   * Documented limitation. When one plan scans the same index twice, table identity cannot say
   * which occurrence a column came from, so pass-throughs of that table fall back to the
   * pre-existing immediate-parent rule and the multi-level child stays visible. This keeps the
   * scope of the fix to cases the schema can decide unambiguously; it matches the behaviour before
   * the fix.
   */
  @Test
  public void repeated_scan_of_the_same_index_falls_back_to_previous_behaviour()
      throws IOException {
    verifySchema(
        executeQuery("source=" + FLAT + " | append [ source=" + FLAT + " ]"),
        schema("attributes", "struct"),
        schema("attributes.log.file.path", "string"));
  }

  /**
   * A computed column reusing a mapped name must survive even when its expression draws on more
   * than one input column. Calcite reports one derived origin per referenced column, so such a
   * value has several origins - all derived - and must not be mistaken for provenance that could
   * not be determined. The parent {@code attributes} stays in the row throughout.
   */
  @Test
  public void computed_column_from_two_inputs_reusing_a_mapped_name_survives() throws IOException {
    JSONObject result =
        executeQuery(
            "source="
                + TWO_ROOTS
                + " | fields - `attributes.log.file.path` | eval `attributes.log.file.path` ="
                + " concat(id, tag)");
    verifySchema(
        result,
        schema("attributes", "struct"),
        schema("id", "string"),
        schema("tag", "string"),
        schema("attributes.log.file.path", "string"));
    verifyDataRows(
        result,
        rows(Map.of("log", Map.of("file", Map.of("path", "/var/log/app.log"))), "a", "b", "ab"));
  }

  /**
   * A column the mapping does not declare keeps the pre-existing immediate-parent rule, which
   * retains it. Here {@code eval} creates a literal {@code doc.user.name} column beside the {@code
   * doc} MAP; treating every prefix as a parent would wrongly delete the computed value.
   */
  @Test
  public void computed_dotted_column_is_retained_beside_its_prefix() throws IOException {
    JSONObject result = executeQuery("source=" + UNMAPPED + " | eval `doc.user.name` = 'edited'");
    verifySchema(result, schema("doc", "struct"), schema("doc.user.name", "string"));
    // How the unmapped MAP itself renders is incidental; what matters is the computed column
    // survived the implicit trailing `*` even though its prefix `doc` is also in the output.
    assertEquals("edited", result.getJSONArray("datarows").getJSONArray(0).getString(1));
  }
}
