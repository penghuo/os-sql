/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.progress.QueryProgress;

class ProgressEnvelopeTest {

  @Test
  @DisplayName("adds progress.fraction_done without disturbing existing fields")
  void addsProgressPreservingFields() {
    String body =
        """
        {
          "schema": [{"name": "c", "type": "bigint"}],
          "datarows": [[4]],
          "total": 1,
          "size": 1
        }\
        """;

    JsonObject merged =
        JsonParser.parseString(ProgressEnvelope.merge(body, QueryProgress.COMPLETE))
            .getAsJsonObject();

    assertEquals(1.0, merged.getAsJsonObject("progress").get("fraction_done").getAsDouble());
    assertEquals(1, merged.get("total").getAsInt());
    assertEquals(1, merged.get("size").getAsInt());
    assertEquals(1, merged.getAsJsonArray("datarows").size());
    assertEquals(
        "c", merged.getAsJsonArray("schema").get(0).getAsJsonObject().get("name").getAsString());
  }

  @Test
  @DisplayName("produces exactly the body documented for an inline async success")
  void matchesDocumentedInlineBody() {
    // The synchronous row formatter's output, verbatim.
    String rendered =
        """
        {
          "schema": [
            {
              "name": "c",
              "type": "bigint"
            }
          ],
          "datarows": [
            [
              4
            ]
          ],
          "total": 1,
          "size": 1
        }\
        """;

    // What docs/user/ppl/interfaces/endpoint.md promises, and what doctest compares against. Pinned
    // here so a
    // formatting change in the merge is caught by a unit test rather than by a documentation test.
    String documented =
        """
        {
          "schema": [
            {
              "name": "c",
              "type": "bigint"
            }
          ],
          "datarows": [
            [
              4
            ]
          ],
          "total": 1,
          "size": 1,
          "progress": {
            "fraction_done": 1.0
          }
        }\
        """;

    assertEquals(documented, ProgressEnvelope.merge(rendered, QueryProgress.COMPLETE));
  }

  @Test
  @DisplayName("keeps the original member order, appending progress last")
  void preservesMemberOrder() {
    String merged =
        ProgressEnvelope.merge("{\"schema\":[],\"datarows\":[],\"total\":0}", QueryProgress.ZERO);
    assertTrue(
        merged.indexOf("\"schema\"") < merged.indexOf("\"datarows\""),
        "existing order must be preserved: " + merged);
    assertTrue(
        merged.indexOf("\"total\"") < merged.indexOf("\"progress\""),
        "progress must be appended, not interleaved: " + merged);
  }

  @Test
  @DisplayName("merges into an explain-shaped body alongside the plan fields")
  void mergesIntoExplainBody() {
    String explain =
        "{\"calcite\":{\"logical\":\"LogicalProject\",\"physical\":\"EnumerableCalc\"}}";

    JsonObject merged =
        JsonParser.parseString(ProgressEnvelope.merge(explain, QueryProgress.COMPLETE))
            .getAsJsonObject();

    assertTrue(merged.has("calcite"), "plan fields must survive");
    assertEquals("LogicalProject", merged.getAsJsonObject("calcite").get("logical").getAsString());
    assertEquals(1.0, merged.getAsJsonObject("progress").get("fraction_done").getAsDouble());
  }

  @Test
  @DisplayName("a fractional value round-trips exactly")
  void fractionalValueRoundTrips() {
    JsonObject merged =
        JsonParser.parseString(ProgressEnvelope.merge("{}", new QueryProgress(0.44)))
            .getAsJsonObject();
    assertEquals(0.44, merged.getAsJsonObject("progress").get("fraction_done").getAsDouble());
  }

  @Test
  @DisplayName("a non-object body is returned unchanged rather than replaced by an error")
  void nonObjectBodyUnchanged() {
    assertEquals("[1,2,3]", ProgressEnvelope.merge("[1,2,3]", QueryProgress.COMPLETE));
    assertEquals("not json", ProgressEnvelope.merge("not json", QueryProgress.COMPLETE));
  }

  @Test
  @DisplayName("a blank body is returned unchanged")
  void blankBodyUnchanged() {
    assertSame(null, ProgressEnvelope.merge(null, QueryProgress.COMPLETE));
    assertEquals("", ProgressEnvelope.merge("", QueryProgress.COMPLETE));
  }

  @Test
  @DisplayName("replaces an existing progress field rather than duplicating it")
  void replacesExistingProgress() {
    String merged =
        ProgressEnvelope.merge("{\"progress\":{\"fraction_done\":0.2}}", QueryProgress.COMPLETE);
    JsonObject root = JsonParser.parseString(merged).getAsJsonObject();
    assertEquals(1.0, root.getAsJsonObject("progress").get("fraction_done").getAsDouble());
    assertEquals(1, root.keySet().size());
  }
}
