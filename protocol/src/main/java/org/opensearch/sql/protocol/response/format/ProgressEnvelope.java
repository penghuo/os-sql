/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.Objects;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.executor.progress.QueryProgress;
import org.opensearch.sql.utils.SerializeUtils;

/**
 * Adds {@code progress.fraction_done} to an already-rendered JSON response body.
 *
 * <h2>Why merge rather than render</h2>
 *
 * An asynchronous query can return through several renderers — the row formatter, the explain
 * formatter — and every one of them must carry progress so a client never has to special-case a
 * missing field as "zero". Teaching each renderer about progress would mean changing the
 * synchronous output of all of them; merging afterwards keeps the field strictly additive and
 * confined to the asynchronous paths that call this.
 *
 * <p>Gson is used rather than a generic JSON library because its object model preserves member
 * order and this class re-serializes with the same pretty-printing configuration the renderers use.
 * The existing fields therefore come back out byte-identical, with one object appended.
 *
 * @see QueryProgress
 */
@UtilityClass
public class ProgressEnvelope {

  /** Response field holding the progress object. */
  public static final String FIELD = "progress";

  /** Only member of the progress object in this release. */
  public static final String FRACTION_DONE = "fraction_done";

  private static final Gson PRETTY_GSON =
      SerializeUtils.getGsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();

  /**
   * Returns {@code body} with a progress object added.
   *
   * <p>Bodies that are not JSON objects are returned unchanged. That is not expected on any
   * asynchronous path — async submission is gated to the JSON format — but a response is better off
   * missing an advisory field than replaced by a parse error.
   *
   * @param body rendered JSON response body
   * @param progress fraction to publish
   * @return the body with {@code progress.fraction_done} added
   */
  public static String merge(String body, QueryProgress progress) {
    Objects.requireNonNull(progress, "progress must not be null");
    if (body == null || body.isBlank()) {
      return body;
    }
    try {
      JsonElement parsed = JsonParser.parseString(body);
      if (!parsed.isJsonObject()) {
        return body;
      }
      JsonObject root = parsed.getAsJsonObject();
      root.add(FIELD, object(progress));
      return PRETTY_GSON.toJson(root);
    } catch (RuntimeException e) {
      return body;
    }
  }

  /**
   * Builds the progress object.
   *
   * <p>Nested under an object rather than emitted as a flat field so later signals — rows scanned,
   * bytes read, a per-source breakdown — can be added without claiming another top-level response
   * name.
   */
  private static JsonObject object(QueryProgress progress) {
    JsonObject json = new JsonObject();
    json.addProperty(FRACTION_DONE, progress.fractionDone());
    return json;
  }
}
