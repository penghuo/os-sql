/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.request;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;
import org.opensearch.dqe.plugin.settings.DqeSettings;

/**
 * Parses and validates DQE query requests from REST request bodies. Extracts query, engine,
 * fetch_size, session_properties and produces an immutable {@link DqeQueryRequest}.
 *
 * <p>Validation rules:
 *
 * <ul>
 *   <li>Query must be non-null and non-empty
 *   <li>Query length must not exceed {@link #MAX_QUERY_LENGTH}
 *   <li>fetch_size must be non-negative
 *   <li>Session property keys must be from the allowed set
 *   <li>Session property values must be valid for their type
 * </ul>
 */
public class DqeRequestParser {

  /** Maximum allowed query length in characters. */
  public static final int MAX_QUERY_LENGTH = 16384;

  /** Supported fields in the request body. */
  static final Set<String> SUPPORTED_FIELDS =
      Set.of("query", "engine", "fetch_size", "session_properties", "parameters", "cursor");

  /** Supported session property keys. */
  static final Set<String> ALLOWED_SESSION_PROPERTIES =
      Set.of("query_max_memory", "query_timeout", "scan_batch_size");

  private final DqeSettings settings;

  /**
   * Creates a new request parser.
   *
   * @param settings the DQE settings for default values
   */
  public DqeRequestParser(DqeSettings settings) {
    this.settings = settings;
  }

  /**
   * Parse a DQE query request from a JSON request body string.
   *
   * @param requestBody the raw JSON string from the REST request
   * @return a validated DqeQueryRequest
   * @throws DqeException with INVALID_REQUEST on validation failure
   */
  public DqeQueryRequest parse(String requestBody) throws DqeException {
    if (requestBody == null || requestBody.isBlank()) {
      throw new DqeException("Request body must not be empty", DqeErrorCode.INVALID_REQUEST);
    }

    String query = null;
    String engine = "dqe";
    int fetchSize = 0;
    Map<String, String> sessionProps = new HashMap<>();

    try (XContentParser parser =
        XContentType.JSON
            .xContent()
            .createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, requestBody)) {

      XContentParser.Token token = parser.nextToken();
      if (token != XContentParser.Token.START_OBJECT) {
        throw new DqeException("Request body must be a JSON object", DqeErrorCode.INVALID_REQUEST);
      }

      while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
        String fieldName = parser.currentName();
        parser.nextToken();

        if (!SUPPORTED_FIELDS.contains(fieldName)) {
          throw new DqeException(
              "Unsupported field in request body: '"
                  + fieldName
                  + "'. Supported fields: "
                  + SUPPORTED_FIELDS,
              DqeErrorCode.INVALID_REQUEST);
        }

        switch (fieldName) {
          case "query":
            query = parser.text();
            break;
          case "engine":
            engine = parser.text();
            break;
          case "fetch_size":
            fetchSize = parser.intValue();
            break;
          case "session_properties":
            sessionProps = parseSessionProperties(parser);
            break;
          default:
            // Skip "parameters" and "cursor" — consumed by other layers
            parser.skipChildren();
            break;
        }
      }
    } catch (DqeException e) {
      throw e;
    } catch (Exception e) {
      throw new DqeException(
          "Invalid JSON in request body: " + e.getMessage(), DqeErrorCode.INVALID_REQUEST);
    }

    // Validate query
    if (query == null || query.trim().isEmpty()) {
      throw new DqeException("Missing required field: 'query'", DqeErrorCode.INVALID_REQUEST);
    }
    query = query.trim();
    if (query.length() > MAX_QUERY_LENGTH) {
      throw new DqeException(
          "Query length " + query.length() + " exceeds maximum of " + MAX_QUERY_LENGTH,
          DqeErrorCode.INVALID_REQUEST);
    }
    if (fetchSize < 0) {
      throw new DqeException(
          "fetch_size must be non-negative, got: " + fetchSize, DqeErrorCode.INVALID_REQUEST);
    }

    DqeQueryRequest.Builder builder =
        DqeQueryRequest.builder()
            .query(query)
            .engine(engine)
            .fetchSize(fetchSize)
            .sessionProperties(sessionProps);

    // Apply session property overrides
    if (sessionProps.containsKey("query_max_memory")) {
      builder.queryMaxMemoryBytes(parseMemoryBytes(sessionProps.get("query_max_memory")));
    }
    if (sessionProps.containsKey("query_timeout")) {
      builder.queryTimeout(parseTimeout(sessionProps.get("query_timeout")));
    }

    return builder.build();
  }

  private Map<String, String> parseSessionProperties(XContentParser parser) throws Exception {
    Map<String, String> props = new HashMap<>();
    if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
      throw new DqeException(
          "session_properties must be a JSON object", DqeErrorCode.INVALID_REQUEST);
    }

    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
      String key = parser.currentName();
      parser.nextToken();

      if (!ALLOWED_SESSION_PROPERTIES.contains(key)) {
        throw new DqeException(
            "Unknown session property: '" + key + "'. Allowed: " + ALLOWED_SESSION_PROPERTIES,
            DqeErrorCode.INVALID_REQUEST);
      }
      props.put(key, parser.text());
    }
    return props;
  }

  long parseMemoryBytes(String value) {
    try {
      String lower = value.toLowerCase().trim();
      if (lower.endsWith("gb")) {
        return Long.parseLong(lower.substring(0, lower.length() - 2).trim()) * 1024 * 1024 * 1024;
      } else if (lower.endsWith("mb")) {
        return Long.parseLong(lower.substring(0, lower.length() - 2).trim()) * 1024 * 1024;
      } else if (lower.endsWith("kb")) {
        return Long.parseLong(lower.substring(0, lower.length() - 2).trim()) * 1024;
      } else {
        return Long.parseLong(lower);
      }
    } catch (NumberFormatException e) {
      throw new DqeException(
          "Invalid query_max_memory value: '"
              + value
              + "'. Use bytes or suffixes like '256mb', '1gb'",
          DqeErrorCode.INVALID_REQUEST);
    }
  }

  Duration parseTimeout(String value) {
    try {
      String lower = value.toLowerCase().trim();
      if (lower.endsWith("ms")) {
        return Duration.ofMillis(Long.parseLong(lower.substring(0, lower.length() - 2).trim()));
      } else if (lower.endsWith("s")) {
        return Duration.ofSeconds(Long.parseLong(lower.substring(0, lower.length() - 1).trim()));
      } else if (lower.endsWith("m")) {
        return Duration.ofMinutes(Long.parseLong(lower.substring(0, lower.length() - 1).trim()));
      } else {
        return Duration.ofMillis(Long.parseLong(lower));
      }
    } catch (NumberFormatException e) {
      throw new DqeException(
          "Invalid query_timeout value: '" + value + "'. Use '30s', '5m', or milliseconds",
          DqeErrorCode.INVALID_REQUEST);
    }
  }
}
