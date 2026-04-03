/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Extracts a low-cardinality command structure summary from PPL queries. */
public class QuerySummaryExtractor {

  private static final Pattern PPL_COMMAND_PATTERN =
      Pattern.compile(
          "(?:^|\\|)\\s*(source|where|fields|stats|sort|head|tail|top|rare|"
              + "eval|dedup|rename|parse|grok|patterns|lookup|join|append|"
              + "subquery|trendline|fillnull|flatten|expand|describe|"
              + "fieldsummary|ad|ml|kmeans|explain)\\b",
          Pattern.CASE_INSENSITIVE);

  /**
   * Extracts the PPL command structure as a pipe-delimited string.
   *
   * @param query the raw PPL query
   * @return command summary, e.g. "source | where | stats"
   */
  public static String extractPPLSummary(String query) {
    List<String> commands = new ArrayList<>();
    Matcher matcher = PPL_COMMAND_PATTERN.matcher(query);
    while (matcher.find()) {
      commands.add(matcher.group(1).toLowerCase());
    }
    return commands.isEmpty() ? "unknown" : String.join(" | ", commands);
  }

  private QuerySummaryExtractor() {}
}
