/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution;

import lombok.Getter;
import org.apache.commons.lang3.RandomStringUtils;

public class ReplQueryId {
  @Getter private String queryId;

  private ReplQueryId(String queryId) {
    if (!queryId.startsWith("REPL")) {
      throw new IllegalArgumentException("invalid REPL query id: " + queryId);
    }
    this.queryId = queryId;
  }

  public static ReplQueryId generate() {
    return new ReplQueryId("REPL" + RandomStringUtils.random(10, false, true));
  }

  public static ReplQueryId from(String queryId) {
    return new ReplQueryId(queryId);
  }
}
