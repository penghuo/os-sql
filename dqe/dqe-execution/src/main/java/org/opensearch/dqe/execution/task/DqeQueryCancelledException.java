/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.task;

import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;

/** Thrown when a DQE query is cancelled by the user, timeout, or system. */
public class DqeQueryCancelledException extends DqeException {

  private final String queryId;
  private final String reason;

  public DqeQueryCancelledException(String queryId, String reason) {
    super("Query [" + queryId + "] cancelled: " + reason, DqeErrorCode.QUERY_CANCELLED);
    this.queryId = queryId;
    this.reason = reason;
  }

  public String getQueryId() {
    return queryId;
  }

  public String getReason() {
    return reason;
  }
}
