/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.logging;

import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Audit logger for DQE query events. Logs query text, user identity, indices accessed, and
 * execution outcome (success/failure/cancel) to the OpenSearch security audit log.
 *
 * <p>In Phase 1, this writes to a dedicated Log4j logger at INFO level. Full integration with the
 * OpenSearch Security plugin's audit log framework is deferred to Phase 2.
 *
 * <p>Logged events:
 *
 * <ul>
 *   <li>QUERY_STARTED: query submitted, user, indices
 *   <li>QUERY_SUCCEEDED: query completed, elapsed time, rows returned
 *   <li>QUERY_FAILED: query failed, error reason
 *   <li>QUERY_CANCELLED: query cancelled by user or timeout
 * </ul>
 */
public class DqeAuditLogger {

  private static final Logger AUDIT_LOG = LogManager.getLogger("dqe.audit");

  /** Maximum query text length in audit log entries. */
  static final int MAX_AUDIT_QUERY_LENGTH = 500;

  public DqeAuditLogger() {}

  /**
   * Log that a query has started.
   *
   * @param queryId the unique query identifier
   * @param userName the authenticated user name
   * @param query the SQL query text
   * @param indices the indices accessed by the query
   */
  public void logQueryStarted(String queryId, String userName, String query, List<String> indices) {
    AUDIT_LOG.info(
        "DQE_QUERY_STARTED [query_id={}] [user={}] [indices={}] [query={}]",
        queryId,
        userName,
        indices,
        truncateQuery(query));
  }

  /**
   * Log that a query has succeeded.
   *
   * @param queryId the unique query identifier
   * @param userName the authenticated user name
   * @param elapsedMs execution time in milliseconds
   * @param rowsReturned number of rows in the result set
   */
  public void logQuerySucceeded(
      String queryId, String userName, long elapsedMs, long rowsReturned) {
    AUDIT_LOG.info(
        "DQE_QUERY_SUCCEEDED [query_id={}] [user={}] [elapsed_ms={}] [rows_returned={}]",
        queryId,
        userName,
        elapsedMs,
        rowsReturned);
  }

  /**
   * Log that a query has failed.
   *
   * @param queryId the unique query identifier
   * @param userName the authenticated user name
   * @param errorReason the failure reason
   */
  public void logQueryFailed(String queryId, String userName, String errorReason) {
    AUDIT_LOG.info(
        "DQE_QUERY_FAILED [query_id={}] [user={}] [reason={}]", queryId, userName, errorReason);
  }

  /**
   * Log that a query has been cancelled.
   *
   * @param queryId the unique query identifier
   * @param userName the authenticated user name
   * @param reason the cancellation reason (e.g., "user_cancelled", "timeout")
   */
  public void logQueryCancelled(String queryId, String userName, String reason) {
    AUDIT_LOG.info(
        "DQE_QUERY_CANCELLED [query_id={}] [user={}] [reason={}]", queryId, userName, reason);
  }

  private static String truncateQuery(String query) {
    if (query == null) {
      return "<null>";
    }
    if (query.length() <= MAX_AUDIT_QUERY_LENGTH) {
      return query;
    }
    return query.substring(0, MAX_AUDIT_QUERY_LENGTH) + "...(truncated)";
  }
}
