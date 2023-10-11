/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statement;

import lombok.Data;
import org.opensearch.sql.spark.execution.session.SessionId;

/** Statement represent query to execute in session. One statement map to one session. */
@Data
public class Statement {
  private final String version;
  private final String statementId;
  private final StatementState state;
  private final SessionId sessionId;
  private final String query;
  private final String lang;
}
