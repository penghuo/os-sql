/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.execution.QueryRequest;
import org.opensearch.sql.spark.execution.statement.Statement;
import org.opensearch.sql.spark.execution.statement.StatementId;
import org.opensearch.sql.spark.execution.statestore.SessionStateStore;

@Builder
@Getter
@AllArgsConstructor
public class InteractiveSession implements Session {
  private static final Logger LOG = LogManager.getLogger();

  private final SessionStateStore sessionStateStore;
  private final EMRServerlessClient serverlessClient;
  private SessionModel sessionModel;
  private StartJobRequest startJobRequest;

  @Override
  public void open() {
    // todo, startJob();
//    try {
//      sessionModel = initInteractiveSession();
//      sessionModel = sessionStateStore.create(sessionModel);
//      // todo, startJob();
//    } catch (VersionConflictEngineException e) {
//      String errorMsg = "session with same sessionId already exist";
//      LOG.error(errorMsg);
//      throw new RuntimeException(errorMsg);
//    }
  }

  @Override
  public void close() {
    // todo, cancelJob();
  }

  @Override
  public StatementId submit(QueryRequest request) {
    // todo
    return null;
  }

  @Override
  public Statement get(StatementId stID) {
    // todo
    return null;
  }

  @Override
  public List<Statement> list() {
    return null;
  }
}
