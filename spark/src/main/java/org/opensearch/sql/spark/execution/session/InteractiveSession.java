/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.spark.execution.session.SessionModel.initInteractiveSession;

import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.sql.spark.execution.QueryRequest;
import org.opensearch.sql.spark.execution.connection.Connection;
import org.opensearch.sql.spark.execution.statement.Statement;
import org.opensearch.sql.spark.execution.statement.StatementId;
import org.opensearch.sql.spark.execution.statestore.SessionStateStore;

@Getter
@RequiredArgsConstructor
public class InteractiveSession implements Session {
  private static final Logger LOG = LogManager.getLogger();

  private final SessionStateStore sessionStateStore;
  private final Connection connection;

  private SessionModel sessionModel;

  @Override
  public void open() {
    try {
      sessionModel = initInteractiveSession();
      sessionModel = sessionStateStore.create(sessionModel);
      // todo, startJob();
    } catch (VersionConflictEngineException e) {
      String errorMsg = "session with same sessionId already exist";
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }
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
