/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.spark.execution.session.SessionModel.initInteractiveSession;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import lombok.SneakyThrows;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.spark.execution.QueryRequest;
import org.opensearch.sql.spark.execution.statement.Statement;
import org.opensearch.sql.spark.execution.statement.StatementId;
import org.opensearch.sql.spark.execution.statestore.IndexStateStore;

@Getter
@RequiredArgsConstructor
public class InteractiveSession implements Session {
  private final IndexStateStore stateStore;

  private SessionModel sessionModel;

  @Override
  public void open() {
    sessionModel = initInteractiveSession();
    updateState(SessionState.NOT_STARTED);
  }

  @Override
  public void close() {

  }

  @Override
  public StatementId submit(QueryRequest request) {
    return null;
  }

  @Override
  public Statement get(StatementId stID) {
    return null;
  }

  @Override
  public List<Statement> list() {
    return null;
  }

  @SneakyThrows
  @VisibleForTesting
  public void updateState(SessionState newState) {
    CountDownLatch cdl = new CountDownLatch(1);
    switch (newState) {
      case NOT_STARTED:
        stateStore.create(sessionModel.toStateModel(), ActionListener.wrap(
            Void -> {
              cdl.countDown();
            },
            exception -> {
              cdl.countDown();
              throw new RuntimeException(exception);
            }
        ));
        break;
      default:
        throw new RuntimeException("unexpected " + newState);
    }
    cdl.await();
  }
}
