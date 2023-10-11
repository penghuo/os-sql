/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.spark.execution.session.InteractiveSessionTest.TestSession.testSession;
import static org.opensearch.sql.spark.execution.session.SessionState.NOT_STARTED;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.junit.Test;
import org.mockito.Mock;
import org.opensearch.sql.spark.execution.connection.Connection;
import org.opensearch.sql.spark.execution.statestore.SessionStateStore;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

public class InteractiveSessionTest extends OpenSearchSingleNodeTestCase {

  private static final String indexName = "mockindex";

  @Mock private Connection connection;

  @Test
  public void openSession() {
    createIndex(indexName);
    SessionStateStore stateStore = new SessionStateStore(indexName, client());
    InteractiveSession session = new InteractiveSession(stateStore, connection);

    testSession(session).open().assertSessionState(NOT_STARTED);

    //    session.open();
    //    SessionModel currentSessionModel = session.getSessionModel();
    //    assertEquals(SessionState.STARTING, session.getSessionModel().getSessionState());
    //
    //    String sessionId = currentSessionModel.getSessionID().getSessionId();
    //    assertFalse(sessionId.isEmpty());
    //
    //    assertIndexSessionState(sessionId, stateStore, SessionState.STARTING);
  }

  @RequiredArgsConstructor
  static class TestSession {
    private final InteractiveSession session;

    public static TestSession testSession(InteractiveSession session) {
      return new TestSession(session);
    }

    public void assertSessionState(SessionState expected) {
      assertEquals(expected, session.getSessionModel().getSessionState());

      Optional<SessionModel> sessionStoreState =
          session.getSessionStateStore().get(session.getSessionModel().getSessionID());
      assertTrue(sessionStoreState.isPresent());
      assertEquals(expected, sessionStoreState.get().getSessionState());
    }

    public TestSession open() {
      session.open();
      return this;
    }

    public TestSession close() {
      session.close();
      return this;
    }
  }
}
