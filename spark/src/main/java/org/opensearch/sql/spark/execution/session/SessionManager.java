/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.spark.execution.session.SessionId.newSessionId;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.execution.statestore.SessionStateStore;

/**
 * Singleton Class
 */
@RequiredArgsConstructor
public class SessionManager {
  private final SessionStateStore stateStore;
  private final EMRServerlessClient emrServerlessClient;

  private Map<SessionId, Session> sessionMap = new ConcurrentHashMap<>();

  public Session createSession(CreateSessionRequest request) {
    return sessionMap.computeIfAbsent(newSessionId(), sid -> InteractiveSession
        .builder()
        .sessionStateStore(stateStore)
        .serverlessClient(emrServerlessClient)
        .sessionModel(SessionModel.initInteractiveSession(sid, request.getDatasourceName()))
        .startJobRequest(request.getStartJobRequest())
        .build());
  }

  public Optional<Session> getSession(SessionId sid) {
    if (sessionMap.containsKey(sid)) {
      return Optional.of(sessionMap.get(sid));
    } else {
      Optional<SessionModel> model = stateStore.get(sid);
      if (model.isPresent()) {
        InteractiveSession session = InteractiveSession.builder()
            .sessionStateStore(stateStore)
            .serverlessClient(emrServerlessClient)
            .sessionModel(model.get())
            .build();
        return Optional.ofNullable(sessionMap.putIfAbsent(model.get().getSessionID(), session));
      }
      return Optional.empty();
    }
  }

//  todo. add listSessions
//  public List<Session> listSessions() {
//
//  }
}
