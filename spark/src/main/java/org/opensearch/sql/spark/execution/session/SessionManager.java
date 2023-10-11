/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.execution.ReplSession;
import org.opensearch.sql.spark.execution.ReplSessionManager;

@RequiredArgsConstructor
public abstract class SessionManager {
  private static final Logger LOG = LogManager.getLogger(ReplSessionManager.class);

  public static final String SESSION_ID = RandomStringUtils.random(3, false, true);

  private final String indexName;

  private final Client client;

  private final EMRServerlessClient serverlessClient;

  private Map<String, ReplSession> sessionMap = new ConcurrentHashMap<>();

//  public String createSession(StartJobRequest request) {
//    ReplSession replSession = new ReplSession(DATASOURCE_REPL_INDEX, SESSION_ID, client);
//    replSession.startSession(() -> serverlessClient.startJobRun(request));
//
//    LOG.info("REPL Session created");
//    sessionMap.computeIfAbsent(replSession.getSessionId(), id -> replSession);
//
//    return replSession.getSessionId();
//  }
//
//  public Optional<ReplSession> get(String sessionId) {
//    return Optional.ofNullable(sessionMap.get(sessionId));
//  }

  abstract public Session createSession(CreateSessionRequest request);

  abstract public Session getSession(SessionId sessionID);

  abstract public List<Session> listSessions();
}
