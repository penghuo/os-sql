/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.pit;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;
import org.opensearch.transport.client.Client;

/**
 * Manages Point-in-Time (PIT) lifecycle for DQE queries. Creates PITs at query start, tracks them
 * per query, and ensures cleanup on both success and failure paths.
 */
public class PitManager {

  private final Client client;
  private final Map<String, List<PitHandle>> queryPits = new ConcurrentHashMap<>();

  public PitManager(Client client) {
    this.client = Objects.requireNonNull(client, "client must not be null");
  }

  /**
   * Creates a PIT for the given index.
   *
   * @param indexName the index to create the PIT for
   * @param keepAlive keep-alive duration (query_timeout + 1m)
   * @return a PitHandle containing the PIT ID
   */
  public PitHandle createPit(String indexName, TimeValue keepAlive) {
    try {
      CreatePitRequest request = new CreatePitRequest(keepAlive, false, indexName);
      CreatePitResponse response = client.execute(CreatePitAction.INSTANCE, request).get();
      return new PitHandle(response.getId(), indexName, keepAlive);
    } catch (Exception e) {
      throw new DqeException(
          "Failed to create PIT for index [" + indexName + "]: " + e.getMessage(),
          DqeErrorCode.EXECUTION_ERROR,
          e);
    }
  }

  /** Releases a PIT. Idempotent. */
  public void releasePit(PitHandle pitHandle) {
    if (pitHandle.isReleased()) {
      return;
    }
    pitHandle.markReleased();
    try {
      DeletePitRequest request = new DeletePitRequest(List.of(pitHandle.getPitId()));
      client.execute(DeletePitAction.INSTANCE, request).get();
    } catch (Exception e) {
      // Log but do not throw -- PIT cleanup failures should not block query completion
    }
  }

  /** Releases all PITs associated with a query. */
  public void releaseAllPits(String queryId) {
    List<PitHandle> pits = queryPits.remove(queryId);
    if (pits != null) {
      for (PitHandle pit : pits) {
        releasePit(pit);
      }
    }
  }

  /** Registers a PIT with a query for cleanup tracking. */
  public void registerPit(String queryId, PitHandle pitHandle) {
    queryPits.computeIfAbsent(queryId, k -> new CopyOnWriteArrayList<>()).add(pitHandle);
  }
}
