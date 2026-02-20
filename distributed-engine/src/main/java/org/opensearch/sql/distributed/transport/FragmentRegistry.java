/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.transport;

import java.util.concurrent.ConcurrentHashMap;
import org.opensearch.sql.distributed.planner.plan.StageFragment;

/**
 * In-memory registry for StageFragments, keyed by "queryId:stageId". Avoids the need for full
 * PlanNode serialization in single-node (integ-test) execution. The coordinator registers fragments
 * before dispatching ShardQueryRequests; TransportShardQueryAction looks them up on the receiving
 * node (same JVM in single-node clusters).
 */
public final class FragmentRegistry {

  private static final ConcurrentHashMap<String, StageFragment> REGISTRY =
      new ConcurrentHashMap<>();

  private FragmentRegistry() {}

  /** Builds the registry key from queryId and stageId. */
  public static String key(String queryId, int stageId) {
    return queryId + ":" + stageId;
  }

  /** Registers a fragment. */
  public static void put(String queryId, int stageId, StageFragment fragment) {
    REGISTRY.put(key(queryId, stageId), fragment);
  }

  /** Retrieves and removes a fragment. Returns null if not found. */
  public static StageFragment get(String queryId, int stageId) {
    return REGISTRY.get(key(queryId, stageId));
  }

  /** Removes a fragment from the registry. */
  public static StageFragment remove(String queryId, int stageId) {
    return REGISTRY.remove(key(queryId, stageId));
  }

  /** Clears all registered fragments (useful in tests). */
  public static void clear() {
    REGISTRY.clear();
  }
}
