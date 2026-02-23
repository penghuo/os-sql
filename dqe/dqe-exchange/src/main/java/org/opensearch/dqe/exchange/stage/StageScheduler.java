/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.stage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.dqe.exchange.action.DqeStageCancelAction;
import org.opensearch.dqe.exchange.action.DqeStageCancelRequest;
import org.opensearch.dqe.exchange.action.DqeStageCancelResponse;
import org.opensearch.dqe.exchange.action.DqeStageExecuteAction;
import org.opensearch.dqe.exchange.action.DqeStageExecuteRequest;
import org.opensearch.dqe.exchange.action.DqeStageExecuteRequest.ShardSplitInfo;
import org.opensearch.dqe.exchange.action.DqeStageExecuteResponse;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;
import org.opensearch.transport.TransportService;

/**
 * Coordinator-side scheduler that dispatches plan fragments (stages) to data nodes.
 *
 * <p>Given a set of shard splits, the scheduler groups them by node, creates a {@link
 * DqeStageExecuteRequest} for each node, and sends them via {@link TransportService}. It tracks
 * which nodes are active for each query to support cancellation.
 *
 * <p>For cancellation, it sends {@link DqeStageCancelRequest} to all active nodes for a query.
 */
public class StageScheduler {

  private static final Logger logger = LogManager.getLogger(StageScheduler.class);

  private final TransportService transportService;
  private final ClusterService clusterService;

  // queryId -> set of active node IDs
  private final ConcurrentHashMap<String, Set<String>> activeNodesByQuery;

  /**
   * Create a stage scheduler.
   *
   * @param transportService the transport service for sending requests to data nodes
   * @param clusterService the cluster service for resolving node IDs to DiscoveryNodes
   */
  public StageScheduler(TransportService transportService, ClusterService clusterService) {
    this.transportService =
        Objects.requireNonNull(transportService, "transportService must not be null");
    this.clusterService = Objects.requireNonNull(clusterService, "clusterService must not be null");
    this.activeNodesByQuery = new ConcurrentHashMap<>();
  }

  /**
   * Schedule a stage for execution across data nodes.
   *
   * <p>Groups the shard splits by node, sends a stage execute request to each node, and notifies
   * the listener when all nodes have responded.
   *
   * @param queryId query identifier
   * @param stageId stage identifier
   * @param serializedPlanFragment opaque plan fragment bytes
   * @param shardSplits shard split descriptors (grouped by node internally)
   * @param coordinatorNodeId the coordinator node ID for gather exchange
   * @param queryMemoryBudgetBytes per-query memory budget in bytes
   * @param listener callback for the schedule result
   */
  public void scheduleStage(
      String queryId,
      int stageId,
      byte[] serializedPlanFragment,
      List<ShardSplitInfo> shardSplits,
      String coordinatorNodeId,
      long queryMemoryBudgetBytes,
      ActionListener<StageScheduleResult> listener) {

    Objects.requireNonNull(queryId, "queryId must not be null");
    Objects.requireNonNull(listener, "listener must not be null");

    // Group splits by nodeId
    Map<String, List<ShardSplitInfo>> splitsByNode = new HashMap<>();
    for (ShardSplitInfo split : shardSplits) {
      splitsByNode.computeIfAbsent(split.getNodeId(), k -> new ArrayList<>()).add(split);
    }

    if (splitsByNode.isEmpty()) {
      listener.onResponse(new StageScheduleResult(queryId, stageId, 0));
      return;
    }

    // Track active nodes for this query
    Set<String> activeNodes =
        activeNodesByQuery.computeIfAbsent(
            queryId, k -> Collections.synchronizedSet(new HashSet<>()));
    activeNodes.addAll(splitsByNode.keySet());

    int totalNodes = splitsByNode.size();
    AtomicInteger responsesReceived = new AtomicInteger(0);
    AtomicInteger acceptedCount = new AtomicInteger(0);

    for (Map.Entry<String, List<ShardSplitInfo>> entry : splitsByNode.entrySet()) {
      String nodeId = entry.getKey();
      List<ShardSplitInfo> nodeSplits = entry.getValue();

      DqeStageExecuteRequest request =
          new DqeStageExecuteRequest(
              queryId,
              stageId,
              serializedPlanFragment,
              nodeSplits,
              coordinatorNodeId,
              queryMemoryBudgetBytes);

      DiscoveryNode targetNode = clusterService.state().nodes().get(nodeId);

      if (targetNode == null) {
        logger.warn(
            "[{}] Node [{}] not found in cluster state, skipping {} splits",
            queryId,
            nodeId,
            nodeSplits.size());
        int received = responsesReceived.incrementAndGet();
        if (received == totalNodes) {
          completeSchedule(queryId, stageId, acceptedCount.get(), listener);
        }
        continue;
      }

      transportService.sendRequest(
          targetNode,
          DqeStageExecuteAction.NAME,
          request,
          new ActionListenerResponseHandler<>(
              new ActionListener<>() {
                @Override
                public void onResponse(DqeStageExecuteResponse response) {
                  if (response.isAccepted()) {
                    acceptedCount.incrementAndGet();
                  } else {
                    logger.warn("[{}] Stage {} rejected by node [{}]", queryId, stageId, nodeId);
                  }
                  int received = responsesReceived.incrementAndGet();
                  if (received == totalNodes) {
                    completeSchedule(queryId, stageId, acceptedCount.get(), listener);
                  }
                }

                @Override
                public void onFailure(Exception e) {
                  logger.error(
                      "[{}] Failed to dispatch stage {} to node [{}]", queryId, stageId, nodeId, e);
                  activeNodes.remove(nodeId);
                  int received = responsesReceived.incrementAndGet();
                  if (received == totalNodes) {
                    if (acceptedCount.get() == 0) {
                      listener.onFailure(
                          new DqeException(
                              String.format(
                                  "Failed to dispatch stage %d for query [%s]:"
                                      + " no data nodes accepted the request",
                                  stageId, queryId),
                              DqeErrorCode.STAGE_EXECUTION_FAILED,
                              e));
                    } else {
                      completeSchedule(queryId, stageId, acceptedCount.get(), listener);
                    }
                  }
                }
              },
              DqeStageExecuteResponse::new));
    }
  }

  /**
   * Cancel all stages for a query across all active data nodes.
   *
   * @param queryId query identifier
   * @param listener callback when cancellation is complete
   */
  public void cancelQuery(String queryId, ActionListener<Void> listener) {
    Set<String> activeNodes = activeNodesByQuery.remove(queryId);
    if (activeNodes == null || activeNodes.isEmpty()) {
      listener.onResponse(null);
      return;
    }

    // Send cancel to all known stage IDs. For Phase 1, stageId is always 0.
    cancelNodes(queryId, 0, activeNodes, listener);
  }

  /**
   * Cancel a specific stage on all nodes running it.
   *
   * @param queryId query identifier
   * @param stageId stage identifier
   * @param listener callback when cancellation is complete
   */
  public void cancelStage(String queryId, int stageId, ActionListener<Void> listener) {
    Set<String> activeNodes = activeNodesByQuery.get(queryId);
    if (activeNodes == null || activeNodes.isEmpty()) {
      listener.onResponse(null);
      return;
    }
    cancelNodes(queryId, stageId, new HashSet<>(activeNodes), listener);
  }

  /**
   * Get the set of node IDs with active stages for a query.
   *
   * @param queryId query identifier
   * @return set of active node IDs (empty if none)
   */
  public Set<String> getActiveNodes(String queryId) {
    Set<String> nodes = activeNodesByQuery.get(queryId);
    return nodes == null
        ? Collections.emptySet()
        : Collections.unmodifiableSet(new HashSet<>(nodes));
  }

  /**
   * Remove tracking for a query. Called after query completion.
   *
   * @param queryId query identifier
   */
  public void deregisterQuery(String queryId) {
    activeNodesByQuery.remove(queryId);
  }

  private void completeSchedule(
      String queryId,
      int stageId,
      int acceptedCount,
      ActionListener<StageScheduleResult> listener) {
    listener.onResponse(new StageScheduleResult(queryId, stageId, acceptedCount));
  }

  private void cancelNodes(
      String queryId, int stageId, Set<String> nodeIds, ActionListener<Void> listener) {

    if (nodeIds.isEmpty()) {
      listener.onResponse(null);
      return;
    }

    AtomicInteger responsesReceived = new AtomicInteger(0);
    int totalNodes = nodeIds.size();

    for (String nodeId : nodeIds) {
      DiscoveryNode targetNode = clusterService.state().nodes().get(nodeId);

      if (targetNode == null) {
        logger.warn("[{}] Node [{}] not found for cancel, skipping", queryId, nodeId);
        if (responsesReceived.incrementAndGet() == totalNodes) {
          listener.onResponse(null);
        }
        continue;
      }

      DqeStageCancelRequest request = new DqeStageCancelRequest(queryId, stageId);

      transportService.sendRequest(
          targetNode,
          DqeStageCancelAction.NAME,
          request,
          new ActionListenerResponseHandler<>(
              new ActionListener<>() {
                @Override
                public void onResponse(DqeStageCancelResponse response) {
                  if (!response.isAcknowledged()) {
                    logger.warn(
                        "[{}] Stage {} cancel not acknowledged by node [{}]",
                        queryId,
                        stageId,
                        nodeId);
                  }
                  if (responsesReceived.incrementAndGet() == totalNodes) {
                    listener.onResponse(null);
                  }
                }

                @Override
                public void onFailure(Exception e) {
                  logger.error(
                      "[{}] Failed to cancel stage {} on node [{}]", queryId, stageId, nodeId, e);
                  if (responsesReceived.incrementAndGet() == totalNodes) {
                    listener.onResponse(null);
                  }
                }
              },
              DqeStageCancelResponse::new));
    }
  }
}
