/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.stage;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.dqe.exchange.action.DqeStageExecuteRequest;
import org.opensearch.dqe.exchange.action.DqeStageExecuteResponse;
import org.opensearch.dqe.memory.DqeMemoryTracker;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportService;

/**
 * Data-node-side handler for stage execution requests.
 *
 * <p>When the coordinator dispatches a stage via {@code internal:dqe/stage/execute}, this handler
 * receives the request, creates Drivers for each shard split, and starts execution on the {@code
 * dqe_worker} thread pool.
 *
 * <p>For Phase 1, actual Driver/Pipeline creation is delegated to a {@link StageExecutionCallback}
 * that will be wired by the plugin layer when dqe-execution is available. The handler itself
 * manages the lifecycle: tracking active stages, handling cancellation, and cleanup.
 */
public class StageExecutionHandler implements TransportRequestHandler<DqeStageExecuteRequest> {

  private static final Logger logger = LogManager.getLogger(StageExecutionHandler.class);

  /**
   * Callback for creating and running drivers. Allows dqe-execution integration without a compile
   * dependency.
   */
  @FunctionalInterface
  public interface StageExecutionCallback {
    /**
     * Execute a stage on this data node.
     *
     * @param request the stage execute request
     * @param onComplete called when execution completes (null exception = success)
     */
    void execute(DqeStageExecuteRequest request, StageCompletionListener onComplete);
  }

  /** Listener for stage execution completion. */
  @FunctionalInterface
  public interface StageCompletionListener {
    void onComplete(Exception failure);
  }

  private final ThreadPool threadPool;
  private final TransportService transportService;
  private final DqeMemoryTracker memoryTracker;

  // queryId -> set of active stageIds
  private final ConcurrentHashMap<String, Set<Integer>> activeStages;

  // queryId:stageId -> cancellation Runnables
  private final ConcurrentHashMap<String, Runnable> cancelActions;

  // Optional execution callback (set by plugin layer after dqe-execution is ready)
  private volatile StageExecutionCallback executionCallback;

  /**
   * Create a stage execution handler.
   *
   * @param threadPool the thread pool for dispatching execution
   * @param transportService the transport service
   * @param memoryTracker the node-level memory tracker
   */
  public StageExecutionHandler(
      ThreadPool threadPool, TransportService transportService, DqeMemoryTracker memoryTracker) {
    this.threadPool = Objects.requireNonNull(threadPool, "threadPool must not be null");
    this.transportService =
        Objects.requireNonNull(transportService, "transportService must not be null");
    this.memoryTracker = Objects.requireNonNull(memoryTracker, "memoryTracker must not be null");
    this.activeStages = new ConcurrentHashMap<>();
    this.cancelActions = new ConcurrentHashMap<>();
  }

  /**
   * Set the execution callback. Called by the plugin layer when dqe-execution is wired.
   *
   * @param callback the execution callback
   */
  public void setExecutionCallback(StageExecutionCallback callback) {
    this.executionCallback = Objects.requireNonNull(callback, "callback must not be null");
  }

  @Override
  public void messageReceived(DqeStageExecuteRequest request, TransportChannel channel, Task task)
      throws Exception {

    String queryId = request.getQueryId();
    int stageId = request.getStageId();
    String stageKey = stageKey(queryId, stageId);

    logger.debug(
        "[{}] Received stage {} execution request with {} splits",
        queryId,
        stageId,
        request.getShardSplitInfos().size());

    // Track the active stage
    activeStages.computeIfAbsent(queryId, k -> ConcurrentHashMap.newKeySet()).add(stageId);

    // ACK immediately that the stage was accepted
    channel.sendResponse(new DqeStageExecuteResponse(queryId, stageId, true));

    // Execute asynchronously
    if (executionCallback != null) {
      executionCallback.execute(
          request,
          failure -> {
            if (failure != null) {
              logger.error("[{}] Stage {} execution failed", queryId, stageId, failure);
            } else {
              logger.debug("[{}] Stage {} completed successfully", queryId, stageId);
            }
            cleanupStage(queryId, stageId);
          });
    } else {
      logger.warn(
          "[{}] No execution callback set — stage {} accepted but not executed", queryId, stageId);
      cleanupStage(queryId, stageId);
    }
  }

  /**
   * Cancel all stages for a query on this node.
   *
   * @param queryId query identifier
   */
  public void cancelQuery(String queryId) {
    Set<Integer> stages = activeStages.remove(queryId);
    if (stages != null) {
      for (int stageId : stages) {
        String stageKey = stageKey(queryId, stageId);
        Runnable cancelAction = cancelActions.remove(stageKey);
        if (cancelAction != null) {
          try {
            cancelAction.run();
          } catch (Exception e) {
            logger.error("[{}] Error cancelling stage {}", queryId, stageId, e);
          }
        }
      }
    }
    logger.debug("[{}] All stages cancelled on this node", queryId);
  }

  /**
   * Cancel a specific stage on this node.
   *
   * @param queryId query identifier
   * @param stageId stage identifier
   */
  public void cancelStage(String queryId, int stageId) {
    String stageKey = stageKey(queryId, stageId);
    Runnable cancelAction = cancelActions.remove(stageKey);
    if (cancelAction != null) {
      try {
        cancelAction.run();
      } catch (Exception e) {
        logger.error("[{}] Error cancelling stage {}", queryId, stageId, e);
      }
    }
    cleanupStage(queryId, stageId);
  }

  /**
   * Register a cancellation action for a stage. Called by the execution layer to provide a way to
   * interrupt running drivers.
   *
   * @param queryId query identifier
   * @param stageId stage identifier
   * @param cancelAction runnable that interrupts drivers and releases resources
   */
  public void registerCancelAction(String queryId, int stageId, Runnable cancelAction) {
    cancelActions.put(stageKey(queryId, stageId), cancelAction);
  }

  /** Check if a stage is active on this node. */
  public boolean isStageActive(String queryId, int stageId) {
    Set<Integer> stages = activeStages.get(queryId);
    return stages != null && stages.contains(stageId);
  }

  /** Get all active query IDs on this node. */
  public Set<String> getActiveQueryIds() {
    return activeStages.keySet();
  }

  private void cleanupStage(String queryId, int stageId) {
    cancelActions.remove(stageKey(queryId, stageId));
    Set<Integer> stages = activeStages.get(queryId);
    if (stages != null) {
      stages.remove(stageId);
      if (stages.isEmpty()) {
        activeStages.remove(queryId, stages);
      }
    }
  }

  private static String stageKey(String queryId, int stageId) {
    return queryId + ":" + stageId;
  }
}
