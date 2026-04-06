/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.Session;
import io.trino.execution.PartitionedSplitsInfo;
import io.trino.execution.RemoteTask;
import io.trino.execution.ScheduledSplit;
import io.trino.execution.SplitAssignment;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.SpoolingOutputStats;
import io.trino.operator.TaskStats;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanNodeId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.TransportService;

/**
 * Coordinator-side {@link RemoteTask} that dispatches work to a worker node via OpenSearch
 * TransportActions. Replaces Trino's {@code HttpRemoteTask}.
 *
 * <p>Lifecycle:
 *
 * <ol>
 *   <li>{@link #start()} — sends PlanFragment + initial splits via trino:task/update
 *   <li>{@link #addSplits(Multimap)} — batches new splits, sends via trino:task/update
 *   <li>{@link #cancel()} / {@link #abort()} — sends trino:task/cancel
 * </ol>
 */
public class TransportRemoteTask implements RemoteTask {

  private static final Logger LOG = LogManager.getLogger(TransportRemoteTask.class);

  private final TransportService transportService;
  private final DiscoveryNode targetNode;
  private final TaskId taskId;
  private final InternalNode trinoNode;
  private final PlanFragment planFragment;
  private final Session session;
  private final TrinoJsonCodec codec;

  private final AtomicReference<TaskInfo> lastTaskInfo = new AtomicReference<>();
  private final AtomicReference<TaskStatus> lastTaskStatus = new AtomicReference<>();
  private final AtomicReference<OutputBuffers> outputBuffers = new AtomicReference<>();
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean firstUpdateSent = new AtomicBoolean(false);
  private final AtomicInteger pendingSplitCount = new AtomicInteger(0);
  private final AtomicInteger queuedSplitCount = new AtomicInteger(0);

  private final Multimap<PlanNodeId, Split> pendingSplits =
      HashMultimap.create();
  private final Set<PlanNodeId> noMoreSplitsSources = new HashSet<>();
  private final Set<PlanNodeId> sentNoMoreSplits = new HashSet<>();
  private final AtomicLong splitSequenceId = new AtomicLong(0);

  private final CopyOnWriteArrayList<StateChangeListener<TaskStatus>> statusListeners =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<StateChangeListener<TaskInfo>> infoListeners =
      new CopyOnWriteArrayList<>();

  private static final ScheduledExecutorService STATUS_POLLER =
      Executors.newScheduledThreadPool(2,
          r -> { Thread t = new Thread(r, "transport-task-status-poller"); t.setDaemon(true); return t; });
  private volatile ScheduledFuture<?> statusPollFuture;

  public TransportRemoteTask(
      TransportService transportService,
      DiscoveryNode targetNode,
      TaskId taskId,
      InternalNode trinoNode,
      PlanFragment planFragment,
      Session session,
      TrinoJsonCodec codec,
      OutputBuffers initialOutputBuffers,
      Multimap<PlanNodeId, Split> initialSplits) {
    this.transportService = transportService;
    this.targetNode = targetNode;
    this.taskId = taskId;
    this.trinoNode = trinoNode;
    this.planFragment = planFragment;
    this.session = session;
    this.codec = codec;
    this.outputBuffers.set(initialOutputBuffers);
    this.pendingSplits.putAll(initialSplits);
    this.pendingSplitCount.set(initialSplits.size());

    // Initialize with a placeholder TaskInfo (same pattern as HttpRemoteTask).
    // The self URI must include the task path for the exchange client to construct
    // correct results URLs: {self}/results/{bufferId}/{token}
    java.net.URI selfUri = java.net.URI.create(
        trinoNode.getInternalUri() + "/v1/task/" + taskId);
    TaskStatus initialStatus =
        TaskStatus.initialTaskStatus(taskId, selfUri,
            trinoNode.getNodeIdentifier(), false);
    TaskStats initialStats = new TaskStats(
        org.joda.time.DateTime.now(), org.joda.time.DateTime.now());
    TaskInfo initialInfo =
        TaskInfo.createInitialTask(taskId, selfUri,
            trinoNode.getNodeIdentifier(), false, Optional.empty(), initialStats);
    this.lastTaskInfo.set(initialInfo);
    this.lastTaskStatus.set(initialStatus);
  }

  @Override
  public TaskId getTaskId() {
    return taskId;
  }

  @Override
  public String getNodeId() {
    return trinoNode.getNodeIdentifier();
  }

  @Override
  public TaskInfo getTaskInfo() {
    return lastTaskInfo.get();
  }

  @Override
  public TaskStatus getTaskStatus() {
    return lastTaskStatus.get();
  }

  @Override
  public void start() {
    if (!started.compareAndSet(false, true)) {
      return;
    }
    LOG.info("Starting TransportRemoteTask {} on node {} (URI: {})",
        taskId, targetNode.getName(), trinoNode.getInternalUri());

    // Send the initial update with the fragment SYNCHRONOUSLY.
    // This ensures the task is created on the worker before any subsequent
    // addSplits/setOutputBuffers calls send fragment-less updates.
    CountDownLatch latch = new CountDownLatch(1);
    sendUpdateWithLatch(Optional.of(planFragment), latch);
    try {
      latch.await(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted waiting for initial task creation for {}", taskId);
    }
    firstUpdateSent.set(true);
    // Start polling task status — required for the coordinator to know when tasks finish
    statusPollFuture = STATUS_POLLER.scheduleWithFixedDelay(
        this::pollStatus, 200, 200, TimeUnit.MILLISECONDS);
  }

  @Override
  public void addSplits(Multimap<PlanNodeId, Split> splits) {
    synchronized (pendingSplits) {
      pendingSplits.putAll(splits);
      pendingSplitCount.addAndGet(splits.size());
    }
    // Only send updates after the first update (with fragment) has been sent
    if (firstUpdateSent.get()) {
      sendUpdate(Optional.empty());
    }
  }

  @Override
  public void noMoreSplits(PlanNodeId sourceId) {
    synchronized (pendingSplits) {
      noMoreSplitsSources.add(sourceId);
    }
    if (firstUpdateSent.get()) {
      sendUpdate(Optional.empty());
    }
  }

  @Override
  public void setOutputBuffers(OutputBuffers newOutputBuffers) {
    outputBuffers.set(newOutputBuffers);
    if (firstUpdateSent.get()) {
      sendUpdate(Optional.empty());
    }
  }

  @Override
  public void setSpeculative(boolean speculative) {
    // Not supported in transport-based execution
  }

  @Override
  public void cancel() {
    if (statusPollFuture != null) {
      statusPollFuture.cancel(false);
    }
    sendCancel();
  }

  @Override
  public void abort() {
    if (statusPollFuture != null) {
      statusPollFuture.cancel(false);
    }
    sendCancel();
  }

  @Override
  public void failLocallyImmediately(Throwable cause) {
    LOG.error("Task {} failed locally: {}", taskId, cause.getMessage());
    sendCancel();
  }

  @Override
  public void failRemotely(Throwable cause) {
    LOG.error("Task {} failed remotely: {}", taskId, cause.getMessage());
  }

  @Override
  public void addStateChangeListener(StateChangeListener<TaskStatus> listener) {
    statusListeners.add(listener);
  }

  @Override
  public void addFinalTaskInfoListener(StateChangeListener<TaskInfo> listener) {
    infoListeners.add(listener);
  }

  @Override
  public ListenableFuture<Void> whenSplitQueueHasSpace(long maxQueuedBytes) {
    // Always ready — no backpressure yet
    return Futures.immediateVoidFuture();
  }

  @Override
  public PartitionedSplitsInfo getPartitionedSplitsInfo() {
    return PartitionedSplitsInfo.forZeroSplits();
  }

  @Override
  public PartitionedSplitsInfo getQueuedPartitionedSplitsInfo() {
    return PartitionedSplitsInfo.forZeroSplits();
  }

  @Override
  public int getUnacknowledgedPartitionedSplitCount() {
    return pendingSplitCount.get();
  }

  @Override
  public SpoolingOutputStats.Snapshot retrieveAndDropSpoolingOutputStats() {
    return null;
  }

  private void sendUpdateWithLatch(Optional<PlanFragment> fragment, CountDownLatch latch) {
    sendUpdateInternal(fragment, latch);
  }

  private void sendUpdate(Optional<PlanFragment> fragment) {
    sendUpdateInternal(fragment, null);
  }

  private void sendUpdateInternal(Optional<PlanFragment> fragment, CountDownLatch latch) {
    try {
      byte[] fragmentJson = fragment.map(codec::serializePlanFragment).orElse(new byte[0]);

      // Convert pending splits to SplitAssignment list
      List<SplitAssignment> splitAssignments;
      synchronized (pendingSplits) {
        splitAssignments = buildSplitAssignments();
        pendingSplits.clear();
      }
      byte[] splitsJson = codec.serializeSplitAssignments(splitAssignments);
      byte[] buffersJson = codec.serializeOutputBuffers(outputBuffers.get());
      byte[] sessionJson = new byte[0];

      TrinoTaskUpdateRequest request =
          new TrinoTaskUpdateRequest(
              taskId.toString(), fragmentJson, splitsJson, buffersJson, sessionJson);

      transportService.sendRequest(
          targetNode,
          TrinoTaskUpdateAction.NAME,
          request,
          new org.opensearch.transport.TransportResponseHandler<TrinoTaskUpdateResponse>() {
            @Override
            public TrinoTaskUpdateResponse read(
                org.opensearch.core.common.io.stream.StreamInput in)
                throws java.io.IOException {
              return new TrinoTaskUpdateResponse(in);
            }

            @Override
            public void handleResponse(TrinoTaskUpdateResponse response) {
              try {
                TaskInfo info = codec.deserializeTaskInfo(response.getTaskInfoJson());
                lastTaskInfo.set(info);
                lastTaskStatus.set(info.getTaskStatus());
                LOG.debug("Task update response for {}: state={}, self={}",
                    taskId, info.getTaskStatus().getState(), info.getTaskStatus().getSelf());
              } catch (Exception e) {
                LOG.warn("Failed to deserialize task update response for {}", taskId, e);
              } finally {
                if (latch != null) {
                  latch.countDown();
                }
              }
            }

            @Override
            public void handleException(org.opensearch.transport.TransportException exp) {
              LOG.error("Transport error sending task update for {}: {}", taskId, exp.getMessage());
              if (latch != null) {
                latch.countDown();
              }
            }

            @Override
            public String executor() {
              return org.opensearch.threadpool.ThreadPool.Names.SAME;
            }
          });
    } catch (Exception e) {
      LOG.error("Failed to send task update for {}", taskId, e);
    }
  }

  private void pollStatus() {
    try {
      TrinoTaskStatusRequest request = new TrinoTaskStatusRequest(taskId.toString());
      transportService.sendRequest(
          targetNode,
          TrinoTaskStatusAction.NAME,
          request,
          new org.opensearch.transport.TransportResponseHandler<TrinoTaskStatusResponse>() {
            @Override
            public TrinoTaskStatusResponse read(
                org.opensearch.core.common.io.stream.StreamInput in)
                throws java.io.IOException {
              return new TrinoTaskStatusResponse(in);
            }

            @Override
            public void handleResponse(TrinoTaskStatusResponse response) {
              try {
                TaskInfo info = codec.deserializeTaskInfo(response.getTaskInfoJson());
                TaskInfo prev = lastTaskInfo.getAndSet(info);
                TaskStatus status = info.getTaskStatus();
                TaskStatus prevStatus = lastTaskStatus.getAndSet(status);
                LOG.info("Status poll for {}: state={}, self={}",
                    taskId, status.getState(), status.getSelf());
                // Notify listeners on status change
                if (prevStatus == null || !prevStatus.getState().equals(status.getState())) {
                  for (StateChangeListener<TaskStatus> listener : statusListeners) {
                    listener.stateChanged(status);
                  }
                }
                // If task is done, notify final info listeners and stop polling
                if (status.getState().isDone()) {
                  for (StateChangeListener<TaskInfo> listener : infoListeners) {
                    listener.stateChanged(info);
                  }
                  if (statusPollFuture != null) {
                    statusPollFuture.cancel(false);
                  }
                }
              } catch (Exception e) {
                LOG.debug("Failed to process status poll response for {}", taskId, e);
              }
            }

            @Override
            public void handleException(org.opensearch.transport.TransportException exp) {
              LOG.debug("Status poll failed for {}: {}", taskId, exp.getMessage());
            }

            @Override
            public String executor() {
              return org.opensearch.threadpool.ThreadPool.Names.SAME;
            }
          });
    } catch (Exception e) {
      LOG.debug("Failed to poll status for {}", taskId, e);
    }
  }

  /** Convert pending splits multimap to a list of SplitAssignment for serialization. */
  private List<SplitAssignment> buildSplitAssignments() {
    List<SplitAssignment> assignments = new ArrayList<>();
    // Group splits by PlanNodeId
    for (PlanNodeId planNodeId : new HashSet<>(pendingSplits.keySet())) {
      Set<ScheduledSplit> scheduledSplits = new LinkedHashSet<>();
      for (Split split : pendingSplits.get(planNodeId)) {
        scheduledSplits.add(
            new ScheduledSplit(splitSequenceId.getAndIncrement(), planNodeId, split));
      }
      boolean noMore = noMoreSplitsSources.contains(planNodeId)
          && !sentNoMoreSplits.contains(planNodeId);
      if (noMore) {
        sentNoMoreSplits.add(planNodeId);
      }
      assignments.add(new SplitAssignment(planNodeId, scheduledSplits, noMore));
    }
    // Also add no-more-splits signals for sources that have no pending splits
    for (PlanNodeId planNodeId : noMoreSplitsSources) {
      if (!pendingSplits.containsKey(planNodeId) && !sentNoMoreSplits.contains(planNodeId)) {
        sentNoMoreSplits.add(planNodeId);
        assignments.add(new SplitAssignment(planNodeId, Set.of(), true));
      }
    }
    return assignments;
  }

  private void sendCancel() {
    try {
      TrinoTaskCancelRequest request = new TrinoTaskCancelRequest(taskId.toString());

      transportService.sendRequest(
          targetNode,
          TrinoTaskCancelAction.NAME,
          request,
          new org.opensearch.transport.TransportResponseHandler<TrinoTaskCancelResponse>() {
            @Override
            public TrinoTaskCancelResponse read(
                org.opensearch.core.common.io.stream.StreamInput in)
                throws java.io.IOException {
              return new TrinoTaskCancelResponse(in);
            }

            @Override
            public void handleResponse(TrinoTaskCancelResponse response) {
              LOG.debug("Task {} cancelled successfully", taskId);
            }

            @Override
            public void handleException(org.opensearch.transport.TransportException exp) {
              LOG.warn("Error cancelling task {}: {}", taskId, exp.getMessage());
            }

            @Override
            public String executor() {
              return org.opensearch.threadpool.ThreadPool.Names.SAME;
            }
          });
    } catch (Exception e) {
      LOG.error("Failed to send cancel for {}", taskId, e);
    }
  }
}
