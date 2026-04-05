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
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.SpoolingOutputStats;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanNodeId;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
  private final AtomicInteger pendingSplitCount = new AtomicInteger(0);
  private final AtomicInteger queuedSplitCount = new AtomicInteger(0);

  private final Multimap<PlanNodeId, Split> pendingSplits =
      HashMultimap.create();
  private final Set<PlanNodeId> noMoreSplitsSources = new HashSet<>();

  private final CopyOnWriteArrayList<StateChangeListener<TaskStatus>> statusListeners =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<StateChangeListener<TaskInfo>> infoListeners =
      new CopyOnWriteArrayList<>();

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
    sendUpdate(Optional.of(planFragment));
  }

  @Override
  public void addSplits(Multimap<PlanNodeId, Split> splits) {
    synchronized (pendingSplits) {
      pendingSplits.putAll(splits);
      pendingSplitCount.addAndGet(splits.size());
    }
    sendUpdate(Optional.empty());
  }

  @Override
  public void noMoreSplits(PlanNodeId sourceId) {
    synchronized (pendingSplits) {
      noMoreSplitsSources.add(sourceId);
    }
    sendUpdate(Optional.empty());
  }

  @Override
  public void setOutputBuffers(OutputBuffers newOutputBuffers) {
    outputBuffers.set(newOutputBuffers);
    if (started.get()) {
      sendUpdate(Optional.empty());
    }
  }

  @Override
  public void setSpeculative(boolean speculative) {
    // Not supported in transport-based execution
  }

  @Override
  public void cancel() {
    sendCancel();
  }

  @Override
  public void abort() {
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

  private void sendUpdate(Optional<PlanFragment> fragment) {
    try {
      byte[] fragmentJson = fragment.map(codec::serializePlanFragment).orElse(new byte[0]);
      // TODO: serialize actual pending splits and session in Task 17
      byte[] splitsJson = codec.serializeSplitAssignments(List.of());
      byte[] buffersJson = codec.serializeOutputBuffers(outputBuffers.get());
      byte[] sessionJson = new byte[0]; // Wired in Task 17

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
              } catch (Exception e) {
                LOG.warn("Failed to deserialize task update response for {}", taskId, e);
              }
            }

            @Override
            public void handleException(org.opensearch.transport.TransportException exp) {
              LOG.error("Transport error sending task update for {}: {}", taskId, exp.getMessage());
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
