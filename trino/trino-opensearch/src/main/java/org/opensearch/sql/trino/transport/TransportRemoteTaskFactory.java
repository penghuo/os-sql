/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import com.google.common.collect.Multimap;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.execution.NodeTaskMap.PartitionedSplitCountTracker;
import io.trino.execution.RemoteTask;
import io.trino.execution.RemoteTaskFactory;
import io.trino.execution.TaskId;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.airlift.units.DataSize;
import java.util.Optional;
import java.util.Set;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.sql.trino.node.OpenSearchNodeManager;
import org.opensearch.transport.TransportService;

/**
 * Factory for creating {@link TransportRemoteTask} instances. Bound in Guice as the {@link
 * RemoteTaskFactory} implementation. {@code SqlQueryScheduler} calls this to create one RemoteTask
 * per (stage, worker node) pair.
 */
public class TransportRemoteTaskFactory implements RemoteTaskFactory {

  private final TransportService transportService;
  private final OpenSearchNodeManager nodeManager;
  private final TrinoJsonCodec codec;

  public TransportRemoteTaskFactory(
      TransportService transportService,
      OpenSearchNodeManager nodeManager,
      TrinoJsonCodec codec) {
    this.transportService = transportService;
    this.nodeManager = nodeManager;
    this.codec = codec;
  }

  @Override
  public RemoteTask createRemoteTask(
      Session session,
      Span span,
      TaskId taskId,
      InternalNode node,
      boolean speculative,
      PlanFragment fragment,
      Multimap<PlanNodeId, Split> initialSplits,
      OutputBuffers outputBuffers,
      PartitionedSplitCountTracker tracker,
      Set<DynamicFilterId> outboundDynamicFilterIds,
      Optional<DataSize> estimatedMemory,
      boolean summarizeTaskInfo) {

    // Map Trino InternalNode to OpenSearch DiscoveryNode
    DiscoveryNode osNode = nodeManager.toDiscoveryNode(node);

    return new TransportRemoteTask(
        transportService,
        osNode,
        taskId,
        node,
        fragment,
        session,
        codec,
        outputBuffers,
        initialSplits);
  }
}
