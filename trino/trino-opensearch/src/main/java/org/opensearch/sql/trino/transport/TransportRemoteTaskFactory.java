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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.sql.trino.node.OpenSearchNodeManager;
import org.opensearch.transport.TransportService;

/**
 * Hybrid {@link RemoteTaskFactory} for split-level distribution. Delegates to the original
 * HTTP-based factory for tasks scheduled on the coordinator (local node), and uses transport-based
 * dispatch for tasks on remote OpenSearch nodes.
 *
 * <p>This is critical because the coordinator's result pipeline (Query → DirectExchangeClient)
 * expects the root stage to be accessible via the standard Trino HTTP path. Dispatching the root
 * stage via transport would bypass this path and break result fetching.
 */
public class TransportRemoteTaskFactory implements RemoteTaskFactory {

  private static final Logger LOG = LogManager.getLogger(TransportRemoteTaskFactory.class);

  private final TransportService transportService;
  private final OpenSearchNodeManager nodeManager;
  private final TrinoJsonCodec codec;
  private final RemoteTaskFactory httpDelegate;

  public TransportRemoteTaskFactory(
      TransportService transportService,
      OpenSearchNodeManager nodeManager,
      TrinoJsonCodec codec,
      RemoteTaskFactory httpDelegate) {
    this.transportService = transportService;
    this.nodeManager = nodeManager;
    this.codec = codec;
    this.httpDelegate = httpDelegate;
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

    // Check if this task targets the local node
    InternalNode currentNode = nodeManager.getCurrentNode();
    boolean isLocal = node.getNodeIdentifier().equals(currentNode.getNodeIdentifier());

    if (isLocal) {
      // Use standard HTTP-based dispatch for local tasks.
      // This is essential for the root stage — the coordinator reads results via the
      // standard Trino HTTP exchange mechanism which requires HttpRemoteTask.
      LOG.debug("Creating HTTP RemoteTask for local node: task={}", taskId);
      return httpDelegate.createRemoteTask(
          session, span, taskId, node, speculative, fragment, initialSplits,
          outputBuffers, tracker, outboundDynamicFilterIds, estimatedMemory, summarizeTaskInfo);
    }

    // Use transport-based dispatch for remote tasks
    LOG.info("Creating TransportRemoteTask for remote node: task={}, node={}",
        taskId, node.getNodeIdentifier());
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
