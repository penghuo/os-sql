/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.opensearch.executor.scheduler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.sql.executor.task.Task;
import org.opensearch.sql.executor.task.TaskId;
import org.opensearch.sql.executor.task.TaskScheduler;
import org.opensearch.sql.opensearch.executor.task.TransportTask;
import org.opensearch.sql.opensearch.storage.splits.OpenSearchSplit;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.storage.splits.Split;
import org.opensearch.sql.storage.splits.SplitManager;

@RequiredArgsConstructor
public class OpenSearchTaskScheduler implements TaskScheduler {

  private static final Logger LOG = LogManager.getLogger();

  private final ClusterService clusterService;

  private final NodeClient client;

  @SneakyThrows
  @Override
  public List<Task> schedule(LogicalPlan plan, Set<TaskId> sourceTaskIds) {
    SplitManager splitManager = plan.accept(new SplitVisitor(), null);
    List<Split> splits = splitManager == null ? Arrays.asList(new OpenSearchSplit()) :
        splitManager.nextBatch();
    List<Task> tasks = new ArrayList<>();

    if (splits.size() == 1 && splits.get(0).onLocalNode()) {
      DiscoveryNode localNode = clusterService.state().getNodes().getLocalNode();
      LOG.info("schedule task on local node {}", localNode.getId());
      final BytesStreamOutput output = new BytesStreamOutput();
      localNode.writeTo(output);
      tasks.add(new TransportTask(TaskId.taskId(output.toString()), plan, client, localNode,
          splits.get(0),
          sourceTaskIds));
    } else {
      List<DiscoveryNode> nodes = new ArrayList<>();
      clusterService.state().getNodes().iterator().forEachRemaining(nodes::add);
      int nodeIndex = 0;
      for (Split split : splits) {
        DiscoveryNode node = nodes.get(nodeIndex++);
        final BytesStreamOutput output = new BytesStreamOutput();
        node.writeTo(output);
        LOG.info("schedule task on node {}", node.getId());
        tasks.add(new TransportTask(TaskId.taskId(output.toString()), plan, client, node, split,
            sourceTaskIds));
        if (nodeIndex == nodes.size()) {
          nodeIndex = 0;
        }
      }
    }
    return tasks;
  }

  private static class SplitVisitor extends LogicalPlanNodeVisitor<SplitManager, Void> {
    @Override
    public SplitManager visitNode(LogicalPlan plan, Void context) {
      List<LogicalPlan> children = plan.getChild();
      if (children.isEmpty()) {
        return null;
      }
      return children.get(0).accept(this, context);
    }

    @Override
    public SplitManager visitRelation(LogicalRelation plan, Void context) {
      return plan.getTable().getSplitManager();
    }
  }
}
