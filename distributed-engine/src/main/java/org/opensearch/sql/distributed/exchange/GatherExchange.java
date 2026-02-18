/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.exchange;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.log4j.Log4j2;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.PagesSerde;
import org.opensearch.sql.distributed.operator.SourceOperator;
import org.opensearch.sql.distributed.scheduler.NodeAssignment;
import org.opensearch.sql.distributed.transport.ShardQueryAction;
import org.opensearch.sql.distributed.transport.ShardQueryRequest;
import org.opensearch.sql.distributed.transport.ShardQueryResponse;
import org.opensearch.transport.client.Client;

/**
 * Source operator on the coordinator side that implements scatter-gather exchange. Sends
 * ShardQueryRequests to each data node, collects response Pages, and outputs them in the pipeline.
 *
 * <p>Supports streaming: each data node may return multiple response chunks. The operator is
 * blocked until at least one page is available or all sources have completed.
 */
@Log4j2
public class GatherExchange implements SourceOperator {

  private final OperatorContext operatorContext;
  private final Client client;
  private final String queryId;
  private final int stageId;
  private final byte[] serializedFragment;
  private final String indexName;
  private final NodeAssignment nodeAssignment;
  private final PagesSerde pagesSerde;

  private final Queue<Page> receivedPages = new ConcurrentLinkedQueue<>();
  private final AtomicInteger pendingResponses = new AtomicInteger(0);
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean finished = new AtomicBoolean(false);
  private volatile SettableFuture<Void> blockedFuture;
  private volatile Exception failure;

  public GatherExchange(
      OperatorContext operatorContext,
      Client client,
      String queryId,
      int stageId,
      byte[] serializedFragment,
      String indexName,
      NodeAssignment nodeAssignment) {
    this.operatorContext = operatorContext;
    this.client = client;
    this.queryId = queryId;
    this.stageId = stageId;
    this.serializedFragment = serializedFragment;
    this.indexName = indexName;
    this.nodeAssignment = nodeAssignment;
    this.pagesSerde = new PagesSerde();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (!receivedPages.isEmpty() || finished.get() || failure != null) {
      return NOT_BLOCKED;
    }
    if (blockedFuture == null || blockedFuture.isDone()) {
      blockedFuture = SettableFuture.create();
    }
    // Double-check after creating the future
    if (!receivedPages.isEmpty() || finished.get() || failure != null) {
      blockedFuture.set(null);
    }
    return blockedFuture;
  }

  @Override
  public Page getOutput() {
    if (failure != null) {
      throw new RuntimeException("GatherExchange failed", failure);
    }

    if (!started.getAndSet(true)) {
      dispatchRequests();
    }

    Page page = receivedPages.poll();
    if (page != null) {
      operatorContext.recordOutputPositions(page.getPositionCount());
    }
    return page;
  }

  @Override
  public void finish() {
    finished.set(true);
    unblock();
  }

  @Override
  public boolean isFinished() {
    return finished.get() && receivedPages.isEmpty();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public void close() throws Exception {
    finished.set(true);
    receivedPages.clear();
    unblock();
  }

  /** Dispatch ShardQueryRequests to each data node. */
  private void dispatchRequests() {
    Map<DiscoveryNode, List<Integer>> assignments = nodeAssignment.getNodeToShards();
    pendingResponses.set(assignments.size());

    for (Map.Entry<DiscoveryNode, List<Integer>> entry : assignments.entrySet()) {
      DiscoveryNode node = entry.getKey();
      List<Integer> shardIds = entry.getValue();

      ShardQueryRequest request =
          new ShardQueryRequest(queryId, stageId, serializedFragment, shardIds, indexName);

      log.debug(
          "Dispatching shard query to node {}: queryId={}, shards={}",
          node.getName(),
          queryId,
          shardIds);

      client.execute(
          ShardQueryAction.INSTANCE,
          request,
          new ActionListener<>() {
            @Override
            public void onResponse(ShardQueryResponse response) {
              try {
                List<Page> pages = response.getPages(pagesSerde);
                receivedPages.addAll(pages);

                if (pendingResponses.decrementAndGet() == 0 && !response.hasMore()) {
                  finished.set(true);
                }

                unblock();
              } catch (Exception e) {
                onFailure(e);
              }
            }

            @Override
            public void onFailure(Exception e) {
              log.error("Shard query failed on node {}: queryId={}", node.getName(), queryId, e);
              failure = e;
              if (pendingResponses.decrementAndGet() == 0) {
                finished.set(true);
              }
              unblock();
            }
          });
    }
  }

  /** Wake up the blocked Driver. */
  private void unblock() {
    SettableFuture<Void> future = blockedFuture;
    if (future != null && !future.isDone()) {
      future.set(null);
    }
  }
}
