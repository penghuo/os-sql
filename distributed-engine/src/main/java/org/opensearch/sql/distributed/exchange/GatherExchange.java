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
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.PagesSerde;
import org.opensearch.sql.distributed.operator.SourceOperator;
import org.opensearch.sql.distributed.scheduler.NodeAssignment;
import org.opensearch.sql.distributed.transport.ShardQueryAction;
import org.opensearch.sql.distributed.transport.ShardQueryRequest;
import org.opensearch.sql.distributed.transport.ShardQueryResponse;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

/**
 * Source operator on the coordinator side that implements scatter-gather exchange. Sends
 * ShardQueryRequests to each data node via TransportService, collects response Pages, and outputs
 * them in the pipeline.
 *
 * <p>Dispatch happens eagerly in {@link #isBlocked()} (the first method the Driver calls), not in
 * {@link #getOutput()}, to avoid deadlock — the Driver calls isBlocked() before getOutput(), and
 * if dispatch hasn't happened yet, the future never resolves.
 *
 * <p>Uses {@code transportService.sendRequest()} instead of {@code client.execute()} to ensure
 * the transport handler runs on the GENERIC thread pool, not inline on the calling thread.
 */
@Log4j2
public class GatherExchange implements SourceOperator {

  private final OperatorContext operatorContext;
  private final TransportService transportService;
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
      TransportService transportService,
      String queryId,
      int stageId,
      byte[] serializedFragment,
      String indexName,
      NodeAssignment nodeAssignment) {
    this.operatorContext = operatorContext;
    this.transportService = transportService;
    this.queryId = queryId;
    this.stageId = stageId;
    this.serializedFragment = serializedFragment;
    this.indexName = indexName;
    this.nodeAssignment = nodeAssignment;
    this.pagesSerde = new PagesSerde();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    // Eagerly dispatch on first call — Driver calls isBlocked() before getOutput(),
    // so dispatching here ensures the transport requests are in flight before
    // we check whether we're blocked.
    if (!started.getAndSet(true)) {
      dispatchRequests();
    }

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

  /** Dispatch ShardQueryRequests to each data node via TransportService. */
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

      transportService.sendRequest(
          node,
          ShardQueryAction.NAME,
          request,
          new TransportResponseHandler<ShardQueryResponse>() {
            @Override
            public ShardQueryResponse read(
                org.opensearch.core.common.io.stream.StreamInput in)
                throws java.io.IOException {
              return new ShardQueryResponse(in);
            }

            @Override
            public String executor() {
              return org.opensearch.threadpool.ThreadPool.Names.SAME;
            }

            @Override
            public void handleResponse(ShardQueryResponse response) {
              try {
                List<Page> pages = response.getPages(pagesSerde);
                receivedPages.addAll(pages);
                if (pendingResponses.decrementAndGet() == 0 && !response.hasMore()) {
                  finished.set(true);
                }
                unblock();
              } catch (Exception e) {
                handleException(new TransportException(e));
              }
            }

            @Override
            public void handleException(TransportException e) {
              log.error(
                  "Shard query failed on node {}: queryId={}", node.getName(), queryId, e);
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
