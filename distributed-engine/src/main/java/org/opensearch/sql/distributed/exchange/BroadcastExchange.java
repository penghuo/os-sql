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
 * Source operator that implements broadcast exchange. Gathers all Pages from the upstream
 * (typically the small/build side of a join) and makes a full copy available on every participating
 * node.
 *
 * <p>Used for small-table joins where the build side is small enough to broadcast to all nodes. The
 * broadcast exchange first gathers all data (like GatherExchange), then replicates the complete
 * dataset to every node that needs it.
 *
 * <p>Memory-bounded: respects the configured maximum buffer size. If the broadcasted data exceeds
 * the buffer limit, the exchange will fail fast.
 *
 * <p>Thread-safe for concurrent page receipt from multiple source nodes.
 */
@Log4j2
public class BroadcastExchange implements SourceOperator {

  private final OperatorContext operatorContext;
  private final Client client;
  private final String queryId;
  private final int stageId;
  private final byte[] serializedFragment;
  private final String indexName;
  private final NodeAssignment nodeAssignment;
  private final PagesSerde pagesSerde;
  private final long maxBufferedBytes;

  private final Queue<Page> receivedPages = new ConcurrentLinkedQueue<>();
  private final AtomicInteger pendingResponses = new AtomicInteger(0);
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean finished = new AtomicBoolean(false);
  private volatile SettableFuture<Void> blockedFuture;
  private volatile Exception failure;

  /**
   * Creates a BroadcastExchange operator.
   *
   * @param operatorContext the operator context
   * @param client the OpenSearch client for inter-node communication
   * @param queryId the unique query identifier
   * @param stageId the stage within the query plan
   * @param serializedFragment the serialized plan fragment to execute on remote nodes
   * @param indexName the target index name
   * @param nodeAssignment the shard-to-node assignment
   */
  public BroadcastExchange(
      OperatorContext operatorContext,
      Client client,
      String queryId,
      int stageId,
      byte[] serializedFragment,
      String indexName,
      NodeAssignment nodeAssignment) {
    this(
        operatorContext,
        client,
        queryId,
        stageId,
        serializedFragment,
        indexName,
        nodeAssignment,
        OutputBuffer.DEFAULT_MAX_BUFFERED_BYTES);
  }

  /**
   * Creates a BroadcastExchange operator with configurable memory limit.
   *
   * @param operatorContext the operator context
   * @param client the OpenSearch client for inter-node communication
   * @param queryId the unique query identifier
   * @param stageId the stage within the query plan
   * @param serializedFragment the serialized plan fragment to execute on remote nodes
   * @param indexName the target index name
   * @param nodeAssignment the shard-to-node assignment
   * @param maxBufferedBytes maximum bytes to buffer before failing
   */
  public BroadcastExchange(
      OperatorContext operatorContext,
      Client client,
      String queryId,
      int stageId,
      byte[] serializedFragment,
      String indexName,
      NodeAssignment nodeAssignment,
      long maxBufferedBytes) {
    this.operatorContext = operatorContext;
    this.client = client;
    this.queryId = queryId;
    this.stageId = stageId;
    this.serializedFragment = serializedFragment;
    this.indexName = indexName;
    this.nodeAssignment = nodeAssignment;
    this.pagesSerde = new PagesSerde();
    this.maxBufferedBytes = maxBufferedBytes;
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
      throw new RuntimeException("BroadcastExchange failed", failure);
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

  /**
   * Dispatches requests to all data nodes. Each node's response pages are collected and made
   * available as output — broadcast semantics mean every node receives the full dataset, so from
   * this operator's perspective we simply gather all pages (like GatherExchange).
   *
   * <p>The "broadcast" semantics are achieved at the scheduler level: every participating node runs
   * an instance of this operator, and each instance gathers the full dataset from all source nodes.
   */
  private void dispatchRequests() {
    Map<DiscoveryNode, List<Integer>> assignments = nodeAssignment.getNodeToShards();
    pendingResponses.set(assignments.size());

    for (Map.Entry<DiscoveryNode, List<Integer>> entry : assignments.entrySet()) {
      DiscoveryNode node = entry.getKey();
      List<Integer> shardIds = entry.getValue();

      ShardQueryRequest request =
          new ShardQueryRequest(queryId, stageId, serializedFragment, shardIds, indexName);

      log.debug(
          "Dispatching broadcast exchange to node {}: queryId={}, stageId={}, shards={}",
          node.getName(),
          queryId,
          stageId,
          shardIds);

      client.execute(
          ShardQueryAction.INSTANCE,
          request,
          new ActionListener<>() {
            @Override
            public void onResponse(ShardQueryResponse response) {
              try {
                List<Page> pages = response.getPages(pagesSerde);

                // Check memory bounds
                long totalSize = 0;
                for (Page page : pages) {
                  totalSize += page.getSizeInBytes();
                }
                if (totalSize > maxBufferedBytes) {
                  onFailure(
                      new IllegalStateException(
                          "BroadcastExchange exceeds memory limit: "
                              + totalSize
                              + " > "
                              + maxBufferedBytes
                              + " bytes. Consider using "
                              + "HashExchange instead."));
                  return;
                }

                // All pages from all nodes are collected (full broadcast)
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
              log.error(
                  "Broadcast exchange failed on node {}: queryId={}, stageId={}",
                  node.getName(),
                  queryId,
                  stageId,
                  e);
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
