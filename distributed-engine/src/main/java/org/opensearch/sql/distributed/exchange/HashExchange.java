/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.exchange;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.PagesSerde;
import org.opensearch.sql.distributed.data.ValueBlock;
import org.opensearch.sql.distributed.operator.SourceOperator;
import org.opensearch.sql.distributed.scheduler.NodeAssignment;
import org.opensearch.sql.distributed.transport.ShardQueryAction;
import org.opensearch.sql.distributed.transport.ShardQueryRequest;
import org.opensearch.sql.distributed.transport.ShardQueryResponse;
import org.opensearch.transport.client.Client;

/**
 * Source operator that implements hash-partitioned exchange. Hash-partitions input Pages by key
 * columns using murmur3 hash, sends each partition to the assigned target node via
 * TransportService, and collects Pages from all sources that map to this node's partition.
 *
 * <p>The hash exchange enables distributed joins and aggregations by ensuring rows with the same
 * key values end up on the same node.
 *
 * <p>Lifecycle:
 *
 * <ol>
 *   <li>On first getOutput(), dispatches upstream fragment execution to all participating nodes
 *   <li>Each node executes the upstream fragment, producing Pages
 *   <li>Each node hash-partitions its output and sends partitions to target nodes
 *   <li>This operator collects Pages destined for its local partition
 * </ol>
 *
 * <p>Thread-safe for concurrent page receipt from multiple source nodes.
 */
@Log4j2
public class HashExchange implements SourceOperator {

  private final OperatorContext operatorContext;
  private final Client client;
  private final String queryId;
  private final int stageId;
  private final byte[] serializedFragment;
  private final String indexName;
  private final NodeAssignment nodeAssignment;
  private final PagesSerde pagesSerde;
  private final List<Integer> partitionKeyChannels;
  private final int localPartitionId;

  private final Queue<Page> receivedPages = new ConcurrentLinkedQueue<>();
  private final AtomicInteger pendingResponses = new AtomicInteger(0);
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean finished = new AtomicBoolean(false);
  private volatile SettableFuture<Void> blockedFuture;
  private volatile Exception failure;

  /**
   * Creates a HashExchange operator.
   *
   * @param operatorContext the operator context
   * @param client the OpenSearch client for inter-node communication
   * @param queryId the unique query identifier
   * @param stageId the stage within the query plan
   * @param serializedFragment the serialized plan fragment to execute on remote nodes
   * @param indexName the target index name
   * @param nodeAssignment the shard-to-node assignment
   * @param partitionKeyChannels the column indices to hash on for partitioning
   * @param localPartitionId the partition ID assigned to this node
   */
  public HashExchange(
      OperatorContext operatorContext,
      Client client,
      String queryId,
      int stageId,
      byte[] serializedFragment,
      String indexName,
      NodeAssignment nodeAssignment,
      List<Integer> partitionKeyChannels,
      int localPartitionId) {
    this.operatorContext = operatorContext;
    this.client = client;
    this.queryId = queryId;
    this.stageId = stageId;
    this.serializedFragment = serializedFragment;
    this.indexName = indexName;
    this.nodeAssignment = nodeAssignment;
    this.pagesSerde = new PagesSerde();
    this.partitionKeyChannels = List.copyOf(partitionKeyChannels);
    this.localPartitionId = localPartitionId;
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
      throw new RuntimeException("HashExchange failed", failure);
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
   * Dispatches upstream fragment execution requests to each data node. Each node will execute the
   * fragment, hash-partition the results, and return only the pages assigned to this node's
   * partition.
   */
  private void dispatchRequests() {
    Map<DiscoveryNode, List<Integer>> assignments = nodeAssignment.getNodeToShards();
    int partitionCount = assignments.size();
    pendingResponses.set(assignments.size());

    HashPartitioner partitioner = new HashPartitioner(partitionKeyChannels, partitionCount);

    for (Map.Entry<DiscoveryNode, List<Integer>> entry : assignments.entrySet()) {
      DiscoveryNode node = entry.getKey();
      List<Integer> shardIds = entry.getValue();

      ShardQueryRequest request =
          new ShardQueryRequest(queryId, stageId, serializedFragment, shardIds, indexName);

      log.debug(
          "Dispatching hash exchange to node {}: queryId={}, stageId={}, "
              + "partitionKeys={}, shards={}",
          node.getName(),
          queryId,
          stageId,
          partitionKeyChannels,
          shardIds);

      client.execute(
          ShardQueryAction.INSTANCE,
          request,
          new ActionListener<>() {
            @Override
            public void onResponse(ShardQueryResponse response) {
              try {
                List<Page> pages = response.getPages(pagesSerde);

                // Hash-partition incoming pages and keep only pages for our
                // partition
                for (Page page : pages) {
                  Page localPage = extractPartition(page, partitioner, localPartitionId);
                  if (localPage != null && localPage.getPositionCount() > 0) {
                    receivedPages.add(localPage);
                  }
                }

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
                  "Hash exchange failed on node {}: queryId={}, stageId={}",
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

  /**
   * Extracts rows from the page that belong to the given partition.
   *
   * @param page the input page
   * @param partitioner the hash partitioner
   * @param targetPartition the partition to extract
   * @return a new page with only rows belonging to the target partition, or null if no rows match
   */
  static Page extractPartition(Page page, HashPartitioner partitioner, int targetPartition) {
    int positionCount = page.getPositionCount();
    if (positionCount == 0) {
      return null;
    }

    int[] partitions = partitioner.partition(page);

    // Count matching positions
    int matchCount = 0;
    for (int partition : partitions) {
      if (partition == targetPartition) {
        matchCount++;
      }
    }

    if (matchCount == 0) {
      return null;
    }

    if (matchCount == positionCount) {
      return page;
    }

    // Build array of selected positions
    int[] selectedPositions = new int[matchCount];
    int idx = 0;
    for (int i = 0; i < positionCount; i++) {
      if (partitions[i] == targetPartition) {
        selectedPositions[idx++] = i;
      }
    }

    // Copy selected positions from each block
    Block[] outputBlocks = new Block[page.getChannelCount()];
    for (int ch = 0; ch < page.getChannelCount(); ch++) {
      Block block = page.getBlock(ch);
      if (block instanceof ValueBlock valueBlock) {
        outputBlocks[ch] = valueBlock.copyPositions(selectedPositions, 0, matchCount);
      } else {
        outputBlocks[ch] = copyBlockPositions(block, selectedPositions, matchCount);
      }
    }

    return new Page(matchCount, outputBlocks);
  }

  /**
   * Splits a page into partitions, returning a map from partition ID to the page for that
   * partition. Used when a sender needs to route different partitions to different nodes.
   *
   * @param page the input page
   * @param partitioner the hash partitioner
   * @return map from partition ID to the sub-page for that partition
   */
  public static Map<Integer, Page> partitionPage(Page page, HashPartitioner partitioner) {
    int positionCount = page.getPositionCount();
    if (positionCount == 0) {
      return Map.of();
    }

    int[] partitions = partitioner.partition(page);
    int partitionCount = partitioner.getPartitionCount();

    // Group positions by partition
    Map<Integer, List<Integer>> partitionPositions = new HashMap<>();
    for (int i = 0; i < positionCount; i++) {
      partitionPositions.computeIfAbsent(partitions[i], k -> new ArrayList<>()).add(i);
    }

    Map<Integer, Page> result = new HashMap<>();
    for (Map.Entry<Integer, List<Integer>> entry : partitionPositions.entrySet()) {
      int partitionId = entry.getKey();
      List<Integer> positions = entry.getValue();
      int[] posArray = positions.stream().mapToInt(Integer::intValue).toArray();

      Block[] outputBlocks = new Block[page.getChannelCount()];
      for (int ch = 0; ch < page.getChannelCount(); ch++) {
        Block block = page.getBlock(ch);
        if (block instanceof ValueBlock valueBlock) {
          outputBlocks[ch] = valueBlock.copyPositions(posArray, 0, posArray.length);
        } else {
          outputBlocks[ch] = copyBlockPositions(block, posArray, posArray.length);
        }
      }

      result.put(partitionId, new Page(posArray.length, outputBlocks));
    }

    return result;
  }

  private static Block copyBlockPositions(Block block, int[] positions, int count) {
    if (block instanceof org.opensearch.sql.distributed.data.DictionaryBlock dict) {
      int[] newIds = new int[count];
      for (int i = 0; i < count; i++) {
        newIds[i] = dict.getId(positions[i]);
      }
      return dict.getDictionary().copyPositions(newIds, 0, count);
    }
    if (block instanceof org.opensearch.sql.distributed.data.RunLengthEncodedBlock rle) {
      return new org.opensearch.sql.distributed.data.RunLengthEncodedBlock(rle.getValue(), count);
    }
    throw new UnsupportedOperationException(
        "Cannot copy positions for: " + block.getClass().getSimpleName());
  }

  /** Wake up the blocked Driver. */
  private void unblock() {
    SettableFuture<Void> future = blockedFuture;
    if (future != null && !future.isDone()) {
      future.set(null);
    }
  }
}
