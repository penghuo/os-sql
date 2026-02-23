/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.action;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

/**
 * Transport request to execute a plan fragment (stage) on a data node.
 *
 * <p>The coordinator sends this to each data node with the relevant shard splits. The data node
 * deserializes the plan fragment, creates a {@code Driver} + {@code Pipeline}, and starts execution
 * on the {@code dqe_worker} thread pool.
 *
 * <p>Fields:
 *
 * <ul>
 *   <li>{@code queryId} — identifies the query
 *   <li>{@code stageId} — identifies the stage
 *   <li>{@code serializedPlanFragment} — opaque plan fragment bytes (serialized by dqe-execution's
 *       PlanFragmentSerializer)
 *   <li>{@code shardSplitInfos} — serialized shard split data for this node
 *   <li>{@code coordinatorNodeId} — coordinator node for gather exchange
 *   <li>{@code queryMemoryBudgetBytes} — per-query memory budget
 * </ul>
 *
 * <p>Note: This request serializes shard split info as a list of {@link ShardSplitInfo} value
 * objects rather than directly depending on {@code DqeShardSplit} from dqe-metadata, keeping the
 * wire format self-contained within the exchange module. The conversion between {@code
 * DqeShardSplit} and {@code ShardSplitInfo} is done by the caller (StageScheduler).
 */
public class DqeStageExecuteRequest extends TransportRequest {

  private final String queryId;
  private final int stageId;
  private final byte[] serializedPlanFragment;
  private final List<ShardSplitInfo> shardSplitInfos;
  private final String coordinatorNodeId;
  private final long queryMemoryBudgetBytes;

  /**
   * Construct a stage execute request.
   *
   * @param queryId query identifier
   * @param stageId stage identifier
   * @param serializedPlanFragment opaque plan fragment bytes
   * @param shardSplitInfos shard split descriptors for this data node
   * @param coordinatorNodeId coordinator node ID for gather exchange
   * @param queryMemoryBudgetBytes per-query memory budget in bytes
   */
  public DqeStageExecuteRequest(
      String queryId,
      int stageId,
      byte[] serializedPlanFragment,
      List<ShardSplitInfo> shardSplitInfos,
      String coordinatorNodeId,
      long queryMemoryBudgetBytes) {
    this.queryId = Objects.requireNonNull(queryId, "queryId must not be null");
    this.stageId = stageId;
    this.serializedPlanFragment =
        Objects.requireNonNull(serializedPlanFragment, "serializedPlanFragment must not be null");
    this.shardSplitInfos =
        Collections.unmodifiableList(
            new ArrayList<>(
                Objects.requireNonNull(shardSplitInfos, "shardSplitInfos must not be null")));
    this.coordinatorNodeId =
        Objects.requireNonNull(coordinatorNodeId, "coordinatorNodeId must not be null");
    this.queryMemoryBudgetBytes = queryMemoryBudgetBytes;
  }

  /** Deserialization constructor. */
  public DqeStageExecuteRequest(StreamInput in) throws IOException {
    super(in);
    this.queryId = in.readString();
    this.stageId = in.readVInt();
    this.serializedPlanFragment = in.readByteArray();
    int splitCount = in.readVInt();
    List<ShardSplitInfo> splits = new ArrayList<>(splitCount);
    for (int i = 0; i < splitCount; i++) {
      splits.add(new ShardSplitInfo(in));
    }
    this.shardSplitInfos = Collections.unmodifiableList(splits);
    this.coordinatorNodeId = in.readString();
    this.queryMemoryBudgetBytes = in.readVLong();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(queryId);
    out.writeVInt(stageId);
    out.writeByteArray(serializedPlanFragment);
    out.writeVInt(shardSplitInfos.size());
    for (ShardSplitInfo split : shardSplitInfos) {
      split.writeTo(out);
    }
    out.writeString(coordinatorNodeId);
    out.writeVLong(queryMemoryBudgetBytes);
  }

  public String getQueryId() {
    return queryId;
  }

  public int getStageId() {
    return stageId;
  }

  public byte[] getSerializedPlanFragment() {
    return serializedPlanFragment;
  }

  public List<ShardSplitInfo> getShardSplitInfos() {
    return shardSplitInfos;
  }

  public String getCoordinatorNodeId() {
    return coordinatorNodeId;
  }

  public long getQueryMemoryBudgetBytes() {
    return queryMemoryBudgetBytes;
  }

  /**
   * Lightweight shard split descriptor for wire serialization within the exchange module.
   *
   * <p>Contains the same logical fields as {@code DqeShardSplit} from dqe-metadata but is
   * self-contained for transport serialization.
   */
  public static class ShardSplitInfo {

    private final int shardId;
    private final String nodeId;
    private final String indexName;
    private final boolean primary;

    public ShardSplitInfo(int shardId, String nodeId, String indexName, boolean primary) {
      this.shardId = shardId;
      this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
      this.indexName = Objects.requireNonNull(indexName, "indexName must not be null");
      this.primary = primary;
    }

    public ShardSplitInfo(StreamInput in) throws IOException {
      this.shardId = in.readVInt();
      this.nodeId = in.readString();
      this.indexName = in.readString();
      this.primary = in.readBoolean();
    }

    public void writeTo(StreamOutput out) throws IOException {
      out.writeVInt(shardId);
      out.writeString(nodeId);
      out.writeString(indexName);
      out.writeBoolean(primary);
    }

    public int getShardId() {
      return shardId;
    }

    public String getNodeId() {
      return nodeId;
    }

    public String getIndexName() {
      return indexName;
    }

    public boolean isPrimary() {
      return primary;
    }
  }
}
