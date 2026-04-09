/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.iceberg;

import io.trino.spi.type.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.opensearch.sql.dqe.coordinator.fragment.PlanFragment;
import org.opensearch.sql.dqe.coordinator.fragment.PlanFragmenter;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanVisitor;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;

/**
 * Fragments an Iceberg query plan into per-file splits. Each split corresponds to one Parquet data
 * file (or a range within it). All splits are assigned to the local node for single-node execution.
 */
public class IcebergFragmenter {

  /**
   * Fragment the plan into per-file splits using Iceberg's scan planning API.
   *
   * @param plan the optimized DQE plan
   * @param warehousePath the Iceberg warehouse root path
   * @param columnTypeMap column name to Trino type mapping
   * @param localNodeId the local node ID for fragment assignment
   * @return a FragmentResult with one PlanFragment per Parquet file
   */
  public PlanFragmenter.FragmentResult fragment(
      DqePlanNode plan,
      String warehousePath,
      Map<String, Type> columnTypeMap,
      String localNodeId) {
    String tableName = findTableName(plan);
    IcebergTableResolver resolver = new IcebergTableResolver(warehousePath);
    Table table = resolver.loadTable(tableName);

    List<IcebergSplitInfo> splits = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        splits.add(
            new IcebergSplitInfo(
                task.file().path().toString(),
                task.start(),
                task.length(),
                task.file().fileSizeInBytes(),
                tableName));
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to plan Iceberg file scan", e);
    }

    // Build one PlanFragment per split, all assigned to local node
    List<PlanFragment> fragments = new ArrayList<>(splits.size());
    for (int i = 0; i < splits.size(); i++) {
      fragments.add(new PlanFragment(plan, tableName, i, localNodeId));
    }

    // Build coordinator plan (same logic as PlanFragmenter)
    DqePlanNode coordinatorPlan = buildCoordinatorPlan(plan);
    return new PlanFragmenter.FragmentResult(fragments, coordinatorPlan);
  }

  /** Extract the list of splits from the table (used by the coordinator for dispatch). */
  public List<IcebergSplitInfo> planSplits(String tableName, String warehousePath) {
    IcebergTableResolver resolver = new IcebergTableResolver(warehousePath);
    Table table = resolver.loadTable(tableName);
    List<IcebergSplitInfo> splits = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        splits.add(
            new IcebergSplitInfo(
                task.file().path().toString(),
                task.start(),
                task.length(),
                task.file().fileSizeInBytes(),
                tableName));
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to plan Iceberg file scan", e);
    }
    return splits;
  }

  /**
   * Assign splits to cluster nodes via round-robin for load balancing.
   * Any node can process any split on shared storage (local FS, S3).
   */
  public List<PlanFragment> assignSplitsToNodes(
      DqePlanNode plan,
      List<IcebergSplitInfo> splits,
      org.opensearch.cluster.node.DiscoveryNodes clusterNodes) {
    List<String> nodeIds = new ArrayList<>();
    for (org.opensearch.cluster.node.DiscoveryNode node : clusterNodes) {
      if (node.isDataNode()) {
        nodeIds.add(node.getId());
      }
    }
    if (nodeIds.isEmpty()) {
      for (org.opensearch.cluster.node.DiscoveryNode node : clusterNodes) {
        nodeIds.add(node.getId());
      }
    }
    List<PlanFragment> fragments = new ArrayList<>(splits.size());
    for (int i = 0; i < splits.size(); i++) {
      String nodeId = nodeIds.get(i % nodeIds.size());
      fragments.add(new PlanFragment(plan, splits.get(i).tableName(), i, nodeId));
    }
    return fragments;
  }

  private static String findTableName(DqePlanNode plan) {
    return plan.accept(
        new DqePlanVisitor<String, Void>() {
          @Override
          public String visitTableScan(TableScanNode node, Void context) {
            return node.getIndexName();
          }

          @Override
          public String visitPlan(DqePlanNode node, Void context) {
            for (DqePlanNode child : node.getChildren()) {
              String result = child.accept(this, context);
              if (result != null) return result;
            }
            return null;
          }
        },
        null);
  }

  public static DqePlanNode buildCoordinatorPlan(DqePlanNode plan) {
    AggregationNode aggNode =
        plan.accept(
            new DqePlanVisitor<AggregationNode, Void>() {
              @Override
              public AggregationNode visitAggregation(AggregationNode node, Void context) {
                return node;
              }

              @Override
              public AggregationNode visitPlan(DqePlanNode node, Void context) {
                for (DqePlanNode child : node.getChildren()) {
                  AggregationNode result = child.accept(this, context);
                  if (result != null) return result;
                }
                return null;
              }
            },
            null);
    if (aggNode != null && aggNode.getStep() == AggregationNode.Step.PARTIAL) {
      return new AggregationNode(
          null,
          aggNode.getGroupByKeys(),
          aggNode.getAggregateFunctions(),
          AggregationNode.Step.FINAL);
    }
    if (aggNode != null) {
      return new AggregationNode(
          null,
          aggNode.getGroupByKeys(),
          aggNode.getAggregateFunctions(),
          AggregationNode.Step.SINGLE);
    }
    return null;
  }
}
