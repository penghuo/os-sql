/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.fragment;

import io.trino.spi.type.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanVisitor;
import org.opensearch.sql.dqe.planner.plan.EvalNode;
import org.opensearch.sql.dqe.planner.plan.FilterNode;
import org.opensearch.sql.dqe.planner.plan.LimitNode;
import org.opensearch.sql.dqe.planner.plan.ProjectNode;
import org.opensearch.sql.dqe.planner.plan.SortNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;

/**
 * Splits a logical plan into per-shard fragments using cluster routing state. For aggregation
 * queries, produces PARTIAL shard fragments and a FINAL coordinator plan.
 */
public class PlanFragmenter {

  /**
   * Pattern to extract aggregate function name, DISTINCT flag, and argument from expressions like
   * {@code COUNT(DISTINCT UserID)}, {@code SUM(amount)}, etc.
   */
  private static final Pattern AGG_PATTERN =
      Pattern.compile(
          "^(COUNT|SUM|MIN|MAX|AVG)\\((DISTINCT\\s+)?(.+?)\\)$", Pattern.CASE_INSENSITIVE);

  /** Result of fragmenting a plan: per-shard fragments and an optional coordinator plan. */
  public record FragmentResult(List<PlanFragment> shardFragments, DqePlanNode coordinatorPlan) {}

  /**
   * Fragment the given plan across shards of the target index.
   *
   * @param plan the logical plan to fragment
   * @param clusterState the current cluster state for shard routing
   * @return fragment result with per-shard plans and optional coordinator plan
   */
  public FragmentResult fragment(DqePlanNode plan, ClusterState clusterState) {
    return fragment(plan, clusterState, Map.of());
  }

  /**
   * Fragment the given plan across shards of the target index, with column type information for
   * optimized shard plan generation (e.g., shard-level dedup for COUNT(DISTINCT) on numeric cols).
   *
   * @param plan the logical plan to fragment
   * @param clusterState the current cluster state for shard routing
   * @param columnTypeMap mapping from column names to Trino types
   * @return fragment result with per-shard plans and optional coordinator plan
   */
  public FragmentResult fragment(
      DqePlanNode plan, ClusterState clusterState, Map<String, Type> columnTypeMap) {
    // 1. Walk plan to find TableScanNode -> get index name
    String indexName = findIndexName(plan);

    // 2. Look up routing table to get shard information
    IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexName);
    Map<Integer, IndexShardRoutingTable> shards = indexRoutingTable.shards();

    // 3. Build shard plan — only strip Sort/Limit/HAVING for multi-shard aggregation
    boolean multiShard = shards.size() > 1;
    DqePlanNode shardPlan = multiShard ? buildShardPlan(plan, columnTypeMap, shards.size()) : plan;

    // 4. Create shard fragments
    List<PlanFragment> shardFragments = new ArrayList<>();
    for (Map.Entry<Integer, IndexShardRoutingTable> entry : shards.entrySet()) {
      int shardId = entry.getKey();
      ShardRouting primaryShard = entry.getValue().primaryShard();
      String nodeId = primaryShard.currentNodeId();
      shardFragments.add(new PlanFragment(shardPlan, indexName, shardId, nodeId));
    }

    // 5. If multi-shard and AggregationNode present, create coordinator plan for merging
    DqePlanNode coordinatorPlan = multiShard ? buildCoordinatorPlan(plan) : null;

    return new FragmentResult(shardFragments, coordinatorPlan);
  }

  /**
   * Build the shard plan. For aggregation queries:
   *
   * <ul>
   *   <li>PARTIAL: Strip Sort/Limit above aggregation (coordinator applies those after merge).
   *       Shard runs: [EvalNode →] AggregationNode(PARTIAL) → Filter → Scan.
   *   <li>SINGLE (COUNT DISTINCT) with GROUP BY and only COUNT(DISTINCT) aggregates: Dedup at shard
   *       level by creating a GROUP BY (original_keys + distinct_columns) with COUNT(*)
   *       aggregation. This reduces data sent to coordinator by deduplicating per shard.
   *   <li>SINGLE (other): Strip aggregation and above. Shard runs: Filter → Scan.
   * </ul>
   *
   * For non-aggregation queries: use the full plan.
   */
  private DqePlanNode buildShardPlan(
      DqePlanNode plan, Map<String, Type> columnTypeMap, int numShards) {
    AggregationNode aggNode = findAggregationNode(plan);
    if (aggNode == null) {
      return plan;
    }
    if (aggNode.getStep() == AggregationNode.Step.PARTIAL) {
      // Optimization: for Limit -> Sort -> Agg(PARTIAL) patterns, keep Sort/Limit on the
      // shard plan with an inflated limit. This lets each shard apply top-K pre-filtering,
      // dramatically reducing the number of groups sent to the coordinator. For example,
      // Q34 (GROUP BY URL COUNT(*) ORDER BY c DESC LIMIT 10) sends ~21K groups per shard
      // without pre-filtering but only ~max(1000, 10*8) groups with it.
      DqePlanNode shardPlanWithTopN = buildShardPlanWithInflatedLimit(plan, aggNode, numShards);
      if (shardPlanWithTopN != null) {
        return shardPlanWithTopN;
      }
      // Strip Sort, Limit, and Project above the aggregation.
      // Walk up from the aggregation to find it in the tree and return it as the root.
      return stripAboveAggregation(plan);
    }
    // SINGLE with GROUP BY: try shard-level dedup for COUNT(DISTINCT)-only queries.
    // Each shard performs GROUP BY (original_keys + distinct_columns) with COUNT(*),
    // deduplicating values locally. The coordinator then runs the full aggregation
    // on pre-deduped data, significantly reducing work for high-cardinality datasets.
    // This applies to any column type (numeric and VARCHAR) since the generic hash
    // aggregation path handles all types.
    if (!aggNode.getGroupByKeys().isEmpty() && !columnTypeMap.isEmpty()) {
      List<String> distinctColumns = extractCountDistinctColumns(aggNode.getAggregateFunctions());
      if (distinctColumns != null) {
        // Build dedup GROUP BY: original keys + distinct columns, with COUNT(*)
        List<String> dedupKeys = new ArrayList<>(aggNode.getGroupByKeys());
        dedupKeys.addAll(distinctColumns);
        return new AggregationNode(
            aggNode.getChild(), dedupKeys, List.of("COUNT(*)"), AggregationNode.Step.PARTIAL);
      }
      // Mixed aggregate dedup: queries with both COUNT(DISTINCT) and decomposable aggregates.
      // Instead of sending all raw rows, shards GROUP BY (original_keys + distinct_columns)
      // with partial aggregates for decomposable functions. This reduces data volume by the
      // dedup factor (e.g., 1M rows → ~80K deduped rows for Q10).
      DqePlanNode mixedDedupPlan = buildMixedDedupShardPlan(aggNode);
      if (mixedDedupPlan != null) {
        return mixedDedupPlan;
      }
    }
    // SINGLE: strip aggregation and above, shards only scan+filter
    return aggNode.getChild();
  }

  /**
   * Build a shard plan for mixed-aggregate queries containing both COUNT(DISTINCT) and decomposable
   * aggregates (SUM, COUNT(*), AVG, MIN, MAX). The shard plan groups by (original_keys +
   * distinct_columns) with partial aggregates for the decomposable functions. This deduplicates the
   * distinct column values at the shard level while preserving enough information for the
   * coordinator to compute all aggregate results.
   *
   * <p>For Q10: GROUP BY RegionID with SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth),
   * COUNT(DISTINCT UserID) becomes shard plan: GROUP BY (RegionID, UserID) with SUM(AdvEngineID),
   * COUNT(*), SUM(ResolutionWidth), COUNT(ResolutionWidth)
   *
   * @return the shard plan node, or null if the query is not eligible
   */
  private static DqePlanNode buildMixedDedupShardPlan(AggregationNode aggNode) {
    List<String> aggFunctions = aggNode.getAggregateFunctions();
    List<String> distinctCols = new ArrayList<>();
    boolean hasCountDistinct = false;
    boolean hasDecomposable = false;

    for (String func : aggFunctions) {
      Matcher m = AGG_PATTERN.matcher(func);
      if (!m.matches()) {
        return null; // Unsupported aggregate
      }
      String funcName = m.group(1).toUpperCase(Locale.ROOT);
      boolean isDistinct = m.group(2) != null;
      String arg = m.group(3).trim();

      if (isDistinct) {
        if (!"COUNT".equals(funcName)) {
          return null; // Only COUNT(DISTINCT) supported
        }
        hasCountDistinct = true;
        if (!distinctCols.contains(arg)) {
          distinctCols.add(arg);
        }
      } else {
        hasDecomposable = true;
      }
    }

    // Only apply when both COUNT(DISTINCT) and decomposable aggregates are present
    if (!hasCountDistinct || !hasDecomposable) {
      return null;
    }

    // Build dedup GROUP BY keys: original keys + distinct columns
    List<String> dedupKeys = new ArrayList<>(aggNode.getGroupByKeys());
    dedupKeys.addAll(distinctCols);

    // Build partial aggregates for decomposable functions.
    // AVG(col) is decomposed into SUM(col) + COUNT(col) for correct weighted merge.
    // COUNT(DISTINCT col) is omitted — it becomes COUNT(*) at the coordinator level.
    List<String> shardAggs = new ArrayList<>();
    for (String func : aggFunctions) {
      Matcher m = AGG_PATTERN.matcher(func);
      if (!m.matches()) continue;
      String funcName = m.group(1).toUpperCase(Locale.ROOT);
      boolean isDistinct = m.group(2) != null;
      String arg = m.group(3).trim();

      if (isDistinct) {
        // COUNT(DISTINCT) handled by dedup GROUP BY — no shard aggregate needed
        continue;
      }
      if ("AVG".equals(funcName)) {
        // Decompose AVG into SUM + COUNT for weighted merge at coordinator
        shardAggs.add("SUM(" + arg + ")");
        shardAggs.add("COUNT(" + arg + ")");
      } else {
        // COUNT(*), SUM, MIN, MAX — pass through as-is
        shardAggs.add(func);
      }
    }

    return new AggregationNode(
        aggNode.getChild(), dedupKeys, shardAggs, AggregationNode.Step.PARTIAL);
  }

  /**
   * Check if all aggregate functions are COUNT(DISTINCT col) and extract the distinct column names.
   * Returns the list of distinct column names if eligible, or null if any aggregate is not
   * COUNT(DISTINCT).
   */
  private static List<String> extractCountDistinctColumns(List<String> aggregateFunctions) {
    if (aggregateFunctions.isEmpty()) {
      return null;
    }
    List<String> distinctCols = new ArrayList<>();
    for (String func : aggregateFunctions) {
      Matcher m = AGG_PATTERN.matcher(func);
      if (!m.matches()) {
        return null;
      }
      String funcName = m.group(1).toUpperCase(Locale.ROOT);
      boolean isDistinct = m.group(2) != null;
      if (!"COUNT".equals(funcName) || !isDistinct) {
        return null;
      }
      distinctCols.add(m.group(3).trim());
    }
    return distinctCols;
  }

  /**
   * For PARTIAL aggregation with Limit -> [Project ->] Sort -> Agg pattern, build a shard plan that
   * keeps Sort/Limit but inflates the limit to capture enough groups for correct coordinator merge.
   * Only applies when sorting by a single aggregate column (e.g., ORDER BY COUNT(*) DESC) and no
   * HAVING clause is present.
   *
   * <p>The inflated limit uses max(1000, originalLimit * numShards * 2) to ensure that globally
   * top-N groups are captured even when counts are distributed across shards. This is a heuristic
   * that works well in practice for decomposable aggregates (COUNT, SUM).
   *
   * @return shard plan with inflated Limit -> Sort -> Agg, or null if pattern doesn't match
   */
  private static DqePlanNode buildShardPlanWithInflatedLimit(
      DqePlanNode plan, AggregationNode aggNode, int numShards) {
    // Extract Limit -> [Project ->] Sort -> Agg pattern
    if (!(plan instanceof LimitNode limitNode)) {
      return null;
    }
    DqePlanNode belowLimit = limitNode.getChild();
    // Skip optional ProjectNode
    if (belowLimit instanceof ProjectNode projectNode) {
      belowLimit = projectNode.getChild();
    }
    if (!(belowLimit instanceof SortNode sortNode)) {
      return null;
    }
    // Primary sort key must be an aggregate column (not a group-by key) for pre-filter safety.
    // Tiebreaker sort keys (added by LogicalPlanner) can be group-by or aggregate columns.
    List<String> sortKeys = sortNode.getSortKeys();
    List<String> groupByKeys = aggNode.getGroupByKeys();
    List<String> aggFunctions = aggNode.getAggregateFunctions();
    if (sortKeys.isEmpty()) {
      return null;
    }
    // Check that the primary sort key is an aggregate output column
    List<String> aggOutput = new ArrayList<>(groupByKeys);
    aggOutput.addAll(aggFunctions);
    String primarySortKey = sortKeys.get(0);
    int primaryIdx = aggOutput.indexOf(primarySortKey);
    if (primaryIdx < 0 || primaryIdx < groupByKeys.size()) {
      return null; // Primary sort is by group-by key or unknown column — not safe for pre-filter
    }
    // No HAVING clause (FilterNode above aggregation)
    if (hasFilterAboveAggregation(plan)) {
      return null;
    }

    // Inflate limit: use max(1000, limit * numShards * 2) to ensure coverage
    long originalLimit = limitNode.getCount() + limitNode.getOffset();
    long inflatedLimit = Math.max(1000, originalLimit * numShards * 2);

    // Build: LimitNode(inflated) -> SortNode -> AggregationNode(PARTIAL) -> child
    return new LimitNode(
        new SortNode(
            aggNode, sortNode.getSortKeys(), sortNode.getAscending(), sortNode.getNullsFirst()),
        inflatedLimit);
  }

  /** Check if there's a FilterNode (HAVING) between the root and the AggregationNode. */
  private static boolean hasFilterAboveAggregation(DqePlanNode node) {
    if (node instanceof AggregationNode) {
      return false;
    }
    if (node instanceof FilterNode) {
      return true;
    }
    for (DqePlanNode child : node.getChildren()) {
      if (hasFilterAboveAggregation(child)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Return the subtree rooted at the AggregationNode, stripping Sort/Limit/Project nodes above it.
   * If the plan has structure like Limit(Sort(Project(Agg(...)))) or Limit(Sort(Agg(...))), returns
   * the Agg subtree. Also handles EvalNode above aggregation.
   */
  private DqePlanNode stripAboveAggregation(DqePlanNode node) {
    if (node instanceof AggregationNode) {
      return node;
    }
    if (node instanceof LimitNode) {
      return stripAboveAggregation(((LimitNode) node).getChild());
    }
    if (node instanceof SortNode) {
      return stripAboveAggregation(((SortNode) node).getChild());
    }
    if (node instanceof ProjectNode) {
      return stripAboveAggregation(((ProjectNode) node).getChild());
    }
    // FilterNode above aggregation = HAVING clause. Strip it — it must be
    // applied at the coordinator after merging partial results from all shards.
    if (node instanceof FilterNode) {
      return stripAboveAggregation(((FilterNode) node).getChild());
    }
    if (node instanceof EvalNode) {
      // EvalNode above aggregation may compute expressions on agg output.
      // Keep it — it can run on the shard with partial results.
      DqePlanNode child = stripAboveAggregation(((EvalNode) node).getChild());
      if (child instanceof AggregationNode) {
        return new EvalNode(
            child, ((EvalNode) node).getExpressions(), ((EvalNode) node).getOutputColumnNames());
      }
      return child;
    }
    // For any other node type, return as-is
    return node;
  }

  /**
   * Walk the plan tree to find the TableScanNode and extract the index name.
   *
   * @param plan the root plan node
   * @return the index name from the TableScanNode
   * @throws IllegalArgumentException if no TableScanNode is found
   */
  private String findIndexName(DqePlanNode plan) {
    String indexName =
        plan.accept(
            new DqePlanVisitor<String, Void>() {
              @Override
              public String visitTableScan(TableScanNode node, Void context) {
                return node.getIndexName();
              }

              @Override
              public String visitPlan(DqePlanNode node, Void context) {
                for (DqePlanNode child : node.getChildren()) {
                  String result = child.accept(this, context);
                  if (result != null) {
                    return result;
                  }
                }
                return null;
              }
            },
            null);

    if (indexName == null) {
      throw new IllegalArgumentException("Plan does not contain a TableScanNode");
    }
    return indexName;
  }

  /**
   * Build the coordinator plan for aggregation queries. For PARTIAL aggregation, creates a FINAL
   * merge node. For non-PARTIAL (SINGLE) aggregation, creates a SINGLE node so the coordinator runs
   * the full aggregation over concatenated shard scan results.
   */
  private DqePlanNode buildCoordinatorPlan(DqePlanNode plan) {
    AggregationNode aggNode = findAggregationNode(plan);

    if (aggNode != null && aggNode.getStep() == AggregationNode.Step.PARTIAL) {
      return new AggregationNode(
          null,
          aggNode.getGroupByKeys(),
          aggNode.getAggregateFunctions(),
          AggregationNode.Step.FINAL);
    }
    if (aggNode != null) {
      // Non-PARTIAL (SINGLE): coordinator runs full aggregation over raw shard data
      return new AggregationNode(
          null,
          aggNode.getGroupByKeys(),
          aggNode.getAggregateFunctions(),
          AggregationNode.Step.SINGLE);
    }
    return null;
  }

  /** Find the AggregationNode in the plan tree. */
  private AggregationNode findAggregationNode(DqePlanNode plan) {
    return plan.accept(
        new DqePlanVisitor<AggregationNode, Void>() {
          @Override
          public AggregationNode visitAggregation(AggregationNode node, Void context) {
            return node;
          }

          @Override
          public AggregationNode visitPlan(DqePlanNode node, Void context) {
            for (DqePlanNode child : node.getChildren()) {
              AggregationNode result = child.accept(this, context);
              if (result != null) {
                return result;
              }
            }
            return null;
          }
        },
        null);
  }
}
