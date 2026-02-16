/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.planner.distributed.DistributedPlanConverter;
import org.opensearch.sql.planner.distributed.Fragment;
import org.opensearch.sql.planner.distributed.FragmentPlanner;
import org.opensearch.sql.planner.distributed.ShardSplit;
import org.opensearch.sql.planner.distributed.ShardSplitManager;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.distributed.DistributedExchangeOperator;
import org.opensearch.sql.planner.physical.distributed.ExchangeBuffer;

/**
 * Distributed execution engine that breaks a Calcite RelNode plan into fragments and executes them
 * across multiple data nodes for parallel query processing.
 *
 * <p>When true multi-node distributed execution is not yet available (e.g., single-node cluster or
 * transport layer not fully wired), the engine falls back to the provided single-node {@link
 * ExecutionEngine} to ensure correct results while still exercising the distributed routing path.
 */
@Log4j2
public class DistributedExecutionEngine implements ExecutionEngine {

    private final FragmentPlanner fragmentPlanner;
    private final ShardSplitManager shardSplitManager;
    private final DistributedPlanConverter planConverter;
    private final ExchangeService exchangeService;
    private final ExecutionEngine fallbackEngine;
    private final String localNodeId;

    /**
     * Creates a new DistributedExecutionEngine.
     *
     * @param fragmentPlanner the planner that fragments RelNode trees
     * @param shardSplitManager the manager for shard split information
     * @param exchangeService the service managing exchange buffers
     * @param fallbackEngine the single-node execution engine used as fallback
     * @param localNodeId the local node's identifier
     */
    public DistributedExecutionEngine(
            FragmentPlanner fragmentPlanner,
            ShardSplitManager shardSplitManager,
            ExchangeService exchangeService,
            ExecutionEngine fallbackEngine,
            String localNodeId) {
        this.fragmentPlanner = fragmentPlanner;
        this.shardSplitManager = shardSplitManager;
        this.planConverter = new DistributedPlanConverter();
        this.exchangeService = exchangeService;
        this.fallbackEngine = fallbackEngine;
        this.localNodeId = localNodeId;
    }

    @Override
    public void execute(RelNode relNode, CalcitePlanContext context,
            ResponseListener<QueryResponse> listener) {
        String queryId = UUID.randomUUID().toString();
        try {
            // 1. Get shard splits for the index
            String indexName = extractIndexName(relNode);
            List<ShardSplit> splits = shardSplitManager.getSplits(indexName, localNodeId);
            int sourceCount = splits.isEmpty() ? 1 : splits.size();

            // 2. Fragment the plan
            Optional<Fragment> fragmentOpt = fragmentPlanner.plan(relNode, splits);
            if (fragmentOpt.isEmpty()) {
                // Plan doesn't benefit from distribution - use fallback
                log.info("Distributed engine: plan for [{}] does not require distribution, "
                        + "delegating to single-node engine", indexName);
                fallbackEngine.execute(relNode, context, listener);
                return;
            }
            Fragment rootFragment = fragmentOpt.get();

            // 3. Check if all shards are local (single-node cluster)
            boolean allLocal = splits.isEmpty() || splits.stream().allMatch(ShardSplit::isLocal);

            if (allLocal) {
                // Single-node cluster: delegate to fallback engine for correct execution.
                // The distributed path (coordinator plan + exchange buffers) requires
                // multi-node transport to feed data. On single-node, the fallback engine
                // produces identical results more efficiently.
                log.info("Distributed engine: single-node cluster detected for [{}] "
                        + "({} shards, all local). Delegating to single-node engine with "
                        + "distributed plan: {}", indexName, sourceCount,
                        describeFragmentBrief(rootFragment));
                fallbackEngine.execute(relNode, context, listener);
                return;
            }

            // 4. Multi-node distributed execution path
            log.info("Distributed engine: executing query {} across {} shards for [{}]",
                    queryId, sourceCount, indexName);

            // Build the coordinator-side PhysicalPlan
            PhysicalPlan coordinatorPlan =
                    planConverter.buildCoordinatorPlan(relNode, rootFragment, sourceCount);

            // Find exchange operators in the coordinator plan
            List<DistributedExchangeOperator> exchanges = new ArrayList<>();
            collectExchangeOperators(coordinatorPlan, exchanges);

            // Open the coordinator plan (creates exchange buffers)
            coordinatorPlan.open();

            // TODO: Dispatch source fragments to remote data nodes via transport actions.
            // For now, feed exchange buffers will be done by TransportShardExecutionAction
            // responses arriving via the transport layer.
            for (DistributedExchangeOperator exchange : exchanges) {
                ExchangeBuffer buffer = exchange.getBuffer();
                if (buffer != null) {
                    log.debug("Exchange buffer created for query {} with {} sources",
                            queryId, sourceCount);
                }
            }

            // Consume results from the coordinator plan
            List<ExprValue> results = new ArrayList<>();
            Integer querySizeLimit = context.sysLimit.querySizeLimit();
            while (coordinatorPlan.hasNext()
                    && (querySizeLimit == null || results.size() < querySizeLimit)) {
                results.add(coordinatorPlan.next());
            }

            // Build schema from the RelNode's row type
            Schema schema = buildSchema(relNode);

            // Return response
            QueryResponse response = new QueryResponse(schema, results, Cursor.None);
            listener.onResponse(response);

        } catch (Exception e) {
            log.error("Distributed execution failed for query {}, falling back to single-node",
                    queryId, e);
            // On any distributed execution failure, try the fallback engine
            try {
                fallbackEngine.execute(relNode, context, listener);
            } catch (Exception fallbackError) {
                listener.onFailure(e);
            }
        } finally {
            exchangeService.cleanup(queryId);
        }
    }

    @Override
    public void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener) {
        // Delegate PhysicalPlan execution to the fallback engine
        fallbackEngine.execute(plan, listener);
    }

    @Override
    public void execute(
            PhysicalPlan plan,
            ExecutionContext context,
            ResponseListener<QueryResponse> listener) {
        // Delegate PhysicalPlan execution to the fallback engine
        fallbackEngine.execute(plan, context, listener);
    }

    @Override
    public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
        fallbackEngine.explain(plan, listener);
    }

    @Override
    public void explain(
            RelNode relNode,
            ExplainMode mode,
            CalcitePlanContext context,
            ResponseListener<ExplainResponse> listener) {
        try {
            String indexName = extractIndexName(relNode);
            List<ShardSplit> splits = shardSplitManager.getSplits(indexName, localNodeId);

            Optional<Fragment> fragmentOpt = fragmentPlanner.plan(relNode, splits);
            if (fragmentOpt.isEmpty()) {
                // Delegate to fallback for explain
                fallbackEngine.explain(relNode, mode, context, listener);
                return;
            }

            Fragment rootFragment = fragmentOpt.get();
            StringBuilder sb = new StringBuilder();
            sb.append("Distributed Execution Plan:\n");
            describeFragment(rootFragment, sb, 0);

            // Also include the single-node explain for comparison
            listener.onResponse(new ExplainResponse(
                    new ExplainResponseNodeV2(sb.toString(), null, null)));

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Extracts the primary index name from a RelNode tree by finding the first TableScan node.
     */
    static String extractIndexName(RelNode relNode) {
        if (relNode instanceof TableScan) {
            TableScan scan = (TableScan) relNode;
            List<String> qualifiedName = scan.getTable().getQualifiedName();
            if (!qualifiedName.isEmpty()) {
                return qualifiedName.get(qualifiedName.size() - 1);
            }
        }
        for (RelNode input : relNode.getInputs()) {
            String name = extractIndexName(input);
            if (!"unknown".equals(name)) {
                return name;
            }
        }
        return "unknown";
    }

    private Schema buildSchema(RelNode relNode) {
        List<Schema.Column> columns = new ArrayList<>();
        for (org.apache.calcite.rel.type.RelDataTypeField field
                : relNode.getRowType().getFieldList()) {
            ExprCoreType exprType =
                    DistributedPlanConverter.mapSqlTypeToExprType(field.getType());
            columns.add(new Schema.Column(field.getName(), null, exprType));
        }
        return new Schema(columns);
    }

    private void collectExchangeOperators(
            PhysicalPlan plan, List<DistributedExchangeOperator> exchanges) {
        if (plan instanceof DistributedExchangeOperator) {
            exchanges.add((DistributedExchangeOperator) plan);
        }
        for (PhysicalPlan child : plan.getChild()) {
            collectExchangeOperators(child, exchanges);
        }
    }

    private String describeFragmentBrief(Fragment fragment) {
        StringBuilder sb = new StringBuilder();
        sb.append(fragment.getType());
        if (fragment.getExchangeSpec() != null) {
            sb.append("(").append(fragment.getExchangeSpec().getType()).append(")");
        }
        for (Fragment child : fragment.getChildren()) {
            sb.append(" → ").append(describeFragmentBrief(child));
        }
        return sb.toString();
    }

    private void describeFragment(Fragment fragment, StringBuilder sb, int indent) {
        String prefix = "  ".repeat(indent);
        sb.append(prefix)
                .append("Fragment[id=")
                .append(fragment.getFragmentId())
                .append(", type=")
                .append(fragment.getType())
                .append("]");

        if (fragment.getExchangeSpec() != null) {
            sb.append(" exchange=").append(fragment.getExchangeSpec().getType());
        }
        if (!fragment.getSplits().isEmpty()) {
            sb.append(" splits=").append(fragment.getSplits().size());
        }
        sb.append("\n");

        for (Fragment child : fragment.getChildren()) {
            describeFragment(child, sb, indent + 1);
        }
    }
}
