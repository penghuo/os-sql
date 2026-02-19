/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.opensearch.executor.ShardRoutingResolver;
import org.opensearch.sql.opensearch.planner.merge.Exchange;
import org.opensearch.sql.opensearch.storage.serde.RelNodeSerializer;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Orchestrates distributed query execution by dispatching shard-level plan fragments to data nodes
 * and collecting results through Exchange operators.
 *
 * <p>The executor walks the distributed plan tree (produced by {@link
 * org.opensearch.sql.opensearch.planner.PlanSplitter}) bottom-up, finding Exchange nodes that mark
 * the boundary between shard-level and coordinator-level execution. For each Exchange:
 *
 * <ol>
 *   <li>The child subtree (shard fragment) is serialized via {@link RelNodeSerializer}
 *   <li>Active primary shards are resolved via {@link ShardRoutingResolver}
 *   <li>The fragment is dispatched to each shard via {@link CalciteShardAction}
 *   <li>Shard responses are collected and injected into the Exchange via {@code setShardResults()}
 * </ol>
 *
 * <p>Finally, the top-level Exchange's {@code scan()} method is called to merge all shard results
 * according to the Exchange strategy (concat, merge-sort, merge-aggregate, etc.).
 */
public class DistributedExecutor {
    private static final Logger LOG = LogManager.getLogger(DistributedExecutor.class);

    /** Default timeout for shard dispatch operations. */
    private static final long SHARD_TIMEOUT_SECONDS = 60;

    private final NodeClient client;
    private final ClusterService clusterService;
    private final RelNodeSerializer serializer;

    public DistributedExecutor(NodeClient client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
        this.serializer = new RelNodeSerializer();
    }

    /**
     * Executes a distributed plan by dispatching shard fragments and merging results.
     *
     * <p>Walks the plan tree bottom-up to find all Exchange nodes. For each Exchange, the child
     * subtree is serialized and dispatched to all active shards of the target index. Results are
     * collected asynchronously and fed into the Exchange for coordinator-side merge.
     *
     * @param distributedPlan the plan with Exchange nodes from PlanSplitter
     * @param indexName the target index name
     * @return list of result rows as Object arrays
     * @throws RuntimeException if any shard execution fails or times out
     */
    public List<Object[]> execute(RelNode distributedPlan, String indexName) {
        // Process all Exchange nodes bottom-up
        processExchanges(distributedPlan, indexName);

        // The top-level node should be an Exchange (or contain one)
        Exchange topExchange = findTopExchange(distributedPlan);
        if (topExchange == null) {
            throw new IllegalStateException("No Exchange node found in distributed plan");
        }

        // Execute the top Exchange's scan to get merged results
        return collectResults(topExchange);
    }

    /**
     * Recursively processes Exchange nodes bottom-up. Leaf Exchanges (those whose children have no
     * further Exchanges) are processed first so that nested Exchange hierarchies are handled
     * correctly.
     */
    private void processExchanges(RelNode node, String indexName) {
        // Process children first (bottom-up)
        for (RelNode child : node.getInputs()) {
            processExchanges(child, indexName);
        }

        // If this node is an Exchange, dispatch its child fragment to shards
        if (node instanceof Exchange exchange) {
            dispatchToShards(exchange, indexName);
        }
    }

    /**
     * Dispatches the shard fragment (child of the Exchange) to all active primary shards and
     * collects the results.
     */
    private void dispatchToShards(Exchange exchange, String indexName) {
        // The child of the Exchange is the shard fragment
        RelNode shardFragment = exchange.getInput();

        // Try to extract index name from the shard fragment if not provided
        String resolvedIndex = indexName;
        if (resolvedIndex == null) {
            resolvedIndex = ShardRoutingResolver.extractIndexName(shardFragment);
        }
        if (resolvedIndex == null) {
            throw new IllegalStateException("Cannot determine index name for shard dispatch");
        }

        // Serialize the shard fragment
        String serializedPlan = serializer.serialize(shardFragment);
        LOG.debug(
                "Serialized shard fragment for {}: {} bytes",
                exchange.getExchangeType(),
                serializedPlan.length());

        // Resolve active primary shards
        List<ShardRouting> shards =
                ShardRoutingResolver.resolveActiveShards(clusterService, resolvedIndex);
        LOG.debug(
                "Dispatching {} fragment to {} shards for index [{}]",
                exchange.getExchangeType(),
                shards.size(),
                resolvedIndex);

        if (shards.isEmpty()) {
            throw new IllegalStateException("No active shards found for index: " + resolvedIndex);
        }

        // Dispatch to all shards and collect responses
        List<CalciteShardResponse> responses =
                dispatchAndCollect(serializedPlan, resolvedIndex, shards);

        // Check for shard errors
        for (CalciteShardResponse response : responses) {
            if (response.isError()) {
                throw new RuntimeException(
                        "Shard "
                                + response.getShardId()
                                + " execution failed: "
                                + response.getErrorMessage());
            }
        }

        // Convert responses to ShardResult and inject into the Exchange
        List<Exchange.ShardResult> shardResults = new ArrayList<>(responses.size());
        for (CalciteShardResponse response : responses) {
            Exchange.ShardResult result =
                    new Exchange.ShardResult(
                            response.getRows(),
                            response.getColumnNames(),
                            response.getShardId(),
                            response.getBinaryFields());
            shardResults.add(result);
        }
        exchange.setShardResults(shardResults);
        LOG.debug(
                "{} exchange received results from {} shards",
                exchange.getExchangeType(),
                shardResults.size());
    }

    /**
     * Dispatches the serialized plan fragment to all target shards concurrently and waits for all
     * responses.
     */
    private List<CalciteShardResponse> dispatchAndCollect(
            String serializedPlan, String indexName, List<ShardRouting> shards) {
        int shardCount = shards.size();
        CalciteShardResponse[] responses = new CalciteShardResponse[shardCount];
        CountDownLatch latch = new CountDownLatch(shardCount);
        AtomicReference<Exception> firstError = new AtomicReference<>();

        for (int i = 0; i < shardCount; i++) {
            ShardRouting shard = shards.get(i);
            CalciteShardRequest request =
                    new CalciteShardRequest(serializedPlan, indexName, shard.getId());

            final int idx = i;
            client.execute(
                    CalciteShardAction.INSTANCE,
                    request,
                    new ActionListener<CalciteShardResponse>() {
                        @Override
                        public void onResponse(CalciteShardResponse response) {
                            responses[idx] = response;
                            latch.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            LOG.error("Shard {} dispatch failed", shard.getId(), e);
                            firstError.compareAndSet(null, e);
                            // Create error response so we don't block forever
                            responses[idx] =
                                    new CalciteShardResponse(shard.getId(), e.getMessage());
                            latch.countDown();
                        }
                    });
        }

        // Wait for all shards to respond
        try {
            if (!latch.await(SHARD_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new RuntimeException(
                        "Timed out waiting for shard responses after "
                                + SHARD_TIMEOUT_SECONDS
                                + " seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for shard responses", e);
        }

        // Check for transport-level errors
        Exception error = firstError.get();
        if (error != null) {
            throw new RuntimeException("Shard dispatch failed", error);
        }

        List<CalciteShardResponse> result = new ArrayList<>(shardCount);
        for (CalciteShardResponse response : responses) {
            result.add(response);
        }
        return result;
    }

    /**
     * Finds the top-level Exchange node in the plan tree. The top Exchange is typically the root
     * node itself (since PlanSplitter places Exchanges at the top).
     */
    private Exchange findTopExchange(RelNode node) {
        if (node instanceof Exchange exchange) {
            return exchange;
        }
        // Check children (shouldn't normally reach here for well-formed distributed plans)
        for (RelNode child : node.getInputs()) {
            Exchange found = findTopExchange(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /** Collects results from the Exchange's scan enumerable into a list of Object arrays. */
    private List<Object[]> collectResults(Exchange exchange) {
        List<Object[]> results = new ArrayList<>();
        Enumerable<?> enumerable = exchange.scan();
        try (Enumerator<?> enumerator = enumerable.enumerator()) {
            while (enumerator.moveNext()) {
                Object current = enumerator.current();
                if (current instanceof Object[] row) {
                    results.add(row);
                } else {
                    // Single-column rows are returned as scalar values
                    results.add(new Object[] {current});
                }
            }
        }
        return results;
    }
}
