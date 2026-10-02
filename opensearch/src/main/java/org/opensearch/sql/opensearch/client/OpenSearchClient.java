/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.client;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.transport.client.node.NodeClient;

/**
 * OpenSearch client abstraction to wrap different OpenSearch client implementation. For example,
 * implementation by node client for OpenSearch plugin or by REST client for standalone mode.
 */
public interface OpenSearchClient {

  String META_CLUSTER_NAME = "CLUSTER_NAME";

  /**
   * Check if the given index exists.
   *
   * @param indexName index name
   * @return true if exists, otherwise false
   */
  boolean exists(String indexName);

  /**
   * Create OpenSearch index based on the given mappings.
   *
   * @param indexName index name
   * @param mappings index mappings
   */
  void createIndex(String indexName, Map<String, Object> mappings);

  /**
   * Fetch index mapping(s) according to index expression given.
   *
   * @param indexExpression index expression
   * @return index mapping(s) from index name to its mapping
   */
  Map<String, IndexMapping> getIndexMappings(String... indexExpression);

  /**
   * Fetch index.max_result_window settings according to index expression given.
   *
   * @param indexExpression index expression
   * @return map from index name to its max result window
   */
  Map<String, Integer> getIndexMaxResultWindows(String... indexExpression);

  /**
   * Perform search query in the search request.
   *
   * @param request search request
   * @return search response
   */
  OpenSearchResponse search(OpenSearchRequest request);

  /**
   * Best-effort source size estimate for progress reporting.
   *
   * <p>Returns the sum of live primary-shard document counts, plus the per-shard counts used to
   * weight shard-level progress within one search. This is an index-size estimate only — it says
   * nothing about filter selectivity and does not enable exact total-hit tracking.
   *
   * <p>Returns empty whenever the answer would be incomplete or slow: the stats call is
   * unauthorized, times out, or fails; any participating primary shard failed; or a primary shard
   * reported no usable document count. Partial stats are discarded rather than blended, because a
   * denominator missing a shard's documents would make progress overshoot. Implementations must not
   * throw — failing to estimate must never fail or delay the query.
   *
   * @param indices concrete index names or patterns the source reads
   * @param timeoutMillis bound on how long the lookup may take
   * @return the estimate, or empty when unavailable
   */
  default Optional<org.opensearch.sql.opensearch.executor.progress.SourceEstimate>
      documentCountEstimate(String[] indices, long timeoutMillis) {
    return Optional.empty();
  }

  /**
   * Get the combination of the indices and the alias.
   *
   * @return the combination of the indices and the alias
   */
  List<String> indices();

  /**
   * Get meta info of the cluster.
   *
   * @return meta info of the cluster.
   */
  Map<String, String> meta();

  /**
   * Force to clean up resources related to the search request.
   *
   * @param request search request
   */
  void forceCleanup(OpenSearchRequest request);

  /**
   * Clean up resources related to the search request, for example scroll context.
   *
   * @param request search request
   */
  void cleanup(OpenSearchRequest request);

  /**
   * Schedule a task to run.
   *
   * @param task task
   */
  void schedule(Runnable task);

  Optional<NodeClient> getNodeClient();

  /**
   * Create PIT for given indices
   *
   * @param createPitRequest Create Point In Time request
   * @return PitId
   */
  String createPit(CreatePitRequest createPitRequest);

  /**
   * Delete PIT
   *
   * @param deletePitRequest Delete Point In Time request
   */
  void deletePit(DeletePitRequest deletePitRequest);
}
