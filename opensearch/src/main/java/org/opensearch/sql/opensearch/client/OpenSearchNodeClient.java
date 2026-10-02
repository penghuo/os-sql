/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.search.*;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexSettings;
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.executor.progress.SourceShardKey;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.sql.opensearch.executor.progress.ObservedSearchRequest;
import org.opensearch.sql.opensearch.executor.progress.ProgressiveQueryContext;
import org.opensearch.sql.opensearch.executor.progress.SearchProgressReport;
import org.opensearch.sql.opensearch.executor.progress.SourceChannel;
import org.opensearch.sql.opensearch.executor.progress.SourceEstimate;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchScrollRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.transport.client.node.NodeClient;

/** OpenSearch connection by node client. */
public class OpenSearchNodeClient implements OpenSearchClient {

  private static final Logger LOG = LogManager.getLogger(OpenSearchNodeClient.class);

  public static final Function<String, Predicate<String>> ALL_FIELDS =
      (anyIndex -> (anyField -> true));

  /** Node client provided by OpenSearch container. */
  private final NodeClient client;

  /** Constructor of OpenSearchNodeClient. */
  public OpenSearchNodeClient(NodeClient client) {
    this.client = client;
  }

  @Override
  public boolean exists(String indexName) {
    try {
      IndicesExistsResponse checkExistResponse =
          client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet();
      return checkExistResponse.isExists();
    } catch (OpenSearchSecurityException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to check if index [" + indexName + "] exists", e);
    }
  }

  @Override
  public void createIndex(String indexName, Map<String, Object> mappings) {
    try {
      // TODO: 1.pass index settings (the number of primary shards, etc); 2.check response?
      CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).mapping(mappings);
      client.admin().indices().create(createIndexRequest).actionGet();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create index [" + indexName + "]", e);
    }
  }

  /**
   * Get field mappings of index by an index expression. Majority is copied from legacy
   * LocalClusterState.
   *
   * <p>For simplicity, removed type (deprecated) and field filter in argument list. Also removed
   * mapping cache, cluster state listener (mainly for performance and debugging).
   *
   * @param indexExpression index name expression
   * @return index mapping(s) in our class to isolate OpenSearch API. IndexNotFoundException is
   *     thrown if no index matched.
   */
  @Override
  public Map<String, IndexMapping> getIndexMappings(String... indexExpression) {
    try {
      GetMappingsResponse mappingsResponse =
          client.admin().indices().prepareGetMappings(indexExpression).setLocal(true).get();
      if (mappingsResponse.mappings().isEmpty()) {
        throw new IndexNotFoundException(indexExpression[0]);
      }
      return mappingsResponse.mappings().entrySet().stream()
          .collect(
              Collectors.toUnmodifiableMap(
                  Map.Entry::getKey, cursor -> new IndexMapping(cursor.getValue())));
    } catch (IndexNotFoundException e) {
      // Re-throw directly to be treated as client error finally
      throw ErrorReport.wrap(e)
          .code(ErrorCode.INDEX_NOT_FOUND)
          .location("while fetching index mappings")
          .context("index_name", indexExpression[0])
          .build();
    } catch (OpenSearchSecurityException e) {
      // Re-throw with permission denied code
      throw ErrorReport.wrap(e)
          .code(ErrorCode.PERMISSION_DENIED)
          .location("while fetching index mappings")
          .context("index_name", indexExpression[0])
          .build();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to read mapping for index pattern ["
              + String.join(",", indexExpression)
              + "]: "
              + e.getMessage(),
          e);
    }
  }

  /**
   * Fetch index.max_result_window settings according to index expression given.
   *
   * @param indexExpression index expression
   * @return map from index name to its max result window
   */
  @Override
  public Map<String, Integer> getIndexMaxResultWindows(String... indexExpression) {
    try {
      GetSettingsResponse settingsResponse =
          client.admin().indices().prepareGetSettings(indexExpression).setLocal(true).get();
      ImmutableMap.Builder<String, Integer> result = ImmutableMap.builder();
      for (Map.Entry<String, Settings> indexToSetting :
          settingsResponse.getIndexToSettings().entrySet()) {
        Settings settings = indexToSetting.getValue();
        result.put(
            indexToSetting.getKey(),
            settings.getAsInt(
                IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(),
                IndexSettings.MAX_RESULT_WINDOW_SETTING.getDefault(settings)));
      }
      return result.build();
    } catch (OpenSearchSecurityException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to read setting for index pattern ["
              + String.join(",", indexExpression)
              + "]: "
              + e.getMessage(),
          e);
    }
  }

  /** TODO: Scroll doesn't work for aggregation. Support aggregation later. */
  @Override
  public OpenSearchResponse search(OpenSearchRequest request) {
    // One channel per physical search: shard bookkeeping is per-search, so a paged scan must not
    // have
    // page N's shard list confused with page N-1's.
    SourceChannel channel = ProgressiveQueryContext.openChannel();
    return request.search(
        req ->
            executeObservedSearch(
                req, channel, searchRequest -> client.search(searchRequest).actionGet()),
        // Scroll continuations carry only a scroll id, so there is nothing to observe on them; the
        // scroll's
        // opening search went through the branch above.
        req -> client.searchScroll(req).actionGet());
  }

  /**
   * Runs one physical search and reports what it contributed.
   *
   * <p>Package-private, and parameterised on the transport call, so tests can exercise this exact
   * sequence — parent-task application, wrapper construction, response reporting — without a live
   * cluster. The wiring between those steps is where progress reporting can silently break existing
   * behaviour, so it is worth testing as a unit rather than only through its parts.
   */
  SearchResponse executeObservedSearch(
      SearchRequest req,
      SourceChannel channel,
      Function<SearchRequest, SearchResponse> searchAction) {
    applyParentTask(req);
    SearchResponse response = searchAction.apply(ObservedSearchRequest.wrap(req, channel));
    reportSearchProgress(channel, req, response);
    return response;
  }

  /**
   * Reports what one search response contributed.
   *
   * <p>The scan already declared its shape, so this only supplies the numbers — it never re-derives
   * the shape, because a response cannot tell a paged page from a single request's only answer.
   * Source completion is likewise not decided here: only the scan knows whether another page will
   * be requested.
   */
  private static void reportSearchProgress(
      SourceChannel channel, SearchRequest request, SearchResponse response) {
    if (channel.isNoop() || response == null) {
      return;
    }
    try {
      SearchProgressReport report =
          SearchProgressReport.of(request, response, channel.declaredUnit());
      channel.rowsObserved(report.completedUnits(), report.pageSize(), report.observedTotal());
    } catch (RuntimeException e) {
      // Progress reporting is strictly advisory. A malformed aggregation tree must not turn a
      // successful
      // search into a failed query.
      LOG.debug("Failed to report search progress", e);
    }
  }

  private void applyParentTask(SearchRequest req) {
    CancellableTask task = OpenSearchQueryManager.getCancellableTask();
    if (task != null) {
      req.setParentTask(new TaskId(client.getLocalNodeId(), task.getId()));
    }
  }

  @Override
  public Optional<SourceEstimate> documentCountEstimate(String[] indices, long timeoutMillis) {
    if (indices == null || indices.length == 0) {
      return Optional.empty();
    }
    try {
      IndicesStatsRequest statsRequest = new IndicesStatsRequest().clear().docs(true);
      statsRequest.indices(indices);
      // Bounded wait: §8.2 requires that an unavailable estimate select the documented fallback
      // immediately rather than delaying execution.
      IndicesStatsResponse stats =
          client
              .admin()
              .indices()
              .stats(statsRequest)
              .actionGet(timeoutMillis, TimeUnit.MILLISECONDS);
      if (stats.getFailedShards() > 0) {
        // Partial stats are discarded: a denominator that omits a failed shard's documents would
        // make
        // progress overshoot and then stall at the clamp.
        return Optional.empty();
      }
      Map<SourceShardKey, Long> shardDocs = new HashMap<>();
      long total = 0L;
      for (ShardStats shard : stats.getShards()) {
        if (!shard.getShardRouting().primary()) {
          continue;
        }
        if (shard.getStats() == null || shard.getStats().getDocs() == null) {
          return Optional.empty();
        }
        long count = shard.getStats().getDocs().getCount();
        if (count < 0) {
          return Optional.empty();
        }
        ShardId shardId = shard.getShardRouting().shardId();
        shardDocs.put(new SourceShardKey(shardId.getIndex().getUUID(), shardId.id()), count);
        total += count;
      }
      if (shardDocs.isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(new SourceEstimate(total, shardDocs));
    } catch (Exception e) {
      // Includes OpenSearchSecurityException (caller cannot read _stats) and timeouts. Progress is
      // advisory, so every failure mode degrades to equal-weight accounting.
      LOG.debug("Document count estimate unavailable for {}", Arrays.toString(indices), e);
      return Optional.empty();
    }
  }

  /**
   * Get the combination of the indices and the alias.
   *
   * @return the combination of the indices and the alias
   */
  @Override
  public List<String> indices() {
    final GetIndexResponse indexResponse =
        client.admin().indices().prepareGetIndex().setLocal(true).get();
    final Stream<String> aliasStream =
        ImmutableList.copyOf(indexResponse.aliases().values()).stream()
            .flatMap(Collection::stream)
            .map(AliasMetadata::alias);

    return Stream.concat(Arrays.stream(indexResponse.getIndices()), aliasStream)
        .collect(Collectors.toList());
  }

  /**
   * Get meta info of the cluster.
   *
   * @return meta info of the cluster.
   */
  @Override
  public Map<String, String> meta() {
    return ImmutableMap.of(
        META_CLUSTER_NAME,
        client.settings().get("cluster.name", "opensearch"),
        "plugins.sql.pagination.api",
        client.settings().get("plugins.sql.pagination.api", "true"));
  }

  @Override
  public void forceCleanup(OpenSearchRequest request) {
    if (request instanceof OpenSearchScrollRequest) {
      request.forceClean(
          scrollId -> {
            try {
              client.prepareClearScroll().addScrollId(scrollId).get();
            } catch (Exception e) {
              throw new IllegalStateException(
                  "Failed to clean up resources for search request " + request, e);
            }
          });
    } else {
      request.forceClean(
          pitId -> {
            DeletePitRequest deletePitRequest = new DeletePitRequest(pitId);
            deletePit(deletePitRequest);
          });
    }
  }

  @Override
  public void cleanup(OpenSearchRequest request) {
    if (request instanceof OpenSearchScrollRequest) {
      request.clean(
          scrollId -> {
            try {
              client.prepareClearScroll().addScrollId(scrollId).get();
            } catch (Exception e) {
              throw new IllegalStateException(
                  "Failed to clean up resources for search request " + request, e);
            }
          });
    } else {
      request.clean(
          pitId -> {
            DeletePitRequest deletePitRequest = new DeletePitRequest(pitId);
            deletePit(deletePitRequest);
          });
    }
  }

  @Override
  public void schedule(Runnable task) {
    // at that time, task already running the sql-worker ThreadPool.
    task.run();
  }

  @Override
  public Optional<NodeClient> getNodeClient() {
    return Optional.of(client);
  }

  @Override
  public String createPit(CreatePitRequest createPitRequest) {
    ActionFuture<CreatePitResponse> execute =
        this.client.execute(CreatePitAction.INSTANCE, createPitRequest);
    try {
      CreatePitResponse pitResponse = execute.get();
      return pitResponse.getId();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof OpenSearchSecurityException) {
        throw (OpenSearchSecurityException) e.getCause();
      }
      throw new RuntimeException(
          "Error occurred while creating PIT for internal plugin operation", e);
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Error occurred while creating PIT for internal plugin operation", e);
    }
  }

  @Override
  public void deletePit(DeletePitRequest deletePitRequest) {
    ActionFuture<DeletePitResponse> execute =
        this.client.execute(DeletePitAction.INSTANCE, deletePitRequest);
    try {
      execute.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof OpenSearchSecurityException) {
        throw (OpenSearchSecurityException) e.getCause();
      }
      throw new RuntimeException(
          "Error occurred while deleting PIT for internal plugin operation", e);
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Error occurred while deleting PIT for internal plugin operation", e);
    }
  }
}
