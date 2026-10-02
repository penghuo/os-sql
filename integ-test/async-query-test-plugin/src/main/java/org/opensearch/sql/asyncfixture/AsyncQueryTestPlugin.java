/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.asyncfixture;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.watcher.ResourceWatcherService;

/**
 * Test-only plugin for the async PPL integration tests. Exposes the node-local {@link
 * QueryObserver} under {@code /_plugins/_async_query_fixture}:
 *
 * <ul>
 *   <li>{@code POST arm?index=<name>[&hold=false]}: observe the next PPL query on the index and, by
 *       default, hold its first search;
 *   <li>{@code POST release[?fail=true]}: let the held search proceed, or fail it;
 *   <li>{@code POST disarm}: release any held search and stop observing;
 *   <li>{@code GET status}: captured-task state, search count, PIT ids, and SQL pool activity.
 * </ul>
 */
public class AsyncQueryTestPlugin extends Plugin implements ActionPlugin {

  private static final String BASE = "/_plugins/_async_query_fixture/";

  private final QueryObserver observer = new QueryObserver();

  @Override
  public Collection<Object> createComponents(
      Client client,
      ClusterService clusterService,
      ThreadPool threadPool,
      ResourceWatcherService resourceWatcherService,
      ScriptService scriptService,
      NamedXContentRegistry xContentRegistry,
      Environment environment,
      NodeEnvironment nodeEnvironment,
      NamedWriteableRegistry namedWriteableRegistry,
      IndexNameExpressionResolver indexNameExpressionResolver,
      Supplier<RepositoriesService> repositoriesServiceSupplier) {
    observer.setThreadPool(threadPool);
    return List.of();
  }

  @Override
  public List<org.opensearch.action.support.ActionFilter> getActionFilters() {
    return List.of(observer);
  }

  @Override
  public List<RestHandler> getRestHandlers(
      Settings settings,
      RestController restController,
      ClusterSettings clusterSettings,
      IndexScopedSettings indexScopedSettings,
      SettingsFilter settingsFilter,
      IndexNameExpressionResolver indexNameExpressionResolver,
      Supplier<DiscoveryNodes> nodesInCluster) {
    return List.of(new FixtureRestHandler());
  }

  private final class FixtureRestHandler extends BaseRestHandler {

    @Override
    public String getName() {
      return "async_query_test_fixture";
    }

    @Override
    public List<Route> routes() {
      return List.of(
          new Route(POST, BASE + "arm"),
          new Route(POST, BASE + "release"),
          new Route(POST, BASE + "disarm"),
          new Route(GET, BASE + "status"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
      String path = request.path();
      if (path.endsWith("/arm")) {
        String index = request.param("index");
        boolean hold = request.paramAsBoolean("hold", true);
        observer.arm(index, hold);
      } else if (path.endsWith("/release")) {
        boolean released = observer.release(request.paramAsBoolean("fail", false));
        if (!released) {
          return channel ->
              channel.sendResponse(new BytesRestResponse(RestStatus.CONFLICT, "no search is held"));
        }
      } else if (path.endsWith("/disarm")) {
        observer.disarm();
      }
      return channel -> {
        XContentBuilder builder = channel.newBuilder();
        builder.map(observer.status());
        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
      };
    }
  }
}
