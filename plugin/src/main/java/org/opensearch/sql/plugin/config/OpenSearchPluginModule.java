/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.config;

import lombok.RequiredArgsConstructor;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.AbstractModule;
import org.opensearch.common.inject.Provides;
import org.opensearch.common.inject.Singleton;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.analysis.ExpressionAnalyzer;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.client.OpenSearchNodeClient;
import org.opensearch.sql.opensearch.executor.OpenSearchExecutionEngine;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.sql.opensearch.executor.distributed.DistributedExecutionEngine;
import org.opensearch.sql.opensearch.executor.distributed.ExchangeService;
import org.opensearch.sql.opensearch.executor.distributed.OpenSearchShardSplitManager;
import org.opensearch.sql.opensearch.executor.protector.ExecutionProtector;
import org.opensearch.sql.opensearch.executor.protector.OpenSearchExecutionProtector;
import org.opensearch.sql.opensearch.monitor.OpenSearchMemoryHealthy;
import org.opensearch.sql.opensearch.monitor.OpenSearchResourceMonitor;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.distributed.FragmentPlanner;
import org.opensearch.sql.planner.distributed.ShardSplitManager;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.sql.SQLService;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.transport.client.node.NodeClient;

@RequiredArgsConstructor
public class OpenSearchPluginModule extends AbstractModule {

  private final BuiltinFunctionRepository functionRepository =
      BuiltinFunctionRepository.getInstance();

  @Override
  protected void configure() {}

  @Provides
  public OpenSearchClient openSearchClient(NodeClient nodeClient) {
    return new OpenSearchNodeClient(nodeClient);
  }

  @Provides
  public StorageEngine storageEngine(OpenSearchClient client, Settings settings) {
    return new OpenSearchStorageEngine(client, settings);
  }

  @Provides
  public ExecutionEngine executionEngine(
      OpenSearchClient client, ExecutionProtector protector, PlanSerializer planSerializer) {
    return new OpenSearchExecutionEngine(client, protector, planSerializer);
  }

  @Provides
  public ResourceMonitor resourceMonitor(Settings settings) {
    return new OpenSearchResourceMonitor(settings, new OpenSearchMemoryHealthy(settings));
  }

  @Provides
  public ExecutionProtector protector(ResourceMonitor resourceMonitor) {
    return new OpenSearchExecutionProtector(resourceMonitor);
  }

  @Provides
  public PlanSerializer planSerializer(StorageEngine storageEngine) {
    return new PlanSerializer(storageEngine);
  }

  @Provides
  @Singleton
  public QueryManager queryManager(NodeClient nodeClient, Settings settings) {
    return new OpenSearchQueryManager(nodeClient, settings);
  }

  @Provides
  public PPLService pplService(
      QueryManager queryManager, QueryPlanFactory queryPlanFactory, Settings settings) {
    return new PPLService(new PPLSyntaxParser(), queryManager, queryPlanFactory, settings);
  }

  @Provides
  public SQLService sqlService(QueryManager queryManager, QueryPlanFactory queryPlanFactory) {
    return new SQLService(new SQLSyntaxParser(), queryManager, queryPlanFactory);
  }

  @Provides
  @Singleton
  public ShardSplitManager shardSplitManager(ClusterService clusterService) {
    return new OpenSearchShardSplitManager(clusterService);
  }

  @Provides
  @Singleton
  public ExchangeService exchangeService() {
    return new ExchangeService();
  }

  /** {@link QueryPlanFactory}. */
  @Provides
  public QueryPlanFactory queryPlanFactory(
      DataSourceService dataSourceService,
      ExecutionEngine executionEngine,
      Settings settings,
      ClusterService clusterService,
      ShardSplitManager shardSplitManager,
      ExchangeService exchangeService) {
    Analyzer analyzer =
        new Analyzer(
            new ExpressionAnalyzer(functionRepository), dataSourceService, functionRepository);
    Planner planner = new Planner(LogicalPlanOptimizer.create());

    // Create distributed execution engine with fallback to the single-node engine
    String localNodeId = clusterService.localNode().getId();
    DistributedExecutionEngine distributedEngine =
        new DistributedExecutionEngine(
            new FragmentPlanner(),
            shardSplitManager,
            exchangeService,
            executionEngine,
            localNodeId);

    QueryService queryService =
        new QueryService(
            analyzer,
            executionEngine,
            planner,
            dataSourceService,
            settings,
            distributedEngine,
            shardSplitManager);
    return new QueryPlanFactory(queryService);
  }
}
