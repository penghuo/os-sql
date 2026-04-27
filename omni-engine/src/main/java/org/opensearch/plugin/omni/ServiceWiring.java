/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.omni;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.node.NodeInfo;
import io.airlift.slice.Slice;
import io.airlift.tracing.SpanSerialization;
import io.airlift.stats.GcMonitor;
import io.airlift.stats.JmxGcMonitor;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.trino.block.BlockJsonSerde;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogServiceProvider;
import io.trino.connector.ConnectorServices;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.connector.CatalogConnector;
import io.trino.connector.ConnectorName;
import io.trino.connector.CatalogProperties;
import io.trino.connector.informationschema.InformationSchemaConnector;
import io.trino.connector.system.CoordinatorSystemTablesProvider;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.connector.system.NodeSystemTable;
import io.trino.connector.system.StaticSystemTablesProvider;
import io.trino.connector.system.SystemConnector;
import io.trino.cost.ComposableStatsCalculator;
import io.trino.cost.CostCalculator;
import io.trino.cost.CostCalculatorUsingExchanges;
import io.trino.cost.CostCalculatorWithEstimatedExchanges;
import io.trino.cost.CostComparator;
import io.trino.cost.FilterStatsCalculator;
import io.trino.cost.ScalarStatsCalculator;
import io.trino.cost.StatsCalculator;
import io.trino.cost.StatsCalculatorModule;
import io.trino.cost.StatsNormalizer;
import io.trino.cost.TaskCountEstimator;
import io.trino.dispatcher.DispatchExecutor;
import io.trino.dispatcher.DispatchManager;
import io.trino.dispatcher.FailedDispatchQueryFactory;
import io.trino.dispatcher.LocalDispatchQueryFactory;
import io.trino.event.QueryMonitor;
import io.trino.event.QueryMonitorConfig;
import io.trino.event.SplitMonitor;
import io.trino.eventlistener.EventListenerConfig;
import io.trino.eventlistener.EventListenerManager;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.ClusterSizeMonitor;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.LocationFactory;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.QueryExecution.QueryExecutionFactory;
import io.trino.execution.QueryIdGenerator;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.QueryPreparer;
import io.trino.execution.RemoteTaskFactory;
import io.trino.execution.SqlQueryExecution;
import io.trino.execution.SqlQueryManager;
import io.trino.execution.SqlTaskManager;
import io.trino.execution.StageInfo;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskStatus;
import io.trino.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import io.trino.server.FailTaskRequest;
import io.trino.execution.TaskManagementExecutor;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.executor.timesharing.MultilevelSplitQueue;
import io.trino.execution.executor.timesharing.TimeSharingTaskExecutor;
import io.trino.execution.resourcegroups.InternalResourceGroupManager;
import io.trino.execution.resourcegroups.LegacyResourceGroupConfigurationManager;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.scheduler.SplitSchedulerStats;
import io.trino.execution.scheduler.TaskExecutionStats;
import io.trino.execution.scheduler.UniformNodeSelectorFactory;
import io.trino.execution.scheduler.faulttolerant.EventDrivenTaskSourceFactory;
import io.trino.execution.scheduler.faulttolerant.NoMemoryPartitionMemoryEstimator;
import io.trino.execution.scheduler.faulttolerant.NodeAllocatorService;
import io.trino.execution.scheduler.faulttolerant.OutputStatsEstimatorFactory;
import io.trino.execution.scheduler.faulttolerant.PartitionMemoryEstimatorFactory;
import io.trino.execution.scheduler.faulttolerant.TaskDescriptorStorage;
import io.trino.execution.scheduler.policy.AllAtOnceExecutionPolicy;
import io.trino.execution.scheduler.policy.PhasedExecutionPolicy;
import io.trino.execution.scheduler.policy.ExecutionPolicy;
import io.trino.execution.warnings.WarningCollector;
import io.trino.failuredetector.NoOpFailureDetector;
import io.trino.memory.ClusterMemoryManager;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.MemoryManagerConfig;
import io.trino.memory.NoneLowMemoryKiller;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.AnalyzePropertyManager;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.ColumnPropertyManager;
import io.trino.metadata.DisabledSystemSecurityMetadata;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.GlobalFunctionCatalog;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.LanguageFunctionManager;
import io.trino.metadata.LiteralFunction;
import io.trino.metadata.MaterializedViewPropertyManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.SchemaPropertyManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.Split;
import io.trino.metadata.SystemFunctionBundle;
import io.trino.metadata.TableFunctionRegistry;
import io.trino.metadata.TableProceduresPropertyManager;
import io.trino.metadata.TableProceduresRegistry;
import io.trino.metadata.TablePropertyManager;
import io.trino.operator.DirectExchangeClientConfig;
import io.trino.operator.DirectExchangeClientFactory;
import io.trino.operator.DirectExchangeClientSupplier;
import io.trino.operator.OperatorStats;
import io.trino.operator.PagesIndex;
import io.trino.operator.index.IndexManager;
import io.trino.cost.StatsAndCosts;
import io.trino.security.AccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.server.DynamicFilterService;
import io.trino.server.QuerySessionSupplier;
import io.trino.server.ServerConfig;
import io.trino.server.SessionPropertyDefaults;
import io.trino.server.SliceSerialization;
import io.trino.server.TaskUpdateRequest;
import io.trino.spi.QueryId;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorIndexHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;
import io.trino.spiller.FileSingleStreamSpillerFactory;
import io.trino.spiller.GenericPartitioningSpillerFactory;
import io.trino.spiller.GenericSpillerFactory;
import io.trino.spiller.LocalSpillManager;
import io.trino.spiller.NodeSpillConfig;
import io.trino.spiller.SpillerStats;
import io.trino.split.PageSinkManager;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.SqlEnvironmentConfig;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.analyzer.SessionTimeProvider;
import io.trino.sql.analyzer.StatementAnalyzerFactory;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.gen.JoinFilterFunctionCompiler;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.CompilerConfig;
import io.trino.execution.DynamicFilterConfig;
import io.trino.sql.planner.IrTypeAnalyzer;
import io.trino.sql.planner.LocalExecutionPlanner;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.sql.planner.PlanFragmenter;
import io.trino.sql.planner.PlanOptimizers;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.SplitSourceFactory;
import io.trino.sql.analyzer.QueryExplainerFactory;
import io.trino.sql.rewrite.DescribeInputRewrite;
import io.trino.sql.rewrite.DescribeOutputRewrite;
import io.trino.sql.rewrite.ExplainRewrite;
import io.trino.sql.rewrite.ShowQueriesRewrite;
import io.trino.sql.rewrite.StatementRewrite;
import io.trino.transaction.InMemoryTransactionManager;
import io.trino.transaction.TransactionManager;
import io.trino.transaction.TransactionManagerConfig;
import io.trino.type.BlockTypeOperators;
import io.trino.type.InternalTypeManager;
import io.trino.type.TypeDeserializer;
import io.trino.type.TypeSignatureKeyDeserializer;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.AbstractTypedJacksonModule;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.metadata.TypeRegistry;
import io.trino.FeaturesConfig;
import io.trino.SystemSessionProperties;
import io.trino.util.EmbedVersion;
import io.trino.util.FinalizerService;
import io.trino.operator.index.IndexJoinLookupStats;
import io.trino.execution.TableExecuteContextManager;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.Session;
import io.trino.sql.tree.Commit;
import io.trino.sql.tree.Deallocate;
import io.trino.sql.tree.Prepare;
import io.trino.sql.tree.ResetSession;
import io.trino.sql.tree.Rollback;
import io.trino.sql.tree.SetSession;
import io.trino.sql.tree.StartTransaction;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Use;
import io.trino.execution.CommitTask;
import io.trino.execution.DataDefinitionExecution;
import io.trino.execution.DataDefinitionTask;
import io.trino.execution.DeallocateTask;
import io.trino.execution.PrepareTask;
import io.trino.execution.ResetSessionTask;
import io.trino.execution.RollbackTask;
import io.trino.execution.SetSessionTask;
import io.trino.execution.StartTransactionTask;
import io.trino.execution.UseTask;

import org.opensearch.plugin.omni.exchange.ApacheHttpClientAdapter;
import org.opensearch.plugin.omni.ppl.PplTranslator;
import org.opensearch.transport.client.node.NodeClient;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.testing.TestingMBeanServer;
import org.opensearch.common.settings.Settings;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.spi.connector.CatalogHandle.createInformationSchemaCatalogHandle;
import static io.trino.spi.connector.CatalogHandle.createSystemTablesCatalogHandle;
import static java.util.concurrent.Executors.newScheduledThreadPool;

/**
 * Manually constructs the Trino service graph, replacing Trino's Airlift/Guice DI framework.
 * Called from {@link OmniPlugin#createComponents} to wire all services in dependency order.
 */
public class ServiceWiring implements Closeable {

    // Services exposed via getters for REST handlers and transport actions
    private final SqlParser sqlParser;
    private final PlannerContext plannerContext;
    private final StatementAnalyzerFactory statementAnalyzerFactory;
    private final SqlTaskManager sqlTaskManager;
    private final Metadata metadata;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final SessionPropertyManager sessionPropertyManager;
    private final SplitManager splitManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final PageSourceManager pageSourceManager;
    private final PageSinkManager pageSinkManager;
    private final FunctionManager functionManager;
    private final TypeManager typeManager;
    private final BlockEncodingSerde blockEncodingSerde;
    private final LanguageFunctionManager languageFunctionManager;
    private final DispatchManager dispatchManager;
    private final PplTranslator pplTranslator;
    private final QuerySessionSupplier sessionSupplier;

    // Resources to close
    private final FinalizerService finalizerService;
    private final TimeSharingTaskExecutor taskExecutor;
    private final TaskManagementExecutor taskManagementExecutor;
    private final GcMonitor gcMonitor;
    private final DispatchExecutor dispatchExecutor;
    private final SqlQueryManager sqlQueryManager;
    private final ClusterSizeMonitor clusterSizeMonitor;
    private volatile ApacheHttpClientAdapter exchangeHttpClient;
    private final DirectExchangeClientSupplier directExchangeClientSupplier;
    private final ExchangeManagerRegistry exchangeManagerRegistry;
    private final java.util.concurrent.ExecutorService queryResultsExecutor;
    private final java.util.concurrent.ScheduledExecutorService queryResultsTimeoutExecutor;

    // Transport codecs for remote task communication
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<TaskStatus> taskStatusCodec;
    private final JsonCodec<FailTaskRequest> failTaskRequestCodec;
    private final JsonCodec<VersionedDynamicFilterDomains> dynamicFilterDomainsCodec;

    public ServiceWiring(Settings settings, InternalNodeManager nodeManager, NodeClient nodeClient,
                         org.opensearch.cluster.service.ClusterService clusterService,
                         java.util.function.Supplier<org.opensearch.indices.IndicesService> indicesServiceSupplier) {
        // ── Layer 1: Config & Foundation ──
        FeaturesConfig featuresConfig = new FeaturesConfig()
                .setExchangeDataIntegrityVerification(FeaturesConfig.DataIntegrityVerification.NONE);
        QueryManagerConfig queryManagerConfig = new QueryManagerConfig();
        TaskManagerConfig taskManagerConfig = new TaskManagerConfig();
        NodeMemoryConfig nodeMemoryConfig = new NodeMemoryConfig();
        NodeSpillConfig nodeSpillConfig = new NodeSpillConfig();
        CompilerConfig compilerConfig = new CompilerConfig();
        DynamicFilterConfig dynamicFilterConfig = new DynamicFilterConfig();
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig();
        NodeVersion nodeVersion = new NodeVersion("omni-1.0");
        NodeInfo nodeInfo = new NodeInfo("omni");

        // ── Layer 2: Type System ──
        TypeOperators typeOperators = new TypeOperators();
        BlockTypeOperators blockTypeOperators = new BlockTypeOperators(typeOperators);
        TypeRegistry typeRegistry = new TypeRegistry(typeOperators, featuresConfig);
        this.typeManager = new InternalTypeManager(typeRegistry);
        BlockEncodingManager blockEncodingManager = new BlockEncodingManager();
        this.blockEncodingSerde = new InternalBlockEncodingSerde(blockEncodingManager, typeManager);

        // ── Layer 3: Functions & Metadata ──
        // Circular deps resolved via AtomicReference
        AtomicReference<Metadata> metadataRef = new AtomicReference<>();
        AtomicReference<FunctionManager> functionManagerRef = new AtomicReference<>();
        GlobalFunctionCatalog globalFunctionCatalog = new GlobalFunctionCatalog(
                metadataRef::get, () -> typeManager, functionManagerRef::get);

        globalFunctionCatalog.addFunctions(SystemFunctionBundle.create(
                featuresConfig, typeOperators, blockTypeOperators, nodeVersion));
        globalFunctionCatalog.addFunctions(new InternalFunctionBundle(new LiteralFunction(blockEncodingSerde)));

        // PPL UDFs: functions emitted by os-sql's PPL-to-SQL transpiler
        globalFunctionCatalog.addFunctions(InternalFunctionBundle.builder()
                .scalars(org.opensearch.plugin.omni.ppl.udf.SpanFunction.class)
                .scalars(org.opensearch.plugin.omni.ppl.udf.UnixTimestampFunction.class)
                .scalars(org.opensearch.plugin.omni.ppl.udf.TimestampFunction.class)
                .scalars(org.opensearch.plugin.omni.ppl.udf.QueryStringFunction.class)
                .scalars(org.opensearch.plugin.omni.ppl.udf.DivideFunction.class)
                .scalars(org.opensearch.plugin.omni.ppl.udf.RexExtractFunction.class)
                .build());

        this.sqlParser = new SqlParser();
        GroupProvider groupProvider = user -> ImmutableSet.of();
        this.languageFunctionManager = new LanguageFunctionManager(sqlParser, typeManager, groupProvider);

        // Transaction manager — use InMemoryTransactionManager with a mutable CatalogManager
        // so we can register the system catalog after metadata is created
        java.util.concurrent.ConcurrentHashMap<String, Catalog> catalogMap = new java.util.concurrent.ConcurrentHashMap<>();
        CatalogManager catalogManager = new CatalogManager() {
            @Override
            public Set<String> getCatalogNames() { return ImmutableSet.copyOf(catalogMap.keySet()); }
            @Override
            public Optional<Catalog> getCatalog(String catalogName) { return Optional.ofNullable(catalogMap.get(catalogName)); }
            @Override
            public Optional<CatalogProperties> getCatalogProperties(CatalogHandle catalogHandle) { return Optional.empty(); }
            @Override
            public Set<CatalogHandle> getActiveCatalogs() { return ImmutableSet.of(); }
            @Override
            public void createCatalog(String catalogName, ConnectorName connectorName, java.util.Map<String, String> properties, boolean notExists) { throw new UnsupportedOperationException(); }
            @Override
            public void dropCatalog(String catalogName, boolean exists) { throw new UnsupportedOperationException(); }
        };

        ScheduledExecutorService txnIdleCheckExecutor = Executors.newSingleThreadScheduledExecutor(
                io.airlift.concurrent.Threads.daemonThreadsNamed("txn-idle-check"));
        this.transactionManager = InMemoryTransactionManager.create(
                new TransactionManagerConfig(), txnIdleCheckExecutor, catalogManager, Executors.newCachedThreadPool());

        DisabledSystemSecurityMetadata systemSecurityMetadata = new DisabledSystemSecurityMetadata();
        MetadataManager metadataManager = new MetadataManager(
                systemSecurityMetadata, transactionManager, globalFunctionCatalog,
                languageFunctionManager, typeManager);
        this.metadata = metadataManager;
        metadataRef.set(metadataManager);

        FunctionManager fm = new FunctionManager(
                CatalogServiceProvider.fail(), globalFunctionCatalog, languageFunctionManager);
        this.functionManager = fm;
        functionManagerRef.set(fm);

        // ── Layer 4: Planner Context & Analyzers ──
        Tracer tracer = noopTracer();
        this.plannerContext = new PlannerContext(
                metadataManager, typeOperators, blockEncodingSerde, typeManager,
                functionManager, languageFunctionManager, tracer);
        languageFunctionManager.setPlannerContext(plannerContext);

        IrTypeAnalyzer irTypeAnalyzer = new IrTypeAnalyzer(plannerContext);

        EventListenerManager eventListenerManager = new EventListenerManager(new EventListenerConfig());
        AccessControlManager accessControlManager = new AccessControlManager(
                nodeVersion, transactionManager, eventListenerManager,
                new AccessControlConfig(), OpenTelemetry.noop(), "allow-all");
        accessControlManager.loadSystemAccessControl("allow-all", ImmutableMap.of());
        this.accessControl = accessControlManager;

        // ── Register system catalog ──
        // Create GlobalSystemConnector with NodeSystemTable
        NodeSystemTable nodeSystemTable = new NodeSystemTable(nodeManager);
        GlobalSystemConnector globalSystemConnector = new GlobalSystemConnector(
                ImmutableSet.of(nodeSystemTable), ImmutableSet.of(), ImmutableSet.of());

        // Build ConnectorServices for the system catalog
        CatalogHandle systemCatalogHandle = GlobalSystemConnector.CATALOG_HANDLE;
        ConnectorServices systemCatalogConnector = new ConnectorServices(tracer, systemCatalogHandle, globalSystemConnector);

        // Information schema connector for the system catalog
        ConnectorServices systemInfoSchemaConnector = new ConnectorServices(
                tracer,
                createInformationSchemaCatalogHandle(systemCatalogHandle),
                new InformationSchemaConnector(GlobalSystemConnector.NAME, nodeManager, metadataManager, accessControlManager, 20));

        // System tables connector for the system catalog
        StaticSystemTablesProvider staticSystemTablesProvider = new StaticSystemTablesProvider(systemCatalogConnector.getSystemTables());
        CoordinatorSystemTablesProvider coordinatorSystemTablesProvider = new CoordinatorSystemTablesProvider(
                transactionManager, metadataManager, GlobalSystemConnector.NAME, staticSystemTablesProvider);
        ConnectorServices systemTablesConnector = new ConnectorServices(
                tracer,
                createSystemTablesCatalogHandle(systemCatalogHandle),
                new SystemConnector(
                        nodeManager,
                        coordinatorSystemTablesProvider,
                        transactionId -> transactionManager.getConnectorTransaction(transactionId, systemCatalogHandle)));

        CatalogConnector systemCatalogConnectorObj = new CatalogConnector(
                systemCatalogHandle,
                new ConnectorName(GlobalSystemConnector.NAME),
                systemCatalogConnector,
                systemInfoSchemaConnector,
                systemTablesConnector,
                Optional.empty());
        catalogMap.put(GlobalSystemConnector.NAME, systemCatalogConnectorObj.getCatalog());

        // Connector session properties map — populated when connector is created
        java.util.concurrent.ConcurrentHashMap<CatalogHandle, Map<String, io.trino.spi.session.PropertyMetadata<?>>> connectorSessionPropsMap = new java.util.concurrent.ConcurrentHashMap<>();
        this.sessionPropertyManager = new SessionPropertyManager(
                ImmutableSet.of(new SystemSessionProperties()),
                catalogHandle -> connectorSessionPropsMap.getOrDefault(catalogHandle, ImmutableMap.of()));
        TablePropertyManager tablePropertyManager = new TablePropertyManager(CatalogServiceProvider.fail());
        AnalyzePropertyManager analyzePropertyManager = new AnalyzePropertyManager(CatalogServiceProvider.fail());
        TableProceduresPropertyManager tableProceduresPropertyManager =
                new TableProceduresPropertyManager(CatalogServiceProvider.fail());
        TableProceduresRegistry tableProceduresRegistry = new TableProceduresRegistry(CatalogServiceProvider.fail());
        TableFunctionRegistry tableFunctionRegistry = new TableFunctionRegistry(CatalogServiceProvider.fail());

        this.statementAnalyzerFactory = new StatementAnalyzerFactory(
                plannerContext, sqlParser, SessionTimeProvider.DEFAULT, accessControlManager,
                transactionManager, groupProvider, tableProceduresRegistry, tableFunctionRegistry,
                tablePropertyManager, analyzePropertyManager, tableProceduresPropertyManager);

        // ── Layer 5: Compilers & Execution Infrastructure ──
        PageFunctionCompiler pageFunctionCompiler = new PageFunctionCompiler(functionManager, compilerConfig);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(functionManager, pageFunctionCompiler);
        JoinCompiler joinCompiler = new JoinCompiler(typeOperators);
        JoinFilterFunctionCompiler joinFilterFunctionCompiler = new JoinFilterFunctionCompiler(functionManager);
        OrderingCompiler orderingCompiler = new OrderingCompiler(typeOperators);

        this.finalizerService = new FinalizerService();
        finalizerService.start();

        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        NodeScheduler nodeScheduler = new NodeScheduler(
                new UniformNodeSelectorFactory(nodeManager, nodeSchedulerConfig, nodeTaskMap));

        SpillerStats spillerStats = new SpillerStats();
        LocalSpillManager localSpillManager = new LocalSpillManager(nodeSpillConfig);
        FileSingleStreamSpillerFactory singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(
                blockEncodingSerde, spillerStats, featuresConfig, nodeSpillConfig);
        GenericSpillerFactory spillerFactory = new GenericSpillerFactory(singleStreamSpillerFactory);
        GenericPartitioningSpillerFactory partitioningSpillerFactory =
                new GenericPartitioningSpillerFactory(singleStreamSpillerFactory);

        PagesIndex.DefaultFactory pagesIndexFactory = new PagesIndex.DefaultFactory(
                orderingCompiler, joinCompiler, featuresConfig, blockTypeOperators);

        this.exchangeManagerRegistry = new ExchangeManagerRegistry(OpenTelemetry.noop(), tracer);

        // Lazy ConnectorServicesProvider reference — populated in Layer 7
        AtomicReference<ConnectorServicesProvider> connectorServicesProviderRef = new AtomicReference<>();
        // CatalogServiceProvider that delegates to ConnectorServicesProvider
        CatalogServiceProvider<ConnectorSplitManager> splitManagerProvider = catalogHandle ->
                connectorServicesProviderRef.get().getConnectorServices(catalogHandle).getSplitManager()
                        .orElseThrow(() -> new IllegalStateException("No split manager for " + catalogHandle));
        CatalogServiceProvider<ConnectorPageSourceProvider> pageSourceProvider = catalogHandle ->
                connectorServicesProviderRef.get().getConnectorServices(catalogHandle).getPageSourceProvider()
                        .orElseThrow(() -> new IllegalStateException("No page source provider for " + catalogHandle));

        this.splitManager = new SplitManager(splitManagerProvider, tracer, queryManagerConfig);
        this.pageSourceManager = new PageSourceManager(pageSourceProvider);
        this.pageSinkManager = new PageSinkManager(CatalogServiceProvider.fail());
        IndexManager indexManager = new IndexManager(CatalogServiceProvider.fail());
        this.nodePartitioningManager = new NodePartitioningManager(
                nodeScheduler, typeOperators, CatalogServiceProvider.fail());

        IndexJoinLookupStats indexJoinLookupStats = new IndexJoinLookupStats();
        TableExecuteContextManager tableExecuteContextManager = new TableExecuteContextManager();

        LocalMemoryManager localMemoryManager = new LocalMemoryManager(nodeMemoryConfig);

        MultilevelSplitQueue splitQueue = new MultilevelSplitQueue(taskManagerConfig);
        EmbedVersion versionEmbedder = new EmbedVersion(nodeVersion);
        this.taskExecutor = new TimeSharingTaskExecutor(taskManagerConfig, versionEmbedder, tracer, splitQueue);
        taskExecutor.start();
        this.taskManagementExecutor = new TaskManagementExecutor();

        this.gcMonitor = new JmxGcMonitor();
        SplitMonitor splitMonitor = new SplitMonitor(eventListenerManager,
                new ObjectMapper()
                        .registerModule(new com.fasterxml.jackson.datatype.joda.JodaModule())
                        .registerModule(new com.fasterxml.jackson.datatype.jdk8.Jdk8Module())
                        .registerModule(new com.fasterxml.jackson.datatype.guava.GuavaModule())
                        .disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS));

        // ── Layer 6: LocalExecutionPlanner ──
        // Wire DirectExchangeClientFactory with ApacheHttpClientAdapter for real HTTP exchange.
        // SqlTaskManager is created in Layer 7, so we use a lazy reference.
        AtomicReference<SqlTaskManager> sqlTaskManagerRef = new AtomicReference<>();
        ScheduledExecutorService exchangeScheduler = Executors.newScheduledThreadPool(4,
                io.airlift.concurrent.Threads.daemonThreadsNamed("exchange-client-%s"));
        DirectExchangeClientConfig exchangeClientConfig = new DirectExchangeClientConfig();

        // Wire DirectExchangeClientFactory with ApacheHttpClientAdapter for real HTTP exchange.
        int exchangePort = OmniSettings.EXCHANGE_PORT.get(settings);
        // Match Trino's exchange HTTP client config (ServerMainModule.java:360):
        //   maxConnectionsPerServer=250, maxContentLength=32MB
        // HC5 defaults are maxConnPerRoute=5, maxConnTotal=25 — far too low for exchange.
        org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager connManager =
                org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder.create()
                        .setMaxConnPerRoute(250)
                        .setMaxConnTotal(250)
                        .build();
        org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient hc5Client =
                org.apache.hc.client5.http.impl.async.HttpAsyncClients.custom()
                        .setConnectionManager(connManager)
                        .build();
        ApacheHttpClientAdapter exchangeHttpClient = new ApacheHttpClientAdapter(
                hc5Client, exchangeClientConfig.getMaxResponseSize().toBytes());
        this.exchangeHttpClient = exchangeHttpClient;

        DirectExchangeClientFactory directExchangeClientFactory = new DirectExchangeClientFactory(
                nodeInfo,
                featuresConfig.getExchangeDataIntegrityVerification(),
                exchangeClientConfig.getMaxBufferSize(),
                exchangeClientConfig.getDeduplicationBufferSize(),
                exchangeClientConfig.getMaxResponseSize(),
                exchangeClientConfig.getConcurrentRequestMultiplier(),
                exchangeClientConfig.getMaxErrorDuration(),
                exchangeClientConfig.isAcknowledgePages(),
                exchangeClientConfig.getPageBufferClientMaxCallbackThreads(),
                exchangeHttpClient,
                exchangeScheduler,
                exchangeManagerRegistry);
        DirectExchangeClientSupplier directExchangeClientSupplier = directExchangeClientFactory;
        this.directExchangeClientSupplier = directExchangeClientSupplier;

        LocalExecutionPlanner localExecutionPlanner = new LocalExecutionPlanner(
                plannerContext, irTypeAnalyzer, Optional.empty(),
                pageSourceManager, indexManager, nodePartitioningManager, pageSinkManager,
                directExchangeClientSupplier, expressionCompiler, pageFunctionCompiler,
                joinFilterFunctionCompiler, indexJoinLookupStats, taskManagerConfig,
                spillerFactory, singleStreamSpillerFactory, partitioningSpillerFactory,
                pagesIndexFactory, joinCompiler, orderingCompiler, dynamicFilterConfig,
                blockTypeOperators, typeOperators, tableExecuteContextManager,
                exchangeManagerRegistry, nodeVersion, compilerConfig);

        // ── Layer 7: SqlTaskManager ──
        LocationFactory locationFactory = new LocationFactory() {
            @Override
            public URI createQueryLocation(QueryId queryId) {
                return URI.create("http://localhost/query/" + queryId);
            }

            @Override
            public URI createLocalTaskLocation(TaskId taskId) {
                // Must use the node's network-reachable address, not 127.0.0.1.
                // HttpRemoteTask reads TaskStatus.getSelf() (which embeds this URL) to
                // construct subsequent HTTP requests. If self=127.0.0.1, remote coordinators
                // would send requests to their own localhost instead of this node.
                if (nodeManager instanceof org.opensearch.plugin.omni.cluster.ClusterStateNodeManager csm) {
                    io.trino.metadata.InternalNode current = csm.getCurrentNode();
                    int port = csm.getExchangePort(current);
                    return URI.create("http://" + current.getHostAndPort().getHostText() + ":" + port + "/v1/task/" + taskId);
                }
                return URI.create("http://127.0.0.1:" + exchangePort + "/v1/task/" + taskId);
            }

            @Override
            public URI createTaskLocation(InternalNode node, TaskId taskId) {
                int nodePort = (nodeManager instanceof org.opensearch.plugin.omni.cluster.ClusterStateNodeManager csm)
                        ? csm.getExchangePort(node)
                        : exchangePort;
                return URI.create("http://" + node.getHostAndPort().getHostText() + ":" + nodePort + "/v1/task/" + taskId);
            }

            @Override
            public URI createMemoryInfoLocation(InternalNode node) {
                return URI.create("http://" + node.getHostAndPort() + "/memory");
            }
        };

        // ── Iceberg Connector (conditional — only if catalog URI is configured) ──
        String icebergCatalogName = "hive";
        CatalogHandle icebergCatalogHandle = CatalogHandle.createRootCatalogHandle(
                icebergCatalogName, new CatalogHandle.CatalogVersion("1"));
        CatalogConnector icebergCatalogConnectorObj = null;

        String catalogType = OmniSettings.CATALOG_TYPE.get(settings);
        boolean hiveMode = "hive".equals(catalogType);
        boolean icebergMode = "rest".equals(catalogType);
        String catalogUri = OmniSettings.CATALOG_URI.get(settings);
        String metastoreDir = OmniSettings.CATALOG_METASTORE_DIR.get(settings);
        boolean hasCatalogConfig = hiveMode
                ? (metastoreDir != null && !metastoreDir.isEmpty())
                : (catalogUri != null && !catalogUri.isEmpty());

        // Shared by all connectors (Hive/Iceberg and OpenSearch)
        io.trino.spi.PageSorter pageSorter = new io.trino.operator.PagesIndexPageSorter(pagesIndexFactory);
        io.trino.spi.PageIndexerFactory pageIndexerFactory = new io.trino.operator.GroupByHashPageIndexerFactory(joinCompiler);

        if (hasCatalogConfig) {
            // Build ConnectorContext (shared by Hive and Iceberg)
            io.trino.connector.ConnectorContextInstance connectorContext = new io.trino.connector.ConnectorContextInstance(
                    icebergCatalogHandle,
                    OpenTelemetry.noop(),
                    tracer,
                    new io.trino.connector.ConnectorAwareNodeManager(nodeManager, "omni", icebergCatalogHandle, true),
                    versionEmbedder,
                    typeManager,
                    new io.trino.connector.InternalMetadataProvider(metadataManager, typeManager),
                    pageSorter,
                    pageIndexerFactory);

            // Create connector via appropriate factory
            io.trino.spi.connector.Connector connector;
            if (hiveMode) {
                Map<String, String> hiveConfig = ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "file")
                        .put("hive.metastore.catalog.dir", metastoreDir)
                        .put("hive.non-managed-table-writes-enabled", "true")
                        .put("hive.metastore.version-compatibility", "UNSAFE_ASSUME_COMPATIBILITY")
                        .put("fs.hadoop.enabled", "false")
                        .buildOrThrow();
                // Provide a local filesystem factory for file:// URIs via extra Guice module
                com.google.inject.Module localFsModule = binder -> {
                    com.google.inject.multibindings.MapBinder<String, io.trino.filesystem.TrinoFileSystemFactory> fsBinder =
                            com.google.inject.multibindings.MapBinder.newMapBinder(binder, String.class, io.trino.filesystem.TrinoFileSystemFactory.class);
                    fsBinder.addBinding("file").toInstance(new org.opensearch.plugin.omni.filesystem.FileSchemeFileSystemFactory());
                };
                connector = io.trino.plugin.hive.HiveConnectorFactory.createConnector(
                        icebergCatalogName, hiveConfig, connectorContext, localFsModule,
                        Optional.empty(), Optional.empty());
            } else {
                Map<String, String> icebergConfig = ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", catalogType)
                        .put("iceberg.rest-catalog.uri", catalogUri)
                        .put("fs.native-s3.enabled", "true")
                        .put("s3.region", OmniSettings.S3_REGION.get(settings))
                        .buildOrThrow();
                connector = new io.trino.plugin.iceberg.IcebergConnectorFactory()
                        .create(icebergCatalogName, icebergConfig, connectorContext);
            }
            ConnectorServices icebergConnectorServices = new ConnectorServices(tracer, icebergCatalogHandle, connector);
            connectorSessionPropsMap.put(icebergCatalogHandle, icebergConnectorServices.getSessionProperties());

            // Build information_schema and system connectors for Iceberg catalog
            ConnectorServices icebergInfoSchemaConnector = new ConnectorServices(
                    tracer,
                    createInformationSchemaCatalogHandle(icebergCatalogHandle),
                    new InformationSchemaConnector(icebergCatalogName, nodeManager, metadataManager, accessControlManager, 20));
            StaticSystemTablesProvider icebergSystemTablesProvider = new StaticSystemTablesProvider(icebergConnectorServices.getSystemTables());
            CoordinatorSystemTablesProvider icebergCoordSystemTablesProvider = new CoordinatorSystemTablesProvider(
                    transactionManager, metadataManager, icebergCatalogName, icebergSystemTablesProvider);
            ConnectorServices icebergSystemConnector = new ConnectorServices(
                    tracer,
                    createSystemTablesCatalogHandle(icebergCatalogHandle),
                    new SystemConnector(
                            nodeManager,
                            icebergCoordSystemTablesProvider,
                            transactionId -> transactionManager.getConnectorTransaction(transactionId, icebergCatalogHandle)));

            icebergCatalogConnectorObj = new CatalogConnector(
                    icebergCatalogHandle,
                    new ConnectorName("iceberg"),
                    icebergConnectorServices,
                    icebergInfoSchemaConnector,
                    icebergSystemConnector,
                    Optional.empty());
            catalogMap.put(icebergCatalogName, icebergCatalogConnectorObj.getCatalog());
        }

        // ── OpenSearch Connector (always created) ──
        String opensearchCatalogName = "opensearch";
        CatalogHandle opensearchCatalogHandle = CatalogHandle.createRootCatalogHandle(
                opensearchCatalogName, new CatalogHandle.CatalogVersion("opensearch"));

        org.opensearch.plugin.omni.connector.opensearch.OpenSearchConnectorFactory opensearchFactory =
                new org.opensearch.plugin.omni.connector.opensearch.OpenSearchConnectorFactory(
                        indicesServiceSupplier, clusterService);

        io.trino.connector.ConnectorContextInstance opensearchConnectorContext = new io.trino.connector.ConnectorContextInstance(
                opensearchCatalogHandle,
                OpenTelemetry.noop(),
                tracer,
                new io.trino.connector.ConnectorAwareNodeManager(nodeManager, "omni", opensearchCatalogHandle, true),
                versionEmbedder,
                typeManager,
                new io.trino.connector.InternalMetadataProvider(metadataManager, typeManager),
                pageSorter,
                pageIndexerFactory);

        io.trino.spi.connector.Connector opensearchConnector = opensearchFactory.create(
                opensearchCatalogName, opensearchConnectorContext);

        ConnectorServices opensearchConnectorServices = new ConnectorServices(
                tracer, opensearchCatalogHandle, opensearchConnector);
        connectorSessionPropsMap.put(opensearchCatalogHandle, opensearchConnectorServices.getSessionProperties());

        // Information schema + system tables for OpenSearch catalog
        ConnectorServices opensearchInfoSchemaConnector = new ConnectorServices(
                tracer,
                createInformationSchemaCatalogHandle(opensearchCatalogHandle),
                new InformationSchemaConnector(opensearchCatalogName, nodeManager, metadataManager, accessControlManager, 20));

        StaticSystemTablesProvider opensearchSystemTablesProvider = new StaticSystemTablesProvider(
                opensearchConnectorServices.getSystemTables());
        CoordinatorSystemTablesProvider opensearchCoordSystemTablesProvider = new CoordinatorSystemTablesProvider(
                transactionManager, metadataManager, opensearchCatalogName, opensearchSystemTablesProvider);
        ConnectorServices opensearchSystemConnector = new ConnectorServices(
                tracer,
                createSystemTablesCatalogHandle(opensearchCatalogHandle),
                new SystemConnector(
                        nodeManager,
                        opensearchCoordSystemTablesProvider,
                        transactionId -> transactionManager.getConnectorTransaction(transactionId, opensearchCatalogHandle)));

        CatalogConnector opensearchCatalogConnectorObj = new CatalogConnector(
                opensearchCatalogHandle,
                new ConnectorName("opensearch"),
                opensearchConnectorServices,
                opensearchInfoSchemaConnector,
                opensearchSystemConnector,
                Optional.empty());

        catalogMap.put(opensearchCatalogName, opensearchCatalogConnectorObj.getCatalog());

        // ConnectorServicesProvider — handles system + iceberg + opensearch catalogs
        final CatalogConnector finalIcebergCatalogConnectorObj = icebergCatalogConnectorObj;
        ConnectorServicesProvider connectorServicesProvider = new ConnectorServicesProvider() {
            @Override
            public void loadInitialCatalogs() {}

            @Override
            public void ensureCatalogsLoaded(Session session, List<CatalogProperties> catalogs) {}

            @Override
            public void pruneCatalogs(Set<CatalogHandle> catalogsInUse) {}

            @Override
            public ConnectorServices getConnectorServices(CatalogHandle catalogHandle) {
                // Check if this is the OpenSearch catalog (by name match)
                if (catalogHandle.getCatalogName().equals(opensearchCatalogName)) {
                    return opensearchCatalogConnectorObj.getMaterializedConnector(catalogHandle.getType());
                }
                // Check if this is the Iceberg/Hive catalog (by name match)
                if (finalIcebergCatalogConnectorObj != null && catalogHandle.getCatalogName().equals(icebergCatalogName)) {
                    return finalIcebergCatalogConnectorObj.getMaterializedConnector(catalogHandle.getType());
                }
                return systemCatalogConnectorObj.getMaterializedConnector(catalogHandle.getType());
            }
        };
        connectorServicesProviderRef.set(connectorServicesProvider);

        this.sqlTaskManager = new SqlTaskManager(
                versionEmbedder, connectorServicesProvider, localExecutionPlanner,
                languageFunctionManager, locationFactory, taskExecutor, splitMonitor,
                nodeInfo, localMemoryManager, taskManagementExecutor, taskManagerConfig,
                nodeMemoryConfig, localSpillManager, nodeSpillConfig, gcMonitor, tracer,
                exchangeManagerRegistry);
        sqlTaskManagerRef.set(this.sqlTaskManager);

        // ── Layer 8: Query Execution Pipeline ──

        // Create a properly configured JsonCodecFactory with Trino's custom serializers
        HandleResolver handleResolver = new HandleResolver();
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonSerializers(ImmutableMap.of(
                Span.class, new SpanSerialization.SpanSerializer(OpenTelemetry.noop()),
                Slice.class, new SliceSerialization.SliceSerializer(),
                Block.class, new BlockJsonSerde.Serializer(blockEncodingSerde)));
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(
                Span.class, new SpanSerialization.SpanDeserializer(OpenTelemetry.noop()),
                Slice.class, new SliceSerialization.SliceDeserializer(),
                Type.class, new TypeDeserializer(typeManager),
                Block.class, new BlockJsonSerde.Deserializer(blockEncodingSerde)));
        objectMapperProvider.setKeyDeserializers(ImmutableMap.of(
                TypeSignature.class, new TypeSignatureKeyDeserializer()));
        objectMapperProvider.setModules(ImmutableSet.of(
                new AbstractTypedJacksonModule<>(ConnectorTableHandle.class, handleResolver::getId, handleResolver::getHandleClass) {},
                new AbstractTypedJacksonModule<>(ColumnHandle.class, handleResolver::getId, handleResolver::getHandleClass) {},
                new AbstractTypedJacksonModule<>(ConnectorSplit.class, handleResolver::getId, handleResolver::getHandleClass) {},
                new AbstractTypedJacksonModule<>(ConnectorOutputTableHandle.class, handleResolver::getId, handleResolver::getHandleClass) {},
                new AbstractTypedJacksonModule<>(ConnectorInsertTableHandle.class, handleResolver::getId, handleResolver::getHandleClass) {},
                new AbstractTypedJacksonModule<>(ConnectorTableExecuteHandle.class, handleResolver::getId, handleResolver::getHandleClass) {},
                new AbstractTypedJacksonModule<>(ConnectorMergeTableHandle.class, handleResolver::getId, handleResolver::getHandleClass) {},
                new AbstractTypedJacksonModule<>(ConnectorIndexHandle.class, handleResolver::getId, handleResolver::getHandleClass) {},
                new AbstractTypedJacksonModule<>(ConnectorTransactionHandle.class, handleResolver::getId, handleResolver::getHandleClass) {},
                new AbstractTypedJacksonModule<>(ConnectorPartitioningHandle.class, handleResolver::getId, handleResolver::getHandleClass) {},
                new AbstractTypedJacksonModule<>(ExchangeSinkInstanceHandle.class, handleResolver::getId, handleResolver::getHandleClass) {},
                new AbstractTypedJacksonModule<>(ExchangeSourceHandle.class, handleResolver::getId, handleResolver::getHandleClass) {},
                new AbstractTypedJacksonModule<>(ConnectorTableFunctionHandle.class, handleResolver::getId, handleResolver::getHandleClass) {},
                new com.fasterxml.jackson.datatype.joda.JodaModule()));
        JsonCodecFactory jsonCodecFactory = new JsonCodecFactory(objectMapperProvider);

        JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec = jsonCodecFactory.jsonCodec(TaskUpdateRequest.class);
        JsonCodec<TaskInfo> taskInfoCodec = jsonCodecFactory.jsonCodec(TaskInfo.class);
        JsonCodec<TaskStatus> taskStatusCodec = jsonCodecFactory.jsonCodec(TaskStatus.class);
        JsonCodec<FailTaskRequest> failTaskRequestCodec = jsonCodecFactory.jsonCodec(FailTaskRequest.class);
        JsonCodec<VersionedDynamicFilterDomains> dynamicFilterDomainsCodec = jsonCodecFactory.jsonCodec(VersionedDynamicFilterDomains.class);
        this.taskUpdateRequestCodec = taskUpdateRequestCodec;
        this.taskInfoCodec = taskInfoCodec;
        this.taskStatusCodec = taskStatusCodec;
        this.failTaskRequestCodec = failTaskRequestCodec;
        this.dynamicFilterDomainsCodec = dynamicFilterDomainsCodec;

        // QueryMonitor
        QueryMonitorConfig queryMonitorConfig = new QueryMonitorConfig();
        QueryMonitor queryMonitor = new QueryMonitor(
                jsonCodecFactory.jsonCodec(StageInfo.class),
                jsonCodecFactory.jsonCodec(OperatorStats.class),
                jsonCodecFactory.jsonCodec(ExecutionFailureInfo.class),
                jsonCodecFactory.jsonCodec(StatsAndCosts.class),
                eventListenerManager,
                nodeInfo,
                nodeVersion,
                sessionPropertyManager,
                metadataManager,
                fm,
                queryMonitorConfig);

        // DispatchExecutor
        this.dispatchExecutor = new DispatchExecutor(queryManagerConfig, versionEmbedder);

        // ClusterMemoryManager — requires HttpClient + MBeanExporter; construct with real params
        MBeanExporter mbeanExporter = new MBeanExporter(new TestingMBeanServer());
        ServerConfig serverConfig = new ServerConfig().setCoordinator(true);
        MemoryManagerConfig memoryManagerConfig = new MemoryManagerConfig();
        ClusterMemoryManager clusterMemoryManager = new ClusterMemoryManager(
                new io.airlift.http.client.testing.TestingHttpClient(request -> {
                    throw new UnsupportedOperationException("No HTTP transport for memory manager");
                }),
                nodeManager,
                locationFactory,
                mbeanExporter,
                JsonCodec.jsonCodec(io.trino.memory.MemoryInfo.class),
                new NoneLowMemoryKiller(),
                new NoneLowMemoryKiller(),
                serverConfig,
                memoryManagerConfig);

        // SqlQueryManager
        this.sqlQueryManager = new SqlQueryManager(clusterMemoryManager, tracer, queryManagerConfig);
        sqlQueryManager.start();

        // Resource group manager
        LegacyResourceGroupConfigurationManager legacyManager =
                new LegacyResourceGroupConfigurationManager(queryManagerConfig);
        InternalResourceGroupManager<?> resourceGroupManager =
                new InternalResourceGroupManager<>(legacyManager, clusterMemoryManager, nodeInfo, mbeanExporter);
        resourceGroupManager.start();

        // ClusterSizeMonitor
        this.clusterSizeMonitor = new ClusterSizeMonitor(nodeManager, nodeSchedulerConfig);
        clusterSizeMonitor.start();

        // Stats & Cost calculators
        StatsNormalizer statsNormalizer = new StatsNormalizer();
        ScalarStatsCalculator scalarStatsCalculator = new ScalarStatsCalculator(plannerContext, irTypeAnalyzer);
        FilterStatsCalculator filterStatsCalculator = new FilterStatsCalculator(
                plannerContext, scalarStatsCalculator, statsNormalizer, irTypeAnalyzer);
        StatsCalculatorModule.StatsRulesProvider statsRulesProvider =
                new StatsCalculatorModule.StatsRulesProvider(plannerContext, scalarStatsCalculator, filterStatsCalculator, statsNormalizer);
        StatsCalculator statsCalculator = new ComposableStatsCalculator(statsRulesProvider.get());

        TaskCountEstimator taskCountEstimator = new TaskCountEstimator(nodeSchedulerConfig, nodeManager);
        CostCalculator costCalculator = new CostCalculatorUsingExchanges(taskCountEstimator);
        CostCalculator costCalculatorWithEstimatedExchanges =
                new CostCalculatorWithEstimatedExchanges(costCalculator, taskCountEstimator);
        CostComparator costComparator = new CostComparator(new OptimizerConfig());

        // DynamicFilterService
        DynamicFilterService dynamicFilterService = new DynamicFilterService(
                metadataManager, fm, typeOperators, dynamicFilterConfig);

        // SplitSourceFactory
        SplitSourceFactory splitSourceFactory = new SplitSourceFactory(
                splitManager, plannerContext, dynamicFilterService, irTypeAnalyzer);

        // PlanOptimizers
        RuleStatsRecorder ruleStatsRecorder = new RuleStatsRecorder();
        PlanOptimizers planOptimizers = new PlanOptimizers(
                plannerContext,
                irTypeAnalyzer,
                taskManagerConfig,
                splitManager,
                pageSourceManager,
                statsCalculator,
                scalarStatsCalculator,
                costCalculator,
                costCalculatorWithEstimatedExchanges,
                costComparator,
                taskCountEstimator,
                nodePartitioningManager,
                ruleStatsRecorder);

        // PlanFragmenter
        PlanFragmenter planFragmenter = new PlanFragmenter(
                metadataManager, fm, transactionManager, catalogManager,
                languageFunctionManager, queryManagerConfig);

        // QueryPreparer (needed by ExplainRewrite)
        QueryPreparer queryPreparer = new QueryPreparer(sqlParser);

        // QueryExplainerFactory for ExplainRewrite
        QueryExplainerFactory queryExplainerFactory = new QueryExplainerFactory(
                planOptimizers, planFragmenter, plannerContext,
                statsCalculator, costCalculator, nodeVersion);

        // Property managers needed for ShowQueriesRewrite
        SchemaPropertyManager schemaPropertyManager = new SchemaPropertyManager(CatalogServiceProvider.fail());
        ColumnPropertyManager columnPropertyManager = new ColumnPropertyManager(CatalogServiceProvider.fail());
        MaterializedViewPropertyManager materializedViewPropertyManager = new MaterializedViewPropertyManager(CatalogServiceProvider.fail());

        // StatementRewrite with ExplainRewrite, Describe rewrites, and ShowQueriesRewrite
        StatementRewrite statementRewrite = new StatementRewrite(ImmutableSet.of(
                new ExplainRewrite(queryExplainerFactory, queryPreparer),
                new DescribeInputRewrite(sqlParser),
                new DescribeOutputRewrite(sqlParser),
                new ShowQueriesRewrite(
                        metadataManager, sqlParser, accessControlManager, sessionPropertyManager,
                        schemaPropertyManager, columnPropertyManager, tablePropertyManager,
                        materializedViewPropertyManager)));
        AnalyzerFactory analyzerFactory = new AnalyzerFactory(statementAnalyzerFactory, statementRewrite, tracer);

        // Executors for query execution
        ExecutorService queryExecutor = Executors.newCachedThreadPool(
                io.airlift.concurrent.Threads.daemonThreadsNamed("query-execution-%s"));
        ScheduledExecutorService schedulerExecutor = Executors.newScheduledThreadPool(2,
                io.airlift.concurrent.Threads.daemonThreadsNamed("query-scheduler-%s"));

        // Execution policies
        Map<String, ExecutionPolicy> executionPolicies = ImmutableMap.of(
                "all-at-once", new AllAtOnceExecutionPolicy(),
                "phased", new PhasedExecutionPolicy(dynamicFilterService));

        // Fault-tolerant components (no-op for MVP)
        EventDrivenTaskSourceFactory eventDrivenTaskSourceFactory = new EventDrivenTaskSourceFactory(
                splitSourceFactory, queryExecutor, nodeManager, tableExecuteContextManager, queryManagerConfig);
        TaskDescriptorStorage taskDescriptorStorage = new TaskDescriptorStorage(
                queryManagerConfig, JsonCodec.jsonCodec(Split.class));

        // SqlQueryExecutionFactory (package-private constructor, use reflection)
        SqlQueryExecution.SqlQueryExecutionFactory sqlQueryExecutionFactory;
        try {
            var ctor = SqlQueryExecution.SqlQueryExecutionFactory.class.getDeclaredConstructors()[0];
            ctor.setAccessible(true);
            sqlQueryExecutionFactory = (SqlQueryExecution.SqlQueryExecutionFactory) ctor.newInstance(
                        tracer,
                        queryManagerConfig,
                        plannerContext,
                        analyzerFactory,
                        splitSourceFactory,
                        nodePartitioningManager,
                        nodeScheduler,
                        // NodeAllocatorService — no-op for pipelined execution
                        (NodeAllocatorService) session1 -> {
                            throw new UnsupportedOperationException("NodeAllocator not supported in pipelined mode");
                        },
                        // PartitionMemoryEstimatorFactory — no-op
                        (PartitionMemoryEstimatorFactory) (session1, fragment, lookup) -> NoMemoryPartitionMemoryEstimator.INSTANCE,
                        // OutputStatsEstimatorFactory — no-op
                        (OutputStatsEstimatorFactory) session1 -> (stageExecution, stageExecutionLookup, parentEager) -> Optional.empty(),
                        new TaskExecutionStats(),
                        planOptimizers,
                        planFragmenter,
                        // RemoteTaskFactory — wired via HTTP exchange transport
                        new io.trino.server.HttpRemoteTaskFactory(
                                queryManagerConfig, taskManagerConfig,
                                exchangeHttpClient, locationFactory,
                                taskStatusCodec, dynamicFilterDomainsCodec,
                                taskInfoCodec, taskUpdateRequestCodec, failTaskRequestCodec,
                                io.opentelemetry.api.trace.TracerProvider.noop().get("noop"),
                                new io.trino.server.remotetask.RemoteTaskStats(),
                                dynamicFilterService),
                        queryExecutor,
                        schedulerExecutor,
                        new NoOpFailureDetector(),
                        nodeTaskMap,
                        executionPolicies,
                        new SplitSchedulerStats(),
                        statsCalculator,
                        costCalculator,
                        dynamicFilterService,
                        tableExecuteContextManager,
                        irTypeAnalyzer,
                        sqlTaskManager,
                        exchangeManagerRegistry,
                        eventDrivenTaskSourceFactory,
                        taskDescriptorStorage);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create SqlQueryExecutionFactory", e);
        }

        // DDL tasks
        Map<Class<? extends Statement>, DataDefinitionTask<?>> ddlTasks = ImmutableMap.<Class<? extends Statement>, DataDefinitionTask<?>>builder()
                .put(SetSession.class, new SetSessionTask(plannerContext, accessControlManager, sessionPropertyManager))
                .put(ResetSession.class, new ResetSessionTask(metadataManager, sessionPropertyManager))
                .put(Use.class, new UseTask(metadataManager, accessControlManager))
                .put(Prepare.class, new PrepareTask(sqlParser))
                .put(Deallocate.class, new DeallocateTask())
                .put(StartTransaction.class, new StartTransactionTask(transactionManager))
                .put(Commit.class, new CommitTask(transactionManager))
                .put(Rollback.class, new RollbackTask(transactionManager))
                .buildOrThrow();

        DataDefinitionExecution.DataDefinitionExecutionFactory dataDefinitionExecutionFactory =
                new DataDefinitionExecution.DataDefinitionExecutionFactory(ddlTasks);

        // Execution factories map
        ImmutableMap.Builder<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactoriesBuilder =
                ImmutableMap.<Class<? extends Statement>, QueryExecutionFactory<?>>builder()
                        .put(io.trino.sql.tree.Query.class, sqlQueryExecutionFactory)
                        .put(io.trino.sql.tree.ExplainAnalyze.class, sqlQueryExecutionFactory)
                        // SHOW/DESCRIBE statements rewritten to Query by ShowQueriesRewrite
                        .put(io.trino.sql.tree.ShowCatalogs.class, sqlQueryExecutionFactory)
                        .put(io.trino.sql.tree.ShowSchemas.class, sqlQueryExecutionFactory)
                        .put(io.trino.sql.tree.ShowTables.class, sqlQueryExecutionFactory)
                        .put(io.trino.sql.tree.ShowColumns.class, sqlQueryExecutionFactory)
                        .put(io.trino.sql.tree.ShowFunctions.class, sqlQueryExecutionFactory)
                        .put(io.trino.sql.tree.ShowSession.class, sqlQueryExecutionFactory)
                        .put(io.trino.sql.tree.ShowCreate.class, sqlQueryExecutionFactory)
                        .put(io.trino.sql.tree.ShowGrants.class, sqlQueryExecutionFactory)
                        .put(io.trino.sql.tree.ShowRoles.class, sqlQueryExecutionFactory)
                        .put(io.trino.sql.tree.ShowRoleGrants.class, sqlQueryExecutionFactory)
                        .put(io.trino.sql.tree.ShowStats.class, sqlQueryExecutionFactory)
                        .put(io.trino.sql.tree.DescribeInput.class, sqlQueryExecutionFactory)
                        .put(io.trino.sql.tree.DescribeOutput.class, sqlQueryExecutionFactory)
                        .put(io.trino.sql.tree.Explain.class, sqlQueryExecutionFactory);
        for (Class<? extends Statement> ddlStatement : ddlTasks.keySet()) {
            executionFactoriesBuilder.put(ddlStatement, dataDefinitionExecutionFactory);
        }
        Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories = executionFactoriesBuilder.buildOrThrow();

        // LocalDispatchQueryFactory
        LocalDispatchQueryFactory localDispatchQueryFactory = new LocalDispatchQueryFactory(
                sqlQueryManager,
                queryManagerConfig,
                transactionManager,
                accessControlManager,
                metadataManager,
                queryMonitor,
                locationFactory,
                executionFactories,
                () -> WarningCollector.NOOP,
                clusterSizeMonitor,
                dispatchExecutor,
                featuresConfig,
                nodeVersion);

        // FailedDispatchQueryFactory
        FailedDispatchQueryFactory failedDispatchQueryFactory = new FailedDispatchQueryFactory(
                queryMonitor, locationFactory, dispatchExecutor, nodeVersion);

        // SessionSupplier & SessionPropertyDefaults
        SqlEnvironmentConfig sqlEnvironmentConfig = new SqlEnvironmentConfig();
        QuerySessionSupplier sessionSupplier = new QuerySessionSupplier(
                metadataManager, accessControlManager, sessionPropertyManager, sqlEnvironmentConfig);
        this.sessionSupplier = sessionSupplier;
        SessionPropertyDefaults sessionPropertyDefaults = new SessionPropertyDefaults(nodeInfo, accessControlManager);

        // QueryIdGenerator
        QueryIdGenerator queryIdGenerator = new QueryIdGenerator();

        // DispatchManager
        this.dispatchManager = new DispatchManager(
                queryIdGenerator,
                queryPreparer,
                resourceGroupManager,
                localDispatchQueryFactory,
                failedDispatchQueryFactory,
                accessControlManager,
                sessionSupplier,
                sessionPropertyDefaults,
                sessionPropertyManager,
                tracer,
                queryManagerConfig,
                dispatchExecutor,
                queryMonitor);
        dispatchManager.start();

        // Executors for Query.waitForResults()
        this.queryResultsExecutor = java.util.concurrent.Executors.newCachedThreadPool(
                io.airlift.concurrent.Threads.daemonThreadsNamed("query-results-%s"));
        this.queryResultsTimeoutExecutor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
                io.airlift.concurrent.Threads.daemonThreadsNamed("query-results-timeout-%s"));

        // PPL translator
        this.pplTranslator = new PplTranslator(metadata, transactionManager, accessControl);
    }

    // ── Public Getters ──

    public SqlParser getSqlParser() { return sqlParser; }

    public PlannerContext getPlannerContext() { return plannerContext; }

    public StatementAnalyzerFactory getStatementAnalyzerFactory() { return statementAnalyzerFactory; }

    public SqlTaskManager getSqlTaskManager() { return sqlTaskManager; }

    public Metadata getMetadata() { return metadata; }

    public TransactionManager getTransactionManager() { return transactionManager; }

    public AccessControl getAccessControl() { return accessControl; }

    public SessionPropertyManager getSessionPropertyManager() { return sessionPropertyManager; }

    public SplitManager getSplitManager() { return splitManager; }

    public NodePartitioningManager getNodePartitioningManager() { return nodePartitioningManager; }

    public PageSourceManager getPageSourceManager() { return pageSourceManager; }

    public PageSinkManager getPageSinkManager() { return pageSinkManager; }

    public FunctionManager getFunctionManager() { return functionManager; }

    public TypeManager getTypeManager() { return typeManager; }

    public BlockEncodingSerde getBlockEncodingSerde() { return blockEncodingSerde; }

    public LanguageFunctionManager getLanguageFunctionManager() { return languageFunctionManager; }

    public DispatchManager getDispatchManager() { return dispatchManager; }

    public SqlQueryManager getSqlQueryManager() { return sqlQueryManager; }

    public JsonCodec<TaskUpdateRequest> getTaskUpdateRequestCodec() { return taskUpdateRequestCodec; }

    public JsonCodec<TaskInfo> getTaskInfoCodec() { return taskInfoCodec; }

    public JsonCodec<TaskStatus> getTaskStatusCodec() { return taskStatusCodec; }

    public JsonCodec<FailTaskRequest> getFailTaskRequestCodec() { return failTaskRequestCodec; }

    public JsonCodec<VersionedDynamicFilterDomains> getDynamicFilterDomainsCodec() { return dynamicFilterDomainsCodec; }

    public DirectExchangeClientSupplier getDirectExchangeClientSupplier() { return directExchangeClientSupplier; }

    public ExchangeManagerRegistry getExchangeManagerRegistry() { return exchangeManagerRegistry; }

    public java.util.concurrent.ExecutorService getQueryResultsExecutor() { return queryResultsExecutor; }

    public java.util.concurrent.ScheduledExecutorService getQueryResultsTimeoutExecutor() { return queryResultsTimeoutExecutor; }

    public PplTranslator getPplTranslator() { return pplTranslator; }

    public QuerySessionSupplier getSessionSupplier() { return sessionSupplier; }

    @Override
    public void close() throws IOException {
        try {
            dispatchManager.stop();
        }
        catch (Exception e) {
            // log and continue shutdown
        }
        try {
            sqlQueryManager.stop();
        }
        catch (Exception e) {
            // log and continue shutdown
        }
        try {
            clusterSizeMonitor.stop();
        }
        catch (Exception e) {
            // log and continue shutdown
        }
        try {
            dispatchExecutor.shutdown();
        }
        catch (Exception e) {
            // log and continue shutdown
        }
        try {
            sqlTaskManager.close();
        }
        catch (Exception e) {
            // log and continue shutdown
        }
        taskExecutor.stop();
        taskManagementExecutor.close();
        finalizerService.destroy();
        if (gcMonitor instanceof JmxGcMonitor) {
            ((JmxGcMonitor) gcMonitor).stop();
        }
        if (exchangeHttpClient != null) {
            exchangeHttpClient.close();
        }
    }
}
