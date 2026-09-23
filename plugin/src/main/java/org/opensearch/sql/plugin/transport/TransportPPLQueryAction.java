/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.opensearch.rest.BaseRestHandler.MULTI_ALLOW_EXPLICIT_INDEX;
import static org.opensearch.sql.executor.ExecutionEngine.ExplainResponse.normalizeLf;
import static org.opensearch.sql.lang.PPLLangSpec.PPL_SPEC;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Guice;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.executor.AnalyzeResponse;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ProgressiveQueryExecution;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.monitor.profile.ProfileScope;
import org.opensearch.sql.monitor.profile.QueryProfiling;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.sql.opensearch.executor.tracing.TracingPhaseListener;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.plugin.config.EngineExtensionsHolder;
import org.opensearch.sql.plugin.config.OpenSearchPluginModule;
import org.opensearch.sql.plugin.rest.AnalyticsEngineFormatSupport;
import org.opensearch.sql.plugin.rest.AnalyticsExecutorHolder;
import org.opensearch.sql.plugin.rest.RestUnifiedQueryAction;
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.CsvResponseFormatter;
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.RawResponseFormatter;
import org.opensearch.sql.protocol.response.format.ResponseFormatter;
import org.opensearch.sql.protocol.response.format.SimpleJsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.VisualizationResponseFormatter;
import org.opensearch.sql.protocol.response.format.YamlResponseFormatter;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanCreationContext;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.attributes.Attributes;
import org.opensearch.telemetry.tracing.listener.TraceableActionListener;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

/** Send PPL query transport action. */
public class TransportPPLQueryAction
    extends HandledTransportAction<ActionRequest, TransportPPLQueryResponse> {

  private static final Logger LOG = LogManager.getLogger(TransportPPLQueryAction.class);

  private final Injector injector;

  private final Tracer tracer;

  private final Supplier<Boolean> pplEnabled;

  /** Null when analytics-engine plugin is absent; set via {@link #setQueryPlanExecutor}. */
  private volatile RestUnifiedQueryAction unifiedQueryHandler;

  private final NodeClient clientRef;
  private final ClusterService clusterServiceRef;
  private final org.opensearch.sql.common.setting.Settings pluginSettingsRef;
  private final PPLAsyncQueryService asyncQueryService;
  private final PPLAsyncQueryResponseFormatter asyncResponseFormatter;
  private final TaskManager taskManager;

  @Inject
  public TransportPPLQueryAction(
      TransportService transportService,
      ActionFilters actionFilters,
      NodeClient client,
      ClusterService clusterService,
      DataSourceServiceImpl dataSourceService,
      org.opensearch.common.settings.Settings clusterSettings,
      EngineExtensionsHolder extensionsHolder,
      Tracer tracer,
      PPLAsyncQueryService asyncQueryService) {
    super(PPLQueryAction.NAME, transportService, actionFilters, TransportPPLQueryRequest::new);
    this.clientRef = client;
    this.clusterServiceRef = clusterService;
    this.asyncQueryService = asyncQueryService;
    this.asyncResponseFormatter = new PPLAsyncQueryResponseFormatter();
    this.taskManager = transportService.getTaskManager();
    this.asyncQueryService.attachTaskManager(taskManager);

    ModulesBuilder modules = new ModulesBuilder();
    modules.add(new OpenSearchPluginModule(extensionsHolder.engines(), tracer));
    org.opensearch.sql.common.setting.Settings pluginSettings =
        new OpenSearchSettings(clusterService.getClusterSettings());
    this.pluginSettingsRef = pluginSettings;
    modules.add(
        b -> {
          b.bind(NodeClient.class).toInstance(client);
          b.bind(org.opensearch.sql.common.setting.Settings.class).toInstance(pluginSettings);
          b.bind(DataSourceService.class).toInstance(dataSourceService);
        });
    this.injector = Guice.createInjector(modules);
    this.tracer = tracer;
    ProfileScope.installListener(new TracingPhaseListener(tracer));
    this.pplEnabled =
        () ->
            MULTI_ALLOW_EXPLICIT_INDEX.get(clusterSettings)
                && (Boolean)
                    injector
                        .getInstance(org.opensearch.sql.common.setting.Settings.class)
                        .getSettingValue(Settings.Key.PPL_ENABLED);
  }

  /** Invoked by Guice iff analytics-engine bound {@code QueryPlanExecutor}. */
  @Inject(optional = true)
  public void setQueryPlanExecutor(
      QueryPlanExecutor<RelNode, Iterable<Object[]>> queryPlanExecutor) {
    AnalyticsExecutorHolder.set(queryPlanExecutor);
    // Build the SQL router once both bridges are populated (engine context might arrive
    // first or last depending on Guice ordering). buildUnifiedQueryHandler is idempotent.
    buildUnifiedQueryHandlerIfReady();
  }

  /** Invoked by Guice iff analytics-engine bound {@code EngineContextProvider}. */
  @Inject(optional = true)
  public void setEngineContext(org.opensearch.analytics.EngineContextProvider contextProvider) {
    org.opensearch.sql.plugin.rest.EngineContextProviderHolder.set(contextProvider);
    buildUnifiedQueryHandlerIfReady();
  }

  private void buildUnifiedQueryHandlerIfReady() {
    QueryPlanExecutor<RelNode, Iterable<Object[]>> executor = AnalyticsExecutorHolder.get();
    org.opensearch.analytics.EngineContextProvider contextProvider =
        org.opensearch.sql.plugin.rest.EngineContextProviderHolder.get();
    if (executor != null && contextProvider != null) {
      this.unifiedQueryHandler =
          new RestUnifiedQueryAction(
              clientRef,
              clusterServiceRef,
              executor,
              contextProvider,
              pluginSettingsRef,
              new org.opensearch.sql.opensearch.executor.ThreadPoolExecutionDispatcher(
                  clientRef.threadPool(), pluginSettingsRef));
    }
  }

  /**
   * {@inheritDoc} Transform the request and call super.doExecute() to support call from other
   * plugins.
   */
  @Override
  protected void doExecute(
      Task task, ActionRequest request, ActionListener<TransportPPLQueryResponse> listener) {
    if (!pplEnabled.get()) {
      listener.onFailure(
          new IllegalAccessException(
              "Either plugins.ppl.enabled or rest.action.multi.allow_explicit_index setting is"
                  + " false"));
      return;
    }

    TransportPPLQueryRequest transportRequest = TransportPPLQueryRequest.fromActionRequest(request);
    if (transportRequest.isGrammarRequest()) {
      // Authorization is enforced by this transport action before returning grammar metadata in
      // REST.
      listener.onResponse(new TransportPPLQueryResponse("{}"));
      return;
    }

    if (task instanceof PPLQueryTask pplQueryTask) {
      OpenSearchQueryManager.setCancellableTask(pplQueryTask);
    }
    Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_TOTAL).increment();
    Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_COUNT_TOTAL).increment();

    QueryContext.addRequestId();

    // in order to use PPL service, we need to convert TransportPPLQueryRequest to PPLQueryRequest
    PPLQueryRequest transformedRequest = transportRequest.toPPLQueryRequest();
    QueryContext.setProfile(transformedRequest.profile());
    // Only the JSON shape carries warnings; gate partial results on it so CSV/RAW/VIZ never drop
    // data silently. Carried on the request (not Log4j ThreadContext) so it survives the
    // transport→worker handoff, which the security plugin's interceptor does not preserve.
    transformedRequest.warningsSupported(warningsSupported(transformedRequest));
    boolean asyncRequest = transportRequest.isAsyncQueryRequest();
    // The per-request partial-result override (e.g. a Dashboards toggle) rides on the request →
    // plan → worker thread (see PPLService/QueryPlan), not Log4j ThreadContext, for the same
    // handoff-survival reason as warningsSupported. null defers to the cluster setting.

    // Start root span with OTel DB semantic convention attributes
    Span rootSpan =
        tracer.startSpan(
            SpanCreationContext.client()
                .name("opensearch.query")
                .attributes(
                    Attributes.create()
                        .addAttribute("db.system.name", "opensearch")
                        .addAttribute("db.query.type", "ppl")
                        .addAttribute("db.query.id", QueryContext.getRequestId())
                        .addAttribute(
                            "db.operation.name",
                            transformedRequest.isExplainRequest() ? "EXPLAIN" : "EXECUTE")));

    // Put span in scope so ThreadContext propagation captures it
    SpanScope spanScope = tracer.withSpanInScope(rootSpan);

    // Trace wrapper: ends span in async callback, sets error on failure.
    ActionListener<TransportPPLQueryResponse> tracedListener =
        TraceableActionListener.create(listener, rootSpan, tracer);
    ActionListener<TransportPPLQueryResponse> clearingListener =
        wrapWithProfilingClear(tracedListener);

    try {
      if (asyncRequest) {
        validateAsyncRequest(transformedRequest);
      }

      // Route to analytics engine for non-Lucene (e.g., Parquet-backed) indices.
      if (unifiedQueryHandler != null
          && unifiedQueryHandler.isAnalyticsIndex(transformedRequest.getRequest(), QueryType.PPL)) {
        if (asyncRequest) {
          clearingListener.onFailure(
              new IllegalArgumentException(
                  "Asynchronous PPL execution supports the Calcite execution path only"));
          return;
        }
        LOG.info("[{}] Routing PPL query to analytics engine", QueryContext.getRequestId());
        // Pass this PPL task so the analytics engine links its query task to it for cancellation.
        if (transformedRequest.isExplainRequest()) {
          unifiedQueryHandler.explain(
              transformedRequest.getRequest(),
              QueryType.PPL,
              transformedRequest.mode(),
              task,
              createExplainResponseListener(transformedRequest, clearingListener));
        } else {
          // Analytics route only emits JSON; reject unsupported formats (e.g. csv) with a 4xx.
          try {
            AnalyticsEngineFormatSupport.validateFormat(format(transformedRequest));
          } catch (Exception e) {
            clearingListener.onFailure(e);
            return;
          }
          unifiedQueryHandler.execute(
              transformedRequest.getRequest(),
              QueryType.PPL,
              transformedRequest.profile(),
              transformedRequest.getFetchSize(),
              task,
              clearingListener);
        }
        return;
      }

      Consumer<String> anonymizedQuerySink =
          anonymized -> rootSpan.addAttribute("db.query.text", anonymized);
      PPLService pplService = injector.getInstance(PPLService.class);
      if (transformedRequest.isExplainRequest()) {
        pplService.explain(
            transformedRequest,
            createExplainResponseListener(transformedRequest, clearingListener),
            anonymizedQuerySink);
      } else if (transformedRequest.analyze()) {
        pplService.analyze(
            transformedRequest,
            createAnalyzeResponseListener(transformedRequest, clearingListener),
            anonymizedQuerySink);
      } else {
        if (asyncRequest) {
          startAsyncQuery(
              transportRequest,
              transformedRequest,
              pplService,
              clearingListener,
              anonymizedQuerySink);
        } else {
          pplService.execute(
              transformedRequest,
              createListener(transformedRequest, clearingListener),
              createExplainResponseListener(transformedRequest, clearingListener),
              anonymizedQuerySink);
        }
      }
    } catch (Exception e) {
      clearingListener.onFailure(e);
    } finally {
      spanScope.close();
    }
  }

  private ResponseListener<AnalyzeResponse> createAnalyzeResponseListener(
      PPLQueryRequest request, ActionListener<TransportPPLQueryResponse> listener) {
    return new ResponseListener<AnalyzeResponse>() {
      @Override
      public void onResponse(AnalyzeResponse response) {
        JsonResponseFormatter<AnalyzeResponse> formatter =
            new JsonResponseFormatter<>(PRETTY) {
              @Override
              protected Object buildJsonObject(AnalyzeResponse response) {
                return response;
              }
            };
        listener.onResponse(
            new TransportPPLQueryResponse(formatter.format(response), formatter.contentType()));
      }

      @Override
      public void onFailure(Exception e) {
        listener.onFailure(e);
      }
    };
  }

  /**
   * TODO: need to extract an interface for both SQL and PPL action handler and move these common
   * methods to the interface. This is not easy to do now because SQL action handler is still in
   * legacy module.
   */
  private ResponseListener<ExecutionEngine.ExplainResponse> createExplainResponseListener(
      PPLQueryRequest request, ActionListener<TransportPPLQueryResponse> listener) {
    return new ResponseListener<ExecutionEngine.ExplainResponse>() {
      @Override
      public void onResponse(ExecutionEngine.ExplainResponse response) {
        Optional<Format> isYamlFormat =
            Format.ofExplain(request.getFormat()).filter(format -> format.equals(Format.YAML));
        ResponseFormatter<ExecutionEngine.ExplainResponse> formatter;
        if (isYamlFormat.isPresent()) {
          formatter =
              new YamlResponseFormatter<>() {
                @Override
                protected Object buildYamlObject(ExecutionEngine.ExplainResponse response) {
                  return normalizeLf(response);
                }
              };
        } else {
          formatter =
              new JsonResponseFormatter<>(PRETTY) {
                @Override
                protected Object buildJsonObject(ExecutionEngine.ExplainResponse response) {
                  // For json_tree format, use parsed tree objects instead of strings
                  if (response.getCalcite() != null
                      && response.getCalcite().getLogicalTree() != null) {
                    Map<String, Object> result = new LinkedHashMap<>();
                    Map<String, Object> calcite = new LinkedHashMap<>();
                    calcite.put("logical", response.getCalcite().getLogicalTree());
                    if (response.getCalcite().getPhysicalTree() != null) {
                      calcite.put("physical", response.getCalcite().getPhysicalTree());
                    }
                    result.put("calcite", calcite);
                    return result;
                  }
                  return response;
                }
              };
        }
        listener.onResponse(
            new TransportPPLQueryResponse(formatter.format(response), formatter.contentType()));
      }

      @Override
      public void onFailure(Exception e) {
        listener.onFailure(e);
      }
    };
  }

  private ResponseListener<ExecutionEngine.QueryResponse> createListener(
      PPLQueryRequest pplRequest, ActionListener<TransportPPLQueryResponse> listener) {
    Format format = format(pplRequest);
    ResponseFormatter<QueryResult> formatter;
    if (format.equals(Format.CSV)) {
      formatter = new CsvResponseFormatter(pplRequest.sanitize());
    } else if (format.equals(Format.RAW)) {
      formatter = new RawResponseFormatter();
    } else if (format.equals(Format.VIZ)) {
      formatter = new VisualizationResponseFormatter(pplRequest.style());
    } else {
      formatter = new SimpleJsonResponseFormatter(JsonResponseFormatter.Style.PRETTY);
    }

    return new ResponseListener<ExecutionEngine.QueryResponse>() {
      @Override
      public void onResponse(ExecutionEngine.QueryResponse response) {
        String responseContent =
            formatter.format(
                new QueryResult(
                    response.getSchema(),
                    response.getResults(),
                    response.getCursor(),
                    PPL_SPEC,
                    response.getWarnings()));
        listener.onResponse(new TransportPPLQueryResponse(responseContent));
      }

      @Override
      public void onFailure(Exception e) {
        listener.onFailure(e);
      }
    };
  }

  private void startAsyncQuery(
      TransportPPLQueryRequest transportRequest,
      PPLQueryRequest request,
      PPLService pplService,
      ActionListener<TransportPPLQueryResponse> submitListener,
      Consumer<String> anonymizedQuerySink) {
    TimeValue keepAlive = requestedKeepAlive(request);
    TimeValue waitForCompletion = requestedWaitForCompletion(request);
    asyncQueryService.validateKeepAlive(keepAlive);
    asyncQueryService.validateWaitForCompletion(waitForCompletion);

    PPLAsyncQueryUser owner = PPLAsyncQueryUser.current(clientRef.threadPool().getThreadContext());
    RegisteredAsyncTask registeredTask = registerAsyncTask(transportRequest);
    String jobId;
    try {
      jobId = asyncQueryService.create(owner, keepAlive, registeredTask.task());
    } catch (RuntimeException e) {
      registeredTask.close();
      throw e;
    }
    boolean ready =
        asyncQueryService.awaitSubmit(
            jobId,
            waitForCompletion,
            ActionListener.wrap(
                snapshot -> submitListener.onResponse(asyncResponseFormatter.format(snapshot)),
                submitListener::onFailure));
    if (!ready) {
      registeredTask.close();
      return;
    }

    OpenSearchQueryManager.setCancellableTask(registeredTask.task());
    try {
      ProgressiveQueryExecution execution =
          pplService.executeProgressively(
              request, createAsyncExplainListener(jobId, registeredTask), anonymizedQuerySink);
      asyncQueryService.attachExecution(jobId, execution);
      execution
          .completion()
          .whenComplete(
              (ignored, failure) -> {
                try {
                  if (failure == null) {
                    asyncQueryService.complete(jobId);
                  } else {
                    asyncQueryService.fail(jobId, asException(failure));
                  }
                } finally {
                  registeredTask.close();
                  clearRequestScopedState();
                }
              });
    } catch (Exception e) {
      try {
        asyncQueryService.fail(jobId, e);
      } finally {
        registeredTask.close();
        clearRequestScopedState();
      }
    } finally {
      OpenSearchQueryManager.clearCancellableTask();
    }
  }

  private RegisteredAsyncTask registerAsyncTask(TransportPPLQueryRequest request) {
    Task registered = taskManager.register("transport", PPLQueryAction.NAME, request);
    if (!(registered instanceof PPLQueryTask pplQueryTask)) {
      taskManager.unregister(registered);
      throw new IllegalStateException("Failed to create PPL asynchronous query task");
    }
    return new RegisteredAsyncTask(taskManager, pplQueryTask);
  }

  private static Exception asException(Throwable failure) {
    Throwable cause =
        failure instanceof java.util.concurrent.CompletionException && failure.getCause() != null
            ? failure.getCause()
            : failure;
    return cause instanceof Exception exception ? exception : new RuntimeException(cause);
  }

  private ResponseListener<ExecutionEngine.ExplainResponse> createAsyncExplainListener(
      String jobId, RegisteredAsyncTask registeredTask) {
    return new ResponseListener<>() {
      @Override
      public void onResponse(ExecutionEngine.ExplainResponse response) {
        try {
          asyncQueryService.fail(
              jobId,
              new IllegalArgumentException(
                  "Asynchronous PPL execution does not support explain queries"));
        } finally {
          registeredTask.close();
          clearRequestScopedState();
        }
      }

      @Override
      public void onFailure(Exception e) {
        try {
          asyncQueryService.fail(jobId, e);
        } finally {
          registeredTask.close();
          clearRequestScopedState();
        }
      }
    };
  }

  private static TimeValue requestedKeepAlive(PPLQueryRequest request) {
    if (request.getJsonContent() == null || !request.getJsonContent().has("keep_alive")) {
      return PPLAsyncQueryService.DEFAULT_KEEP_ALIVE;
    }
    return TimeValue.parseTimeValue(request.getJsonContent().getString("keep_alive"), "keep_alive");
  }

  private static TimeValue requestedWaitForCompletion(PPLQueryRequest request) {
    if (request.getJsonContent() == null
        || !request.getJsonContent().has("wait_for_completion_timeout")) {
      return PPLAsyncQueryService.DEFAULT_WAIT_FOR_COMPLETION;
    }
    return TimeValue.parseTimeValue(
        request.getJsonContent().getString("wait_for_completion_timeout"),
        "wait_for_completion_timeout");
  }

  private void validateAsyncRequest(PPLQueryRequest request) {
    if (request.isExplainRequest()
        || request.profile()
        || request.analyze()
        || request.getRequest().trim().toLowerCase(Locale.ROOT).startsWith("explain")) {
      throw new IllegalArgumentException(
          "Asynchronous PPL execution supports query execution only");
    }
    if (request.getJsonContent() != null && request.getJsonContent().has("partial_result")) {
      throw new IllegalArgumentException(
          "Asynchronous PPL execution does not support partial results");
    }
    if (!format(request).equals(Format.JDBC)) {
      throw new IllegalArgumentException("Asynchronous PPL execution supports JSON responses only");
    }
    if (!(Boolean) pluginSettingsRef.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED)) {
      throw new IllegalArgumentException(
          "Asynchronous PPL execution requires the Calcite PPL engine to be enabled");
    }
  }

  private Format format(PPLQueryRequest pplRequest) {
    String format = pplRequest.getFormat();
    Optional<Format> optionalFormat = Format.of(format);
    if (optionalFormat.isPresent()) {
      return optionalFormat.get();
    } else {
      throw new IllegalArgumentException(
          String.format(Locale.ROOT, "response in %s format is not supported.", format));
    }
  }

  /**
   * Whether the requested response format carries a warnings channel. Only the JSON shape (built by
   * {@code SimpleJsonResponseFormatter} -- the fallback for anything that is not CSV/RAW/VIZ) emits
   * warnings; the others have no slot for them. Mirrors the format branching in {@link
   * #createListener}. Explain requests are excluded up front: their {@code format} is an
   * explain-only value (e.g. {@code json}/{@code yaml}) that {@link #format} cannot resolve, and an
   * explain response never carries query warnings.
   */
  private boolean warningsSupported(PPLQueryRequest pplRequest) {
    if (pplRequest.isExplainRequest()) {
      return false;
    }
    Format format = format(pplRequest);
    return !(format.equals(Format.CSV) || format.equals(Format.RAW) || format.equals(Format.VIZ));
  }

  private ActionListener<TransportPPLQueryResponse> wrapWithProfilingClear(
      ActionListener<TransportPPLQueryResponse> delegate) {
    return new ActionListener<>() {
      @Override
      public void onResponse(TransportPPLQueryResponse transportPPLQueryResponse) {
        try {
          delegate.onResponse(transportPPLQueryResponse);
        } finally {
          clearRequestScopedState();
        }
      }

      @Override
      public void onFailure(Exception e) {
        try {
          delegate.onFailure(e);
        } finally {
          clearRequestScopedState();
        }
      }
    };
  }

  /**
   * Clear the per-request state carried in {@link QueryContext}'s thread-locals. Transport threads
   * are pooled, so anything left behind is inherited by the next query to run on this thread. (The
   * partial-result override no longer lives here -- it rides on the plan to the worker thread and
   * is reset per query in {@code CalcitePlanContext}.)
   */
  private static void clearRequestScopedState() {
    QueryProfiling.clear();
  }

  private record RegisteredAsyncTask(
      TaskManager taskManager, PPLQueryTask task, AtomicBoolean closed) implements AutoCloseable {

    private RegisteredAsyncTask(TaskManager taskManager, PPLQueryTask task) {
      this(taskManager, task, new AtomicBoolean());
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        taskManager.unregister(task);
      }
    }
  }
}
