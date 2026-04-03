# PPL OpenTelemetry Tracing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add distributed tracing spans to the PPL Calcite query execution pipeline using OpenSearch's telemetry framework.

**Architecture:** SQLPlugin implements TelemetryAwarePlugin. Tracer is injected via Guice into transport action, PPLService, QueryService, and ExecutionEngine. Root span is created in TransportPPLQueryAction with async-safe listener wrapper. Phase spans are created by each component that owns a phase. Parent-child relationships propagate automatically via ThreadContext.

**Tech Stack:** OpenSearch Telemetry API (`org.opensearch.telemetry.tracing.Tracer`, `Span`, `SpanScope`), Guice DI, JUnit 5 + Mockito

**Design Spec:** `docs/plans/2026-04-03_ppl_otel_tracing_design.md`

---

## File Structure

### New Files

| File | Responsibility |
|------|---------------|
| `plugin/src/test/java/.../transport/TransportPPLQueryActionTraceTest.java` | Unit tests for root span lifecycle |
| `opensearch/src/test/java/.../executor/OpenSearchExecutionEngineAsyncErrorTest.java` | Unit tests for async-boundary hardening |
| `ppl/src/test/java/.../PPLServiceTraceTest.java` | Unit tests for parse span |
| `core/src/test/java/.../executor/QueryServiceTraceTest.java` | Unit tests for analyze span |
| `core/src/main/java/.../utils/QuerySummaryExtractor.java` | Extracts command structure for `db.query.summary` |
| `core/src/test/java/.../utils/QuerySummaryExtractorTest.java` | Tests for summary extraction |

### Modified Files

| File | Change |
|------|--------|
| `plugin/.../SQLPlugin.java` | Implement `TelemetryAwarePlugin`, pass `Tracer` to Guice |
| `plugin/.../config/OpenSearchPluginModule.java` | Bind `Tracer` in Guice module |
| `plugin/.../transport/TransportPPLQueryAction.java` | Root span creation with async listener wrapper |
| `opensearch/.../executor/OpenSearchExecutionEngine.java` | Fix async error handling + execute/materialize spans |
| `ppl/.../PPLService.java` | Parse span |
| `core/.../executor/QueryService.java` | Analyze span |
| `core/.../calcite/utils/CalciteToolsHelper.java` | Optimize + compile spans |

---

## Phase 1: TelemetryAwarePlugin + Root Span + Async Hardening

### Task 1: Fix Async Error Handling in OpenSearchExecutionEngine

The `execute(RelNode, ...)` method throws `RuntimeException` from the async lambda instead of calling `listener.onFailure()`. This must be fixed first — it's a prerequisite for safe root span lifecycle.

**Files:**
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/executor/OpenSearchExecutionEngine.java:211-229`
- Test: `opensearch/src/test/java/org/opensearch/sql/opensearch/executor/OpenSearchExecutionEngineTest.java`

- [ ] **Step 1: Write failing test for SQLException routing to listener**

Add to `OpenSearchExecutionEngineTest.java`:

```java
@Test
void execute_calcite_sqlException_routes_to_listener() {
  // Setup: mock client.schedule to run synchronously
  doAnswer(invocation -> {
    Runnable task = invocation.getArgument(0);
    task.run();
    return null;
  }).when(client).schedule(any(Runnable.class));

  CalcitePlanContext context = mock(CalcitePlanContext.class);
  RelNode rel = mock(RelNode.class);
  ResponseListener<QueryResponse> listener = mock(ResponseListener.class);

  // OpenSearchRelRunners.run() will throw since context is mocked
  engine.execute(rel, context, listener);

  // Verify listener.onFailure was called (not an uncaught RuntimeException)
  verify(listener).onFailure(any(Exception.class));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :opensearch:test --tests "org.opensearch.sql.opensearch.executor.OpenSearchExecutionEngineTest.execute_calcite_sqlException_routes_to_listener" -x spotlessCheck`
Expected: FAIL — `RuntimeException` is thrown instead of routing to `listener.onFailure()`

- [ ] **Step 3: Fix the async lambda in execute(RelNode, ...)**

In `OpenSearchExecutionEngine.java`, replace lines 211-229:

```java
@Override
public void execute(
    RelNode rel, CalcitePlanContext context, ResponseListener<QueryResponse> listener) {
  client.schedule(
      () -> {
        try (PreparedStatement statement = OpenSearchRelRunners.run(context, rel)) {
          ProfileMetric metric = QueryProfiling.current().getOrCreateMetric(MetricName.EXECUTE);
          long execTime = System.nanoTime();
          ResultSet result = statement.executeQuery();
          QueryResponse response =
              buildResultSet(result, rel.getRowType(), context.sysLimit.querySizeLimit());
          metric.add(System.nanoTime() - execTime);
          listener.onResponse(response);
        } catch (Throwable t) {
          if (t instanceof VirtualMachineError) {
            throw (VirtualMachineError) t;
          }
          Exception e = (t instanceof Exception) ? (Exception) t : new RuntimeException(t);
          listener.onFailure(e);
        }
      });
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :opensearch:test --tests "org.opensearch.sql.opensearch.executor.OpenSearchExecutionEngineTest.execute_calcite_sqlException_routes_to_listener" -x spotlessCheck`
Expected: PASS

- [ ] **Step 5: Run full opensearch module tests**

Run: `./gradlew :opensearch:test -x spotlessCheck`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add opensearch/src/main/java/org/opensearch/sql/opensearch/executor/OpenSearchExecutionEngine.java
git add opensearch/src/test/java/org/opensearch/sql/opensearch/executor/OpenSearchExecutionEngineTest.java
git commit -s -m "fix: route all async exceptions to listener.onFailure in Calcite execute path

Previously, SQLException and RuntimeException from the async lambda in
execute(RelNode, ...) were re-thrown instead of being routed to
listener.onFailure(). This bypasses the listener chain and could leak
resources. Now catches Throwable, re-throws only VirtualMachineError,
and routes everything else to listener.onFailure()."
```

---

### Task 2: Implement TelemetryAwarePlugin in SQLPlugin

**Files:**
- Modify: `plugin/src/main/java/org/opensearch/sql/plugin/SQLPlugin.java:131-319`
- Modify: `plugin/src/main/java/org/opensearch/sql/plugin/config/OpenSearchPluginModule.java`

- [ ] **Step 1: Add TelemetryAwarePlugin interface to SQLPlugin**

In `SQLPlugin.java`, add the import and interface:

```java
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.telemetry.metrics.MetricsRegistry;
```

Change class declaration (line 131-136):

```java
public class SQLPlugin extends Plugin
    implements ActionPlugin,
        ScriptPlugin,
        SystemIndexPlugin,
        JobSchedulerExtension,
        ExtensiblePlugin,
        TelemetryAwarePlugin {
```

Add tracer field after line 140:

```java
private Tracer tracer = NoopTracer.getInstance();
```

- [ ] **Step 2: Add telemetry-aware createComponents() override**

Add a new overload of `createComponents()` that accepts `Tracer` and `MetricsRegistry`. Refactor the existing `createComponents()` to delegate to a shared `createComponentsInternal()`:

```java
// Telemetry-enabled path (called when FeatureFlags.TELEMETRY is on)
@Override
public Collection<Object> createComponents(
    Client client,
    ClusterService clusterService,
    ThreadPool threadPool,
    ResourceWatcherService resourceWatcherService,
    ScriptService scriptService,
    NamedXContentRegistry contentRegistry,
    Environment environment,
    NodeEnvironment nodeEnvironment,
    NamedWriteableRegistry namedWriteableRegistry,
    IndexNameExpressionResolver indexNameResolver,
    Supplier<RepositoriesService> repositoriesServiceSupplier,
    Tracer tracer,
    MetricsRegistry metricsRegistry) {
  this.tracer = tracer;
  return createComponents(
      client, clusterService, threadPool, resourceWatcherService,
      scriptService, contentRegistry, environment, nodeEnvironment,
      namedWriteableRegistry, indexNameResolver, repositoriesServiceSupplier);
}
```

Note: The existing `createComponents()` (telemetry-disabled path) stays unchanged — `this.tracer` remains `NoopTracer`.

- [ ] **Step 3: Bind Tracer in OpenSearchPluginModule**

In `OpenSearchPluginModule.java`, add Tracer binding. First, update the constructor and add a field:

```java
import org.opensearch.telemetry.tracing.Tracer;

@RequiredArgsConstructor
public class OpenSearchPluginModule extends AbstractModule {

  private final List<ExecutionEngine> executionEngineExtensions;
  private final Tracer tracer;

  public OpenSearchPluginModule() {
    this(List.of(), NoopTracer.getInstance());
  }

  // ... existing fields ...
```

Add a `@Provides` method:

```java
@Provides
@Singleton
public Tracer tracer() {
  return tracer;
}
```

- [ ] **Step 4: Pass Tracer from SQLPlugin to OpenSearchPluginModule**

In `SQLPlugin.createComponents()` (line 274-275), update the module creation:

```java
modules.add(new OpenSearchPluginModule(executionEngineExtensions, tracer));
```

In `TransportPPLQueryAction` constructor (line 74), update similarly:

```java
modules.add(new OpenSearchPluginModule(extensionsHolder.engines(), tracer));
```

This requires `TransportPPLQueryAction` to receive `Tracer`. Since it's Guice-injected from the plugin module, we need to pass it through. The simplest approach: bind `Tracer` in the plugin-level Guice module that `TransportPPLQueryAction` uses. Update `TransportPPLQueryAction`'s constructor bindings (line 73-82):

```java
ModulesBuilder modules = new ModulesBuilder();
modules.add(new OpenSearchPluginModule(extensionsHolder.engines(), tracer));
modules.add(
    b -> {
      b.bind(NodeClient.class).toInstance(client);
      b.bind(org.opensearch.sql.common.setting.Settings.class)
          .toInstance(new OpenSearchSettings(clusterService.getClusterSettings()));
      b.bind(DataSourceService.class).toInstance(dataSourceService);
    });
```

Note: `TransportPPLQueryAction` needs to accept `Tracer` as a constructor parameter. Since it's instantiated by OpenSearch's transport action framework, the `Tracer` must come from the Guice bindings available to it. Check how the `EngineExtensionsHolder` is bound and follow the same pattern for `Tracer` — it should be part of `createComponents()` return list or bound in the plugin module.

- [ ] **Step 5: Verify build compiles**

Run: `./gradlew :plugin:compileJava :opensearch:compileJava :core:compileJava :ppl:compileJava -x spotlessCheck`
Expected: BUILD SUCCESSFUL

- [ ] **Step 6: Run plugin tests**

Run: `./gradlew :plugin:test -x spotlessCheck`
Expected: All tests PASS (NoopTracer used by default)

- [ ] **Step 7: Commit**

```bash
git add plugin/src/main/java/org/opensearch/sql/plugin/SQLPlugin.java
git add plugin/src/main/java/org/opensearch/sql/plugin/config/OpenSearchPluginModule.java
git add plugin/src/main/java/org/opensearch/sql/plugin/transport/TransportPPLQueryAction.java
git commit -s -m "feat: implement TelemetryAwarePlugin and bind Tracer via Guice

SQLPlugin now implements TelemetryAwarePlugin. When the telemetry
feature flag is on, a real Tracer is injected; when off, NoopTracer
is used. Tracer is bound in OpenSearchPluginModule and available
for injection in all components."
```

---

### Task 3: Add Root Span with Async-Safe Listener Wrapper

**Files:**
- Modify: `plugin/src/main/java/org/opensearch/sql/plugin/transport/TransportPPLQueryAction.java:62-138`
- Create: `plugin/src/test/java/org/opensearch/sql/plugin/transport/TransportPPLQueryActionTraceTest.java`

- [ ] **Step 1: Write failing test for root span creation**

Create `TransportPPLQueryActionTraceTest.java`:

```java
package org.opensearch.sql.plugin.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.core.action.ActionListener;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;

@ExtendWith(MockitoExtension.class)
class TransportPPLQueryActionTraceTest {

  @Mock private Tracer tracer;
  @Mock private Span rootSpan;
  @Mock private SpanScope spanScope;

  @BeforeEach
  void setup() {
    when(tracer.startSpan(any())).thenReturn(rootSpan);
    when(tracer.withSpanInScope(rootSpan)).thenReturn(spanScope);
  }

  @Test
  void root_span_ends_on_successful_response() {
    // Verify that rootSpan.endSpan() is called when listener.onResponse fires
    // and spanScope.close() is called on the transport thread (not in the callback)
    // This test will be refined during implementation to match the actual Guice setup
    verify(rootSpan, never()).endSpan(); // placeholder assertion
  }
}
```

Note: The actual test setup will depend on how Guice wiring is configured. The engineer should create a focused test that mocks the Tracer and verifies span lifecycle. Use the existing `PPLServiceTest` and `TransportPPLQueryRequestTest` patterns for reference.

- [ ] **Step 2: Add root span and listener wrapper to doExecute()**

In `TransportPPLQueryAction.java`, add imports:

```java
import java.util.concurrent.atomic.AtomicBoolean;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanCreationContext;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.attributes.Attributes;
```

Add `Tracer` field and inject via constructor:

```java
private final Tracer tracer;

@Inject
public TransportPPLQueryAction(
    TransportService transportService,
    ActionFilters actionFilters,
    NodeClient client,
    ClusterService clusterService,
    DataSourceServiceImpl dataSourceService,
    org.opensearch.common.settings.Settings clusterSettings,
    EngineExtensionsHolder extensionsHolder,
    Tracer tracer) {
  // ... existing code ...
  this.tracer = tracer;
}
```

Replace `doExecute()` body (lines 97-138):

```java
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
    listener.onResponse(new TransportPPLQueryResponse("{}"));
    return;
  }

  if (task instanceof PPLQueryTask pplQueryTask) {
    OpenSearchQueryManager.setCancellableTask(pplQueryTask);
  }
  Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_TOTAL).increment();
  Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_COUNT_TOTAL).increment();

  QueryContext.addRequestId();

  PPLQueryRequest transformedRequest = transportRequest.toPPLQueryRequest();
  QueryContext.setProfile(transformedRequest.profile());

  // Start root span
  Span rootSpan = tracer.startSpan(
      SpanCreationContext.client().name("opensearch.query")
          .attributes(Attributes.create()
              .addAttribute("db.system.name", "opensearch")
              .addAttribute("db.query.type", "ppl")
              .addAttribute("db.query.id", QueryContext.getRequestId())
              .addAttribute("db.operation.name",
                  transformedRequest.isExplainRequest() ? "EXPLAIN" : "EXECUTE")));

  // Put span in scope for ThreadContext propagation
  SpanScope spanScope = tracer.withSpanInScope(rootSpan);

  try {
    // Wrap listener — only Span ends in async callback (not SpanScope)
    ActionListener<TransportPPLQueryResponse> tracedListener = new ActionListener<>() {
      private final AtomicBoolean ended = new AtomicBoolean(false);

      @Override
      public void onResponse(TransportPPLQueryResponse response) {
        try {
          listener.onResponse(response);
        } finally {
          endSpan();
        }
      }

      @Override
      public void onFailure(Exception e) {
        try {
          rootSpan.setError(e);
          listener.onFailure(e);
        } finally {
          endSpan();
        }
      }

      private void endSpan() {
        if (ended.compareAndSet(false, true)) {
          rootSpan.endSpan();
        }
      }
    };

    ActionListener<TransportPPLQueryResponse> clearingListener =
        wrapWithProfilingClear(tracedListener);

    PPLService pplService = injector.getInstance(PPLService.class);
    if (transformedRequest.isExplainRequest()) {
      pplService.explain(
          transformedRequest,
          createExplainResponseListener(transformedRequest, clearingListener));
    } else {
      pplService.execute(
          transformedRequest,
          createListener(transformedRequest, clearingListener),
          createExplainResponseListener(transformedRequest, clearingListener));
    }
  } catch (Exception e) {
    rootSpan.setError(e);
    rootSpan.endSpan();
    listener.onFailure(e);
  } finally {
    // Close scope on transport thread — span context already captured by withCurrentContext()
    spanScope.close();
  }
}
```

- [ ] **Step 3: Write comprehensive tests for span lifecycle**

Update `TransportPPLQueryActionTraceTest.java` with tests covering:
- Root span created with correct attributes on execute request
- Root span created with `db.operation.name=EXPLAIN` on explain request
- `spanScope.close()` called on transport thread (in finally block)
- `rootSpan.endSpan()` called exactly once on successful response
- `rootSpan.setError()` + `rootSpan.endSpan()` on failure
- Span not created for grammar requests or when PPL is disabled
- `NoopTracer` path works without errors

- [ ] **Step 4: Run tests**

Run: `./gradlew :plugin:test -x spotlessCheck`
Expected: All tests PASS

- [ ] **Step 5: Run spotless**

Run: `./gradlew spotlessApply`

- [ ] **Step 6: Commit**

```bash
git add plugin/src/main/java/org/opensearch/sql/plugin/transport/TransportPPLQueryAction.java
git add plugin/src/test/java/org/opensearch/sql/plugin/transport/TransportPPLQueryActionTraceTest.java
git commit -s -m "feat: add root opensearch.query span with async-safe listener wrapper

Creates the root CLIENT span in TransportPPLQueryAction.doExecute().
SpanScope is opened on the transport thread and closed in the finally
block after execute()/explain() returns. The Span itself is ended in
the async listener callback via AtomicBoolean guard. Includes
db.system.name, db.query.type, db.query.id, and db.operation.name
attributes following OTel DB semantic conventions."
```

---

## Phase 2: Phase-Level Child Spans

### Task 4: Add Parse Span to PPLService

**Files:**
- Modify: `ppl/src/main/java/org/opensearch/sql/ppl/PPLService.java:30-112`
- Modify: `plugin/src/main/java/org/opensearch/sql/plugin/config/OpenSearchPluginModule.java:103-106`
- Create: `ppl/src/test/java/org/opensearch/sql/ppl/PPLServiceTraceTest.java`

- [ ] **Step 1: Write failing test for parse span**

Create `PPLServiceTraceTest.java`:

```java
package org.opensearch.sql.ppl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;

@ExtendWith(MockitoExtension.class)
class PPLServiceTraceTest {

  @Mock private Tracer tracer;
  @Mock private Span parseSpan;
  @Mock private SpanScope spanScope;
  @Mock private QueryManager queryManager;
  @Mock private QueryPlanFactory queryPlanFactory;
  @Mock private Settings settings;

  @Test
  void execute_creates_parse_span() {
    when(tracer.startSpan(any())).thenReturn(parseSpan);
    when(tracer.withSpanInScope(parseSpan)).thenReturn(spanScope);

    PPLService pplService = new PPLService(
        new PPLSyntaxParser(), queryManager, queryPlanFactory, settings, tracer);

    // Execute with a valid query — parse span should be created and ended
    // (will need mock setup for queryManager.submit and queryPlanFactory.create)
    // Verify:
    verify(tracer).startSpan(argThat(ctx ->
        ctx.getSpanName().equals("opensearch.query.parse")));
    verify(parseSpan).endSpan();
    verify(spanScope).close();
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :ppl:test --tests "org.opensearch.sql.ppl.PPLServiceTraceTest" -x spotlessCheck`
Expected: FAIL — PPLService constructor doesn't accept Tracer yet

- [ ] **Step 3: Add Tracer to PPLService**

In `PPLService.java`, add `Tracer` parameter:

```java
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanCreationContext;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;

public class PPLService {
  private final PPLSyntaxParser parser;
  private final QueryManager queryManager;
  private final QueryPlanFactory queryExecutionFactory;
  private final Settings settings;
  private final QueryType PPL_QUERY = QueryType.PPL;
  private final PPLQueryDataAnonymizer anonymizer;
  private final Tracer tracer;

  public PPLService(
      PPLSyntaxParser parser,
      QueryManager queryManager,
      QueryPlanFactory queryExecutionFactory,
      Settings settings,
      Tracer tracer) {
    this.parser = parser;
    this.queryManager = queryManager;
    this.queryExecutionFactory = queryExecutionFactory;
    this.settings = settings;
    this.anonymizer = new PPLQueryDataAnonymizer(settings);
    this.tracer = tracer;
  }
```

Wrap the `plan()` method body with a parse span:

```java
private AbstractPlan plan(
    PPLQueryRequest request,
    ResponseListener<QueryResponse> queryListener,
    ResponseListener<ExplainResponse> explainListener) {
  Span parseSpan = tracer.startSpan(
      SpanCreationContext.internal().name("opensearch.query.parse"));
  try (SpanScope scope = tracer.withSpanInScope(parseSpan)) {
    ParseTree cst = parser.parse(request.getRequest());
    Statement statement =
        cst.accept(
            new AstStatementBuilder(
                new AstBuilder(request.getRequest(), settings),
                AstStatementBuilder.StatementBuilderContext.builder()
                    .isExplain(request.isExplainRequest())
                    .fetchSize(request.getFetchSize())
                    .highlightConfig(request.getHighlightConfig())
                    .format(request.getFormat())
                    .explainMode(request.getExplainMode())
                    .build()));

    log.info(
        "[{}] Incoming request {}",
        QueryContext.getRequestId(),
        anonymizer.anonymizeStatement(statement));

    return queryExecutionFactory.create(statement, queryListener, explainListener);
  } catch (Exception e) {
    parseSpan.setError(e);
    throw e;
  } finally {
    parseSpan.endSpan();
  }
}
```

- [ ] **Step 4: Update OpenSearchPluginModule to pass Tracer to PPLService**

In `OpenSearchPluginModule.java`, update the `pplService` provider:

```java
@Provides
public PPLService pplService(
    QueryManager queryManager, QueryPlanFactory queryPlanFactory,
    Settings settings, Tracer tracer) {
  return new PPLService(new PPLSyntaxParser(), queryManager, queryPlanFactory, settings, tracer);
}
```

- [ ] **Step 5: Run tests**

Run: `./gradlew :ppl:test :plugin:test -x spotlessCheck`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add ppl/src/main/java/org/opensearch/sql/ppl/PPLService.java
git add ppl/src/test/java/org/opensearch/sql/ppl/PPLServiceTraceTest.java
git add plugin/src/main/java/org/opensearch/sql/plugin/config/OpenSearchPluginModule.java
git commit -s -m "feat: add opensearch.query.parse span to PPLService

Wraps parsing (PPLSyntaxParser + AstStatementBuilder) in an INTERNAL
span. Tracer is injected via constructor and Guice binding."
```

---

### Task 5: Add Analyze Span to QueryService

**Files:**
- Modify: `core/src/main/java/org/opensearch/sql/executor/QueryService.java:55-160`
- Modify: `plugin/src/main/java/org/opensearch/sql/plugin/config/OpenSearchPluginModule.java:113-124`
- Create: `core/src/test/java/org/opensearch/sql/executor/QueryServiceTraceTest.java`

- [ ] **Step 1: Write failing test for analyze span**

Create `QueryServiceTraceTest.java` that verifies:
- `opensearch.query.analyze` span is created when `executeWithCalcite()` is called
- Span wraps `analyze(plan, context)` + `convertToCalcitePlan()`
- Span ends in finally block
- Error is recorded on span if analysis fails

- [ ] **Step 2: Add Tracer to QueryService**

In `QueryService.java`, add `Tracer` field and constructor parameter:

```java
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanCreationContext;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;

@RequiredArgsConstructor
public class QueryService {
  private final Analyzer analyzer;
  private final ExecutionEngine executionEngine;
  private final Planner planner;
  private DataSourceService dataSourceService;
  private Settings settings;
  private final Tracer tracer;
```

Wrap the analysis portion of `executeWithCalcite()` (lines 138-148):

```java
// Inside executeWithCalcite(), after profiling setup:
Span analyzeSpan = tracer.startSpan(
    SpanCreationContext.internal().name("opensearch.query.analyze"));
try (SpanScope scope = tracer.withSpanInScope(analyzeSpan)) {
  CalcitePlanContext context =
      CalcitePlanContext.create(
          buildFrameworkConfig(), SysLimit.fromSettings(settings), queryType);
  context.setHighlightConfig(highlightConfig);
  RelNode relNode = analyze(plan, context);
  RelNode calcitePlan = convertToCalcitePlan(relNode, context);
  analyzeMetric.set(System.nanoTime() - analyzeStart);
  executionEngine.execute(calcitePlan, context, listener);
} catch (Exception e) {
  analyzeSpan.setError(e);
  throw e;
} finally {
  analyzeSpan.endSpan();
}
```

Note: The try/catch/finally here re-throws, and the outer `catch (Throwable t)` in `executeWithCalcite()` still handles fallback to v2. The span just records the error on the analyze phase.

- [ ] **Step 3: Update OpenSearchPluginModule to pass Tracer to QueryService**

In `OpenSearchPluginModule.java`, update `queryPlanFactory` provider:

```java
@Provides
public QueryPlanFactory queryPlanFactory(
    DataSourceService dataSourceService, ExecutionEngine executionEngine,
    Settings settings, Tracer tracer) {
  Analyzer analyzer =
      new Analyzer(
          new ExpressionAnalyzer(functionRepository), dataSourceService, functionRepository);
  Planner planner = new Planner(LogicalPlanOptimizer.create());
  QueryService queryService =
      new QueryService(analyzer, executionEngine, planner, dataSourceService, settings, tracer);
  return new QueryPlanFactory(queryService);
}
```

- [ ] **Step 4: Run tests**

Run: `./gradlew :core:test :plugin:test -x spotlessCheck`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add core/src/main/java/org/opensearch/sql/executor/QueryService.java
git add core/src/test/java/org/opensearch/sql/executor/QueryServiceTraceTest.java
git add plugin/src/main/java/org/opensearch/sql/plugin/config/OpenSearchPluginModule.java
git commit -s -m "feat: add opensearch.query.analyze span to QueryService

Wraps CalciteRelNodeVisitor analysis and plan conversion in an
INTERNAL span within executeWithCalcite()."
```

---

### Task 6: Add Optimize and Compile Spans to CalciteToolsHelper

**Files:**
- Modify: `core/src/main/java/org/opensearch/sql/calcite/utils/CalciteToolsHelper.java:400-445`
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/executor/OpenSearchExecutionEngine.java:211-229`

- [ ] **Step 1: Pass Tracer to OpenSearchRelRunners.run()**

The `run()` method is static. Add `Tracer` as a parameter:

```java
public static PreparedStatement run(CalcitePlanContext context, RelNode rel, Tracer tracer) {
  ProfileMetric optimizeTime = QueryProfiling.current().getOrCreateMetric(OPTIMIZE);
  long startTime = System.nanoTime();

  // Optimize span
  Span optimizeSpan = tracer.startSpan(
      SpanCreationContext.internal().name("opensearch.query.optimize"));
  try (SpanScope scope = tracer.withSpanInScope(optimizeSpan)) {
    rel = CalciteToolsHelper.optimize(rel, context);
    final RelShuttle shuttle = new RelHomogeneousShuttle() {
      @Override
      public RelNode visit(TableScan scan) {
        final RelOptTable table = scan.getTable();
        if (scan instanceof LogicalTableScan
            && Bindables.BindableTableScan.canHandle(table)) {
          return Bindables.BindableTableScan.create(scan.getCluster(), table);
        }
        return super.visit(scan);
      }
    };
    rel = rel.accept(shuttle);
  } catch (Exception e) {
    optimizeSpan.setError(e);
    throw e;
  } finally {
    optimizeSpan.endSpan();
  }

  // Compile span
  Span compileSpan = tracer.startSpan(
      SpanCreationContext.internal().name("opensearch.query.compile"));
  try (SpanScope scope = tracer.withSpanInScope(compileSpan);
       Connection connection = context.connection) {
    final RelRunner runner = connection.unwrap(RelRunner.class);
    PreparedStatement preparedStatement = runner.prepareStatement(rel);
    optimizeTime.set(System.nanoTime() - startTime);
    return preparedStatement;
  } catch (SQLException e) {
    compileSpan.setError(e);
    String errorMsg = e.getMessage();
    if (errorMsg != null
        && errorMsg.contains("Error while preparing plan")
        && errorMsg.contains("WIDTH_BUCKET")) {
      throw new UnsupportedOperationException(
          "The 'bins' parameter on timestamp fields requires: ...");
    }
    throw Util.throwAsRuntime(e);
  } finally {
    compileSpan.endSpan();
  }
}
```

- [ ] **Step 2: Update all callers to pass Tracer**

In `OpenSearchExecutionEngine.java`, update the `execute(RelNode, ...)` call:

```java
try (PreparedStatement statement = OpenSearchRelRunners.run(context, rel, tracer)) {
```

This requires `OpenSearchExecutionEngine` to have a `Tracer` field. Add it:

```java
private final Tracer tracer;

public OpenSearchExecutionEngine(
    OpenSearchClient client, ExecutionProtector executionProtector,
    PlanSerializer planSerializer, Tracer tracer) {
  // ... existing init ...
  this.tracer = tracer;
}
```

Update `OpenSearchPluginModule.executionEngine()` to pass `Tracer`:

```java
@Provides
@Singleton
public ExecutionEngine executionEngine(
    OpenSearchClient client, ExecutionProtector protector,
    PlanSerializer planSerializer, Tracer tracer) {
  ExecutionEngine defaultEngine =
      new OpenSearchExecutionEngine(client, protector, planSerializer, tracer);
  // ... rest unchanged ...
}
```

Also update the `explain(RelNode, ...)` method's call to `OpenSearchRelRunners.run()` to pass tracer.

- [ ] **Step 3: Run tests**

Run: `./gradlew :core:test :opensearch:test :plugin:test -x spotlessCheck`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add core/src/main/java/org/opensearch/sql/calcite/utils/CalciteToolsHelper.java
git add opensearch/src/main/java/org/opensearch/sql/opensearch/executor/OpenSearchExecutionEngine.java
git add plugin/src/main/java/org/opensearch/sql/plugin/config/OpenSearchPluginModule.java
git commit -s -m "feat: add opensearch.query.optimize and opensearch.query.compile spans

Wraps HepPlanner optimization and prepareStatement compilation in
separate INTERNAL spans within OpenSearchRelRunners.run(). Tracer
passed as parameter to the static method."
```

---

### Task 7: Add Execute and Materialize Spans to OpenSearchExecutionEngine

**Files:**
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/executor/OpenSearchExecutionEngine.java:211-229`

- [ ] **Step 1: Add execute and materialize spans to the async lambda**

Update the `execute(RelNode, ...)` method (already modified in Task 1 and Task 6):

```java
@Override
public void execute(
    RelNode rel, CalcitePlanContext context, ResponseListener<QueryResponse> listener) {
  client.schedule(
      () -> {
        try (PreparedStatement statement = OpenSearchRelRunners.run(context, rel, tracer)) {
          ProfileMetric metric = QueryProfiling.current().getOrCreateMetric(MetricName.EXECUTE);
          long execTime = System.nanoTime();

          // Execute span
          Span executeSpan = tracer.startSpan(
              SpanCreationContext.internal().name("opensearch.query.execute"));
          ResultSet result;
          try (SpanScope scope = tracer.withSpanInScope(executeSpan)) {
            result = statement.executeQuery();
          } catch (Exception e) {
            executeSpan.setError(e);
            throw e;
          } finally {
            executeSpan.endSpan();
          }

          // Materialize span
          Span materializeSpan = tracer.startSpan(
              SpanCreationContext.internal().name("opensearch.query.materialize"));
          try (SpanScope scope = tracer.withSpanInScope(materializeSpan)) {
            QueryResponse response =
                buildResultSet(result, rel.getRowType(), context.sysLimit.querySizeLimit());
            metric.add(System.nanoTime() - execTime);
            listener.onResponse(response);
          } catch (Exception e) {
            materializeSpan.setError(e);
            throw e;
          } finally {
            materializeSpan.endSpan();
          }

        } catch (Throwable t) {
          if (t instanceof VirtualMachineError) {
            throw (VirtualMachineError) t;
          }
          Exception e = (t instanceof Exception) ? (Exception) t : new RuntimeException(t);
          listener.onFailure(e);
        }
      });
}
```

- [ ] **Step 2: Write tests**

Add tests to `OpenSearchExecutionEngineTest.java` verifying:
- `opensearch.query.execute` span wraps `executeQuery()`
- `opensearch.query.materialize` span wraps `buildResultSet()`
- Error on execute records error on execute span
- Error on materialize records error on materialize span

- [ ] **Step 3: Run tests**

Run: `./gradlew :opensearch:test -x spotlessCheck`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add opensearch/src/main/java/org/opensearch/sql/opensearch/executor/OpenSearchExecutionEngine.java
git add opensearch/src/test/java/org/opensearch/sql/opensearch/executor/OpenSearchExecutionEngineTest.java
git commit -s -m "feat: add opensearch.query.execute and opensearch.query.materialize spans

Wraps executeQuery() and buildResultSet() in separate INTERNAL spans
within the async lambda of execute(RelNode, ...). Errors are recorded
on the appropriate phase span."
```

---

### Task 8: Add db.query.summary Utility

**Files:**
- Create: `core/src/main/java/org/opensearch/sql/utils/QuerySummaryExtractor.java`
- Create: `core/src/test/java/org/opensearch/sql/utils/QuerySummaryExtractorTest.java`
- Modify: `plugin/src/main/java/org/opensearch/sql/plugin/transport/TransportPPLQueryAction.java`

- [ ] **Step 1: Write tests for summary extraction**

Create `QuerySummaryExtractorTest.java`:

```java
package org.opensearch.sql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class QuerySummaryExtractorTest {

  @Test
  void extracts_command_structure_from_ppl() {
    assertEquals(
        "source | where | stats",
        QuerySummaryExtractor.extractPPLSummary(
            "source=logs | where status=500 | stats count() by host"));
  }

  @Test
  void extracts_source_only() {
    assertEquals(
        "source",
        QuerySummaryExtractor.extractPPLSummary("source=logs"));
  }

  @Test
  void extracts_complex_pipeline() {
    assertEquals(
        "source | where | sort | head | fields",
        QuerySummaryExtractor.extractPPLSummary(
            "source=logs | where age > 30 | sort age | head 10 | fields name, age"));
  }
}
```

- [ ] **Step 2: Implement QuerySummaryExtractor**

Create `QuerySummaryExtractor.java`:

```java
package org.opensearch.sql.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.List;

public class QuerySummaryExtractor {

  private static final Pattern PPL_COMMAND_PATTERN = Pattern.compile(
      "(?:^|\\|)\\s*(source|where|fields|stats|sort|head|tail|top|rare|"
      + "eval|dedup|rename|parse|grok|patterns|lookup|join|append|"
      + "subquery|trendline|fillnull|flatten|expand|describe|"
      + "fieldsummary|ad|ml|kmeans|explain)\\b",
      Pattern.CASE_INSENSITIVE);

  public static String extractPPLSummary(String query) {
    List<String> commands = new ArrayList<>();
    Matcher matcher = PPL_COMMAND_PATTERN.matcher(query);
    while (matcher.find()) {
      commands.add(matcher.group(1).toLowerCase());
    }
    return commands.isEmpty() ? "unknown" : String.join(" | ", commands);
  }

  private QuerySummaryExtractor() {}
}
```

- [ ] **Step 3: Run tests**

Run: `./gradlew :core:test --tests "org.opensearch.sql.utils.QuerySummaryExtractorTest" -x spotlessCheck`
Expected: PASS

- [ ] **Step 4: Add db.query.summary to root span**

In `TransportPPLQueryAction.doExecute()`, add to the root span attributes:

```java
.addAttribute("db.query.summary",
    QuerySummaryExtractor.extractPPLSummary(transformedRequest.getRequest()))
```

- [ ] **Step 5: Commit**

```bash
git add core/src/main/java/org/opensearch/sql/utils/QuerySummaryExtractor.java
git add core/src/test/java/org/opensearch/sql/utils/QuerySummaryExtractorTest.java
git add plugin/src/main/java/org/opensearch/sql/plugin/transport/TransportPPLQueryAction.java
git commit -s -m "feat: add db.query.summary attribute via QuerySummaryExtractor

Extracts low-cardinality command structure from PPL queries
(e.g. 'source | where | stats') for the root span's db.query.summary
attribute, following OTel DB semantic conventions."
```

---

### Task 9: Add Remaining Root Span Attributes

**Files:**
- Modify: `plugin/src/main/java/org/opensearch/sql/plugin/transport/TransportPPLQueryAction.java`

- [ ] **Step 1: Add db.query.text (sanitized) and other attributes**

In the root span creation, add the remaining attributes. The `PPLQueryDataAnonymizer` is already available in `PPLService` — for the transport action, we can use the anonymizer or pass the query text. Since `PPLQueryDataAnonymizer` requires `Settings` and operates on AST (not raw text), for `db.query.text` at the transport level use the raw query (sanitization happens best-effort at this level — the full anonymization happens in PPLService):

```java
Span rootSpan = tracer.startSpan(
    SpanCreationContext.client().name("opensearch.query")
        .attributes(Attributes.create()
            .addAttribute("db.system.name", "opensearch")
            .addAttribute("db.query.type", "ppl")
            .addAttribute("db.query.id", QueryContext.getRequestId())
            .addAttribute("db.operation.name",
                transformedRequest.isExplainRequest() ? "EXPLAIN" : "EXECUTE")
            .addAttribute("db.query.text", transformedRequest.getRequest())
            .addAttribute("db.query.summary",
                QuerySummaryExtractor.extractPPLSummary(transformedRequest.getRequest()))));
```

Note: `db.namespace` (cluster name), `server.address`, `server.port`, `db.collection.name`, and `opensearch.query.datasource` require runtime context (ClusterService, TransportService). Add these by injecting `ClusterService` and `TransportService` into `TransportPPLQueryAction` (they are already available in the constructor) and calling:

```java
.addAttribute("db.namespace", clusterService.getClusterName().value())
.addAttribute("server.address", transportService.getLocalNode().getHostAddress())
.addAttribute("server.port", transportService.getLocalNode().getAddress().getPort())
```

`db.collection.name` and `opensearch.query.datasource` are not available until after parsing — they can be added to the root span later in the pipeline via `rootSpan.addAttribute()` if the Span object is propagated, or omitted from the root span (available from the query plan). For Phase 2 scope, these two attributes are best-effort — add them if easily accessible, skip if they require significant plumbing.

- [ ] **Step 2: Run tests**

Run: `./gradlew :plugin:test -x spotlessCheck`
Expected: All tests PASS

- [ ] **Step 3: Run spotless and full build**

Run: `./gradlew spotlessApply && ./gradlew build -x integTest`
Expected: BUILD SUCCESSFUL

- [ ] **Step 4: Commit**

```bash
git add plugin/src/main/java/org/opensearch/sql/plugin/transport/TransportPPLQueryAction.java
git commit -s -m "feat: add remaining OTel semconv attributes to root span

Adds db.query.text, db.query.summary, db.namespace (cluster name),
server.address, server.port to the root opensearch.query span,
following OTel DB semantic conventions for Elasticsearch/PostgreSQL."
```

---

### Task 10: Integration Test — Telemetry On/Off

**Files:**
- Create or modify integration tests in `integ-test/src/test/java/org/opensearch/sql/ppl/`

- [ ] **Step 1: Add integration test for telemetry-off path**

Add a test that runs a PPL query with the default settings (telemetry off) and verifies:
- Query returns correct results
- No NPEs or errors related to tracing
- This is essentially a regression test — existing PPL integration tests should pass unchanged

Run: `./gradlew :integ-test:integTest --tests "*PPL*" -x spotlessCheck`
Expected: All existing tests PASS (NoopTracer path works)

- [ ] **Step 2: Add yamlRestTest for telemetry-on path**

Add YAML REST test in `integ-test/src/yamlRestTest/resources/rest-api-spec/test/` that:
- Enables telemetry settings
- Runs a PPL query
- Verifies the query succeeds (span export verification requires LoggingSpanExporter setup which is environment-specific)

- [ ] **Step 3: Run full integration tests**

Run: `./gradlew :integ-test:integTest -x spotlessCheck`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add integ-test/
git commit -s -m "test: add integration tests for PPL OTEL tracing on/off paths

Verifies that PPL queries work correctly with both telemetry enabled
and disabled. NoopTracer path confirmed with no regressions."
```

---

## Post-Implementation Checklist

- [ ] Run `./gradlew spotlessApply` — all formatting correct
- [ ] Run `./gradlew build -x integTest` — full build passes
- [ ] Run `./gradlew :integ-test:integTest` — integration tests pass
- [ ] Verify NoopTracer path: all existing tests pass without telemetry enabled
- [ ] Review all span names match spec: `opensearch.query`, `opensearch.query.parse`, `opensearch.query.analyze`, `opensearch.query.optimize`, `opensearch.query.compile`, `opensearch.query.execute`, `opensearch.query.materialize`
- [ ] Review all attributes match spec (Section 3 of design doc)
- [ ] Verify SpanScope is always closed on the same thread that opened it
- [ ] Verify Span.endSpan() is called exactly once per span (AtomicBoolean for root, finally for phases)
