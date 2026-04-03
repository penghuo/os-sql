# PPL OpenTelemetry Tracing Integration — Design Spec

**Date:** 2026-04-03
**RFC:** https://github.com/opensearch-project/sql/issues/5300
**Scope:** Phases 1 + 2 (TelemetryAwarePlugin interface + root span + phase-level child spans)
**Engine:** Calcite path only, PPL only

## Problem Statement

OpenSearch SQL/PPL plugin has no integration with OpenSearch's core telemetry framework. There is no distributed tracing for query execution, making it difficult to diagnose latency issues across the parse, analyze, optimize, compile, execute, materialize pipeline.

## Goals

- Add distributed tracing spans to the **PPL Calcite** query execution pipeline
- Follow OTel semantic conventions for database spans (Elasticsearch for `db.namespace`/`db.collection.name`, PostgreSQL/MySQL for `db.operation.name`)
- Integrate with OpenSearch's `TelemetryAwarePlugin` interface (`Tracer`, `Span`, `SpanScope`)
- Graceful degradation via `NoopTracer` when telemetry is disabled

## Non-Goals

- Metrics migration (P1 — separate follow-up)
- Tracing the legacy v2 engine path (used by SQL queries — `QueryService.shouldUseCalcite()` gates Calcite to PPL only)
- SQL query tracing (SQL currently uses the v2 engine exclusively; see `QueryService.java` line 323: `queryType == QueryType.PPL`)
- Cursor-based pagination (PPL does not support it — `fetchSize` is a `Head` node limit in a single request/response)

---

## 1. Plugin Interface & Tracer Wiring

`SQLPlugin` implements `TelemetryAwarePlugin` and receives `Tracer` + `MetricsRegistry` via `createComponents()`.

```java
public class SQLPlugin extends Plugin
    implements ActionPlugin, ScriptPlugin, SystemIndexPlugin,
               JobSchedulerExtension, ExtensiblePlugin, TelemetryAwarePlugin {

    private Tracer tracer = NoopTracer.getInstance();
    private MetricsRegistry metricsRegistry;

    // Called when telemetry feature flag ON
    @Override
    public Collection<Object> createComponents(
        ..., Tracer tracer, MetricsRegistry metricsRegistry) {
        this.tracer = tracer;
        this.metricsRegistry = metricsRegistry;
        return createComponentsInternal(...);
    }

    // Called when telemetry feature flag OFF
    @Override
    public Collection<Object> createComponents(...) {
        // this.tracer remains NoopTracer
        return createComponentsInternal(...);
    }
}
```

`Tracer` is passed into `OpenSearchPluginModule` and bound via Guice so any component can `@Inject` it.

### Telemetry Control

Two levels, both from OpenSearch core — the SQL plugin has no separate toggle:

| Level | Setting | Default | Effect |
|-------|---------|---------|--------|
| Feature Flag | `FeatureFlags.TELEMETRY` | `false` | Off: `Plugin.createComponents()` called (no Tracer param), `NoopTracer` used. On: `TelemetryAwarePlugin.createComponents()` called with real `Tracer`. |
| Sampling Rate | `telemetry.tracer.sampler.probability` | `0.01` (1%) | Controls what fraction of traces are exported. Only relevant when feature flag is on. |

No conditional checks like `if (tracer.isEnabled())` needed in application code. All span operations are no-ops with `NoopTracer`.

---

## 2. Span Hierarchy

Language-agnostic span naming. SQL vs PPL is an attribute (`db.query.type`), not part of the span name.

### EXECUTE Request

```
[OpenSearch Transport Action]                 <- SERVER (auto, OpenSearch core)
  └── opensearch.query                        <- CLIENT (our root span)
        ├── opensearch.query.parse            <- INTERNAL
        ├── opensearch.query.analyze          <- INTERNAL
        ├── opensearch.query.optimize         <- INTERNAL
        ├── opensearch.query.compile          <- INTERNAL
        ├── opensearch.query.execute          <- INTERNAL
        │     └── opensearch.search           <- automatic from OpenSearch core
        └── opensearch.query.materialize      <- INTERNAL
```

### EXPLAIN Request

EXPLAIN behavior varies by mode. There are 4 modes: SIMPLE, STANDARD, EXTENDED, COST.

**SIMPLE mode** — only generates logical plan string via `RelOptUtil.toString()`, does not call `OpenSearchRelRunners.run()`:

```
[OpenSearch Transport Action]                 <- SERVER (auto, OpenSearch core)
  └── opensearch.query                        <- CLIENT (root span, db.operation.name="EXPLAIN")
        ├── opensearch.query.parse            <- INTERNAL
        ├── opensearch.query.analyze          <- INTERNAL
        └── opensearch.query.optimize         <- INTERNAL
```

**Non-SIMPLE modes (STANDARD, EXTENDED, COST)** — call `OpenSearchRelRunners.run()` which includes `HepPlanner` optimization and `runner.prepareStatement(rel)` (compile) to capture physical plan via Calcite Hooks:

```
[OpenSearch Transport Action]                 <- SERVER (auto, OpenSearch core)
  └── opensearch.query                        <- CLIENT (root span, db.operation.name="EXPLAIN")
        ├── opensearch.query.parse            <- INTERNAL
        ├── opensearch.query.analyze          <- INTERNAL
        ├── opensearch.query.optimize         <- INTERNAL
        └── opensearch.query.compile          <- INTERNAL
```

All EXPLAIN modes skip `opensearch.query.execute` and `opensearch.query.materialize` — no data is fetched.

- **Root span** (`opensearch.query`): `SpanKind.CLIENT` — matches OTel DB semconv (the DB operation from the caller's perspective)
- **Transport action**: `SpanKind.SERVER` — already created by OpenSearch core
- **Phase spans**: `SpanKind.INTERNAL` — in-process execution stages
- **`db.operation.name`**: `"EXECUTE"` for regular queries, `"EXPLAIN"` for explain requests (distinguishable via `PPLQueryRequest.isExplainRequest()`)

---

## 3. Span Attributes

### Root Span (`opensearch.query`)

| Attribute | Convention Source | Example |
|-----------|-----------------|---------|
| `db.system.name` | OTel ES semconv | `"opensearch"` |
| `db.namespace` | OTel ES semconv | cluster name (e.g., `"my-cluster"`) |
| `db.collection.name` | OTel ES semconv | index/data stream (e.g., `"logs"`) |
| `db.operation.name` | OTel PG/MySQL semconv | `"EXECUTE"` or `"EXPLAIN"` |
| `db.query.text` | OTel DB semconv | sanitized query (literals stripped): `"source=logs \| where status=? \| stats count() by host"` |
| `db.query.summary` | OTel DB semconv | command structure: `"source \| where \| stats"` |
| `db.query.type` | custom | `"ppl"` or `"sql"` |
| `db.query.id` | custom | request ID from `QueryContext.getRequestId()` (UUID, always available) |
| `db.query.client_id` | custom (optional) | client-provided query ID from request body, if present |
| `db.response.status_code` | OTel DB semconv | error code on failure |
| `error.type` | OTel DB semconv | exception class on failure (e.g., `"SemanticCheckException"`) |
| `server.address` | OTel DB semconv | OpenSearch node address |
| `server.port` | OTel DB semconv | OpenSearch node port |
| `opensearch.query.datasource` | custom | `"opensearch"`, `"prometheus"`, `"s3"` |

#### `db.query.id` Source

Three ID sources exist in the codebase:

| Source | Format | Available At | Used For `db.query.id`? |
|--------|--------|-------------|------------------------|
| `QueryContext.getRequestId()` | UUID (36-char) | `TransportPPLQueryAction.doExecute()` line 121, before any query processing | **Yes — primary** |
| Client-provided `queryId` | Arbitrary string | Request body (optional, nullable) | No — stored as `db.query.client_id` if present |
| `QueryId` (internal) | 10-char random | `QueryPlanFactory.create()` (after parsing) | No — too late for root span |

`QueryContext.getRequestId()` is the right choice: always available at span creation time, UUID format, propagates via ThreadContext, and already integrated with logging.

### Phase Span Attributes

| Attribute | Span | Example |
|-----------|------|---------|
| `opensearch.query.plan.node_count` | `opensearch.query.optimize` | `7` |
| `opensearch.query.plan.pushed_down` | `opensearch.query.optimize` | `"filter,aggregation"` |
| `opensearch.query.result.rows` | `opensearch.query.materialize` | `1024` |
| `opensearch.query.cache.hit` | `opensearch.query.compile` | `true` / `false` |
| `error` | any span on failure | `true` |
| `error.type` | any span on failure | exception class name |

---

## 4. Instrumentation Points

Approach: **Distributed Tracer injection**. Each component that owns a phase receives `Tracer` via Guice and creates its own span. Parent-child relationships are automatic via OpenSearch's `ThreadContextBasedTracerContextStorage`.

### Span-to-Code Mapping (Calcite Path)

| Span | File | Method | What It Wraps |
|------|------|--------|---------------|
| `opensearch.query` | `TransportPPLQueryAction.java` | `doExecute()` | Entire query lifecycle (async — see Section 6) |
| `opensearch.query.parse` | `PPLService.java` | `plan()` | `parser.parse()` + `cst.accept(AstStatementBuilder)` |
| `opensearch.query.analyze` | `QueryService.java` | `executeWithCalcite()` | `analyze(plan, context)` + `convertToCalcitePlan()` |
| `opensearch.query.optimize` | `CalciteToolsHelper.java` | `OpenSearchRelRunners.run()` | `CalciteToolsHelper.optimize()` (HepPlanner) |
| `opensearch.query.compile` | `CalciteToolsHelper.java` | `OpenSearchRelRunners.run()` | `runner.prepareStatement(rel)` |
| `opensearch.query.execute` | `OpenSearchExecutionEngine.java` | `execute(RelNode)` | `statement.executeQuery()` |
| `opensearch.query.materialize` | `OpenSearchExecutionEngine.java` | `execute(RelNode)` | `buildResultSet()` |

### Guice Injection Points

| Class | Module | Receives |
|-------|--------|----------|
| `TransportPPLQueryAction` | plugin | `Tracer` |
| `PPLService` | ppl | `Tracer` |
| `QueryService` | core | `Tracer` |
| `CalciteToolsHelper` | core | `Tracer` (passed as parameter to static `run()`) |
| `OpenSearchExecutionEngine` | opensearch | `Tracer` |

### Implementation Pattern — Synchronous Phase Spans

Phase spans inside synchronous execution blocks (parse, analyze, optimize, compile) use try/catch/finally:

```java
Span span = tracer.startSpan(
    SpanCreationContext.internal().name("opensearch.query.parse"));
try (SpanScope scope = tracer.withSpanInScope(span)) {
    // phase work
} catch (Exception e) {
    span.setError(e);
    throw e;
} finally {
    span.endSpan();
}
```

### Implementation Pattern — Root Span (Async-Safe)

The root span **must not** use try/finally because `doExecute()` returns before query execution finishes. See Section 6 for the async-safe listener-wrapper pattern with proper scope management.

---

## 5. Query Sanitization

Reuse existing implementations — no new sanitization framework needed:

| Purpose | Implementation | Used For |
|---------|---------------|----------|
| Strip literal values | `PPLQueryDataAnonymizer` (PPL), `AnonymizerListener` (SQL) | `db.query.text` |
| Extract command structure | New lightweight utility in `core` module (extracts command names from UnresolvedPlan AST nodes) | `db.query.summary` |

---

## 6. Async Span Lifecycle

### Problem

`doExecute()` in `TransportPPLQueryAction` is **asynchronous**: it calls `pplService.execute()` which submits work to the `sql-worker` thread pool via `OpenSearchQueryManager.submit()` and returns immediately. A synchronous try/finally pattern would close the root span before execution finishes.

Additionally, the root span **must be put in scope** before `pplService.execute()` is called, so that `OpenSearchQueryManager.withCurrentContext()` captures the span context in ThreadContext for propagation to the worker thread. Without this, child phase spans on the worker thread would not inherit the root span as parent.

### Solution: Listener-Wrapper Pattern with Separated Scope and Span Lifecycles

Follow the existing `wrapWithProfilingClear()` pattern. The key insight is that `SpanScope` and `Span` have **different lifecycles**:

- **`SpanScope`** is thread-local. It must be opened and closed on the **same thread** (transport thread). Its only job is to be active during `pplService.execute()` so that `OpenSearchQueryManager.withCurrentContext()` captures the span context in ThreadContext for worker thread propagation.
- **`Span`** is thread-safe. It is created on the transport thread but ended on the worker thread (in the async listener callback).

```java
// TransportPPLQueryAction.doExecute()
TransportPPLQueryRequest transportRequest =
    TransportPPLQueryRequest.fromActionRequest(request);
QueryContext.addRequestId();

PPLQueryRequest transformedRequest = transportRequest.toPPLQueryRequest();

// 1. Start root span with attributes
Span rootSpan = tracer.startSpan(
    SpanCreationContext.client().name("opensearch.query")
        .attributes(Attributes.create()
            .addAttribute("db.system.name", "opensearch")
            .addAttribute("db.query.type", "ppl")
            .addAttribute("db.query.id", QueryContext.getRequestId())
            .addAttribute("db.operation.name",
                transformedRequest.isExplainRequest() ? "EXPLAIN" : "EXECUTE")
            .addAttribute("db.query.text",
                anonymize(transformedRequest.getRequest()))));

// 2. Put span in scope so withCurrentContext() captures it in ThreadContext.
SpanScope spanScope = tracer.withSpanInScope(rootSpan);

try {
    // 3. Wrap listener — ONLY the Span ends in the async callback (not the scope)
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
                rootSpan.endSpan();  // Only end the span, NOT the scope
            }
        }
    };

    // 4. Pass tracedListener through the chain and submit work
    ActionListener<TransportPPLQueryResponse> clearingListener =
        wrapWithProfilingClear(tracedListener);

    PPLService pplService = injector.getInstance(PPLService.class);
    if (transformedRequest.isExplainRequest()) {
        pplService.explain(transformedRequest,
            createExplainResponseListener(transformedRequest, clearingListener));
    } else {
        pplService.execute(transformedRequest,
            createListener(transformedRequest, clearingListener),
            createExplainResponseListener(transformedRequest, clearingListener));
    }
} catch (Exception e) {
    // Synchronous exception before async submission
    rootSpan.setError(e);
    rootSpan.endSpan();
    throw e;
} finally {
    // 5. Close scope immediately on the TRANSPORT THREAD after execute() returns.
    //    The span context has already been captured by withCurrentContext().
    spanScope.close();
}
// doExecute() returns here — scope is closed, span is still open.
// Span ends when listener callback fires on the worker thread.
```

Key properties:
- **`SpanScope` is opened and closed on the same thread** (transport thread) via try/finally. It only needs to be active during `pplService.execute()` so `withCurrentContext()` captures the span context.
- **`Span` lives across thread pool hops** — ended only when the async listener callback fires on the worker thread. `Span` objects are thread-safe.
- `AtomicBoolean` guarantees `rootSpan.endSpan()` is called exactly once
- If `pplService.execute()` throws synchronously (before async submission), the catch block ends the span, and the finally block closes the scope
- Works for both success and failure paths

### Thread Pool Span Propagation

- `OpenSearchQueryManager.withCurrentContext()` captures `ThreadContext.getImmutableContext()` at submit time (on transport thread) and restores it on the worker thread via `ThreadContext.putAll()`
- Since the root span's `SpanScope` is still open at submit time (closed in the finally block after `execute()` returns), `ThreadContextBasedTracerContextStorage` includes the span context in the captured ThreadContext snapshot
- Child phase spans created on the worker thread automatically inherit the root span as parent via the restored ThreadContext
- Cross-node push-down: Operations pushed to data nodes are traced by OpenSearch's transport-layer instrumentation automatically

### Lifecycle Summary

| Object | Created On | Closed/Ended On | Thread-Safe? |
|--------|-----------|----------------|-------------|
| `Span` | Transport thread | Worker thread (async callback) | Yes — spans are thread-safe |
| `SpanScope` | Transport thread | Transport thread (finally block) | Must be same thread |
| ThreadContext snapshot | Captured on transport thread | Restored on worker thread | Yes — immutable snapshot |

---

## 7. Error Handling

### Synchronous Phase Spans

Phase spans inside synchronous blocks catch exceptions, call `span.setError(e)`, and re-throw. The finally block always calls `span.endSpan()`.

### Async Boundaries

At async boundaries (inside `client.schedule()` lambdas), exceptions **must** be routed to `listener.onFailure()` — never re-thrown. Re-throwing from a scheduled lambda bypasses the listener chain and leaks spans.

**Correct pattern for async phases (execute, materialize):**

```java
client.schedule(() -> {
    Span executeSpan = tracer.startSpan(
        SpanCreationContext.internal().name("opensearch.query.execute"));
    try (SpanScope scope = tracer.withSpanInScope(executeSpan)) {
        // ... execute query ...
        listener.onResponse(response);
    } catch (Throwable t) {
        executeSpan.setError(t);
        if (t instanceof VirtualMachineError) {
            throw (VirtualMachineError) t;  // Fast-fail on OOM
        }
        Exception e = (t instanceof Exception) ? (Exception) t : new RuntimeException(t);
        listener.onFailure(e);
    } finally {
        executeSpan.endSpan();
    }
});
```

### Pre-existing Bug + General Async Failure Gap (Prerequisite Fix)

`OpenSearchExecutionEngine.execute(RelNode)` line 226 has a bug: `SQLException` is caught and re-thrown as `RuntimeException` without calling `listener.onFailure()`. More importantly, this method can also throw **non-`SQLException` runtime exceptions** (for example from `OpenSearchRelRunners.run(...)`) that currently bypass the listener chain.

This **must be fixed in Phase 1** before the root span is introduced, because any re-thrown async exception bypasses `onFailure` and leaks the root span (listener callback never fires, so `endSpan()` is never called).

**Required fix for Phase 1:**
- Wrap the async lambda body with `catch (Throwable t)`
- Re-throw only `VirtualMachineError`
- Route everything else to `listener.onFailure(...)` (wrapping non-`Exception` as needed)

### Error Handling Rules

1. **Synchronous phases**: try/catch/finally with `span.setError()` + re-throw + `span.endSpan()` in finally
2. **Async boundaries**: catch `Throwable`, only re-throw `VirtualMachineError`, always call `listener.onFailure()` for everything else
3. **Root span**: ended in listener wrapper, never in try/finally (see Section 6)
4. **Partial failures**: completed phase spans close normally, failing span gets error attributes, remaining phase spans are never created

---

## 8. Testing Strategy

### Unit Tests
- Verify spans are created with correct names, attributes, and parent-child relationships
- Use OpenSearch's `MockTracer` / `NoopTracer` for isolation
- One test class per instrumented component
- Verify `TracedResponseListener` ends span exactly once on success, failure, and double-call scenarios

### Integration Tests (including yamlRestTest)
- End-to-end span export with `telemetry-otel` plugin and `LoggingSpanExporter`
- Validate full span hierarchy: root + 6 child spans for EXECUTE, root + 3 for EXPLAIN SIMPLE, root + 4 for EXPLAIN non-SIMPLE
- Verify error spans on parse/analyze failures
- **Telemetry ON**: Verify spans are exported, validate span names and key attributes
- **Telemetry OFF**: Verify no NPEs, no behavioral changes, query results identical
- Verify async failure handling routes both `SQLException` and non-`SQLException` runtime failures to `listener.onFailure` (no leaked root span)

### NoopTracer Path
- Verify plugin starts and serves queries when telemetry feature flag is off
- No regressions on existing test suites

---

## 9. Rollout Plan

| Phase | Scope | Gate |
|-------|-------|------|
| Phase 1 | `TelemetryAwarePlugin` interface + root `opensearch.query` span with semconv attributes + `TracedResponseListener` + async-boundary hardening in `OpenSearchExecutionEngine.execute(RelNode)` (`catch Throwable`, re-throw only `VirtualMachineError`, route all other failures to `listener.onFailure`) | Unit tests pass, NoopTracer path verified, no p99 regression |
| Phase 2 | 6 phase-level child spans (parse, analyze, optimize, compile, execute, materialize) | Benchmark: <2% p99 latency regression with telemetry enabled |

---

## 10. Performance

7 spans per query (EXECUTE), 4-5 spans per query (EXPLAIN). At 10K QPS = 70K spans/sec max.

- **Telemetry disabled** (default): `NoopTracer` — near-zero overhead
- **Telemetry enabled**: Overhead bounded by OTel SDK's `BatchSpanProcessor` (async, non-blocking). Sampling rate (default 1%) limits export volume.
- **Benchmark requirement**: Measure p50/p99 query latency with telemetry enabled vs disabled before merging Phase 2. Target: <2% p99 regression.
