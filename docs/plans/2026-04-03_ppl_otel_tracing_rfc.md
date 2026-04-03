# [RFC] Distributed Tracing for PPL Query Execution

## Problem Statement

The OpenSearch SQL/PPL plugin has no integration with OpenSearch's core telemetry framework. There is no distributed tracing for query execution, making it difficult to diagnose latency issues across the parse → analyze → optimize → compile → execute → materialize pipeline. The existing profiling framework (`QueryProfiling`) provides wall-clock timing but is disconnected from the standard OpenSearch telemetry export pipeline (OTel SDK → OTLP → observability backends).

## Goals

- **P0**: Add distributed tracing spans to the PPL Calcite query execution pipeline
- Follow OTel semantic conventions for database spans (Elasticsearch for `db.namespace`/`db.collection.name`, PostgreSQL/MySQL for `db.operation.name`)
- Integrate with OpenSearch's `TelemetryAwarePlugin` interface
- Graceful degradation via `NoopTracer` when telemetry is disabled

## Non-Goals

- Metrics migration (P1 — separate follow-up)
- Tracing the legacy v2 engine path (SQL currently uses v2 exclusively; `QueryService.shouldUseCalcite()` gates Calcite to PPL only)
- SQL query tracing (future work when SQL migrates to Calcite)

## Background

### OpenSearch Telemetry Framework

OpenSearch provides a backend-agnostic telemetry framework:

- **`libs/telemetry/`** — interfaces: `Tracer`, `Span`, `SpanScope`, `MetricsRegistry`
- **`plugins/telemetry-otel/`** — OTel SDK implementation that exports via `BatchSpanProcessor`
- Plugins access the framework by implementing `TelemetryAwarePlugin`, which provides `Tracer` and `MetricsRegistry`

The framework is gated behind `opensearch.experimental.feature.telemetry.enabled` (defaults to `false`). When disabled, all tracing operations are no-ops via `NoopTracer`.

### PPL Query Execution Pipeline (Calcite Path)

```
PPL text
  → Parse (PPLSyntaxParser → UnresolvedPlan AST)
  → Analyze (CalciteRelNodeVisitor → RelNode logical plan)
  → Optimize (HepPlanner → optimized RelNode)
  → Compile (RelRunner.prepareStatement → PreparedStatement)
  → Execute (PreparedStatement.executeQuery → ResultSet)
  → Materialize (ResultSet → QueryResponse)
```

---

## Design

### Plugin Interface

`SQLPlugin` implements `TelemetryAwarePlugin`. OpenSearch calls two `createComponents()` methods separately:

1. `TelemetryAwarePlugin.createComponents(... Tracer, MetricsRegistry)` — called first when telemetry is enabled. Stores the `Tracer` reference. Returns empty (no components).
2. `Plugin.createComponents(...)` — called always. Creates all plugin components. The stored `Tracer` (real or `NoopTracer` default) is passed to `OpenSearchPluginModule` for Guice binding.

```java
public class SQLPlugin extends Plugin
    implements ActionPlugin, ScriptPlugin, SystemIndexPlugin,
               JobSchedulerExtension, ExtensiblePlugin, TelemetryAwarePlugin {

    private Tracer tracer = NoopTracer.INSTANCE;

    // Telemetry-enabled: stores Tracer, returns empty
    @Override
    public Collection<Object> createComponents(
        ..., Tracer tracer, MetricsRegistry metricsRegistry) {
        this.tracer = tracer;
        return Collections.emptyList();
    }

    // Always called: creates all components with stored Tracer
    @Override
    public Collection<Object> createComponents(...) {
        modules.add(new OpenSearchPluginModule(executionEngineExtensions, tracer));
        // ... component creation ...
    }
}
```

`Tracer` is bound via Guice `@Provides @Singleton` in `OpenSearchPluginModule` and injected into all instrumented components.

### Span Hierarchy

Language-agnostic naming. Query language is an attribute (`db.query.type`), not part of the span name.

#### EXECUTE Request (7 spans)

```
[OpenSearch Transport Action]                 ← SERVER (auto, OpenSearch core)
  └── opensearch.query                        ← CLIENT (root span)
        ├── opensearch.query.parse            ← INTERNAL
        ├── opensearch.query.analyze          ← INTERNAL
        ├── opensearch.query.optimize         ← INTERNAL
        ├── opensearch.query.compile          ← INTERNAL
        ├── opensearch.query.execute          ← INTERNAL
        │     └── transport indices:data/read/search  ← auto, OpenSearch core
        │           └── [phase/query] → [phase/fetch] → [phase/expand]
        └── opensearch.query.materialize      ← INTERNAL
```

#### EXPLAIN Request (4-5 spans)

SIMPLE mode skips compile. Non-SIMPLE modes (STANDARD, EXTENDED, COST) include compile since `OpenSearchRelRunners.run()` calls `prepareStatement()` to capture the physical plan.

```
opensearch.query (db.operation.name="EXPLAIN")
  ├── opensearch.query.parse
  ├── opensearch.query.analyze
  ├── opensearch.query.optimize
  └── opensearch.query.compile              ← non-SIMPLE modes only
```

### Span Attributes

#### Root Span (`opensearch.query`)

Following OTel DB semantic conventions (Elasticsearch for cluster/index, PostgreSQL/MySQL for `db.operation.name`):

| Attribute | Convention | Value |
|-----------|-----------|-------|
| `db.system.name` | ES semconv | `"opensearch"` |
| `db.namespace` | ES semconv | Cluster name |
| `db.operation.name` | PG/MySQL semconv | `"EXECUTE"` or `"EXPLAIN"` |
| `db.query.text` | DB semconv | Raw PPL query |
| `db.query.summary` | DB semconv | Command structure (e.g., `"source \| where \| stats"`) |
| `db.query.type` | Custom | `"ppl"` |
| `db.query.id` | Custom | `QueryContext.getRequestId()` (UUID) |
| `server.address` | DB semconv | Node host address |
| `server.port` | DB semconv | Node transport port |

`db.query.summary` is extracted by `QuerySummaryExtractor`, a regex-based utility that produces a low-cardinality pipe-delimited command structure suitable for grouping in observability backends.

`db.query.id` uses `QueryContext.getRequestId()` — a UUID generated at the start of `doExecute()`, before any query processing. It propagates via Log4j ThreadContext and is already used for log correlation.

#### Phase Span Attributes

Phase spans (`INTERNAL`) carry `error` and `error.type` on failure. Additional phase-specific attributes (e.g., `opensearch.query.plan.node_count`, `opensearch.query.result.rows`) are defined in the design spec but not yet implemented — they will be added as the instrumentation matures.

### Instrumentation Points

Each component that owns a phase receives `Tracer` via Guice and creates its own span. Parent-child relationships propagate automatically via `ThreadContextBasedTracerContextStorage`.

| Span | Component | Method |
|------|-----------|--------|
| `opensearch.query` | `TransportPPLQueryAction` | `doExecute()` |
| `opensearch.query.parse` | `PPLService` | `plan()` |
| `opensearch.query.analyze` | `QueryService` | `executeWithCalcite()` |
| `opensearch.query.optimize` | `CalciteToolsHelper.OpenSearchRelRunners` | `run()` |
| `opensearch.query.compile` | `CalciteToolsHelper.OpenSearchRelRunners` | `run()` |
| `opensearch.query.execute` | `OpenSearchExecutionEngine` | `execute(RelNode, ...)` |
| `opensearch.query.materialize` | `OpenSearchExecutionEngine` | `execute(RelNode, ...)` |

### Async Span Lifecycle

The execution model is asynchronous: `doExecute()` returns before query execution finishes. The actual work runs on the `sql-worker` thread pool.

**Key insight:** `SpanScope` and `Span` have different lifecycles.

- **`SpanScope`** is thread-local. Opened and closed on the **transport thread** via `try/finally`. Its only job is to be active during `pplService.execute()` so `OpenSearchQueryManager.withCurrentContext()` captures the span context in ThreadContext for worker thread propagation.

- **`Span`** is thread-safe. Created on the transport thread, ended on the **worker thread** in the async listener callback via `AtomicBoolean` guard for exactly-once semantics.

```java
// Transport thread
Span rootSpan = tracer.startSpan(SpanCreationContext.client().name("opensearch.query")...);
SpanScope spanScope = tracer.withSpanInScope(rootSpan);

try {
    ActionListener<...> tracedListener = new ActionListener<>() {
        private final AtomicBoolean ended = new AtomicBoolean(false);

        @Override public void onResponse(...) {
            try { listener.onResponse(response); }
            finally { if (ended.compareAndSet(false, true)) rootSpan.endSpan(); }
        }

        @Override public void onFailure(Exception e) {
            try { rootSpan.setError(e); listener.onFailure(e); }
            finally { if (ended.compareAndSet(false, true)) rootSpan.endSpan(); }
        }
    };

    pplService.execute(request, tracedListener, ...);
} catch (Exception e) {
    rootSpan.setError(e); rootSpan.endSpan(); listener.onFailure(e);
} finally {
    spanScope.close(); // Close scope on transport thread
}
```

| Object | Created On | Closed/Ended On | Thread-Safe? |
|--------|-----------|----------------|-------------|
| `Span` | Transport thread | Worker thread (async callback) | Yes |
| `SpanScope` | Transport thread | Transport thread (finally block) | Must be same thread |
| ThreadContext snapshot | Captured at submit time | Restored on worker thread | Yes (immutable) |

### Error Handling

#### Synchronous Phases (parse, analyze, optimize, compile)

Standard `try/catch/finally` with `span.setError(e)` + re-throw + `span.endSpan()` in `finally`.

#### Async Boundaries (execute, materialize)

Inside `client.schedule()` lambdas, exceptions **must** route to `listener.onFailure()` — never re-thrown. Re-throwing bypasses the listener chain and leaks the root span.

```java
client.schedule(() -> {
    try (...) {
        // ... phase work ...
        listener.onResponse(response);
    } catch (Throwable t) {
        if (t instanceof VirtualMachineError) throw (VirtualMachineError) t;
        Exception e = (t instanceof Exception) ? (Exception) t : new RuntimeException(t);
        listener.onFailure(e);
    }
});
```

**Pre-existing bug fixed:** `OpenSearchExecutionEngine.execute(RelNode, ...)` previously caught `SQLException` and re-threw as `RuntimeException` without calling `listener.onFailure()`. Fixed to catch `Throwable`, re-throw only `VirtualMachineError`, and route everything else to `listener.onFailure()`.

### Telemetry Control

No SQL plugin-specific toggle. Controlled entirely by OpenSearch core:

| Level | Setting | Default | Effect |
|-------|---------|---------|--------|
| Feature Flag | `opensearch.experimental.feature.telemetry.enabled` | `false` | Gates all telemetry settings |
| Tracer Feature | `telemetry.feature.tracer.enabled` | `false` | Enables tracer infrastructure |
| Tracer Toggle | `telemetry.tracer.enabled` | `false` | Dynamic on/off for tracing |
| Sampling | `telemetry.tracer.sampler.probability` | `0.01` | Fraction of traces exported |

When telemetry is disabled, `Tracer` is `NoopTracer` — all span operations are no-ops with near-zero overhead. No conditional checks needed in application code.

---

## Verified End-to-End Trace

Tested with OpenSearch 3.6.0-SNAPSHOT, telemetry-otel plugin, 100% sampling, OTel Collector → Data Prepper → Stack OpenSearch → Dashboards.

Query: `source=test-logs | where status > 300 | stats count() by host`

```
POST /_plugins/_ppl (SERVER)                              1.48s
  └── POST /_plugins/_ppl (CLIENT)
        └── opensearch.query (CLIENT)                     1.47s
              ├── opensearch.query.parse (INTERNAL)        207ms
              ├── opensearch.query.analyze (INTERNAL)      704ms
              ├── opensearch.query.optimize (INTERNAL)       9ms
              ├── opensearch.query.compile (INTERNAL)      303ms
              ├── opensearch.query.execute (INTERNAL)       62ms
              │     └── transport indices:data/read/search   41ms
              │           └── [phase/query] → [phase/fetch] → [phase/expand]
              └── opensearch.query.materialize (INTERNAL)   30ms
```

Root span attributes captured:
```
db.system.name:   opensearch
db.namespace:     opensearch
db.operation.name: EXECUTE
db.query.type:    ppl
db.query.id:      f8f7b64b-05e9-48c3-ad44-1eeb2a46923f
db.query.text:    source=test-logs | where status > 300 | stats count() by host
db.query.summary: source | where | stats
server.address:   127.0.0.1
server.port:      9300
```

14 total spans per query (7 from SQL plugin + 7 from OpenSearch core search phases).

---

## Performance

7 spans per query (EXECUTE), 4-5 per query (EXPLAIN). At 10K QPS = 70K spans/sec max.

- **Telemetry disabled** (default): `NoopTracer` — near-zero overhead
- **Telemetry enabled**: Overhead bounded by OTel SDK's `BatchSpanProcessor` (async, non-blocking). Sampling rate (default 1%) limits export volume.
- **Benchmark target**: < 2% p99 latency regression with telemetry enabled.

---

## Testing

### Unit Tests
- `TransportPPLQueryActionTest` — root span lifecycle: creation, attributes, SpanScope closure on transport thread, Span end in async callback, exactly-once semantics, EXPLAIN operation name
- `PPLServiceTest`, `QueryServiceTest`, `OpenSearchExecutionEngineTest` — NoopTracer path works without errors
- `QuerySummaryExtractorTest` — command structure extraction for various PPL query patterns

### Integration Tests
- All existing PPL integration tests pass unchanged (NoopTracer path)
- Integration test constructors updated for new Tracer parameter

---

## Implementation Summary

### Files Changed (20 files, +2289/-108 lines)

| File | Change |
|------|--------|
| `SQLPlugin.java` | Implement `TelemetryAwarePlugin`, store Tracer |
| `OpenSearchPluginModule.java` | Bind `Tracer` via Guice, update all providers |
| `TransportPPLQueryAction.java` | Root span + async listener wrapper |
| `PPLService.java` | Parse span |
| `QueryService.java` | Analyze span |
| `CalciteToolsHelper.java` | Optimize + compile spans |
| `OpenSearchExecutionEngine.java` | Execute + materialize spans, async error fix |
| `QuerySummaryExtractor.java` | New utility for `db.query.summary` |
| `core/build.gradle`, `ppl/build.gradle` | Telemetry compile dependencies |
| 6 test files | New tests + NoopTracer parameter updates |
| 4 integration test files | Constructor parameter updates |

### Commits

```
615ea77 fix: route all async exceptions to listener.onFailure in Calcite execute path
0707aa1 feat: implement TelemetryAwarePlugin and bind Tracer via Guice
06c4846 feat: add root opensearch.query span with async-safe listener wrapper
92aa294 feat: add opensearch.query.parse span to PPLService
fc055b4 feat: add opensearch.query.analyze span to QueryService
846baec feat: add opensearch.query.optimize and opensearch.query.compile spans
ac543eb feat: add opensearch.query.execute and opensearch.query.materialize spans
ca15815 feat: add QuerySummaryExtractor for db.query.summary attribute
64bb863 feat: add remaining OTel semconv attributes to root span
d301ec5 fix: update integration test constructors with NoopTracer
2542a19 fix: return empty from TelemetryAwarePlugin.createComponents to avoid duplicate bindings
```

---

## Future Work

- **SQL tracing**: When SQL migrates to Calcite engine, the same span hierarchy applies (language-agnostic naming, `db.query.type=sql`)
- **Metrics migration (P1)**: Migrate custom `Metrics` singleton to `MetricsRegistry` (OTel-backed counters/histograms)
- **Phase-specific attributes**: Add `opensearch.query.plan.node_count`, `opensearch.query.plan.pushed_down`, `opensearch.query.result.rows`, `opensearch.query.cache.hit`
- **Query sanitization**: Replace raw `db.query.text` with AST-level anonymization via `PPLQueryDataAnonymizer`
- **v2 engine tracing**: Instrument the legacy Analyzer → Planner → PhysicalPlan path
