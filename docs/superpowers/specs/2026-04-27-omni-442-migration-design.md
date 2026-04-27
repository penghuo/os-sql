# Omni-442 — Migrate Omni distributed engine into ppl repo as the PPL execution engine

Date: 2026-04-27
Branch: `feat/omni-442`
Reference implementation: `~/oss/omni` branch `feat/ppl` (validated: 43/43 ClickBench + 58/58 Big5 PPL pass)

## Goal

Wholesale replacement of the current Calcite-based PPL execution path (`CalciteRelNodeVisitor` →
`OpenSearchExecutionEngine.execute(RelNode,...)`) with Omni's Trino-442-based distributed engine.

**Success metric:** every integration test (`integ-test/src/test/java`) and yamlRestTest
(`integ-test/src/yamlRestTest`) that targets the Calcite engine passes against the Omni engine.

**No fallback.** The V2 legacy path is removed from the PPL request flow; `isCalciteFallbackAllowed`
is gone. If Omni cannot answer a PPL query, that is a bug to fix, not a reason to fall back.

## Non-goals

- Replacing the SQL endpoint. V1 SQL legacy stays as-is (wholesale replacement covers PPL only;
  SQL has no Calcite path today).
- Reimplementing Omni mechanics. The `feat/ppl` branch has already proven the approach — we copy
  it in, we do not redesign it.
- Exposing new REST endpoints. Omni's `/_plugins/_omni/v1/*` routes are dropped;
  `/_plugins/_ppl` and `/_plugins/_ppl/_explain` stay the entry points.

## Architecture

### Request flow

```
POST /_plugins/_ppl                      (unchanged REST handler: RestPPLQueryAction)
    │
    ▼
TransportPPLQueryAction.doExecute()      (body REPLACED — no fallback, no QueryService/Calcite path)
    │
    ├─ if isExplainRequest():
    │     OmniEngineService.explain(request, explainListener)
    │
    └─ else:
          OmniEngineService.execute(request, queryListener, explainListener)
```

### Inside OmniEngineService

```
1. Build Trino Session from PPLQueryRequest + cluster state
     Identity "ppl", SessionContext catalog/schema resolved from request or defaults

2. PPL → Trino SQL                       [verbatim from omni feat/ppl PplTranslator]
     UnifiedQueryPlanner(ctx).plan(ppl)  where ctx.catalog(name, CalciteSchemaAdapter)
     UnifiedQueryTranspiler.toSql(plan)   with OmniSqlDialect
     + string rewrites (ILIKE / QUERY_STRING / dotted cols / COALESCE)

3. Dispatch & execute                    [verbatim from omni SubmitQueryAction body]
     DispatchManager.createQuery(queryId, slug, sessionContext, sql)
     Query.create(...) + Query.waitForResults(...) loop

4. Adapt results                         [NEW thin shim]
     QueryResults (Trino client protocol) → ExecutionEngine.QueryResponse
       - List<Column>              → ExecutionEngine.Schema with ExprType mapping
       - List<List<Object>> rows   → List<ExprTupleValue>
     (lets existing protocol/ formatters — SimpleJson, CSV, Raw, Viz — keep their wire format)

5. Explain path:
     "EXPLAIN (TYPE DISTRIBUTED) " + sql → run same dispatch/results loop →
     concat rows into plan text → ExecutionEngine.ExplainResponse
```

### Module structure

New top-level Gradle module `omni-engine`, alongside existing modules. Contents:

```
omni-engine/
└── src/main/java/
    ├── io/trino/…                              4,725 vendored files (VERBATIM from omni)
    └── org/opensearch/plugin/omni/
        ├── OmniSettings.java                   plugins.omni.* settings (VERBATIM)
        ├── ServiceWiring.java                  Manual Trino DI graph (VERBATIM)
        ├── OmniEngineService.java              NEW — boundary between ppl repo and Trino
        ├── QueryResponseAdapter.java           NEW — QueryResults → ExecutionEngine.QueryResponse
        ├── TrinoTypeMapper.java                NEW — Trino type string → ExprType
        ├── cluster/ClusterStateNodeManager.java                     (VERBATIM)
        ├── exchange/Netty4ExchangeTransport.java                    (VERBATIM)
        ├── exchange/ExchangeHttpHandler.java                        (VERBATIM)
        ├── exchange/ApacheHttpClientAdapter.java                    (VERBATIM)
        ├── filesystem/FileSchemeFileSystem.java                     (VERBATIM)
        ├── filesystem/FileSchemeFileSystemFactory.java              (VERBATIM)
        ├── connector/opensearch/…              16 files — OpenSearch native connector (VERBATIM)
        └── ppl/
            ├── PplTranslator.java              (VERBATIM)
            ├── CalciteSchemaAdapter.java       (VERBATIM)
            ├── OmniSqlDialect.java             (VERBATIM)
            └── udf/…                           6 UDFs (VERBATIM)
```

- `omni-engine` depends on `:api` (UnifiedAPI — same codebase as the `unified-query-api` jar omni
  consumed). Swap Maven artifact for project dep.
- `omni-engine` depends on `:opensearch` to reach `IndicesService` access patterns used by the
  OpenSearch native connector.
- `plugin` depends on `:omni-engine`. `core`/`ppl`/`sql`/`opensearch` do not.

### Plugin wiring

Omni's `OmniPlugin` is NOT a separate OpenSearch plugin — its duties fold into the existing
`SQLPlugin`:

- `Plugin.createComponents()` → constructs `ServiceWiring` and `ClusterStateNodeManager`.
- `Plugin.getAuxTransports()` → returns `Netty4ExchangeTransport` supplier.
- `Plugin.additionalSettings()` → publishes `node.attr.omni_exchange_port`.
- `Plugin.getGuiceServiceClasses()` → `IndicesServiceBinder` feeds `IndicesService` into
  the OpenSearch connector.
- `Plugin.getSettings()` → includes `OmniSettings.getSettings()`.
- `Plugin.close()` → `ServiceWiring.close()`.

`OpenSearchPluginModule` binds `OmniEngineService` so `TransportPPLQueryAction` can resolve it via
the existing Guice injector.

### Jar-hell controls (ported from omni `build.gradle`)

- `mergeParquetJars` shadow task — merges 7 parquet jars into one.
- `filterCoralJar` shadow task — strips Calcite/Avatica/slf4j/gson out of the coral fat jar.
- Forced versions: jackson 2.16.1 chain, guava 33, netty, httpclient5 5.2.3, antlr 4.13.2,
  calcite 1.41.0, slf4j-api 2.0.17, jakarta.annotation-api 2.1.1.
- `thirdPartyAudit.enabled = false` and `loggerUsageCheck.enabled = false` on the omni-engine module.
- `bundlePlugin` consumes the merged parquet and filtered coral jars, excludes the originals.

## Contracts preserved

- REST surface: `/_plugins/_ppl`, `/_plugins/_ppl/_explain`, `/_plugins/_ppl/_stats`.
- Transport: `TransportPPLQueryRequest` / `TransportPPLQueryResponse` wire format.
- Formatters: `SimpleJsonResponseFormatter`, `CsvResponseFormatter`, `RawResponseFormatter`,
  `VisualizationResponseFormatter`, `YamlResponseFormatter`.
- Response shape: `ExecutionEngine.QueryResponse(Schema, List<ExprValue>, Cursor)` and
  `ExecutionEngine.ExplainResponse`.
- Error classification: `SyntaxCheckException` / `SemanticCheckException` / `QueryEngineException`
  (derived from Trino `QueryResults.error.errorName`).

## Contracts intentionally broken

- **Explain plan text**. Every golden file under
  `integ-test/src/test/resources/expectedOutput/calcite/` is regenerated from Trino's
  EXPLAIN output; the shape is fundamentally different (distributed fragments vs Calcite RelNode).
- **Fallback behavior**. `plugins.calcite.fallback.allowed` is now a no-op; PPL cannot silently
  fall back to V2 legacy.
- **`plugins.calcite.enabled`**. Becomes a no-op (omni is the only path).
- **Removed classes on the PPL path**:
  `QueryService.executeWithCalcite/executeWithLegacy/explainWithCalcite/explainWithLegacy` —
  the PPL branch is gone; legacy V2 SQL keeps its own path through a different
  `QueryService` call chain.
- **Profile metrics**. `QueryProfiling` for PPL goes away in its current form; omni's stage-level
  timing is exposed through a parallel mechanism (deferred to post-migration plan if metrics
  parity is required).

## Error handling

| Trino error condition              | Mapped to                           |
|------------------------------------|-------------------------------------|
| `SYNTAX_ERROR`                     | `SyntaxCheckException`              |
| `MISSING_TABLE` / `MISSING_COLUMN` | `SemanticCheckException`            |
| dispatch future timeout            | `QueryEngineException("timeout")`   |
| waitForResults exception           | `QueryEngineException(cause)`       |
| any other `QueryResults.error`     | `QueryEngineException(msg)`         |

No catch-and-fallback anywhere in the PPL path.

## Type mapping (Trino client column type → ExprType)

| Trino type                         | ExprType    |
|------------------------------------|-------------|
| `bigint`                           | `LONG`      |
| `integer`                          | `INTEGER`   |
| `smallint`                         | `SHORT`     |
| `tinyint`                          | `BYTE`      |
| `double`, `real`                   | `DOUBLE` / `FLOAT` |
| `boolean`                          | `BOOLEAN`   |
| `varchar`, `varchar(n)`, `char(n)` | `STRING`    |
| `date`                             | `DATE`      |
| `timestamp(p)`                     | `TIMESTAMP` |
| `timestamp(p) with time zone`      | `TIMESTAMP` |
| `decimal(p,s)`                     | `DOUBLE`    |
| `array(T)`                         | `ARRAY`     |
| `row(...)`, `map(...)`             | `STRUCT`    |
| `varbinary`                        | `BINARY`    |
| unknown                            | `UNDEFINED` |

## Explain mode mapping

| PPL `ExplainMode` | Trino `EXPLAIN` variant          |
|-------------------|----------------------------------|
| `STANDARD`        | `EXPLAIN (TYPE DISTRIBUTED)`     |
| `SIMPLE`          | `EXPLAIN (TYPE LOGICAL)`         |
| `EXTENDED`        | `EXPLAIN ANALYZE`                |
| `COST`            | `EXPLAIN (TYPE VALIDATE)` + stats |

## Testing strategy

**In scope.** All tests under:
- `integ-test/src/test/java/org/opensearch/sql/calcite/**`
- `integ-test/src/test/java/org/opensearch/sql/ppl/**` (Calcite-mode runs)
- `integ-test/src/yamlRestTest/resources/rest-api-spec/test/**`

**Golden file regeneration.** `expectedOutput/calcite/*.yaml` will be regenerated from the Omni
EXPLAIN output. This is not a regression — the plan structure changed.

**Deleted.** V2 legacy PPL tests are not in scope; V2 legacy for SQL is untouched.

**Merging omni's own tests.** `PPLClickBenchIT` and `PPLBig5IT` from omni move into
`integ-test/src/test/java/org/opensearch/sql/calcite/` as a regression guard.

## Out of scope for this migration (tracked separately)

- Profile metrics parity (`QueryProfiling`).
- `_cursor` pagination (Trino has its own token-based paging; existing cursor wire format
  is dropped).
- Cross-cluster search tests (`CrossClusterSearchIT`) — Omni cross-cluster story is not proven
  in `feat/ppl`. These tests stay on the exclusion list until a follow-up.
- S3/Hive/Iceberg cross-catalog joins — Omni supports it, but the migration's success metric is
  PPL over OpenSearch indices only.

## Risks

1. **Build time / IDE load**. `omni-engine` is the single largest module (~42MB of java, ~4,725
   files). Compile-incremental should still work but clean builds go up several minutes.
2. **Jar-hell surprises**. The parquet-merge + coral-filter trick worked in omni's standalone
   build; replaying it inside a multi-module build with `buildSrc` opensearch-plugin conventions
   may need adjustments to `bundlePlugin` (which is defined in the `plugin` project, not omni's
   root).
3. **Explain-mode coverage gaps**. `EXTENDED` and `COST` mapping is best-effort. Test YAMLs that
   depend on specific Calcite cost-model output will need rewrites.
4. **Calcite version divergence**. Omni consumed calcite-core 1.41.0; this repo's `api` module
   is on 1.41.0 too (confirmed). If versions drift, the `parquetFilter`/`coralFilter` exclusions
   become load-bearing.

## Build sequence

1. Create empty `omni-engine` module, add to `settings.gradle`, wire minimal `build.gradle`.
2. Copy `io/trino/**` verbatim. Get `./gradlew :omni-engine:compileJava` green.
3. Copy adapter files (`org/opensearch/plugin/omni/**`). Swap Maven
   `unified-query-api` dep for `project(':api')`.
4. Add `OmniEngineService` + adapters.
5. Fold plugin duties into `SQLPlugin`; wire `OmniEngineService` into `TransportPPLQueryAction`.
6. Remove Calcite path from `QueryService`.
7. Regenerate explain goldens. Fix test failures until green.

Each step is a separate implementation-plan phase.
