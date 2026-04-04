# Trino-OpenSearch Native Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Embed Trino's SQL engine inside OpenSearch as a plugin, running all 43 ClickBench queries against a local Iceberg table on a single node.

**Architecture:** Use Trino as Maven dependencies (not vendored source). Write `TrinoEngineBootstrap` that initializes Trino's engine via Airlift Bootstrap — never calling `Server.java`, never starting an HTTP server. All queries go through `/_plugins/_trino_sql/v1/statement` REST endpoint. All tests use Trino's own test framework (`AbstractTestQueries`, `AbstractTestAggregations`, etc.) routed through our REST endpoint — no bypassing.

**TDD Strategy:** Fail first. Every test goes through `/_plugins/_trino_sql/v1/statement`. We reuse Trino's ~480+ test methods across `AbstractTestQueries` (34), `AbstractTestAggregations` (147), `AbstractTestJoinQueries` (147), `AbstractTestEngineOnlyQueries` (120), `AbstractTestWindowQueries` (32).

**Tech Stack:** Java 21, Trino (Java 21 compatible version), Apache Iceberg, OpenSearch 3.x plugin framework, Gradle

**Design Spec:** `docs/plans/2026-04-04-trino-opensearch-native-integration-design.md`

---

## TDD Progression

```
Phase 1: Scaffold + Failing Tests
  Task 1 — Gradle modules, dependencies resolve
  Task 2 — OpenSearchTrinoQueryRunner: sends queries via HTTP to /_plugins/_trino_sql
  Task 3 — TrinoPlugin + REST endpoint + TrinoEngine bootstrap (NO server)
  Task 4 — Wire REST endpoint to TrinoEngine: SELECT 1 passes

Phase 2: Trino Test Suites (all via /_plugins/_trino_sql)
  Task 5 — AbstractTestQueries: 34 tests pass
  Task 6 — AbstractTestAggregations: 147 tests pass
  Task 7 — AbstractTestJoinQueries: 147 tests pass

Phase 3: OpenSearch Adapters (replace Trino internals)
  Task 8 — OpenSearchNodeManager replaces Trino's node discovery
  Task 9 — OpenSearchTaskExecutor replaces Trino's thread pools
  Task 10 — CircuitBreakerMemoryPool replaces Trino's memory management

Phase 4: ClickBench
  Task 11 — Iceberg connector + ClickBench 43 queries
```

**Critical invariant:** After every task, all previously passing tests still pass. No regressions.

---

## File Structure

### New Module: `trino/trino-opensearch/`

```
trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/
├── plugin/
│   ├── TrinoPlugin.java                    # OpenSearch Plugin entry point
│   └── TrinoSettings.java                  # Plugin settings
├── bootstrap/
│   ├── TrinoEngineBootstrap.java           # Initializes Trino engine (NO server, NO ports)
│   ├── OpenSearchServerMainModule.java     # Guice module — our bindings
│   └── TrinoEngine.java                    # Facade: submitQuery(), getResults(), cancel()
├── node/
│   └── OpenSearchNodeManager.java          # InternalNodeManager via ClusterStateListener
├── execution/
│   └── OpenSearchTaskExecutor.java         # TaskExecutor via OpenSearch thread pools
├── memory/
│   └── CircuitBreakerMemoryPool.java       # MemoryPool via circuit breaker
├── transport/
│   ├── TransportRemoteTaskFactory.java     # Creates TransportRemoteTask instances
│   ├── TransportRemoteTask.java            # RemoteTask via TransportActions
│   └── ...Action.java                      # Transport actions (task/update, task/results, etc.)
└── rest/
    └── RestTrinoQueryAction.java           # /_plugins/_trino_sql/v1/statement
```

### New Module: `trino/trino-integ-test/`

```
trino/trino-integ-test/src/test/java/org/opensearch/sql/trino/
├── OpenSearchTrinoQueryRunner.java         # QueryRunner → HTTP → /_plugins/_trino_sql
├── TestTrinoQueries.java                   # extends AbstractTestQueries (34 tests)
├── TestTrinoAggregations.java              # extends AbstractTestAggregations (147 tests)
├── TestTrinoJoinQueries.java               # extends AbstractTestJoinQueries (147 tests)
├── TestTrinoEngineOnlyQueries.java         # extends AbstractTestEngineOnlyQueries (120 tests)
├── TestTrinoWindowQueries.java             # extends AbstractTestWindowQueries (32 tests)
├── ClickBenchIT.java                       # 43 ClickBench queries
└── NoExternalServerTest.java               # Verifies no Trino HTTP server running
```

---

## Task 1: Gradle Scaffolding + Dependencies Resolve

**Files:**
- Create: `trino/build.gradle`
- Create: `trino/trino-opensearch/build.gradle`
- Create: `trino/trino-integ-test/build.gradle`
- Modify: `settings.gradle`

- [ ] **Step 1: Add modules to settings.gradle**

Append before the `if (!gradle.startParameter.offline)` block:

```groovy
include 'trino'
include 'trino:trino-opensearch'
project(':trino:trino-opensearch').projectDir = file('trino/trino-opensearch')
include 'trino:trino-integ-test'
project(':trino:trino-integ-test').projectDir = file('trino/trino-integ-test')
```

- [ ] **Step 2: Create trino/build.gradle**

```groovy
subprojects {
    ext {
        // Trino 440 is approximately the last Java 21 compatible version.
        // If compilation fails, try 439, 438, etc.
        trinoVersion = '440'
    }
}
```

- [ ] **Step 3: Create trino/trino-opensearch/build.gradle**

```groovy
plugins {
    id 'java-library'
}

repositories {
    mavenCentral()
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

dependencies {
    // Trino engine — used as library, NEVER as server
    api "io.trino:trino-main:${trinoVersion}"
    api "io.trino:trino-parser:${trinoVersion}"
    api "io.trino:trino-spi:${trinoVersion}"
    api "io.trino:trino-plugin-toolkit:${trinoVersion}"

    // OpenSearch APIs (provided by plugin runtime)
    compileOnly "org.opensearch:opensearch:${opensearch_version}"

    testImplementation "org.opensearch.test:framework:${opensearch_version}"
    testImplementation 'org.junit.jupiter:junit-jupiter:5.9.3'
    testImplementation "org.mockito:mockito-core:${versions.mockito}"
}
```

- [ ] **Step 4: Create trino/trino-integ-test/build.gradle**

```groovy
plugins {
    id 'java'
    id 'opensearch.rest-test'
}

repositories {
    mavenCentral()
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

dependencies {
    testImplementation project(':trino:trino-opensearch')

    // Trino test framework
    testImplementation "io.trino:trino-testing:${trinoVersion}"
    testImplementation("io.trino:trino-main:${trinoVersion}") {
        artifact { classifier = 'tests' }
    }
    testImplementation "io.trino:trino-tpch:${trinoVersion}"
    testImplementation "io.trino:trino-iceberg:${trinoVersion}"
    testImplementation "io.trino:trino-hive:${trinoVersion}"
    testImplementation "io.trino:trino-exchange-filesystem:${trinoVersion}"
    testImplementation "io.trino:trino-jdbc:${trinoVersion}"

    testImplementation 'org.junit.jupiter:junit-jupiter:5.9.3'
    testImplementation 'org.assertj:assertj-core:3.24.2'

    // OpenSearch test framework
    testImplementation "org.opensearch.test:framework:${opensearch_version}"
}

// OpenSearch test cluster with our plugin
testClusters.integTest {
    plugin ':opensearch-sql-plugin'
    testDistribution = 'archive'
    setting 'plugins.trino.enabled', 'true'
}

test {
    useJUnitPlatform()
    jvmArgs '-Xmx4g', '-XX:+UseG1GC'
    jvmArgs '--add-opens=java.base/java.lang=ALL-UNNAMED',
             '--add-opens=java.base/java.nio=ALL-UNNAMED'
}
```

- [ ] **Step 5: Create directory structure**

```bash
mkdir -p trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/{plugin,bootstrap,node,execution,memory,transport,rest}
mkdir -p trino/trino-opensearch/src/test/java/org/opensearch/sql/trino
mkdir -p trino/trino-integ-test/src/test/java/org/opensearch/sql/trino
```

- [ ] **Step 6: Verify dependencies resolve**

```bash
./gradlew :trino:trino-opensearch:dependencies
./gradlew :trino:trino-integ-test:dependencies
```

If Java version mismatch: try trinoVersion 439, 438, etc. Update `trino/build.gradle`.

- [ ] **Step 7: Verify compilation (empty modules)**

```bash
./gradlew :trino:trino-opensearch:compileJava
./gradlew :trino:trino-integ-test:compileTestJava
```

Expected: PASS (no source files yet, just dependency resolution).

- [ ] **Step 8: Commit**

```bash
git add trino/ settings.gradle
git commit -s -m "feat(trino): scaffold Gradle modules with Trino dependencies"
```

---

## Task 2: OpenSearchTrinoQueryRunner — Queries via HTTP

**Files:**
- Create: `trino/trino-integ-test/src/test/java/org/opensearch/sql/trino/OpenSearchTrinoQueryRunner.java`
- Create: `trino/trino-integ-test/src/test/java/org/opensearch/sql/trino/TestTrinoQueries.java`

This is the **key adapter**: a Trino `QueryRunner` that sends all queries via HTTP to `/_plugins/_trino_sql/v1/statement`, parses Trino-format JSON responses into `MaterializedResult` for the test framework.

- [ ] **Step 1: Write TestTrinoQueries extending AbstractTestQueries**

```java
package org.opensearch.sql.trino;

import io.trino.testing.AbstractTestQueries;
import io.trino.testing.QueryRunner;

/**
 * Trino's 34-test SQL correctness suite.
 * Every query goes through /_plugins/_trino_sql/v1/statement.
 * Results compared against H2 oracle by AbstractTestQueryFramework.
 */
public class TestTrinoQueries extends AbstractTestQueries {

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        return OpenSearchTrinoQueryRunner.builder()
            .setOpenSearchPort(9200) // Will be dynamic from test cluster
            .addTpchTables()         // Load TPC-H tiny tables
            .build();
    }
}
```

- [ ] **Step 2: Implement OpenSearchTrinoQueryRunner**

```java
package org.opensearch.sql.trino;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.Session;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.BooleanType;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingTrinoClient;

import java.io.Closeable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * QueryRunner that routes ALL queries through /_plugins/_trino_sql/v1/statement.
 *
 * This is the bridge between Trino's test framework (AbstractTestQueries etc.)
 * and our OpenSearch REST endpoint. The test framework calls execute(session, sql),
 * we send it via HTTP, parse the Trino-format JSON response, and return
 * MaterializedResult for assertion.
 *
 * Uses the Trino /v1/statement pagination protocol:
 *   POST → first response (columns + first data batch + nextUri)
 *   GET nextUri → subsequent pages
 *   Repeat until nextUri absent
 */
public class OpenSearchTrinoQueryRunner implements QueryRunner {

    private final HttpClient httpClient;
    private final String baseUrl;
    private final ObjectMapper mapper;
    private final Lock lock = new ReentrantLock();

    private OpenSearchTrinoQueryRunner(String baseUrl) {
        this.httpClient = HttpClient.newHttpClient();
        this.baseUrl = baseUrl;
        this.mapper = new ObjectMapper();
    }

    @Override
    public MaterializedResult execute(Session session, String sql) {
        try {
            // POST query
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/_plugins/_trino_sql/v1/statement"))
                .header("Content-Type", "application/json")
                .header("X-Trino-Catalog", session.getCatalog().orElse("tpch"))
                .header("X-Trino-Schema", session.getSchema().orElse("tiny"))
                .POST(HttpRequest.BodyPublishers.ofString(
                    "{\"query\": " + mapper.writeValueAsString(sql) + "}"))
                .build();

            HttpResponse<String> response = httpClient.send(request,
                HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new RuntimeException("Query failed: " + response.body());
            }

            // Parse and follow pagination
            return drainResults(response.body());
        } catch (Exception e) {
            throw new RuntimeException("Query execution failed: " + sql, e);
        }
    }

    /**
     * Follow nextUri pagination until query completes.
     * Accumulate all data rows into a single MaterializedResult.
     */
    private MaterializedResult drainResults(String firstResponseBody) throws Exception {
        JsonNode json = mapper.readTree(firstResponseBody);
        List<Type> types = new ArrayList<>();
        List<MaterializedRow> rows = new ArrayList<>();

        // Parse columns from first response
        if (json.has("columns")) {
            for (JsonNode col : json.get("columns")) {
                types.add(parseType(col.get("type").asText()));
            }
        }

        // Collect data rows
        collectData(json, rows, types);

        // Follow nextUri
        while (json.has("nextUri")) {
            String nextUri = json.get("nextUri").asText();
            HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(resolveNextUri(nextUri)))
                .GET()
                .build();
            HttpResponse<String> resp = httpClient.send(req,
                HttpResponse.BodyHandlers.ofString());
            json = mapper.readTree(resp.body());
            collectData(json, rows, types);
        }

        // Check for error
        if (json.has("error")) {
            throw new RuntimeException("Trino query error: "
                + json.get("error").get("message").asText());
        }

        return new MaterializedResult(rows, types);
    }

    private void collectData(JsonNode json, List<MaterializedRow> rows, List<Type> types) {
        if (json.has("data")) {
            for (JsonNode row : json.get("data")) {
                List<Object> values = new ArrayList<>();
                for (int i = 0; i < row.size(); i++) {
                    values.add(parseValue(row.get(i), i < types.size() ? types.get(i) : VarcharType.VARCHAR));
                }
                rows.add(new MaterializedRow(MaterializedRow.DEFAULT_PRECISION, values));
            }
        }
    }

    private Type parseType(String trinoType) {
        // Map Trino type names to Type objects
        // This will need expansion as tests exercise more types
        return switch (trinoType.toLowerCase()) {
            case "bigint" -> BigintType.BIGINT;
            case "integer" -> IntegerType.INTEGER;
            case "double" -> DoubleType.DOUBLE;
            case "boolean" -> BooleanType.BOOLEAN;
            case "varchar" -> VarcharType.VARCHAR;
            default -> {
                if (trinoType.toLowerCase().startsWith("varchar")) yield VarcharType.VARCHAR;
                yield VarcharType.VARCHAR; // Fallback, expand as needed
            }
        };
    }

    private Object parseValue(JsonNode node, Type type) {
        if (node.isNull()) return null;
        if (type == BigintType.BIGINT) return node.asLong();
        if (type == IntegerType.INTEGER) return (int) node.asLong();
        if (type == DoubleType.DOUBLE) return node.asDouble();
        if (type == BooleanType.BOOLEAN) return node.asBoolean();
        return node.asText();
    }

    private String resolveNextUri(String nextUri) {
        if (nextUri.startsWith("http")) return nextUri;
        return baseUrl + nextUri;
    }

    // --- QueryRunner interface methods ---

    @Override
    public void close() {
        // HttpClient doesn't need explicit close in Java 21
    }

    @Override
    public int getNodeCount() {
        return 1;
    }

    @Override
    public Session getDefaultSession() {
        return Session.builder(new io.trino.spi.security.Identity.Builder("test").build())
            .setCatalog("tpch")
            .setSchema("tiny")
            .build();
    }

    @Override
    public Lock getExclusiveLock() {
        return lock;
    }

    // --- Builder ---

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int port = 9200;
        private boolean tpch = false;

        public Builder setOpenSearchPort(int port) {
            this.port = port;
            return this;
        }

        public Builder addTpchTables() {
            this.tpch = true;
            return this;
        }

        public OpenSearchTrinoQueryRunner build() throws Exception {
            String baseUrl = "http://localhost:" + port;
            OpenSearchTrinoQueryRunner runner = new OpenSearchTrinoQueryRunner(baseUrl);

            if (tpch) {
                // TPC-H tables are loaded by the Trino engine inside OpenSearch
                // The plugin must initialize the tpch catalog on startup
            }

            return runner;
        }
    }
}
```

**Note:** This is a simplified implementation. Trino's `QueryRunner` interface has more methods (`getTransactionManager()`, `getPlannerContext()`, etc.). Many of these are only used by `DistributedQueryRunner` for in-process access. For HTTP-based testing, some methods will throw `UnsupportedOperationException` and we'll adapt as specific tests need them.

The key methods are:
- `execute(session, sql)` — sends HTTP, parses response → `MaterializedResult`
- `getDefaultSession()` — returns test session
- `close()` — cleanup

- [ ] **Step 3: Verify compilation**

```bash
./gradlew :trino:trino-integ-test:compileTestJava
```

Expected: Compilation errors from missing `QueryRunner` interface methods. Fix by adding stub implementations that throw `UnsupportedOperationException`. The important methods are `execute()` and `getDefaultSession()`.

- [ ] **Step 4: Run TestTrinoQueries — verify it fails**

```bash
./gradlew :trino:trino-integ-test:test --tests "*TestTrinoQueries*" -v
```

Expected: FAIL — no OpenSearch cluster running, connection refused. This is the correct failure.

- [ ] **Step 5: Commit**

```bash
git add trino/
git commit -s -m "feat(trino): OpenSearchTrinoQueryRunner sends queries via HTTP to /_plugins/_trino_sql"
```

---

## Task 3: TrinoPlugin + REST Endpoint + TrinoEngine Bootstrap

**Files:**
- Create: `trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/plugin/TrinoPlugin.java`
- Create: `trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/plugin/TrinoSettings.java`
- Create: `trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/rest/RestTrinoQueryAction.java`
- Create: `trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/bootstrap/TrinoEngineBootstrap.java`
- Create: `trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/bootstrap/TrinoEngine.java`
- Create: `trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/bootstrap/OpenSearchServerMainModule.java`
- Modify: `plugin/build.gradle`
- Create: `trino/trino-integ-test/src/test/java/org/opensearch/sql/trino/NoExternalServerTest.java`

- [ ] **Step 1: Write failing integration test — no Trino server running**

```java
package org.opensearch.sql.trino;

import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;

/**
 * Verifies:
 * 1. No Trino HTTP server is running (port 8080 is free)
 * 2. Our REST endpoint responds
 * 3. Trino thread pools are registered
 */
public class NoExternalServerTest extends OpenSearchRestTestCase {

    @Test
    public void noTrinoHttpServerOnPort8080() throws Exception {
        // If we can bind to 8080, nothing is listening there
        try (ServerSocket ss = new ServerSocket(8080)) {
            assertTrue("Port 8080 should be free", true);
        }
    }

    @Test
    public void trinoEndpointResponds() throws Exception {
        Request request = new Request("POST", "/_plugins/_trino_sql/v1/statement");
        request.setJsonEntity("{\"query\": \"SELECT 1\"}");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    @Test
    public void trinoThreadPoolsRegistered() throws Exception {
        Request request = new Request("GET", "/_cat/thread_pool?v&h=name");
        Response response = client().performRequest(request);
        String body = new String(
            response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(body, containsString("trino_query"));
        assertThat(body, containsString("trino_exchange"));
        assertThat(body, containsString("trino_scheduler"));
    }
}
```

- [ ] **Step 2: Implement TrinoSettings**

```java
package org.opensearch.sql.trino.plugin;

import org.opensearch.common.settings.Setting;
import java.util.List;

public class TrinoSettings {
    public static final Setting<Boolean> TRINO_ENABLED = Setting.boolSetting(
        "plugins.trino.enabled", false,
        Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<String> TRINO_CATALOG_WAREHOUSE = Setting.simpleString(
        "plugins.trino.catalog.iceberg.warehouse", "",
        Setting.Property.NodeScope);

    public static List<Setting<?>> settings() {
        return List.of(TRINO_ENABLED, TRINO_CATALOG_WAREHOUSE);
    }
}
```

- [ ] **Step 3: Implement TrinoEngineBootstrap and TrinoEngine**

`TrinoEngineBootstrap.java`:

```java
package org.opensearch.sql.trino.bootstrap;

import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeModule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Bootstraps Trino's query engine inside OpenSearch.
 *
 * CRITICAL: NEVER calls Server.java. NEVER starts HTTP server. NEVER opens ports.
 * Uses Airlift Bootstrap for Guice initialization ONLY.
 */
public class TrinoEngineBootstrap {

    public static TrinoEngine create(Map<String, String> properties) throws Exception {
        List<Module> modules = new ArrayList<>();

        // Airlift infrastructure — NO HttpServerModule, NO JaxrsModule
        modules.add(new NodeModule());
        modules.add(new JsonModule());

        // Trino engine core — our bindings, NOT ServerMainModule
        modules.add(new OpenSearchServerMainModule());

        Bootstrap bootstrap = new Bootstrap(modules);
        properties.forEach(bootstrap::setOptionalConfigurationProperty);

        // CRITICAL: Disable HTTP server
        bootstrap.setRequiredConfigurationProperty("http-server.http.enabled", "false");

        Injector injector = bootstrap.initialize();
        return new TrinoEngine(injector);
    }

    public static TrinoEngine createDefault() throws Exception {
        return create(Map.of(
            "node.environment", "opensearch",
            "node.id", "local"
        ));
    }
}
```

`TrinoEngine.java`:

```java
package org.opensearch.sql.trino.bootstrap;

import com.google.inject.Injector;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;

/**
 * Facade for Trino's query engine. A plain Java object — NOT a server.
 */
public class TrinoEngine {

    private final Injector injector;
    private final SqlParser sqlParser;

    TrinoEngine(Injector injector) {
        this.injector = injector;
        this.sqlParser = injector.getInstance(SqlParser.class);
    }

    public Statement parse(String sql) {
        return sqlParser.createStatement(sql);
    }

    public Injector getInjector() {
        return injector;
    }

    public void shutdown() throws Exception {
        injector.getInstance(LifeCycleManager.class).stop();
    }
}
```

`OpenSearchServerMainModule.java`:

```java
package org.opensearch.sql.trino.bootstrap;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.sql.parser.SqlParser;

/**
 * Guice module for Trino engine — no server infrastructure.
 *
 * This starts minimal and grows iteratively. Each test failure
 * reveals the next missing binding to add.
 */
public class OpenSearchServerMainModule implements Module {
    @Override
    public void configure(Binder binder) {
        // Start with SqlParser only. Add more bindings as tests demand.
        binder.bind(SqlParser.class).in(Scopes.SINGLETON);
    }
}
```

- [ ] **Step 4: Implement RestTrinoQueryAction**

```java
package org.opensearch.sql.trino.rest;

import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.sql.trino.bootstrap.TrinoEngine;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implements Trino's /v1/statement wire protocol.
 * Enables: Trino JDBC driver, Trino test suite, ClickBench comparison.
 */
public class RestTrinoQueryAction extends BaseRestHandler {

    private static final String BASE = "/_plugins/_trino_sql/v1/statement";
    private TrinoEngine engine;

    public void setEngine(TrinoEngine engine) {
        this.engine = engine;
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.POST, BASE),
            new Route(RestRequest.Method.GET, BASE + "/{queryId}/{token}"),
            new Route(RestRequest.Method.DELETE, BASE + "/{queryId}"));
    }

    @Override
    public String getName() {
        return "trino_query_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return switch (request.method()) {
            case POST -> handleSubmit(request);
            case GET -> handleFetch(request);
            case DELETE -> handleCancel(request);
            default -> channel -> channel.sendResponse(
                new BytesRestResponse(RestStatus.METHOD_NOT_ALLOWED, "", ""));
        };
    }

    private RestChannelConsumer handleSubmit(RestRequest request) {
        String sql = new JSONObject(request.content().utf8ToString()).getString("query");
        return channel -> {
            try {
                // TODO: Wire to full query execution pipeline
                // For now: stub response that allows tests to progress
                JSONObject resp = new JSONObject();
                resp.put("id", UUID.randomUUID().toString());
                resp.put("stats", new JSONObject()
                    .put("state", "FINISHED")
                    .put("queued", false)
                    .put("scheduled", true)
                    .put("nodes", 1)
                    .put("totalSplits", 0)
                    .put("completedSplits", 0));
                channel.sendResponse(new BytesRestResponse(
                    RestStatus.OK, "application/json", resp.toString()));
            } catch (Exception e) {
                JSONObject error = new JSONObject();
                error.put("error", new JSONObject()
                    .put("message", e.getMessage())
                    .put("errorCode", 1)
                    .put("errorName", "GENERIC_INTERNAL_ERROR")
                    .put("errorType", "INTERNAL_ERROR"));
                channel.sendResponse(new BytesRestResponse(
                    RestStatus.INTERNAL_SERVER_ERROR, "application/json", error.toString()));
            }
        };
    }

    private RestChannelConsumer handleFetch(RestRequest request) {
        return channel -> channel.sendResponse(
            new BytesRestResponse(RestStatus.NOT_FOUND, "application/json", "{}"));
    }

    private RestChannelConsumer handleCancel(RestRequest request) {
        return channel -> channel.sendResponse(
            new BytesRestResponse(RestStatus.NO_CONTENT, "", ""));
    }
}
```

- [ ] **Step 5: Implement TrinoPlugin**

```java
package org.opensearch.sql.trino.plugin;

import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.sql.trino.bootstrap.TrinoEngine;
import org.opensearch.sql.trino.bootstrap.TrinoEngineBootstrap;
import org.opensearch.sql.trino.node.OpenSearchNodeManager;
import org.opensearch.sql.trino.rest.RestTrinoQueryAction;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * OpenSearch plugin embedding Trino's SQL engine.
 * NEVER starts a Trino server. NEVER opens extra ports.
 */
public class TrinoPlugin extends Plugin implements ActionPlugin {

    public static final String TRINO_QUERY_POOL = "trino_query";
    public static final String TRINO_EXCHANGE_POOL = "trino_exchange";
    public static final String TRINO_SCHEDULER_POOL = "trino_scheduler";

    private TrinoEngine engine;
    private OpenSearchNodeManager nodeManager;
    private RestTrinoQueryAction restAction;

    @Override
    public Collection<Object> createComponents(
            Client client, ClusterService clusterService, ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService, ScriptService scriptService,
            NamedXContentRegistry contentRegistry, Environment environment,
            NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver indexNameResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier) {

        if (!TrinoSettings.TRINO_ENABLED.get(environment.settings())) {
            return List.of();
        }

        nodeManager = new OpenSearchNodeManager(clusterService.localNode());
        clusterService.addListener(nodeManager);

        try {
            engine = TrinoEngineBootstrap.createDefault();
        } catch (Exception e) {
            throw new RuntimeException("Failed to bootstrap Trino engine", e);
        }

        if (restAction != null) {
            restAction.setEngine(engine);
        }

        return List.of(engine, nodeManager);
    }

    @Override
    public List<RestHandler> getRestHandlers(
            Settings settings, RestController restController,
            ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        if (!TrinoSettings.TRINO_ENABLED.get(settings)) {
            return List.of();
        }
        restAction = new RestTrinoQueryAction();
        if (engine != null) {
            restAction.setEngine(engine);
        }
        return List.of(restAction);
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        int processors = Runtime.getRuntime().availableProcessors();
        return List.of(
            new FixedExecutorBuilder(settings, TRINO_QUERY_POOL,
                processors * 2, 1000, "thread_pool." + TRINO_QUERY_POOL),
            new ScalingExecutorBuilder(TRINO_EXCHANGE_POOL,
                1, processors,
                org.opensearch.common.unit.TimeValue.timeValueMinutes(5)),
            new FixedExecutorBuilder(settings, TRINO_SCHEDULER_POOL,
                4, 100, "thread_pool." + TRINO_SCHEDULER_POOL));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return TrinoSettings.settings();
    }

    @Override
    public void close() {
        if (engine != null) {
            try { engine.shutdown(); } catch (Exception ignored) {}
        }
    }
}
```

- [ ] **Step 6: Add dependency in plugin/build.gradle**

```groovy
api project(':trino:trino-opensearch')
```

And register Trino handlers/settings in `SQLPlugin.java`, or register `TrinoPlugin` as a separate extension.

- [ ] **Step 7: Run NoExternalServerTest**

```bash
./gradlew :trino:trino-integ-test:integTest --tests "*NoExternalServerTest*" -v
```

Expected: `noTrinoHttpServerOnPort8080` PASSES. `trinoEndpointResponds` PASSES (stub response). `trinoThreadPoolsRegistered` PASSES.

- [ ] **Step 8: Commit**

```bash
git add trino/ plugin/
git commit -s -m "feat(trino): TrinoPlugin with REST endpoint, engine bootstrap, no server"
```

---

## Task 4: Wire REST Endpoint to Trino Engine — SELECT 1 Passes

**Files:**
- Modify: `trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/rest/RestTrinoQueryAction.java`
- Modify: `trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/bootstrap/TrinoEngine.java`
- Modify: `trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/bootstrap/OpenSearchServerMainModule.java`

This is the most iterative task. We wire the REST endpoint to the actual Trino engine and make `SELECT 1` return real results through the full stack.

- [ ] **Step 1: Write failing test**

```java
// Add to NoExternalServerTest.java or create SelectOneIT.java

@Test
public void selectOneReturnsCorrectResult() throws Exception {
    Request request = new Request("POST", "/_plugins/_trino_sql/v1/statement");
    request.setJsonEntity("{\"query\": \"SELECT 1\"}");
    Response response = client().performRequest(request);

    String body = new String(
        response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
    JSONObject json = new JSONObject(body);

    // Must have columns and data in Trino format
    assertTrue(json.has("columns"));
    assertTrue(json.has("data"));
    assertEquals(1, json.getJSONArray("data").getJSONArray(0).getInt(0));
}
```

- [ ] **Step 2: Run test — verify it fails**

```bash
./gradlew :trino:trino-integ-test:integTest --tests "*selectOneReturnsCorrectResult*" -v
```

Expected: FAIL — REST endpoint returns stub response, not real query results.

- [ ] **Step 3: Extend TrinoEngine with query execution**

This step is inherently iterative. Trino's `DispatchManager` handles query lifecycle. We need to:
1. Submit query to Trino's internal `DispatchManager`
2. Wait for results
3. Serialize results as Trino-format JSON

Each missing Guice binding in `OpenSearchServerMainModule` reveals the next dependency to add. The iteration loop:

```
Run test → read error → add binding to OpenSearchServerMainModule → re-run
```

Key bindings that will be needed (added incrementally):
- `SqlParser` (already have)
- `StatementAnalyzerFactory` (query analysis)
- `PlanOptimizers` (cost-based optimizer)
- `LocalExecutionPlanner` (physical plan generation)
- `TypeRegistry` (type system)
- `FunctionManager` (built-in functions)
- `MemoryPool` / `LocalMemoryManager` (memory accounting)
- `TaskExecutor` (split execution)
- `InternalNodeManager` (node topology)
- `SplitManager` (split enumeration)
- `PageSourceManager` (data reading)
- `ExchangeManagerRegistry`
- `TransactionManager`

- [ ] **Step 4: Implement query execution in TrinoEngine**

```java
// Add to TrinoEngine.java — actual implementation depends on Trino 440 API

import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryInfo;
import io.trino.server.ResultQueryInfo;

public String executeAndSerialize(String sql, String catalog, String schema) {
    // Use Trino's internal APIs to:
    // 1. Create a session
    // 2. Submit query via DispatchManager
    // 3. Wait for completion
    // 4. Serialize results as Trino /v1/statement JSON

    // The exact API calls depend on Trino 440's internal structure.
    // This is discovered iteratively by adding Guice bindings and
    // tracing through DispatchManager → QueryExecution → QueryStateMachine
    throw new UnsupportedOperationException("Iterative implementation");
}
```

- [ ] **Step 5: Update RestTrinoQueryAction to use real engine**

Replace the stub in `handleSubmit()`:
```java
private RestChannelConsumer handleSubmit(RestRequest request) {
    String sql = new JSONObject(request.content().utf8ToString()).getString("query");
    String catalog = request.header("X-Trino-Catalog", "tpch");
    String schema = request.header("X-Trino-Schema", "tiny");
    return channel -> {
        try {
            String result = engine.executeAndSerialize(sql, catalog, schema);
            channel.sendResponse(new BytesRestResponse(
                RestStatus.OK, "application/json", result));
        } catch (Exception e) {
            // Trino-format error response
            JSONObject error = new JSONObject();
            error.put("error", new JSONObject()
                .put("message", e.getMessage())
                .put("errorCode", 1)
                .put("errorName", "GENERIC_INTERNAL_ERROR")
                .put("errorType", "INTERNAL_ERROR"));
            error.put("stats", new JSONObject().put("state", "FAILED"));
            channel.sendResponse(new BytesRestResponse(
                RestStatus.OK, "application/json", error.toString()));
        }
    };
}
```

- [ ] **Step 6: Iterate until SELECT 1 passes**

```bash
./gradlew :trino:trino-integ-test:integTest --tests "*selectOneReturnsCorrectResult*" -v
```

This may take many iterations. Each run reveals the next missing binding. Keep going until `SELECT 1` returns `{"columns":[...],"data":[[1]],"stats":{"state":"FINISHED",...}}`.

- [ ] **Step 7: Verify no regression on NoExternalServerTest**

```bash
./gradlew :trino:trino-integ-test:integTest --tests "*NoExternalServerTest*" -v
```

Expected: All PASS — especially `noTrinoHttpServerOnPort8080`.

- [ ] **Step 8: Commit**

```bash
git add trino/
git commit -s -m "feat(trino): SELECT 1 working end-to-end via /_plugins/_trino_sql"
```

---

## Task 5: AbstractTestQueries — 34 Tests Pass

**Files:**
- Modify: `trino/trino-integ-test/src/test/java/org/opensearch/sql/trino/TestTrinoQueries.java`
- Modify: `trino/trino-integ-test/src/test/java/org/opensearch/sql/trino/OpenSearchTrinoQueryRunner.java`

- [ ] **Step 1: Configure TestTrinoQueries to connect to OpenSearch test cluster**

```java
package org.opensearch.sql.trino;

import io.trino.testing.AbstractTestQueries;
import io.trino.testing.QueryRunner;

public class TestTrinoQueries extends AbstractTestQueries {

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        // Test cluster URL comes from system property set by OpenSearch test framework
        String host = System.getProperty("tests.rest.cluster", "http://localhost:9200");
        return OpenSearchTrinoQueryRunner.builder()
            .setBaseUrl(host)
            .build();
    }
}
```

- [ ] **Step 2: Run all 34 tests — record baseline**

```bash
./gradlew :trino:trino-integ-test:integTest --tests "*TestTrinoQueries*" -v 2>&1 | tee trino-test-baseline.txt
grep -E "(PASSED|FAILED)" trino-test-baseline.txt | sort
```

Expected: Many failures initially. This is normal — each failure reveals what needs to be implemented next.

- [ ] **Step 3: Fix failures iteratively**

Common categories of failure:
1. **Type parsing issues** — `OpenSearchTrinoQueryRunner.parseType()` needs more types. Add them.
2. **Missing catalogs** — TPC-H catalog must be loaded by the engine. Add `TpchPlugin` to bootstrap.
3. **Missing functions** — Built-in functions not bound. Add `FunctionManager` to Guice module.
4. **Pagination issues** — `nextUri` handling in `OpenSearchTrinoQueryRunner.drainResults()` needs fixing.
5. **Session handling** — `X-Trino-Catalog` / `X-Trino-Schema` headers not propagated.

For each failure:
1. Read the error
2. Fix in the appropriate file
3. Re-run that specific test: `--tests "*TestTrinoQueries.testCountAll*"`
4. When it passes, run the full suite to check for regressions

- [ ] **Step 4: All 34 tests pass**

```bash
./gradlew :trino:trino-integ-test:integTest --tests "*TestTrinoQueries*" -v
```

Expected: 34/34 PASS

- [ ] **Step 5: Commit**

```bash
git add trino/
git commit -s -m "feat(trino): AbstractTestQueries 34/34 tests passing via /_plugins/_trino_sql"
```

---

## Task 6: AbstractTestAggregations — 147 Tests Pass

**Files:**
- Create: `trino/trino-integ-test/src/test/java/org/opensearch/sql/trino/TestTrinoAggregations.java`

- [ ] **Step 1: Create test class**

```java
package org.opensearch.sql.trino;

import io.trino.testing.AbstractTestAggregations;
import io.trino.testing.QueryRunner;

public class TestTrinoAggregations extends AbstractTestAggregations {

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        String host = System.getProperty("tests.rest.cluster", "http://localhost:9200");
        return OpenSearchTrinoQueryRunner.builder()
            .setBaseUrl(host)
            .build();
    }
}
```

- [ ] **Step 2: Run — record baseline**

```bash
./gradlew :trino:trino-integ-test:integTest --tests "*TestTrinoAggregations*" -v 2>&1 | tee agg-baseline.txt
```

- [ ] **Step 3: Fix failures iteratively**

Aggregation-specific issues:
- GROUP BY, ROLLUP, CUBE, GROUPING SETS require full planner support
- `approx_distinct`, `approx_percentile` require HyperLogLog functions
- Complex aggregation queries stress the optimizer and exchange operators

- [ ] **Step 4: All 147 tests pass**

```bash
./gradlew :trino:trino-integ-test:integTest --tests "*TestTrinoAggregations*" -v
```

Expected: 147/147 PASS

- [ ] **Step 5: Verify no regression on TestTrinoQueries**

```bash
./gradlew :trino:trino-integ-test:integTest --tests "*TestTrinoQueries*" -v
```

Expected: Still 34/34 PASS

- [ ] **Step 6: Commit**

```bash
git add trino/
git commit -s -m "feat(trino): AbstractTestAggregations 147/147 tests passing"
```

---

## Task 7: AbstractTestJoinQueries — 147 Tests Pass

**Files:**
- Create: `trino/trino-integ-test/src/test/java/org/opensearch/sql/trino/TestTrinoJoinQueries.java`

- [ ] **Step 1: Create test class**

```java
package org.opensearch.sql.trino;

import io.trino.testing.AbstractTestJoinQueries;
import io.trino.testing.QueryRunner;

public class TestTrinoJoinQueries extends AbstractTestJoinQueries {

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        String host = System.getProperty("tests.rest.cluster", "http://localhost:9200");
        return OpenSearchTrinoQueryRunner.builder()
            .setBaseUrl(host)
            .build();
    }
}
```

- [ ] **Step 2: Run and fix iteratively**

```bash
./gradlew :trino:trino-integ-test:integTest --tests "*TestTrinoJoinQueries*" -v
```

JOIN tests stress: hash joins, broadcast joins, cross joins, semi-joins, anti-joins, lateral joins. These require full distributed execution support (even on single node, stages must work).

- [ ] **Step 3: All 147 tests pass + no regressions**

```bash
./gradlew :trino:trino-integ-test:integTest --tests "*TestTrinoQueries*" --tests "*TestTrinoAggregations*" --tests "*TestTrinoJoinQueries*" -v
```

Expected: 34 + 147 + 147 = 328 PASS

- [ ] **Step 4: Commit**

```bash
git add trino/
git commit -s -m "feat(trino): AbstractTestJoinQueries 147/147 tests passing"
```

---

## Task 8: OpenSearchNodeManager (Replace Trino Node Discovery)

**Files:**
- Create: `trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/node/OpenSearchNodeManager.java`
- Create: `trino/trino-opensearch/src/test/java/org/opensearch/sql/trino/node/OpenSearchNodeManagerTest.java`
- Modify: `trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/bootstrap/OpenSearchServerMainModule.java`

- [ ] **Step 1: Write unit tests**

(Same as previously defined in Task 4 of the old plan — tests for local node, cluster state changes, listener firing, removed node detection.)

- [ ] **Step 2: Implement OpenSearchNodeManager**

(Same implementation as previously defined — `InternalNodeManager` backed by `ClusterStateListener`.)

- [ ] **Step 3: Bind in OpenSearchServerMainModule**

```java
binder.bind(InternalNodeManager.class).to(OpenSearchNodeManager.class).in(Scopes.SINGLETON);
```

- [ ] **Step 4: Run all 328 tests — no regressions**

```bash
./gradlew :trino:trino-integ-test:integTest --tests "*TestTrino*" -v
```

Expected: All still PASS. The adapter replaced Trino's internal node manager transparently.

- [ ] **Step 5: Commit**

```bash
git add trino/
git commit -s -m "feat(trino): OpenSearchNodeManager replaces Trino node discovery"
```

---

## Task 9: OpenSearchTaskExecutor (Replace Trino Thread Pools)

**Files:**
- Create: `trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/execution/OpenSearchTaskExecutor.java`
- Create: `trino/trino-opensearch/src/test/java/org/opensearch/sql/trino/execution/OpenSearchTaskExecutorTest.java`
- Modify: `OpenSearchServerMainModule.java`

- [ ] **Step 1: Write unit tests**

(Same as previously defined — tests for add/remove task, split completion, parallel splits.)

- [ ] **Step 2: Implement OpenSearchTaskExecutor**

(Same implementation — `TaskExecutor` backed by `ExecutorService`, cooperative scheduling.)

- [ ] **Step 3: Bind in OpenSearchServerMainModule**

```java
binder.bind(TaskExecutor.class).to(OpenSearchTaskExecutor.class).in(Scopes.SINGLETON);
```

- [ ] **Step 4: Run all 328 tests — no regressions**

Expected: All still PASS.

- [ ] **Step 5: Commit**

```bash
git add trino/
git commit -s -m "feat(trino): OpenSearchTaskExecutor replaces Trino thread pools"
```

---

## Task 10: CircuitBreakerMemoryPool (Replace Trino Memory Management)

**Files:**
- Create: `trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/memory/CircuitBreakerMemoryPool.java`
- Create: `trino/trino-opensearch/src/test/java/org/opensearch/sql/trino/memory/CircuitBreakerMemoryPoolTest.java`
- Modify: `OpenSearchServerMainModule.java`

- [ ] **Step 1: Write unit tests**

(Same as previously defined — reserve/free, blocking when exhausted.)

- [ ] **Step 2: Implement CircuitBreakerMemoryPool**

(Same implementation — subclass `MemoryPool`, delegate to circuit breaker in production.)

- [ ] **Step 3: Bind in OpenSearchServerMainModule**

```java
binder.bind(MemoryPool.class).toInstance(
    CircuitBreakerMemoryPool.create(DataSize.of(1, DataSize.Unit.GIGABYTE)));
```

- [ ] **Step 4: Run all 328 tests — no regressions**

Expected: All still PASS.

- [ ] **Step 5: Commit**

```bash
git add trino/
git commit -s -m "feat(trino): CircuitBreakerMemoryPool replaces Trino memory management"
```

---

## Task 11: Iceberg Connector + ClickBench 43 Queries

**Files:**
- Modify: `trino/trino-opensearch/build.gradle` (add Iceberg dependency)
- Create: `trino/trino-integ-test/src/test/java/org/opensearch/sql/trino/ClickBenchIT.java`
- Create: `trino/trino-integ-test/src/test/resources/clickbench/create_table.sql`
- Create: `trino/trino-integ-test/src/test/resources/clickbench/queries.sql`

- [ ] **Step 1: Add Iceberg dependency**

In `trino/trino-opensearch/build.gradle`:
```groovy
api "io.trino:trino-iceberg:${trinoVersion}"
```

- [ ] **Step 2: Create ClickBench DDL**

`trino/trino-integ-test/src/test/resources/clickbench/create_table.sql`:

(Full 105-column DDL matching ClickBench's Trino schema — same as previously defined.)

- [ ] **Step 3: Create ClickBench queries file**

`trino/trino-integ-test/src/test/resources/clickbench/queries.sql`:

(All 43 queries — same as previously defined.)

- [ ] **Step 4: Write ClickBenchIT**

```java
package org.opensearch.sql.trino;

import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * ClickBench: 43 analytical SQL queries.
 * All queries go through /_plugins/_trino_sql/v1/statement.
 * Uses Iceberg table format with Parquet storage.
 */
public class ClickBenchIT extends OpenSearchRestTestCase {

    @Test
    public void allClickBenchQueries() throws Exception {
        List<String> queries = loadQueries("/clickbench/queries.sql");
        assertEquals(43, queries.size());

        int passed = 0;
        int failed = 0;
        List<String> failures = new ArrayList<>();

        for (int i = 0; i < queries.size(); i++) {
            String query = queries.get(i);
            long start = System.currentTimeMillis();
            try {
                String result = executeTrinoQuery(query);
                long elapsed = System.currentTimeMillis() - start;
                // Verify we got results (not an error)
                assertTrue("Q" + i + " should have stats",
                    result.contains("\"state\":\"FINISHED\""));
                passed++;
                System.out.printf("Q%d: PASS (%dms)%n", i, elapsed);
            } catch (Exception e) {
                failed++;
                failures.add("Q" + i + ": " + e.getMessage());
                System.out.printf("Q%d: FAIL — %s%n", i, e.getMessage());
            }
        }

        System.out.printf("%n=== ClickBench Results: %d/%d passed ===%n", passed, queries.size());
        if (!failures.isEmpty()) {
            System.out.println("Failures:");
            failures.forEach(f -> System.out.println("  " + f));
        }

        assertEquals("All 43 ClickBench queries should pass", 0, failed);
    }

    private String executeTrinoQuery(String sql) throws Exception {
        Request request = new Request("POST", "/_plugins/_trino_sql/v1/statement");
        request.setJsonEntity("{\"query\": \"" + sql.replace("\"", "\\\"")
            .replace("'", "\\'") + "\"}");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        String body = new String(
            response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        // Follow pagination (nextUri)
        // TODO: Implement pagination following
        return body;
    }

    private List<String> loadQueries(String resource) throws Exception {
        List<String> queries = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                getClass().getResourceAsStream(resource), StandardCharsets.UTF_8))) {
            StringBuilder current = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("--") || line.isEmpty()) continue;
                current.append(line).append(" ");
                if (line.endsWith(";")) {
                    queries.add(current.toString().trim().replaceAll(";$", ""));
                    current = new StringBuilder();
                }
            }
            if (!current.isEmpty()) {
                queries.add(current.toString().trim().replaceAll(";$", ""));
            }
        }
        return queries;
    }
}
```

- [ ] **Step 5: Run — expect failures, iterate**

```bash
./gradlew :trino:trino-integ-test:integTest --tests "*ClickBenchIT*" -v
```

Issues to fix:
1. Iceberg connector not loaded → add to engine bootstrap
2. ClickBench table not created → add setup step in test @Before
3. No data → insert sample data or use full 14GB file for benchmarking
4. Query-specific failures → fix one at a time

- [ ] **Step 6: All 43 queries pass**

```bash
./gradlew :trino:trino-integ-test:integTest --tests "*ClickBenchIT*" -v
```

Expected: 43/43 PASS

- [ ] **Step 7: Verify no regression on all Trino test suites**

```bash
./gradlew :trino:trino-integ-test:integTest -v
```

Expected: 328 Trino tests + 43 ClickBench = 371 tests PASS. No Trino server running.

- [ ] **Step 8: Commit**

```bash
git add trino/
git commit -s -m "feat(trino): ClickBench 43/43 queries passing via /_plugins/_trino_sql"
```

---

## Success Criteria

| Metric | Target |
|---|---|
| `TestTrinoQueries` (AbstractTestQueries) | 34/34 pass |
| `TestTrinoAggregations` (AbstractTestAggregations) | 147/147 pass |
| `TestTrinoJoinQueries` (AbstractTestJoinQueries) | 147/147 pass |
| `ClickBenchIT` | 43/43 pass |
| No Trino HTTP server | Port 8080 free, verified by test |
| Thread pools visible | `/_cat/thread_pool` shows trino_query, trino_exchange, trino_scheduler |
| All tests via REST endpoint | Every query goes through `/_plugins/_trino_sql/v1/statement` |
| OpenSearch adapters wired | NodeManager, TaskExecutor, MemoryPool replaced |

**Total: 371+ tests, all through `/_plugins/_trino_sql`**

## Risk Register

| Risk | Impact | Mitigation |
|---|---|---|
| Java 21 incompatibility with Trino 440 | HIGH | Try 439, 438... until it compiles. |
| `QueryRunner` interface has many methods we can't implement via HTTP | HIGH | Stub unsupported methods. `AbstractTestQueries` primarily uses `execute()`. |
| Trino's `DispatchManager` tightly coupled to server infrastructure | HIGH | Task 4 is the hardest — iterative binding discovery. |
| REST response format doesn't match Trino JDBC expectations exactly | MEDIUM | Test with `trino-jdbc` driver early. Fix serialization issues. |
| Dependency conflicts (Guice, Jackson) | MEDIUM | Resolve in Task 1 with exclusions/forced versions. |
| `AbstractTestAggregations` needs features our engine doesn't have yet | MEDIUM | Track pass/fail counts. Allow partial progress before 100%. |
| ClickBench data loading is slow (14GB) | LOW | Use small sample for CI, full data for benchmarking. |
