# Omni-442 Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the PPL Calcite execution path with Omni's Trino-442 distributed engine, hooked at the transport layer, verbatim from `~/oss/omni` branch `feat/ppl`.

**Architecture:** New `omni-engine` Gradle module holds the vendored Trino 442 source + Omni adapters. `TransportPPLQueryAction` delegates PPL execution to `OmniEngineService`, which wraps Omni's `DispatchManager` + `Query.waitForResults` pipeline and adapts `QueryResults` → `ExecutionEngine.QueryResponse`. No fallback.

**Tech Stack:** Java 21, Gradle, OpenSearch plugin API, vendored Trino 442, UnifiedAPI (`:api` module), Apache Calcite 1.41.0, Parquet 1.13.1, Netty 4.2.

**Reference:** Spec is `docs/superpowers/specs/2026-04-27-omni-442-migration-design.md`. The fully validated source of truth is `~/oss/omni` branch `feat/ppl`.

---

## Conventions for every task

- Work on branch `feat/omni-442`.
- Omni source tree root: `OMNI=$HOME/oss/omni`. Branch `feat/ppl`. Do not modify omni.
- ppl repo root: `PPL=/local/home/penghuo/oss/ppl`. Branch `feat/omni-442`.
- Copy commands use `rsync -a --delete` so repeating a task is idempotent.
- Every commit is DCO-signed (`git commit -s`). Every commit message uses `feat(omni-442):` / `chore(omni-442):` / `fix(omni-442):` / `test(omni-442):` / `docs(omni-442):`.
- Spotless license header is Apache-2.0 "Copyright OpenSearch Contributors" — but vendored Trino files keep their original headers unchanged (omni already uses `.onlyIfContentMatches` to allow this).
- The `omni-engine` module has its own `spotlessCheck` gated behind `onlyIfContentMatches` so vendored Trino files are skipped.
- Whenever a phase says "run X", expected output is shown; on mismatch, stop and diagnose — don't suppress errors.

---

## Phase 0: Branch setup and guardrails

### Task 0.1: Confirm branch and clean working tree

- [ ] **Step 1: Verify we are on `feat/omni-442` with a clean tree**

Run:
```bash
cd /local/home/penghuo/oss/ppl
git rev-parse --abbrev-ref HEAD
git status --short
```
Expected: branch `feat/omni-442`, empty `git status --short` (ignoring the already-committed design doc).

- [ ] **Step 2: Verify omni feat/ppl is checked out**

Run:
```bash
git -C $HOME/oss/omni rev-parse --abbrev-ref HEAD
```
Expected: `feat/ppl`.

- [ ] **Step 3: Pin the omni reference commit into a file so later tasks can verify**

```bash
git -C $HOME/oss/omni rev-parse HEAD > /local/home/penghuo/oss/ppl/.omni-reference-sha
echo "reference omni sha: $(cat /local/home/penghuo/oss/ppl/.omni-reference-sha)"
```
Expected: a 40-char SHA printed.

- [ ] **Step 4: Commit the pin**

```bash
cd /local/home/penghuo/oss/ppl
git add .omni-reference-sha
git commit -s -m "chore(omni-442): pin omni feat/ppl reference sha"
```

### Task 0.2: Baseline build health

- [ ] **Step 1: Confirm repo currently builds (no surprises before we touch it)**

Run:
```bash
cd /local/home/penghuo/oss/ppl
./gradlew :core:compileJava :ppl:compileJava :plugin:compileJava :api:compileJava -q
```
Expected: `BUILD SUCCESSFUL`. If it fails, stop — fix or revert before proceeding.

- [ ] **Step 2: Run one quick calcite IT to confirm baseline works**

Run:
```bash
./gradlew :integ-test:integTest -Dtests.class="org.opensearch.sql.calcite.standalone.CalcitePPLBasicIT" -q
```
Expected: `BUILD SUCCESSFUL` (some tests may be slow; 10-minute timeout). If this baseline already fails, stop.

---

## Phase 1: Create empty `omni-engine` module

### Task 1.1: Add module to `settings.gradle`

**Files:**
- Modify: `settings.gradle`

- [ ] **Step 1: Add `include 'omni-engine'` after `include 'language-grammar'`**

Current (around line 27):
```gradle
include 'language-grammar'
```
Change to:
```gradle
include 'language-grammar'
include 'omni-engine'
```

- [ ] **Step 2: Verify settings parse**

Run:
```bash
./gradlew projects -q | grep omni-engine
```
Expected: `+--- Project ':omni-engine'` line in output (the directory doesn't exist yet; Gradle tolerates this until we add tasks).

### Task 1.2: Create module skeleton

- [ ] **Step 1: Create directory layout**

```bash
mkdir -p omni-engine/src/main/java
mkdir -p omni-engine/src/main/resources
mkdir -p omni-engine/src/test/java
mkdir -p omni-engine/src/test/resources
```

- [ ] **Step 2: Create minimal `omni-engine/build.gradle`** — everything else is added in later tasks.

File `omni-engine/build.gradle`:
```gradle
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

plugins {
    id 'java-library'
    id "io.freefair.lombok"
}

java {
    targetCompatibility = JavaVersion.VERSION_21
    sourceCompatibility = JavaVersion.VERSION_21
}

tasks.withType(JavaCompile).configureEach {
    options.compilerArgs += ['-Xmaxerrs', '2000', '-parameters']
}

// Skip audits that break on vendored Trino code
loggerUsageCheck.enabled = false
thirdPartyAudit.enabled = false

dependencies {
    // Filled in later tasks
}
```

- [ ] **Step 3: Confirm module compiles empty**

Run:
```bash
./gradlew :omni-engine:compileJava -q
```
Expected: `BUILD SUCCESSFUL` (no sources = trivial success).

- [ ] **Step 4: Commit**

```bash
git add settings.gradle omni-engine/build.gradle
git commit -s -m "feat(omni-442): scaffold empty omni-engine module"
```

---

## Phase 2: Vendor Trino 442 source

### Task 2.1: Copy all `io/trino/**`

- [ ] **Step 1: rsync Trino sources**

```bash
OMNI=$HOME/oss/omni
PPL=/local/home/penghuo/oss/ppl
rsync -a --delete \
    "$OMNI/src/main/java/io/trino/" \
    "$PPL/omni-engine/src/main/java/io/trino/"
```

- [ ] **Step 2: Verify file count matches omni**

Run:
```bash
EXPECTED=$(find $OMNI/src/main/java/io/trino -name '*.java' | wc -l)
ACTUAL=$(find $PPL/omni-engine/src/main/java/io/trino -name '*.java' | wc -l)
echo "expected=$EXPECTED actual=$ACTUAL"
```
Expected: identical counts (~4725).

- [ ] **Step 3: Commit vendored sources in one commit**

```bash
cd $PPL
git add omni-engine/src/main/java/io/trino
git commit -s -m "feat(omni-442): vendor Trino 442 source (from omni feat/ppl $(cat .omni-reference-sha | cut -c1-7))"
```

### Task 2.2: Gate vendored code from spotless

**Files:**
- Modify: `omni-engine/build.gradle`

- [ ] **Step 1: Do NOT enable spotless for `omni-engine`**

Reason: vendored Trino files keep their original Trino/Airlift license headers. We skip formatting entirely rather than trying to rewrite them. Confirm `omni-engine/build.gradle` does not apply `com.diffplug.spotless`. (It already doesn't per Task 1.2.)

- [ ] **Step 2: Exclude from root `spotlessCheck`**

Root `build.gradle` already only applies spotless per-module; no root-level exclusion needed. Verify:
```bash
grep -rn "spotless" build.gradle buildSrc/ | grep -v ".gradle/caches"
```
Expected: no root-level subprojects-wide spotless; each module opts in. No change required.

---

## Phase 3: Wire Trino dependencies

### Task 3.1: Copy dependency block from omni

**Files:**
- Modify: `omni-engine/build.gradle`

- [ ] **Step 1: Port omni's full `dependencies { ... }` block into `omni-engine/build.gradle`**

Source: `$HOME/oss/omni/build.gradle` lines ~215-425 (the dependencies block) — copy every `implementation`/`compileOnly` line verbatim, with two replacements:

1. Replace:
```gradle
implementation('org.opensearch.query:unified-query-api:3.6.0.0-SNAPSHOT') {
    exclude group: 'com.github.babbel', module: 'okhttp-aws-signer'
    exclude group: 'org.opensearch', module: 'opensearch-ml-client'
}
```
With:
```gradle
api project(':api')
```

2. Leave everything else verbatim (slice, jackson, netty, parquet, iceberg, coral, etc.).

- [ ] **Step 2: Port the `configurations { all { resolutionStrategy { force ... } } }` block**

Copy the entire `force` block from `$HOME/oss/omni/build.gradle` lines ~175-214 into `omni-engine/build.gradle`. It pins jackson 2.16.1, guava 33, netty 4.2.9, calcite 1.41.0, lucene 10.3.2, etc.

- [ ] **Step 3: Add the mavenCentral + OpenSearch snapshot repos**

Add:
```gradle
repositories {
    mavenLocal()
    maven { url "https://ci.opensearch.org/ci/dbc/snapshots/maven/" }
    mavenCentral()
    maven { url "https://plugins.gradle.org/m2/" }
    maven {
        url 'https://jitpack.io'
        content { includeGroup "com.github.babbel" }
    }
}
```

- [ ] **Step 4: Compile the module**

Run:
```bash
./gradlew :omni-engine:compileJava 2>&1 | tail -30
```
Expected: `BUILD SUCCESSFUL` with ~4,725 classes compiled. If compilation fails with missing dep: add the dep from omni's block (we did NOT drop anything).

- [ ] **Step 5: Commit**

```bash
git add omni-engine/build.gradle
git commit -s -m "feat(omni-442): add Trino 442 dependencies to omni-engine"
```

---

## Phase 4: Vendor Omni adapter layer

### Task 4.1: Copy all adapter files

- [ ] **Step 1: rsync `org/opensearch/plugin/omni`**

```bash
OMNI=$HOME/oss/omni
PPL=/local/home/penghuo/oss/ppl
rsync -a --delete \
    "$OMNI/src/main/java/org/opensearch/plugin/omni/" \
    "$PPL/omni-engine/src/main/java/org/opensearch/plugin/omni/"
```

- [ ] **Step 2: Remove OmniPlugin.java** — its duties fold into the ppl repo's `SQLPlugin` in Phase 6.

```bash
rm $PPL/omni-engine/src/main/java/org/opensearch/plugin/omni/OmniPlugin.java
```

- [ ] **Step 3: Remove the REST handlers** — not needed, transport is the hook.

```bash
rm $PPL/omni-engine/src/main/java/org/opensearch/plugin/omni/rest/SubmitQueryAction.java
rm $PPL/omni-engine/src/main/java/org/opensearch/plugin/omni/rest/ExplainQueryAction.java
rm $PPL/omni-engine/src/main/java/org/opensearch/plugin/omni/rest/GetQueryStatusAction.java
rm $PPL/omni-engine/src/main/java/org/opensearch/plugin/omni/rest/CancelQueryAction.java
```
Keep `rest/ResultSerializer.java` if present — it's a helper used by the new `OmniEngineService`.

- [ ] **Step 4: Verify everything else is present**

```bash
find $PPL/omni-engine/src/main/java/org/opensearch/plugin/omni -name '*.java' | sort
```
Expected ~44 files across:
- `OmniSettings.java`, `ServiceWiring.java`
- `cluster/ClusterStateNodeManager.java`
- `exchange/{Netty4ExchangeTransport,ExchangeHttpHandler,ApacheHttpClientAdapter}.java`
- `filesystem/{FileSchemeFileSystem,FileSchemeFileSystemFactory}.java`
- `connector/opensearch/*.java` (16)
- `connector/opensearch/decoder/*.java` (14)
- `ppl/{PplTranslator,CalciteSchemaAdapter,OmniSqlDialect}.java`
- `ppl/udf/*.java` (6)

- [ ] **Step 5: Compile**

Run:
```bash
./gradlew :omni-engine:compileJava 2>&1 | tail -20
```
Expected: `BUILD SUCCESSFUL`. If failing because of `org.opensearch.sql.api.UnifiedQueryContext` not found: confirm `api project(':api')` is in dependencies (Phase 3 Step 1). If failing on OpenSearch types (IndicesService, etc.): Task 4.2.

### Task 4.2: Add OpenSearch + opensearch module dependencies

**Files:**
- Modify: `omni-engine/build.gradle`

- [ ] **Step 1: Add OpenSearch core + `:opensearch` project dep**

In `dependencies { }`:
```gradle
implementation "org.opensearch:opensearch:${opensearch_version}"
implementation "org.opensearch:opensearch-core:${opensearch_version}"
implementation "org.opensearch:opensearch-common:${opensearch_version}"
implementation "org.opensearch:opensearch-x-content:${opensearch_version}"
implementation "org.opensearch.plugin:transport-netty4-client:${opensearch_version}"
api project(':opensearch')
```

Where `opensearch_version` is read from the same gradle property as other modules (use `project.version` or `opensearch_version` existing var).

- [ ] **Step 2: Compile**

Run:
```bash
./gradlew :omni-engine:compileJava 2>&1 | tail -20
```
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 3: Commit**

```bash
git add omni-engine
git commit -s -m "feat(omni-442): vendor omni adapter layer (minus OmniPlugin, REST handlers)"
```

### Task 4.3: Port the meta-inf resources omni ships

- [ ] **Step 1: Copy `src/main/resources`**

```bash
rsync -a --delete \
    "$OMNI/src/main/resources/" \
    "$PPL/omni-engine/src/main/resources/"
```

- [ ] **Step 2: Review what came across** (service loader files, Trino plugin descriptors)

```bash
find $PPL/omni-engine/src/main/resources -type f
```
Expected: `META-INF/services/*` entries for Trino `FunctionProvider`, etc.

- [ ] **Step 3: Commit**

```bash
git add omni-engine/src/main/resources
git commit -s -m "feat(omni-442): vendor omni resources (META-INF services)"
```

---

## Phase 5: Build the bridge — `OmniEngineService` and adapters

### Task 5.1: Create `TrinoTypeMapper`

**Files:**
- Create: `omni-engine/src/main/java/org/opensearch/plugin/omni/adapter/TrinoTypeMapper.java`

- [ ] **Step 1: Write the failing test**

File: `omni-engine/src/test/java/org/opensearch/plugin/omni/adapter/TrinoTypeMapperTest.java`
```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.omni.adapter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprCoreType;

public class TrinoTypeMapperTest {
  @Test
  void mapsPrimitives() {
    assertEquals(ExprCoreType.LONG, TrinoTypeMapper.toExprType("bigint"));
    assertEquals(ExprCoreType.INTEGER, TrinoTypeMapper.toExprType("integer"));
    assertEquals(ExprCoreType.SHORT, TrinoTypeMapper.toExprType("smallint"));
    assertEquals(ExprCoreType.BYTE, TrinoTypeMapper.toExprType("tinyint"));
    assertEquals(ExprCoreType.DOUBLE, TrinoTypeMapper.toExprType("double"));
    assertEquals(ExprCoreType.FLOAT, TrinoTypeMapper.toExprType("real"));
    assertEquals(ExprCoreType.BOOLEAN, TrinoTypeMapper.toExprType("boolean"));
    assertEquals(ExprCoreType.STRING, TrinoTypeMapper.toExprType("varchar"));
    assertEquals(ExprCoreType.STRING, TrinoTypeMapper.toExprType("varchar(100)"));
    assertEquals(ExprCoreType.STRING, TrinoTypeMapper.toExprType("char(10)"));
    assertEquals(ExprCoreType.DATE, TrinoTypeMapper.toExprType("date"));
    assertEquals(ExprCoreType.TIMESTAMP, TrinoTypeMapper.toExprType("timestamp(3)"));
    assertEquals(ExprCoreType.TIMESTAMP, TrinoTypeMapper.toExprType("timestamp(6) with time zone"));
    assertEquals(ExprCoreType.DOUBLE, TrinoTypeMapper.toExprType("decimal(10,2)"));
    assertEquals(ExprCoreType.ARRAY, TrinoTypeMapper.toExprType("array(varchar)"));
    assertEquals(ExprCoreType.STRUCT, TrinoTypeMapper.toExprType("row(a bigint)"));
    assertEquals(ExprCoreType.STRUCT, TrinoTypeMapper.toExprType("map(varchar, bigint)"));
    assertEquals(ExprCoreType.BINARY, TrinoTypeMapper.toExprType("varbinary"));
    assertEquals(ExprCoreType.UNDEFINED, TrinoTypeMapper.toExprType("uuid"));
  }
}
```

- [ ] **Step 2: Add JUnit + opensearch project deps to test classpath**

Edit `omni-engine/build.gradle`:
```gradle
dependencies {
    // ... existing ...
    testImplementation 'org.junit.jupiter:junit-jupiter-api:5.9.3'
    testImplementation 'org.junit.jupiter:junit-jupiter-params:5.9.3'
    testRuntimeOnly 'org.junit.jupiter:junit-jupiter-engine'
    testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
    testImplementation "org.hamcrest:hamcrest-library:${hamcrest_version}"
    testImplementation "org.mockito:mockito-core:${mockito_version}"
}

test {
    useJUnitPlatform()
}
```

- [ ] **Step 3: Run test — it should fail to compile**

Run:
```bash
./gradlew :omni-engine:test --tests 'org.opensearch.plugin.omni.adapter.TrinoTypeMapperTest' 2>&1 | tail -10
```
Expected: compile failure, `TrinoTypeMapper` not found.

- [ ] **Step 4: Implement `TrinoTypeMapper.java`**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.omni.adapter;

import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/** Maps a Trino client-protocol type string to an OpenSearch SQL {@link ExprType}. */
public final class TrinoTypeMapper {
  private TrinoTypeMapper() {}

  public static ExprType toExprType(String trinoType) {
    String t = trinoType.toLowerCase();
    // Strip parameters: "varchar(100)" -> "varchar", "timestamp(6) with time zone" -> "timestamp with time zone"
    String base = t.replaceAll("\\([^)]*\\)", "").trim();

    return switch (base) {
      case "bigint" -> ExprCoreType.LONG;
      case "integer", "int" -> ExprCoreType.INTEGER;
      case "smallint" -> ExprCoreType.SHORT;
      case "tinyint" -> ExprCoreType.BYTE;
      case "double" -> ExprCoreType.DOUBLE;
      case "real" -> ExprCoreType.FLOAT;
      case "boolean" -> ExprCoreType.BOOLEAN;
      case "varchar", "char" -> ExprCoreType.STRING;
      case "date" -> ExprCoreType.DATE;
      case "timestamp", "timestamp with time zone" -> ExprCoreType.TIMESTAMP;
      case "decimal" -> ExprCoreType.DOUBLE;
      case "array" -> ExprCoreType.ARRAY;
      case "row", "map" -> ExprCoreType.STRUCT;
      case "varbinary" -> ExprCoreType.BINARY;
      default -> ExprCoreType.UNDEFINED;
    };
  }
}
```

- [ ] **Step 5: Run test — should pass**

```bash
./gradlew :omni-engine:test --tests 'org.opensearch.plugin.omni.adapter.TrinoTypeMapperTest' 2>&1 | tail -10
```
Expected: `BUILD SUCCESSFUL`, 1 test passed.

- [ ] **Step 6: Commit**

```bash
git add omni-engine
git commit -s -m "feat(omni-442): add TrinoTypeMapper (Trino type string -> ExprType)"
```

### Task 5.2: Create `QueryResponseAdapter`

**Files:**
- Create: `omni-engine/src/main/java/org/opensearch/plugin/omni/adapter/QueryResponseAdapter.java`
- Create: `omni-engine/src/test/java/org/opensearch/plugin/omni/adapter/QueryResponseAdapterTest.java`

- [ ] **Step 1: Write test**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.omni.adapter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.trino.client.ClientTypeSignature;
import io.trino.client.Column;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine;

public class QueryResponseAdapterTest {
  @Test
  void adaptsSchemaAndRows() {
    List<Column> cols = List.of(
        new Column("id", "bigint", new ClientTypeSignature("bigint")),
        new Column("name", "varchar", new ClientTypeSignature("varchar")));
    List<List<Object>> rows = List.of(
        List.of(1L, "alice"),
        List.of(2L, "bob"));

    ExecutionEngine.QueryResponse response = QueryResponseAdapter.adapt(cols, rows);

    assertEquals(2, response.getSchema().getColumns().size());
    assertEquals("id", response.getSchema().getColumns().get(0).getName());
    assertEquals(ExprCoreType.LONG, response.getSchema().getColumns().get(0).getExprType());
    assertEquals(2, response.getResults().size());
    assertEquals(1L, response.getResults().get(0).tupleValue().get("id").longValue());
    assertEquals("alice", response.getResults().get(0).tupleValue().get("name").stringValue());
  }

  @Test
  void emptyRowsProducesEmptyResults() {
    List<Column> cols = List.of(new Column("x", "integer", new ClientTypeSignature("integer")));
    ExecutionEngine.QueryResponse response = QueryResponseAdapter.adapt(cols, List.of());
    assertEquals(1, response.getSchema().getColumns().size());
    assertEquals(0, response.getResults().size());
  }
}
```

- [ ] **Step 2: Run test — should fail to compile**

Run:
```bash
./gradlew :omni-engine:test --tests 'org.opensearch.plugin.omni.adapter.QueryResponseAdapterTest' 2>&1 | tail -10
```
Expected: compile failure.

- [ ] **Step 3: Implement `QueryResponseAdapter.java`**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.omni.adapter;

import io.trino.client.Column;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.pagination.Cursor;

/** Converts Trino client-protocol {@code QueryResults} into {@link ExecutionEngine.QueryResponse}. */
public final class QueryResponseAdapter {
  private QueryResponseAdapter() {}

  public static ExecutionEngine.QueryResponse adapt(List<Column> columns, List<List<Object>> rows) {
    List<Schema.Column> schemaCols = new ArrayList<>(columns.size());
    List<ExprType> types = new ArrayList<>(columns.size());
    List<String> names = new ArrayList<>(columns.size());
    for (Column c : columns) {
      ExprType t = TrinoTypeMapper.toExprType(c.getType());
      names.add(c.getName());
      types.add(t);
      schemaCols.add(new Schema.Column(c.getName(), null, t));
    }

    List<ExprValue> results = new ArrayList<>(rows.size());
    for (List<Object> row : rows) {
      Map<String, ExprValue> tuple = new LinkedHashMap<>();
      for (int i = 0; i < row.size() && i < names.size(); i++) {
        Object raw = row.get(i);
        tuple.put(names.get(i), toExprValue(raw, types.get(i)));
      }
      results.add(ExprTupleValue.fromExprValueMap(tuple));
    }

    return new ExecutionEngine.QueryResponse(new Schema(schemaCols), results, Cursor.None);
  }

  private static ExprValue toExprValue(Object raw, ExprType type) {
    if (raw == null) {
      return ExprValueUtils.nullValue();
    }
    return ExprValueUtils.fromObjectValue(raw, type);
  }
}
```

- [ ] **Step 4: Run test — should pass**

```bash
./gradlew :omni-engine:test --tests 'org.opensearch.plugin.omni.adapter.QueryResponseAdapterTest' 2>&1 | tail -10
```
Expected: `BUILD SUCCESSFUL`, 2 tests passed.

If `ExprValueUtils.fromObjectValue(raw, type)` signature differs, adjust to use the correct method — check core/.../ExprValueUtils.java for the available converters and use the one that takes (Object, ExprType).

- [ ] **Step 5: Commit**

```bash
git add omni-engine
git commit -s -m "feat(omni-442): add QueryResponseAdapter (Trino results -> ExecutionEngine.QueryResponse)"
```

### Task 5.3: Create `OmniEngineService` shell

**Files:**
- Create: `omni-engine/src/main/java/org/opensearch/plugin/omni/OmniEngineService.java`

- [ ] **Step 1: Write `OmniEngineService.java`** — the execute and explain methods adapt omni's `SubmitQueryAction.prepareRequest` body into a ResponseListener-driven API matching `TransportPPLQueryAction`'s expectations.

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.omni;

import static java.util.Collections.emptyMap;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.client.Column;
import io.trino.client.ProtocolHeaders;
import io.trino.client.QueryError;
import io.trino.client.QueryResults;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryState;
import io.trino.server.SessionContext;
import io.trino.server.protocol.Query;
import io.trino.server.protocol.QueryResultsResponse;
import io.trino.server.protocol.Slug;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.session.ResourceEstimates;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.exception.QueryEngineException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.exception.SyntaxCheckException;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.plugin.omni.adapter.QueryResponseAdapter;

/**
 * Boundary between TransportPPLQueryAction and the vendored Trino engine. Takes a PPL request,
 * translates it to SQL via PplTranslator, dispatches through Trino, and adapts the client-protocol
 * results back into ExecutionEngine.QueryResponse / ExplainResponse.
 *
 * <p>Ports the body of omni/feat/ppl's SubmitQueryAction.prepareRequest. REST layer removed.
 */
public class OmniEngineService {
  private static final Logger log = LogManager.getLogger(OmniEngineService.class);

  private static final String DEFAULT_CATALOG = "opensearch";
  private static final String DEFAULT_SCHEMA = "default";
  private static final Duration FETCH_TIMEOUT = new Duration(2, TimeUnit.SECONDS);
  private static final DataSize FETCH_BUFFER = DataSize.of(1, DataSize.Unit.MEGABYTE);
  private static final long DISPATCH_TIMEOUT_SEC = 30;
  private static final long RESULT_TIMEOUT_SEC = 60;

  private final ServiceWiring wiring;

  public OmniEngineService(ServiceWiring wiring) {
    this.wiring = wiring;
  }

  public void execute(
      PPLQueryRequest request, ResponseListener<ExecutionEngine.QueryResponse> listener) {
    try {
      DispatchResult dispatch = dispatch(request);
      if (dispatch.earlyFinish != null) {
        // query already failed at dispatch — classify and fail listener
        listener.onFailure(classify(dispatch.earlyFinish));
        return;
      }
      CollectedResults collected = collectResults(dispatch);
      if (collected.error != null) {
        listener.onFailure(classify(collected.error));
        return;
      }
      listener.onResponse(QueryResponseAdapter.adapt(collected.columns, collected.rows));
    } catch (Exception e) {
      listener.onFailure(classify(e));
    }
  }

  public void explain(
      PPLQueryRequest request,
      ExplainMode mode,
      ResponseListener<ExecutionEngine.ExplainResponse> listener) {
    try {
      String explainPrefix = switch (mode == null ? ExplainMode.STANDARD : mode) {
        case STANDARD -> "EXPLAIN (TYPE DISTRIBUTED) ";
        case SIMPLE -> "EXPLAIN (TYPE LOGICAL) ";
        case EXTENDED -> "EXPLAIN ANALYZE ";
        case COST -> "EXPLAIN (TYPE VALIDATE) ";
      };
      DispatchResult dispatch = dispatch(request, explainPrefix);
      if (dispatch.earlyFinish != null) {
        listener.onFailure(classify(dispatch.earlyFinish));
        return;
      }
      CollectedResults collected = collectResults(dispatch);
      if (collected.error != null) {
        listener.onFailure(classify(collected.error));
        return;
      }
      StringBuilder plan = new StringBuilder();
      for (List<Object> row : collected.rows) {
        for (Object cell : row) {
          if (cell != null) plan.append(cell);
        }
        plan.append('\n');
      }
      listener.onResponse(
          new ExecutionEngine.ExplainResponse(
              new ExecutionEngine.ExplainResponse.ExplainResponseNode(plan.toString())));
    } catch (Exception e) {
      listener.onFailure(classify(e));
    }
  }

  // ------- internal -------

  private DispatchResult dispatch(PPLQueryRequest request) throws Exception {
    return dispatch(request, "");
  }

  private DispatchResult dispatch(PPLQueryRequest request, String sqlPrefix) throws Exception {
    DispatchManager dispatchManager = wiring.getDispatchManager();
    String catalog = DEFAULT_CATALOG;
    String schema = DEFAULT_SCHEMA;

    Identity identity = Identity.ofUser("ppl");
    SessionContext sessionContext =
        new SessionContext(
            ProtocolHeaders.TRINO_HEADERS,
            Optional.of(catalog),
            Optional.of(schema),
            Optional.empty(),
            Optional.empty(),
            identity,
            identity,
            new SelectedRole(SelectedRole.Type.NONE, Optional.empty()),
            Optional.of("ppl-plugin"),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            ImmutableSet.of(),
            ImmutableSet.of(),
            new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty()),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            Optional.empty(),
            false,
            Optional.empty());

    QueryId translationQueryId = dispatchManager.createQueryId();
    Session pplSession =
        wiring.getSessionSupplier().createSession(translationQueryId, Span.getInvalid(), sessionContext);

    String sql = wiring.getPplTranslator().translate(request.getRequest(), pplSession);
    log.debug("PPL translated to SQL: {}", sql);

    QueryId queryId = dispatchManager.createQueryId();
    Slug slug = Slug.createNew();
    ListenableFuture<Void> future =
        dispatchManager.createQuery(queryId, Span.getInvalid(), slug, sessionContext, sqlPrefix + sql);
    future.get(DISPATCH_TIMEOUT_SEC, TimeUnit.SECONDS);
    dispatchManager.waitForDispatched(queryId).get(DISPATCH_TIMEOUT_SEC, TimeUnit.SECONDS);

    QueryState state = dispatchManager.getQueryInfo(queryId).getState();
    if (state.isDone() && state != QueryState.FINISHED) {
      Optional<QueryInfo> info = dispatchManager.getFullQueryInfo(queryId);
      String msg =
          info.flatMap(i -> Optional.ofNullable(i.getFailureInfo()))
              .map(fi -> fi.getMessage())
              .orElse("query failed in state " + state);
      return new DispatchResult(queryId, slug, null, new RuntimeException(msg));
    }

    Session session = wiring.getSqlQueryManager().getQuerySession(queryId);
    Query query =
        Query.create(
            session,
            slug,
            wiring.getSqlQueryManager(),
            Optional.empty(),
            wiring.getDirectExchangeClientSupplier(),
            wiring.getExchangeManagerRegistry(),
            wiring.getQueryResultsExecutor(),
            wiring.getQueryResultsTimeoutExecutor(),
            wiring.getBlockEncodingSerde());
    return new DispatchResult(queryId, slug, query, null);
  }

  private CollectedResults collectResults(DispatchResult dispatch) throws Exception {
    Query query = dispatch.query;
    UriInfo uriInfo = new MinimalUriInfo(URI.create("http://ppl"));

    long token = 0;
    List<Column> columns = null;
    List<List<Object>> rows = new ArrayList<>();
    QueryError error = null;

    while (true) {
      QueryResultsResponse response =
          query.waitForResults(token, uriInfo, FETCH_TIMEOUT, FETCH_BUFFER)
              .get(RESULT_TIMEOUT_SEC, TimeUnit.SECONDS);
      QueryResults results = response.queryResults();
      if (columns == null && results.getColumns() != null) {
        columns = results.getColumns();
      }
      if (results.getData() != null) {
        for (List<Object> row : results.getData()) {
          rows.add(row);
        }
      }
      if (results.getError() != null) {
        error = results.getError();
      }
      if (results.getNextUri() == null) {
        break;
      }
      token++;
    }
    query.markResultsConsumedIfReady();
    return new CollectedResults(columns == null ? List.of() : columns, rows, error);
  }

  private Exception classify(Object cause) {
    if (cause instanceof QueryError err) {
      String name = err.getErrorName() == null ? "" : err.getErrorName();
      String msg = err.getMessage() == null ? name : err.getMessage();
      if (name.equals("SYNTAX_ERROR")) return new SyntaxCheckException(msg);
      if (name.startsWith("MISSING_")) return new SemanticCheckException(msg);
      return new QueryEngineException(msg);
    }
    if (cause instanceof Exception e) {
      return e;
    }
    return new QueryEngineException(String.valueOf(cause));
  }

  private record DispatchResult(QueryId queryId, Slug slug, Query query, Throwable earlyFinish) {}

  private record CollectedResults(
      List<Column> columns, List<List<Object>> rows, QueryError error) {}

  /** Minimal UriInfo — Query only calls getBaseUriBuilder(). */
  private static class MinimalUriInfo implements UriInfo {
    private final URI baseUri;

    MinimalUriInfo(URI baseUri) {
      this.baseUri = baseUri;
    }

    @Override
    public UriBuilder getBaseUriBuilder() {
      return UriBuilder.fromUri(baseUri);
    }

    @Override public URI getBaseUri() { return baseUri; }
    @Override public String getPath() { return ""; }
    @Override public String getPath(boolean decode) { return ""; }
    @Override public List<jakarta.ws.rs.core.PathSegment> getPathSegments() { return List.of(); }
    @Override public List<jakarta.ws.rs.core.PathSegment> getPathSegments(boolean decode) { return List.of(); }
    @Override public URI getRequestUri() { return baseUri; }
    @Override public UriBuilder getRequestUriBuilder() { return UriBuilder.fromUri(baseUri); }
    @Override public URI getAbsolutePath() { return baseUri; }
    @Override public UriBuilder getAbsolutePathBuilder() { return UriBuilder.fromUri(baseUri); }
    @Override public jakarta.ws.rs.core.MultivaluedMap<String, String> getPathParameters() { return new jakarta.ws.rs.core.MultivaluedHashMap<>(); }
    @Override public jakarta.ws.rs.core.MultivaluedMap<String, String> getPathParameters(boolean decode) { return new jakarta.ws.rs.core.MultivaluedHashMap<>(); }
    @Override public jakarta.ws.rs.core.MultivaluedMap<String, String> getQueryParameters() { return new jakarta.ws.rs.core.MultivaluedHashMap<>(); }
    @Override public jakarta.ws.rs.core.MultivaluedMap<String, String> getQueryParameters(boolean decode) { return new jakarta.ws.rs.core.MultivaluedHashMap<>(); }
    @Override public List<String> getMatchedURIs() { return List.of(); }
    @Override public List<String> getMatchedURIs(boolean decode) { return List.of(); }
    @Override public List<Object> getMatchedResources() { return List.of(); }
    @Override public URI resolve(URI uri) { return baseUri.resolve(uri); }
    @Override public URI relativize(URI uri) { return baseUri.relativize(uri); }
  }
}
```

- [ ] **Step 2: Add `:ppl` dependency to omni-engine**

Edit `omni-engine/build.gradle` dependencies:
```gradle
api project(':ppl')
```
(Needed for `PPLQueryRequest`.)

- [ ] **Step 3: Compile**

Run:
```bash
./gradlew :omni-engine:compileJava 2>&1 | tail -20
```
Expected: `BUILD SUCCESSFUL`. If `UriBuilder.fromUri(...)` fails (needs JAX-RS RuntimeDelegate at runtime), replace with the simpler `SimpleUriBuilder` implementation from omni's SubmitQueryAction (copy verbatim).

- [ ] **Step 4: Commit**

```bash
git add omni-engine
git commit -s -m "feat(omni-442): add OmniEngineService (boundary between PPL transport and Trino)"
```

### Task 5.4: Port omni's `ResultSerializer` if needed

- [ ] **Step 1: Check whether it already came across**

```bash
ls $PPL/omni-engine/src/main/java/org/opensearch/plugin/omni/rest/ 2>&1
```

- [ ] **Step 2: If ResultSerializer.java is absent but referenced, copy only that file**

```bash
cp $OMNI/src/main/java/org/opensearch/plugin/omni/rest/ResultSerializer.java \
   $PPL/omni-engine/src/main/java/org/opensearch/plugin/omni/rest/ResultSerializer.java 2>/dev/null || true
```

- [ ] **Step 3: Compile check**

```bash
./gradlew :omni-engine:compileJava -q
```
Expected: `BUILD SUCCESSFUL`.

---

## Phase 6: Wire into plugin — SQLPlugin integration

### Task 6.1: Extend `SQLPlugin` to carry Omni lifecycle

**Files:**
- Modify: `plugin/build.gradle`
- Modify: `plugin/src/main/java/org/opensearch/sql/plugin/SQLPlugin.java`

- [ ] **Step 1: Add `:omni-engine` dep to plugin**

`plugin/build.gradle`:
```gradle
dependencies {
    api project(':core')
    api project(':omni-engine')     // NEW
    // ... existing ...
}
```

- [ ] **Step 2: Extend `SQLPlugin` — implement `NetworkPlugin`, add omni lifecycle**

Add imports and class-level state:
```java
import org.opensearch.plugin.omni.OmniEngineService;
import org.opensearch.plugin.omni.OmniSettings;
import org.opensearch.plugin.omni.ServiceWiring;
import org.opensearch.plugin.omni.cluster.ClusterStateNodeManager;
import org.opensearch.plugin.omni.exchange.ExchangeHttpHandler;
import org.opensearch.plugin.omni.exchange.Netty4ExchangeTransport;
import org.opensearch.common.network.NetworkService;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.transport.AuxTransport;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.inject.Inject;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
```

Add `NetworkPlugin` to class declaration:
```java
public class SQLPlugin extends Plugin
    implements ActionPlugin,
        ScriptPlugin,
        SystemIndexPlugin,
        JobSchedulerExtension,
        ExtensiblePlugin,
        NetworkPlugin {
```

Add fields:
```java
  private final AtomicReference<IndicesService> indicesServiceRef = new AtomicReference<>();
  private volatile ServiceWiring omniWiring;
  private volatile ClusterStateNodeManager omniNodeManager;
  private volatile OmniEngineService omniEngineService;
```

In `createComponents`, after the existing setup but before `return`:
```java
    this.omniNodeManager = new ClusterStateNodeManager(nodeEnvironment.nodeId());
    clusterService.addListener(omniNodeManager);
    this.omniWiring =
        new ServiceWiring(
            environment.settings(),
            omniNodeManager,
            (NodeClient) client,
            clusterService,
            indicesServiceRef::get);
    this.omniEngineService = new OmniEngineService(omniWiring);
    LOGGER.info("Omni engine initialized — Trino service graph constructed");
```

Change the return to include the new components:
```java
    return ImmutableList.of(
        dataSourceService,
        asyncQueryExecutorService,
        clusterManagerEventListener,
        pluginSettings,
        directQueryExecutorService,
        extensionsHolder,
        omniWiring,
        omniNodeManager,
        omniEngineService);
```

Add `additionalSettings`:
```java
  @Override
  public Settings additionalSettings() {
    int exchangePort = OmniSettings.EXCHANGE_PORT.get(super.additionalSettings());
    return Settings.builder()
        .put("node.attr.omni_exchange_port", String.valueOf(exchangePort))
        .build();
  }
```

Extend `getSettings()`:
```java
  @Override
  public List<Setting<?>> getSettings() {
    return new ImmutableList.Builder<Setting<?>>()
        .addAll(OpenSearchSettings.pluginSettings())
        .addAll(OpenSearchSettings.pluginNonDynamicSettings())
        .addAll(OmniSettings.getSettings())
        .build();
  }
```

Add `getAuxTransports`:
```java
  @Override
  public Map<String, Supplier<AuxTransport>> getAuxTransports(
      Settings settings,
      ThreadPool threadPool,
      CircuitBreakerService circuitBreakerService,
      NetworkService networkService,
      ClusterSettings clusterSettings,
      Tracer tracer) {
    return Map.of(
        Netty4ExchangeTransport.SETTING_KEY,
        () -> {
          ServiceWiring wiring = this.omniWiring;
          ExchangeHttpHandler handler =
              new ExchangeHttpHandler(
                  () -> wiring.getSqlTaskManager(),
                  false,
                  wiring.getSessionPropertyManager(),
                  wiring.getTaskUpdateRequestCodec(),
                  wiring.getTaskInfoCodec(),
                  wiring.getTaskStatusCodec(),
                  wiring.getFailTaskRequestCodec(),
                  wiring.getDynamicFilterDomainsCodec());
          return new Netty4ExchangeTransport(settings, networkService, handler);
        });
  }
```

Add Guice binding (`createGuiceModules`):
```java
  @Override
  public Collection<org.opensearch.common.inject.Module> createGuiceModules() {
    return Collections.singletonList(binder -> binder.bind(SQLPlugin.class).toInstance(this));
  }

  @Override
  public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
    return Collections.singletonList(IndicesServiceBinder.class);
  }

  public void setIndicesService(IndicesService indicesService) {
    indicesServiceRef.set(indicesService);
  }

  public OmniEngineService getOmniEngineService() {
    return omniEngineService;
  }

  public static class IndicesServiceBinder extends AbstractLifecycleComponent {
    private final IndicesService indicesService;
    private final SQLPlugin plugin;

    @Inject
    public IndicesServiceBinder(IndicesService indicesService, SQLPlugin plugin) {
      this.indicesService = indicesService;
      this.plugin = plugin;
    }

    @Override protected void doStart() { plugin.setIndicesService(indicesService); }
    @Override protected void doStop() {}
    @Override protected void doClose() {}
  }
```

Add `close`:
```java
  @Override
  public void close() {
    if (omniWiring != null) {
      try { omniWiring.close(); } catch (Exception ignored) {}
    }
  }
```

- [ ] **Step 3: Compile plugin**

Run:
```bash
./gradlew :plugin:compileJava 2>&1 | tail -30
```
Expected: `BUILD SUCCESSFUL`. If Netty AuxTransport types not found: add `implementation project(':omni-engine')` is not enough; `omni-engine/build.gradle` needs `api` not `implementation` for those types, which we already set.

- [ ] **Step 4: Commit**

```bash
git add plugin
git commit -s -m "feat(omni-442): wire Omni lifecycle (ServiceWiring, AuxTransport) into SQLPlugin"
```

### Task 6.2: Expose `OmniEngineService` to `TransportPPLQueryAction`

**Files:**
- Modify: `plugin/src/main/java/org/opensearch/sql/plugin/config/OpenSearchPluginModule.java` (Guice binding)
- Modify: `plugin/src/main/java/org/opensearch/sql/plugin/transport/TransportPPLQueryAction.java`

- [ ] **Step 1: Read OpenSearchPluginModule**

```bash
cat plugin/src/main/java/org/opensearch/sql/plugin/config/OpenSearchPluginModule.java
```

- [ ] **Step 2: Bind `OmniEngineService`**

Change constructor to accept plugin ref (current pattern passes via `binder` in `SQLPlugin.createComponents`). Add field:
```java
import org.opensearch.plugin.omni.OmniEngineService;
```

In `configure()`:
```java
    // bind the Omni engine service
    bind(OmniEngineService.class).toProvider(() -> sqlPluginInstance.getOmniEngineService());
```
(The exact wiring depends on what OpenSearchPluginModule already does — if it currently uses `@Provides` methods or concrete bindings, follow the same style. The goal: `injector.getInstance(OmniEngineService.class)` returns the one we created in Phase 6.1.)

An alternative and simpler path: pass the `OmniEngineService` explicitly through `TransportPPLQueryAction`'s constructor, similar to how `EngineExtensionsHolder` is already passed. Look for the `.add(b -> { b.bind(...)... })` block in `createComponents` and add:
```java
b.bind(OmniEngineService.class).toInstance(omniEngineService);
```

- [ ] **Step 3: Replace `TransportPPLQueryAction.doExecute` body**

New `TransportPPLQueryAction.java`:
```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.opensearch.rest.BaseRestHandler.MULTI_ALLOW_EXPLICIT_INDEX;
import static org.opensearch.sql.executor.ExecutionEngine.ExplainResponse.normalizeLf;
import static org.opensearch.sql.lang.PPLLangSpec.PPL_SPEC;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import java.util.Locale;
import java.util.Optional;
import java.util.function.Supplier;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.omni.OmniEngineService;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
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
import org.opensearch.transport.TransportService;

public class TransportPPLQueryAction
    extends HandledTransportAction<ActionRequest, TransportPPLQueryResponse> {

  private final OmniEngineService omniEngineService;
  private final Supplier<Boolean> pplEnabled;

  @Inject
  public TransportPPLQueryAction(
      TransportService transportService,
      ActionFilters actionFilters,
      ClusterService clusterService,
      org.opensearch.common.settings.Settings clusterSettings,
      OmniEngineService omniEngineService) {
    super(PPLQueryAction.NAME, transportService, actionFilters, TransportPPLQueryRequest::new);
    this.omniEngineService = omniEngineService;
    OpenSearchSettings pluginSettings = new OpenSearchSettings(clusterService.getClusterSettings());
    this.pplEnabled =
        () ->
            MULTI_ALLOW_EXPLICIT_INDEX.get(clusterSettings)
                && (Boolean) pluginSettings.getSettingValue(Settings.Key.PPL_ENABLED);
  }

  @Override
  protected void doExecute(
      Task task, ActionRequest request, ActionListener<TransportPPLQueryResponse> listener) {
    if (!pplEnabled.get()) {
      listener.onFailure(
          new IllegalAccessException(
              "Either plugins.ppl.enabled or rest.action.multi.allow_explicit_index setting is false"));
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

    PPLQueryRequest pplRequest = transportRequest.toPPLQueryRequest();

    if (pplRequest.isExplainRequest()) {
      ExplainMode mode = ExplainMode.STANDARD; // TODO: parse from request when multi-mode is exposed
      omniEngineService.explain(pplRequest, mode, createExplainListener(pplRequest, listener));
    } else {
      omniEngineService.execute(pplRequest, createQueryListener(pplRequest, listener));
    }
  }

  private ResponseListener<ExecutionEngine.ExplainResponse> createExplainListener(
      PPLQueryRequest request, ActionListener<TransportPPLQueryResponse> listener) {
    return new ResponseListener<>() {
      @Override
      public void onResponse(ExecutionEngine.ExplainResponse response) {
        Optional<Format> isYamlFormat =
            Format.ofExplain(request.getFormat()).filter(f -> f.equals(Format.YAML));
        ResponseFormatter<ExecutionEngine.ExplainResponse> formatter =
            isYamlFormat.isPresent()
                ? new YamlResponseFormatter<>() {
                    @Override
                    protected Object buildYamlObject(ExecutionEngine.ExplainResponse r) {
                      return normalizeLf(r);
                    }
                  }
                : new JsonResponseFormatter<>(PRETTY) {
                    @Override
                    protected Object buildJsonObject(ExecutionEngine.ExplainResponse r) {
                      return r;
                    }
                  };
        listener.onResponse(
            new TransportPPLQueryResponse(formatter.format(response), formatter.contentType()));
      }

      @Override public void onFailure(Exception e) { listener.onFailure(e); }
    };
  }

  private ResponseListener<ExecutionEngine.QueryResponse> createQueryListener(
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
    return new ResponseListener<>() {
      @Override
      public void onResponse(ExecutionEngine.QueryResponse response) {
        String body =
            formatter.format(
                new QueryResult(response.getSchema(), response.getResults(), response.getCursor(), PPL_SPEC));
        listener.onResponse(new TransportPPLQueryResponse(body));
      }

      @Override public void onFailure(Exception e) { listener.onFailure(e); }
    };
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
}
```

- [ ] **Step 4: Compile**

Run:
```bash
./gradlew :plugin:compileJava 2>&1 | tail -20
```
Expected: `BUILD SUCCESSFUL`. Fix Guice binding if `OmniEngineService` not provided. If there's a mismatch with OpenSearchPluginModule's API, update that module to bind `OmniEngineService` too.

- [ ] **Step 5: Commit**

```bash
git add plugin
git commit -s -m "feat(omni-442): route PPL transport to OmniEngineService (no fallback)"
```

---

## Phase 7: Remove the old Calcite PPL path

### Task 7.1: Strip Calcite path from `QueryService`

**Files:**
- Modify: `core/src/main/java/org/opensearch/sql/executor/QueryService.java`

- [ ] **Step 1: Delete `executeWithCalcite`, `explainWithCalcite`, `executeWithLegacy`, `explainWithLegacy`, `shouldUseCalcite`, `isCalciteEnabled`, `isCalciteFallbackAllowed`, `isCalciteUnsupportedError`, `analyze(UnresolvedPlan, CalcitePlanContext)`, `convertToCalcitePlan`, `buildFrameworkConfig`, and any Calcite imports.**

Keep only the legacy V2 SQL path (used by `RestSqlAction` in legacy module): `execute(UnresolvedPlan, QueryType, ResponseListener)` that calls `executePlan(...)` using `analyzer.analyze(plan, new AnalysisContext(queryType))` + `planner.plan(...)`.

- [ ] **Step 2: Compile core**

Run:
```bash
./gradlew :core:compileJava 2>&1 | tail -20
```
Expected: `BUILD SUCCESSFUL`. Many downstream callers probably don't exist — since PPL now goes through OmniEngineService.

- [ ] **Step 3: Compile everything**

```bash
./gradlew compileJava 2>&1 | tail -30
```
Expected: `BUILD SUCCESSFUL`. Fix any remaining callers of the removed methods.

- [ ] **Step 4: Commit**

```bash
git add core plugin opensearch
git commit -s -m "feat(omni-442): remove Calcite PPL path from QueryService (no fallback)"
```

### Task 7.2: Remove `OpenSearchExecutionEngine` RelNode path

**Files:**
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/executor/OpenSearchExecutionEngine.java`

- [ ] **Step 1: Remove `execute(RelNode, CalcitePlanContext, ResponseListener)` and `explain(RelNode, ExplainMode, CalcitePlanContext, ResponseListener)` methods** — PPL no longer calls them; SQL legacy doesn't use them.

Keep only the PhysicalPlan methods (used by the SQL legacy path).

- [ ] **Step 2: Compile**

```bash
./gradlew compileJava 2>&1 | tail -20
```
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 3: Commit**

```bash
git add opensearch
git commit -s -m "feat(omni-442): remove RelNode execution path from OpenSearchExecutionEngine"
```

---

## Phase 8: Plugin ZIP and jar-hell wiring

### Task 8.1: Port parquet-merge and coral-filter into plugin's bundlePlugin

**Files:**
- Modify: `plugin/build.gradle`

- [ ] **Step 1: Add shadow plugin import and parquet-merge task**

At the top of `plugin/build.gradle` add (or merge with existing) `buildscript`:
```gradle
buildscript {
    repositories {
        mavenCentral()
        maven { url "https://plugins.gradle.org/m2/" }
    }
    dependencies {
        classpath 'com.gradleup.shadow:shadow-gradle-plugin:8.3.9'
    }
}
```

- [ ] **Step 2: Port the full `configurations { parquetMerge }`, `mergeParquetJars`, `filterCoralJar`, and `tasks.named('bundlePlugin')` blocks** from `$HOME/oss/omni/build.gradle` lines ~80-145 into `plugin/build.gradle`. Copy verbatim.

- [ ] **Step 3: Add `thirdPartyAudit.enabled = false` and `loggerUsageCheck.enabled = false`** — the plugin ZIP pulls in the vendored Trino classes which would fail these audits.

Already in plugin/build.gradle per inspection:
```gradle
loggerUsageCheck.enabled = false
```
Add:
```gradle
thirdPartyAudit.enabled = false
```

- [ ] **Step 4: Test bundlePlugin**

Run:
```bash
./gradlew :plugin:bundlePlugin 2>&1 | tail -30
```
Expected: `BUILD SUCCESSFUL`, `plugin/build/distributions/opensearch-sql-*.zip` produced. If jar-hell errors: double-check the `exclude` entries match what omni has.

- [ ] **Step 5: Commit**

```bash
git add plugin/build.gradle
git commit -s -m "chore(omni-442): parquet-merge + coral-filter for plugin ZIP"
```

---

## Phase 9: Wire tests against the new engine

### Task 9.1: Configure integTest cluster settings

**Files:**
- Modify: `integ-test/build.gradle`

- [ ] **Step 1: Add Omni-required settings to test clusters**

In `testClusters.integTest`:
```gradle
setting 'aux.transport.types', '["omni-exchange"]'
systemProperty 'opensearch.jar.hell.skip', 'true'
setting 'plugins.omni.catalog.type', 'hive'
// Default metastore points at a minimal file-metastore embedded in test resources
setting 'plugins.omni.catalog.metastore.dir',
    "file://${project.projectDir}/src/test/resources/omni-metastore"
```

Also on the `integTest` task:
```gradle
systemProperty 'opensearch.jar.hell.skip', 'true'
```

Do the same for `testClusters.yamlRestTest` and `testClusters.integTestWithSecurity`.

- [ ] **Step 2: Compile test classpath**

Run:
```bash
./gradlew :integ-test:compileTestJava 2>&1 | tail -15
```
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 3: Commit**

```bash
git add integ-test/build.gradle
git commit -s -m "chore(omni-442): configure integ-test clusters for Omni AuxTransport + catalog"
```

### Task 9.2: Smoke test — one calcite IT

- [ ] **Step 1: Run a basic PPL IT**

```bash
./gradlew :integ-test:integTest -Dtests.class="org.opensearch.sql.calcite.standalone.CalcitePPLBasicIT" 2>&1 | tail -30
```
Expected: some tests pass, some fail. Expected failures at this point will hint which pieces need fixing. Do not move on until at least ONE IT method passes — that confirms the whole pipeline (transport → omni → Trino → result → formatter) is wired.

- [ ] **Step 2: If zero tests pass, diagnose**

Common issues and fixes:
- "catalog opensearch not found" → `plugins.omni.catalog.type` is not auto-activating the OpenSearch connector. Check `ServiceWiring` for how it wires the OpenSearch connector when plugin starts.
- "table not found" → test setup creates an index but it's not showing up in the `opensearch` catalog. Check `OpenSearchConnectorFactory` logic (auto-scans indices from `IndicesService`).
- "PPL translation fails" → `UnifiedAPI` path. Test by running the translator directly in a unit test.

- [ ] **Step 3: Commit fixes if any**

```bash
git add .
git commit -s -m "fix(omni-442): <specific fix>"
```

### Task 9.3: Broad IT run — find the failure surface

- [ ] **Step 1: Run all calcite IT with a generous timeout, tee to log**

```bash
./gradlew :integ-test:integTest -Dtests.class="org.opensearch.sql.calcite.*" 2>&1 | tee /tmp/omni442-calcite-run.log || true
```

- [ ] **Step 2: Aggregate failures by root-cause category**

```bash
grep -E "FAILED|Error|Exception" /tmp/omni442-calcite-run.log | sort -u > /tmp/omni442-failures.txt
wc -l /tmp/omni442-failures.txt
```

- [ ] **Step 3: Classify and triage** — create a tracking file.

File: `docs/superpowers/plans/omni-442-test-triage.md` (not committed — scratchpad)
```markdown
# Calcite IT failure categories

## A. Explain golden mismatches (expected — regenerate)
- CalciteExplainIT.* — golden files under `integ-test/src/test/resources/expectedOutput/calcite/`

## B. Unsupported PPL feature in PplTranslator
- list here

## C. Type/format drift
- list here

## D. Cluster-setup issues
- list here

## E. True bugs in OmniEngineService
- list here
```

Fix category by category; commit each category as a single change set:
```bash
git commit -s -m "fix(omni-442): <category>"
```

### Task 9.4: Regenerate explain goldens

- [ ] **Step 1: Run the explain test with regeneration flag** (if supported) or write a helper

```bash
# Approach: write a one-off test that runs each explain IT, captures the new plan,
# and overwrites the .yaml golden. NOT committed as a test; just a dev script.
mkdir -p integ-test/scripts
```

File: `integ-test/scripts/regen-explain-goldens.sh` (not committed)
```bash
#!/bin/bash
# For each golden file, run the corresponding query, capture plan, write it back.
# Implementation depends on CalciteExplainIT structure — may need to run in Java.
```

The simpler approach: run `./gradlew :integ-test:integTest -Dtests.class="org.opensearch.sql.calcite.CalciteExplainIT" -Dregen=true`, teaching the test to write instead of assert when `-Dregen=true`. Add the flag support inline:

File: `integ-test/src/test/java/org/opensearch/sql/calcite/CalciteExplainIT.java` — in the assertion helper:
```java
if (Boolean.getBoolean("regen")) {
    Files.writeString(goldenPath, actualPlan);
    return;
}
assertEquals(expected, actual);
```

- [ ] **Step 2: Run with regen flag**

```bash
./gradlew :integ-test:integTest -Dtests.class="org.opensearch.sql.calcite.CalciteExplainIT" -Dregen=true
```

- [ ] **Step 3: Re-run without flag — expect all pass**

```bash
./gradlew :integ-test:integTest -Dtests.class="org.opensearch.sql.calcite.CalciteExplainIT"
```
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 4: Commit regenerated goldens**

```bash
git add integ-test/src/test/resources/expectedOutput/calcite
git commit -s -m "test(omni-442): regenerate explain goldens for Omni EXPLAIN output"
```

### Task 9.5: Full test suite green

- [ ] **Step 1: Full integ-test run**

```bash
./gradlew :integ-test:integTest 2>&1 | tee /tmp/omni442-full-integ.log
```

- [ ] **Step 2: Drive failure count to zero — iterate**

For each failure:
- If it's a true Omni bug: fix in omni-engine, ensure verbatim behavior with `$HOME/oss/omni` feat/ppl's path.
- If it's a missing PPL feature: extend `PplTranslator` or omni's UDF list.
- If it's a cluster config: extend `integ-test/build.gradle`.

Commit one logical fix per commit.

- [ ] **Step 3: Full yamlRestTest run**

```bash
./gradlew :integ-test:yamlRestTest 2>&1 | tee /tmp/omni442-yaml.log
```

- [ ] **Step 4: Drive to zero — iterate**

---

## Phase 10: Port omni's own regression tests

### Task 10.1: Move PPLClickBenchIT + PPLBig5IT

- [ ] **Step 1: Copy test files**

```bash
cp $HOME/oss/omni/src/test/java/org/opensearch/plugin/omni/PPLClickBenchIT.java \
   /local/home/penghuo/oss/ppl/integ-test/src/test/java/org/opensearch/sql/calcite/clickbench/PPLClickBenchIT.java
cp $HOME/oss/omni/src/test/java/org/opensearch/plugin/omni/PPLBig5IT.java \
   /local/home/penghuo/oss/ppl/integ-test/src/test/java/org/opensearch/sql/calcite/big5/PPLBig5IT.java
```

- [ ] **Step 2: Update package declarations and endpoint**

In both files: change the REST endpoint from `/_plugins/_omni/v1/query` to `/_plugins/_ppl`. Change request body from `{"ppl": "..."}` to the PPL-transport shape (`{"query": "..."}`). Change response parser to read PPL's `{schema, datarows}` format instead of `{columns, data, state}`.

- [ ] **Step 3: Copy test resources**

```bash
cp -r $HOME/oss/omni/src/test/resources/clickbench-ppl \
      /local/home/penghuo/oss/ppl/integ-test/src/test/resources/
cp -r $HOME/oss/omni/src/test/resources/big5-ppl \
      /local/home/penghuo/oss/ppl/integ-test/src/test/resources/ 2>/dev/null || true
```

- [ ] **Step 4: Run them**

```bash
./gradlew :integ-test:integTest -Dtests.class="*PPLClickBenchIT" 2>&1 | tail -30
./gradlew :integ-test:integTest -Dtests.class="*PPLBig5IT" 2>&1 | tail -30
```
Expected: 43/43 + 58/58 pass.

- [ ] **Step 5: Commit**

```bash
git add integ-test
git commit -s -m "test(omni-442): port omni's PPLClickBenchIT + PPLBig5IT as regression guards"
```

---

## Phase 11: Verification before completion

### Task 11.1: Final full-suite run

- [ ] **Step 1: Clean build**

```bash
./gradlew clean
./gradlew build -x integTest 2>&1 | tail -20
```
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 2: Full integTest**

```bash
./gradlew :integ-test:integTest 2>&1 | tail -50
```
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 3: Full yamlRestTest**

```bash
./gradlew :integ-test:yamlRestTest 2>&1 | tail -50
```
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 4: Spotless**

```bash
./gradlew spotlessCheck 2>&1 | tail -10
```
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 5: Record the result in the plan**

Append to this plan file: a dated "completed" note with the final SHA and the integTest/yamlRestTest pass counts.

### Task 11.2: Close out

- [ ] **Step 1: Update CLAUDE.md with the new path**

Modify `CLAUDE.md` — replace the "Calcite Engine" section with an "Omni Engine" section that describes the new path.

- [ ] **Step 2: Final commit**

```bash
git add CLAUDE.md docs
git commit -s -m "docs(omni-442): update CLAUDE.md for Omni engine"
```

- [ ] **Step 3: Open PR**

```bash
git push -u origin feat/omni-442
gh pr create --title "Migrate Omni distributed engine into ppl repo as PPL execution engine" \
  --body "$(cat <<'EOF'
## Summary
- Vendored Trino 442 + Omni adapters from `~/oss/omni` feat/ppl into new `omni-engine` module
- Replaced Calcite PPL path in `TransportPPLQueryAction` with `OmniEngineService` (no fallback)
- Regenerated `expectedOutput/calcite/*.yaml` explain goldens for Trino EXPLAIN output
- Ported PPLClickBenchIT + PPLBig5IT as regression guards

Design: `docs/superpowers/specs/2026-04-27-omni-442-migration-design.md`
Plan: `docs/superpowers/plans/2026-04-27-omni-442-migration.md`

## Test plan
- [x] `./gradlew build -x integTest` green
- [x] `./gradlew :integ-test:integTest` green (N tests passing)
- [x] `./gradlew :integ-test:yamlRestTest` green (M tests passing)
- [x] PPLClickBenchIT 43/43 pass
- [x] PPLBig5IT 58/58 pass
EOF
)"
```

---

## Self-review checklist (for plan author)

- **Spec coverage:** Every §1-§3 bullet has a task — §1 flow → Phases 5-6; §2 module layout → Phases 1-4; §3a execute → Task 5.3; §3b explain → Task 5.3; §3c type mapping → Task 5.1; §3d row conversion → Task 5.2; §3e errors → Task 5.3 `classify`; §3f lifecycle → Task 6.1.
- **Placeholder scan:** One `// TODO: parse from request` in Task 6.2 for multi-mode explain — accepted because the current repo's request API doesn't expose explain mode distinctly (STANDARD is the default, and this TODO is explicitly scoped and visible). No TBDs elsewhere.
- **Type consistency:** `OmniEngineService.execute(PPLQueryRequest, ResponseListener)` and `OmniEngineService.explain(PPLQueryRequest, ExplainMode, ResponseListener)` used consistently in Tasks 5.3 and 6.2. `QueryResponseAdapter.adapt(List<Column>, List<List<Object>>)` consistent across Tasks 5.2 and 5.3.
