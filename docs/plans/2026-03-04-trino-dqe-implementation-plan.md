# Trino DQE Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a distributed query execution engine inside OpenSearch that parses SQL with Trino's parser, optimizes with Trino's optimizer, and executes plan fragments on individual shards via physical operators — using OpenSearch's local search API at the leaf level to preserve FLS/DLS.

**Architecture:** A single `dqe/` Gradle module contains forked Trino components (parser, AST, type system, block/page data model) and custom OpenSearch integration code (metadata adapter, page source, transport actions, REST endpoint). The coordinator parses, optimizes, and fragments the plan; each shard receives a fragment via TransportAction, builds an operator pipeline, and executes it against the local search API. Partial results flow back for coordinator-side merge.

**Tech Stack:** Java 21, Trino parser/SPI JARs (shaded), ANTLR 4.13.2, OpenSearch 3.6.x transport framework, JUnit 5, Mockito.

**Design doc:** `docs/plans/2026-03-04-trino-native-integration-design.md`

---

## TDD Protocol

**Every task follows Red-Green-Refactor strictly.** No production code without a failing test first.

```
RED:        Write one failing test (test file first, always)
VERIFY RED: Run test. Confirm it fails for the RIGHT reason (missing class/method, not typo).
GREEN:      Write minimal production code to pass that test.
VERIFY GREEN: Run test. Confirm it passes. Confirm all prior tests still pass.
REFACTOR:   Clean up. Keep tests green.
COMMIT:     Commit the test + implementation together.
REPEAT:     Next failing test for next behavior.
```

**Exceptions** (configuration files only — no behavior to test):
- `build.gradle`, `settings.gradle`: Verified by `./gradlew compileJava` instead of unit tests.
- Plugin wiring (`SQLPlugin.java` modifications): Verified by compilation. Behavior covered by integration tests in Task 17.

**Iron law**: If a test passes immediately on first run, it tests nothing. Fix or delete it.

---

## Task 1: Create the `dqe/` Gradle Module

**Exception: Infrastructure/config — verified by compilation, not unit tests.**

**Files:**
- Create: `dqe/build.gradle`
- Modify: `settings.gradle` (add `include 'dqe'`)
- Modify: `plugin/build.gradle` (add `api project(':dqe')`)

### Step 1: Register module in settings.gradle

Add `include 'dqe'` after the existing `include 'direct-query'` line:

```gradle
include 'direct-query'
include 'dqe'
include 'language-grammar'
```

### Step 2: Create dqe/build.gradle

```gradle
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

plugins {
    id 'java-library'
    id "io.freefair.lombok"
    id 'jacoco'
    id 'com.diffplug.spotless'
}

repositories {
    mavenCentral()
}

dependencies {
    // Trino parser (includes AST tree nodes)
    implementation('io.trino:trino-parser:467') {
        exclude group: 'com.google.guava', module: 'guava'
        exclude group: 'jakarta.annotation', module: 'jakarta.annotation-api'
    }

    // Trino SPI (Block, Page, Type system)
    implementation('io.trino:trino-spi:467') {
        exclude group: 'com.google.guava', module: 'guava'
        exclude group: 'com.fasterxml.jackson.core'
        exclude group: 'io.opentelemetry'
    }

    // Airlift Slice — standalone, used by Trino for string/binary data
    implementation 'io.airlift:slice:2.3'

    // ANTLR runtime (Trino's grammar uses 4.13.2)
    implementation "org.antlr:antlr4-runtime:4.13.2"

    // OpenSearch core
    compileOnly group: 'org.opensearch', name: 'opensearch', version: "${opensearch_version}"
    compileOnly group: 'org.opensearch', name: 'opensearch-x-content', version: "${opensearch_version}"

    // Guava (provided by OpenSearch plugin at runtime)
    implementation group: 'com.google.guava', name: 'guava', version: "${guava_version}"

    // Logging
    implementation group: 'org.apache.logging.log4j', name: 'log4j-core', version: "${versions.log4j}"

    // Test
    testImplementation 'org.junit.jupiter:junit-jupiter:5.9.3'
    testImplementation group: 'org.hamcrest', name: 'hamcrest-library', version: "${hamcrest_version}"
    testImplementation group: 'org.mockito', name: 'mockito-core', version: "${mockito_version}"
    testImplementation group: 'org.mockito', name: 'mockito-junit-jupiter', version: "${mockito_version}"
    testImplementation "org.opensearch.test:framework:${opensearch_version}"
    testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
}

spotless {
    java {
        target fileTree('.') {
            include '**/*.java'
            exclude '**/build/**', '**/build-*/**'
        }
        importOrder()
        licenseHeader("/*\n" +
                " * Copyright OpenSearch Contributors\n" +
                " * SPDX-License-Identifier: Apache-2.0\n" +
                " */\n\n")
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
        googleJavaFormat('1.32.0').reflowLongStrings().groupArtifact('com.google.googlejavaformat:google-java-format')
    }
}

test {
    maxParallelForks = Runtime.runtime.availableProcessors()
    useJUnitPlatform()
    testLogging {
        events "skipped", "failed"
        exceptionFormat "full"
    }
}

jacocoTestReport {
    reports {
        html.required = true
        xml.required = true
    }
}
test.finalizedBy(project.tasks.jacocoTestReport)
jacocoTestCoverageVerification {
    violationRules {
        rule {
            limit {
                minimum = 0.5
            }
        }
    }
}
check.dependsOn jacocoTestCoverageVerification
jacocoTestCoverageVerification.dependsOn jacocoTestReport
```

### Step 3: Add dqe dependency to plugin/build.gradle

In the dependencies block of `plugin/build.gradle`, add:
```gradle
    api project(':dqe')
```

### Step 4: RED — Write a smoke test that uses Trino parser (no production code yet)

Create: `dqe/src/test/java/org/opensearch/sql/dqe/TrinoParserSmokeTest.java`

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Trino parser smoke test")
class TrinoParserSmokeTest {

  @Test
  @DisplayName("Parse a simple SELECT statement")
  void parseSimpleSelect() {
    SqlParser parser = new SqlParser();
    Statement stmt =
        parser.createStatement("SELECT a, b FROM t WHERE x = 1", new ParsingOptions());
    assertNotNull(stmt);
  }

  @Test
  @DisplayName("Parse SELECT with GROUP BY and ORDER BY")
  void parseGroupBy() {
    SqlParser parser = new SqlParser();
    Statement stmt =
        parser.createStatement(
            "SELECT category, COUNT(*) FROM logs WHERE status = 200 "
                + "GROUP BY category ORDER BY COUNT(*) DESC LIMIT 10",
            new ParsingOptions());
    assertNotNull(stmt);
  }
}
```

### Step 5: VERIFY RED

Run: `./gradlew :dqe:test`
Expected: Either compilation error (if Trino JARs not resolved) or PASS (if resolved).
If compilation error: fix `build.gradle` dependency declarations. Re-run until tests compile and pass.
If tests pass: good — the smoke test validates the Trino JAR resolves correctly.

Note: This test exercises Trino's code (not ours), so it's expected to pass immediately. This is acceptable because it validates the build configuration, not production behavior.

### Step 6: Commit

```bash
git add dqe/ settings.gradle plugin/build.gradle
git commit -m "feat(dqe): scaffold dqe module with Trino parser dependency"
```

---

## Task 2: Type Mapping — OpenSearch Field Types to Trino Types

**Files:**
- Test: `dqe/src/test/java/org/opensearch/sql/dqe/common/types/TypeMappingTest.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/common/types/TypeMapping.java`

### Step 1: RED — Write the test first

Create: `dqe/src/test/java/org/opensearch/sql/dqe/common/types/TypeMappingTest.java`

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.common.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@DisplayName("OpenSearch → Trino type mapping")
class TypeMappingTest {

  @ParameterizedTest
  @CsvSource({
    "keyword, VARCHAR",
    "text, VARCHAR",
    "long, BIGINT",
    "integer, INTEGER",
    "short, SMALLINT",
    "byte, TINYINT",
    "double, DOUBLE",
    "float, REAL",
    "boolean, BOOLEAN",
    "date, TIMESTAMP",
    "ip, VARCHAR",
    "binary, VARBINARY"
  })
  @DisplayName("Map scalar OpenSearch types to Trino types")
  void mapScalarTypes(String osType, String expectedTrinoName) {
    var trinoType = TypeMapping.toTrinoType(osType);
    assertEquals(expectedTrinoName, trinoType.getDisplayName().toUpperCase());
  }

  @Test
  @DisplayName("Unknown type throws IllegalArgumentException")
  void unknownTypeThrows() {
    assertThrows(IllegalArgumentException.class, () -> TypeMapping.toTrinoType("unsupported_xyz"));
  }
}
```

### Step 2: VERIFY RED

Run: `./gradlew :dqe:test --tests '*TypeMappingTest*'`
Expected: FAIL — compilation error: `TypeMapping` class does not exist.
This is the correct failure — the test demands a class that doesn't exist yet.

### Step 3: GREEN — Write minimal implementation

Create: `dqe/src/main/java/org/opensearch/sql/dqe/common/types/TypeMapping.java`

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.common.types;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import java.util.Map;

/** Maps OpenSearch field types to Trino types. */
public final class TypeMapping {

  private static final Map<String, Type> OS_TO_TRINO =
      Map.ofEntries(
          Map.entry("keyword", VarcharType.VARCHAR),
          Map.entry("text", VarcharType.VARCHAR),
          Map.entry("long", BigintType.BIGINT),
          Map.entry("integer", IntegerType.INTEGER),
          Map.entry("short", SmallintType.SMALLINT),
          Map.entry("byte", TinyintType.TINYINT),
          Map.entry("double", DoubleType.DOUBLE),
          Map.entry("float", RealType.REAL),
          Map.entry("half_float", RealType.REAL),
          Map.entry("scaled_float", DoubleType.DOUBLE),
          Map.entry("boolean", BooleanType.BOOLEAN),
          Map.entry("date", TimestampType.TIMESTAMP_MILLIS),
          Map.entry("ip", VarcharType.VARCHAR),
          Map.entry("binary", VarbinaryType.VARBINARY));

  private TypeMapping() {}

  public static Type toTrinoType(String openSearchType) {
    Type result = OS_TO_TRINO.get(openSearchType);
    if (result == null) {
      throw new IllegalArgumentException("Unsupported OpenSearch type: " + openSearchType);
    }
    return result;
  }
}
```

### Step 4: VERIFY GREEN

Run: `./gradlew :dqe:test --tests '*TypeMappingTest*'`
Expected: 13 tests PASS (12 parameterized + 1 exception test).

Run: `./gradlew :dqe:test`
Expected: All tests PASS (including Task 1 smoke tests).

### Step 5: Commit

```bash
git add dqe/src/
git commit -m "feat(dqe): add OpenSearch to Trino type mapping"
```

---

## Task 3: Page Builder — Convert SearchHits to Trino Pages

**Files:**
- Test: `dqe/src/test/java/org/opensearch/sql/dqe/shard/source/PageBuilderTest.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/PageBuilder.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/ColumnHandle.java` (created as part of GREEN — needed to make test pass)

### Step 1: RED — Write the test first

Create: `dqe/src/test/java/org/opensearch/sql/dqe/shard/source/PageBuilderTest.java`

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.source;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.trino.spi.Page;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("PageBuilder: convert row maps to Trino Pages")
class PageBuilderTest {

  @Test
  @DisplayName("Build a page from two rows with varchar and bigint columns")
  void buildPageFromRows() {
    List<ColumnHandle> columns =
        List.of(
            new ColumnHandle("name", VarcharType.VARCHAR),
            new ColumnHandle("age", BigintType.BIGINT));

    List<Map<String, Object>> rows =
        List.of(Map.of("name", "alice", "age", 30), Map.of("name", "bob", "age", 25));

    Page page = PageBuilder.build(columns, rows);

    assertEquals(2, page.getPositionCount());
    assertEquals(2, page.getChannelCount());
    assertEquals(30L, BigintType.BIGINT.getLong(page.getBlock(1), 0));
    assertEquals(25L, BigintType.BIGINT.getLong(page.getBlock(1), 1));
  }

  @Test
  @DisplayName("Null values produce null entries in blocks")
  void nullValuesHandled() {
    List<ColumnHandle> columns = List.of(new ColumnHandle("val", BigintType.BIGINT));
    List<Map<String, Object>> rows = List.of(Map.of(), Map.of("val", 42));

    Page page = PageBuilder.build(columns, rows);

    assertEquals(2, page.getPositionCount());
    assertEquals(true, page.getBlock(0).isNull(0));
    assertEquals(42L, BigintType.BIGINT.getLong(page.getBlock(0), 1));
  }
}
```

### Step 2: VERIFY RED

Run: `./gradlew :dqe:test --tests '*PageBuilderTest*'`
Expected: FAIL — compilation error: `ColumnHandle` and `PageBuilder` do not exist.

### Step 3: GREEN — Write minimal ColumnHandle + PageBuilder

Create `ColumnHandle.java` (minimal record the test needs):
```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.source;

import io.trino.spi.type.Type;

public record ColumnHandle(String name, Type type) {}
```

Create `PageBuilder.java`:
```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.source;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import java.util.Map;

public final class PageBuilder {

  private PageBuilder() {}

  public static Page build(List<ColumnHandle> columns, List<Map<String, Object>> rows) {
    BlockBuilder[] builders = new BlockBuilder[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      builders[i] = columns.get(i).type().createBlockBuilder(null, rows.size());
    }

    for (Map<String, Object> row : rows) {
      for (int col = 0; col < columns.size(); col++) {
        Object value = row.get(columns.get(col).name());
        appendValue(builders[col], columns.get(col).type(), value);
      }
    }

    Block[] blocks = new Block[builders.length];
    for (int i = 0; i < builders.length; i++) {
      blocks[i] = builders[i].build();
    }
    return new Page(blocks);
  }

  private static void appendValue(BlockBuilder builder, Type type, Object value) {
    if (value == null) {
      builder.appendNull();
      return;
    }
    if (type instanceof VarcharType) {
      VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(value.toString()));
    } else if (type instanceof BigintType) {
      BigintType.BIGINT.writeLong(builder, ((Number) value).longValue());
    } else if (type instanceof IntegerType) {
      IntegerType.INTEGER.writeLong(builder, ((Number) value).intValue());
    } else if (type instanceof SmallintType) {
      SmallintType.SMALLINT.writeLong(builder, ((Number) value).shortValue());
    } else if (type instanceof TinyintType) {
      TinyintType.TINYINT.writeLong(builder, ((Number) value).byteValue());
    } else if (type instanceof DoubleType) {
      DoubleType.DOUBLE.writeDouble(builder, ((Number) value).doubleValue());
    } else if (type instanceof RealType) {
      RealType.REAL.writeLong(builder, Float.floatToIntBits(((Number) value).floatValue()));
    } else if (type instanceof BooleanType) {
      BooleanType.BOOLEAN.writeBoolean(builder, (Boolean) value);
    } else if (type instanceof TimestampType) {
      long epochMillis = ((Number) value).longValue();
      TimestampType.TIMESTAMP_MILLIS.writeLong(builder, epochMillis * 1000);
    } else if (type instanceof VarbinaryType) {
      VarbinaryType.VARBINARY.writeSlice(builder, Slices.wrappedBuffer((byte[]) value));
    } else {
      throw new UnsupportedOperationException("Unsupported Trino type: " + type.getDisplayName());
    }
  }
}
```

### Step 4: VERIFY GREEN

Run: `./gradlew :dqe:test --tests '*PageBuilderTest*'`
Expected: 2 tests PASS.

Run: `./gradlew :dqe:test`
Expected: All tests PASS.

### Step 5: Commit

```bash
git add dqe/src/
git commit -m "feat(dqe): add PageBuilder for SearchHit to Trino Page conversion"
```

---

## Task 4: OpenSearchMetadata — Cluster State to Trino Schema

**Files:**
- Test: `dqe/src/test/java/org/opensearch/sql/dqe/coordinator/metadata/OpenSearchMetadataTest.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/metadata/TableInfo.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/metadata/OpenSearchMetadata.java`

### Step 1: RED — Write the test first

Create: `dqe/src/test/java/org/opensearch/sql/dqe/coordinator/metadata/OpenSearchMetadataTest.java`

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;

@DisplayName("OpenSearchMetadata: cluster state → table schema")
class OpenSearchMetadataTest {

  @Test
  @DisplayName("Resolves index mapping to TableInfo with Trino types")
  void resolveIndexMapping() {
    ClusterService clusterService = mock(ClusterService.class);
    ClusterState clusterState = mock(ClusterState.class);
    Metadata metadata = mock(Metadata.class);
    IndexMetadata indexMetadata = mock(IndexMetadata.class);
    MappingMetadata mappingMetadata = mock(MappingMetadata.class);

    when(clusterService.state()).thenReturn(clusterState);
    when(clusterState.metadata()).thenReturn(metadata);
    when(metadata.index("logs")).thenReturn(indexMetadata);
    when(indexMetadata.mapping()).thenReturn(mappingMetadata);
    when(mappingMetadata.sourceAsMap())
        .thenReturn(
            Map.of(
                "properties",
                Map.of(
                    "category", Map.of("type", "keyword"),
                    "status", Map.of("type", "long"))));

    OpenSearchMetadata osMetadata = new OpenSearchMetadata(clusterService);
    TableInfo table = osMetadata.getTableInfo("logs");

    assertEquals("logs", table.indexName());
    assertEquals(2, table.columns().size());
  }

  @Test
  @DisplayName("Throws for non-existent index")
  void nonExistentIndex() {
    ClusterService clusterService = mock(ClusterService.class);
    ClusterState clusterState = mock(ClusterState.class);
    Metadata metadata = mock(Metadata.class);

    when(clusterService.state()).thenReturn(clusterState);
    when(clusterState.metadata()).thenReturn(metadata);
    when(metadata.index("missing")).thenReturn(null);

    OpenSearchMetadata osMetadata = new OpenSearchMetadata(clusterService);
    assertThrows(IllegalArgumentException.class, () -> osMetadata.getTableInfo("missing"));
  }

  @Test
  @DisplayName("Skips unsupported field types without error")
  void skipsUnsupportedTypes() {
    ClusterService clusterService = mock(ClusterService.class);
    ClusterState clusterState = mock(ClusterState.class);
    Metadata metadata = mock(Metadata.class);
    IndexMetadata indexMetadata = mock(IndexMetadata.class);
    MappingMetadata mappingMetadata = mock(MappingMetadata.class);

    when(clusterService.state()).thenReturn(clusterState);
    when(clusterState.metadata()).thenReturn(metadata);
    when(metadata.index("idx")).thenReturn(indexMetadata);
    when(indexMetadata.mapping()).thenReturn(mappingMetadata);
    when(mappingMetadata.sourceAsMap())
        .thenReturn(
            Map.of(
                "properties",
                Map.of(
                    "name", Map.of("type", "keyword"),
                    "location", Map.of("type", "geo_shape"))));

    OpenSearchMetadata osMetadata = new OpenSearchMetadata(clusterService);
    TableInfo table = osMetadata.getTableInfo("idx");

    assertEquals(1, table.columns().size());
    assertEquals("name", table.columns().get(0).name());
  }
}
```

### Step 2: VERIFY RED

Run: `./gradlew :dqe:test --tests '*OpenSearchMetadataTest*'`
Expected: FAIL — compilation error: `OpenSearchMetadata`, `TableInfo` do not exist.

### Step 3: GREEN — Write minimal TableInfo + OpenSearchMetadata

Create `TableInfo.java`:
```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.metadata;

import java.util.List;

public record TableInfo(String indexName, List<ColumnInfo> columns) {
  public record ColumnInfo(String name, String openSearchType, io.trino.spi.type.Type trinoType) {}
}
```

Create `OpenSearchMetadata.java`:
```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.dqe.common.types.TypeMapping;
import org.opensearch.sql.dqe.coordinator.metadata.TableInfo.ColumnInfo;

public class OpenSearchMetadata {

  private final ClusterService clusterService;

  public OpenSearchMetadata(ClusterService clusterService) {
    this.clusterService = clusterService;
  }

  @SuppressWarnings("unchecked")
  public TableInfo getTableInfo(String indexName) {
    IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
    if (indexMetadata == null) {
      throw new IllegalArgumentException("Index not found: " + indexName);
    }

    MappingMetadata mapping = indexMetadata.mapping();
    if (mapping == null) {
      return new TableInfo(indexName, List.of());
    }

    Map<String, Object> properties =
        (Map<String, Object>) mapping.sourceAsMap().get("properties");
    if (properties == null) {
      return new TableInfo(indexName, List.of());
    }

    List<ColumnInfo> columns = new ArrayList<>();
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      Map<String, Object> fieldProps = (Map<String, Object>) entry.getValue();
      String fieldType = (String) fieldProps.get("type");
      if (fieldType != null) {
        try {
          columns.add(new ColumnInfo(entry.getKey(), fieldType, TypeMapping.toTrinoType(fieldType)));
        } catch (IllegalArgumentException e) {
          // Skip unsupported field types
        }
      }
    }
    return new TableInfo(indexName, columns);
  }
}
```

### Step 4: VERIFY GREEN

Run: `./gradlew :dqe:test --tests '*OpenSearchMetadataTest*'`
Expected: 3 tests PASS.

Run: `./gradlew :dqe:test`
Expected: All tests PASS.

### Step 5: Commit

```bash
git add dqe/src/
git commit -m "feat(dqe): add OpenSearchMetadata for cluster state to schema resolution"
```

---

## Task 5: SQL Parser Wrapper

**Files:**
- Test: `dqe/src/test/java/org/opensearch/sql/dqe/trino/parser/DqeSqlParserTest.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/trino/parser/DqeSqlParser.java`

### Step 1: RED — Write the test first

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.trino.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("DqeSqlParser")
class DqeSqlParserTest {

  private final DqeSqlParser parser = new DqeSqlParser();

  @Test
  @DisplayName("Parse SELECT returns Query node")
  void parseSelect() {
    Statement stmt = parser.parse("SELECT a FROM t");
    assertInstanceOf(Query.class, stmt);
  }

  @Test
  @DisplayName("Extract table names from query")
  void extractTableNames() {
    Set<String> tables = parser.extractTableNames("SELECT a FROM logs WHERE x = 1");
    assertEquals(Set.of("logs"), tables);
  }

  @Test
  @DisplayName("Extract table names from join")
  void extractTableNamesFromJoin() {
    Set<String> tables = parser.extractTableNames("SELECT * FROM a JOIN b ON a.id = b.id");
    assertEquals(Set.of("a", "b"), tables);
  }

  @Test
  @DisplayName("Invalid SQL throws exception")
  void invalidSqlThrows() {
    assertThrows(Exception.class, () -> parser.parse("NOT VALID SQL !!!"));
  }
}
```

### Step 2: VERIFY RED

Run: `./gradlew :dqe:test --tests '*DqeSqlParserTest*'`
Expected: FAIL — `DqeSqlParser` class does not exist.

### Step 3: GREEN — Write minimal implementation

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.trino.parser;

import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import java.util.HashSet;
import java.util.Set;

public class DqeSqlParser {

  private final SqlParser parser = new SqlParser();
  private final ParsingOptions options = new ParsingOptions();

  public Statement parse(String sql) {
    return parser.createStatement(sql, options);
  }

  public Set<String> extractTableNames(String sql) {
    Statement stmt = parse(sql);
    Set<String> tables = new HashSet<>();
    new AstVisitor<Void, Void>() {
      @Override
      protected Void visitTable(Table node, Void context) {
        tables.add(node.getName().toString());
        return null;
      }
    }.process(stmt, null);
    return tables;
  }
}
```

### Step 4: VERIFY GREEN

Run: `./gradlew :dqe:test --tests '*DqeSqlParserTest*'`
Expected: 4 tests PASS.

Run: `./gradlew :dqe:test`
Expected: All tests PASS.

### Step 5: Commit

```bash
git add dqe/src/
git commit -m "feat(dqe): add DqeSqlParser wrapping Trino SQL parser"
```

---

## Task 6: Plan Nodes — Logical Plan with Writeable Serialization

**Files:**
- Test: `dqe/src/test/java/org/opensearch/sql/dqe/planner/plan/PlanNodeTest.java`
- Create: all plan node classes (as part of GREEN, driven by failing tests)

This task builds the plan node hierarchy iteratively via TDD. Each sub-cycle adds one plan node type driven by a failing test.

### Cycle 6a: TableScanNode

**RED:**
```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.planner.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;

@DisplayName("Plan node serialization round-trip")
class PlanNodeTest {

  @Test
  @DisplayName("TableScanNode round-trips through StreamOutput/StreamInput")
  void tableScanRoundTrip() throws IOException {
    TableScanNode original = new TableScanNode("logs", List.of("category", "status"));

    BytesStreamOutput out = new BytesStreamOutput();
    DqePlanNode.writePlanNode(out, original);

    InputStreamStreamInput in =
        new InputStreamStreamInput(
            new ByteArrayInputStream(out.bytes().toBytesRef().bytes));
    DqePlanNode deserialized = DqePlanNode.readPlanNode(in);

    assertInstanceOf(TableScanNode.class, deserialized);
    TableScanNode scan = (TableScanNode) deserialized;
    assertEquals("logs", scan.getIndexName());
    assertEquals(List.of("category", "status"), scan.getColumns());
  }
}
```

**VERIFY RED:** `./gradlew :dqe:test --tests '*PlanNodeTest*'` — FAIL: `DqePlanNode`, `TableScanNode` do not exist.

**GREEN:** Create `DqePlanNode.java` (base class with `writePlanNode`/`readPlanNode`), `DqePlanVisitor.java`, and `TableScanNode.java` — the minimal code to make this one test pass.

**VERIFY GREEN:** `./gradlew :dqe:test --tests '*PlanNodeTest*tableScanRoundTrip*'` — PASS.

### Cycle 6b: FilterNode

**RED:** Add test to `PlanNodeTest.java`:
```java
  @Test
  @DisplayName("FilterNode round-trips with child and predicate")
  void filterNodeRoundTrip() throws IOException {
    TableScanNode scan = new TableScanNode("logs", List.of("status"));
    FilterNode filter = new FilterNode(scan, "status = 200");

    BytesStreamOutput out = new BytesStreamOutput();
    DqePlanNode.writePlanNode(out, filter);

    InputStreamStreamInput in =
        new InputStreamStreamInput(
            new ByteArrayInputStream(out.bytes().toBytesRef().bytes));
    DqePlanNode deserialized = DqePlanNode.readPlanNode(in);

    assertInstanceOf(FilterNode.class, deserialized);
    FilterNode result = (FilterNode) deserialized;
    assertEquals("status = 200", result.getPredicateString());
    assertInstanceOf(TableScanNode.class, result.getChild());
  }
```

**VERIFY RED:** FAIL — `FilterNode` does not exist.

**GREEN:** Create `FilterNode.java`. Update `DqePlanNode.readPlanNode` switch to include `FilterNode`.

**VERIFY GREEN:** PASS.

### Cycle 6c-6f: ProjectNode, AggregationNode, SortNode, LimitNode

Repeat the same cycle for each: write a failing round-trip test, then create the minimal class. Each node stores its child (serialized recursively via `DqePlanNode.writePlanNode`/`readPlanNode`) plus its specific fields.

`AggregationNode` fields: `List<String> groupByKeys`, `List<String> aggregateFunctions`, `Step step` (enum: PARTIAL, FINAL).

`SortNode` fields: `List<String> sortKeys`, `List<Boolean> ascending`.

`LimitNode` fields: `long count`.

### Cycle 6g: Nested tree round-trip

**RED:** Add test:
```java
  @Test
  @DisplayName("Nested plan tree round-trips correctly")
  void nestedPlanRoundTrip() throws IOException {
    TableScanNode scan = new TableScanNode("logs", List.of("category", "status"));
    FilterNode filter = new FilterNode(scan, "status = 200");
    LimitNode limit = new LimitNode(filter, 10);

    BytesStreamOutput out = new BytesStreamOutput();
    DqePlanNode.writePlanNode(out, limit);

    InputStreamStreamInput in =
        new InputStreamStreamInput(
            new ByteArrayInputStream(out.bytes().toBytesRef().bytes));
    DqePlanNode deserialized = DqePlanNode.readPlanNode(in);

    assertInstanceOf(LimitNode.class, deserialized);
    LimitNode limitResult = (LimitNode) deserialized;
    assertEquals(10, limitResult.getCount());
    assertInstanceOf(FilterNode.class, limitResult.getChild());
    assertInstanceOf(TableScanNode.class, ((FilterNode) limitResult.getChild()).getChild());
  }
```

**VERIFY RED:** Should FAIL if nested serialization isn't working yet. If it passes, good — the per-node cycles already covered this.

**VERIFY GREEN after all cycles.**

### Commit

```bash
git add dqe/src/
git commit -m "feat(dqe): add DQE plan node hierarchy with Writeable serialization"
```

---

## Task 7: Logical Planner — AST to Plan Nodes

**Files:**
- Test: `dqe/src/test/java/org/opensearch/sql/dqe/planner/LogicalPlannerTest.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/planner/LogicalPlanner.java`

### Step 1: RED — Write test for simplest case first

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.tree.Statement;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.dqe.coordinator.metadata.TableInfo;
import org.opensearch.sql.dqe.coordinator.metadata.TableInfo.ColumnInfo;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.FilterNode;
import org.opensearch.sql.dqe.planner.plan.LimitNode;
import org.opensearch.sql.dqe.planner.plan.ProjectNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;
import org.opensearch.sql.dqe.trino.parser.DqeSqlParser;

@DisplayName("LogicalPlanner: AST → DqePlanNode tree")
class LogicalPlannerTest {

  private final DqeSqlParser parser = new DqeSqlParser();

  @Test
  @DisplayName("SELECT a FROM t produces Project(TableScan)")
  void simpleSelect() {
    Statement stmt = parser.parse("SELECT a FROM t");
    DqePlanNode plan = LogicalPlanner.plan(stmt, this::mockTableInfo);
    assertInstanceOf(ProjectNode.class, plan);
    assertInstanceOf(TableScanNode.class, ((ProjectNode) plan).getChild());
  }

  private TableInfo mockTableInfo(String indexName) {
    return new TableInfo(
        indexName,
        List.of(
            new ColumnInfo("a", "keyword", VarcharType.VARCHAR),
            new ColumnInfo("x", "long", BigintType.BIGINT)));
  }
}
```

### Step 2: VERIFY RED

Run: `./gradlew :dqe:test --tests '*LogicalPlannerTest*simpleSelect*'`
Expected: FAIL — `LogicalPlanner` does not exist.

### Step 3: GREEN — Minimal LogicalPlanner for SELECT ... FROM

Write just enough `LogicalPlanner` to handle a simple SELECT/FROM — build `TableScanNode` from the FROM clause, wrap in `ProjectNode` from the SELECT clause.

### Step 4: VERIFY GREEN

Run: `./gradlew :dqe:test --tests '*LogicalPlannerTest*simpleSelect*'`
Expected: PASS.

### Cycle 7b: Add WHERE support

**RED:** Add test:
```java
  @Test
  @DisplayName("SELECT a FROM t WHERE x=1 produces Project(Filter(TableScan))")
  void selectWithFilter() {
    Statement stmt = parser.parse("SELECT a FROM t WHERE x = 1");
    DqePlanNode plan = LogicalPlanner.plan(stmt, this::mockTableInfo);
    assertInstanceOf(ProjectNode.class, plan);
    assertInstanceOf(FilterNode.class, ((ProjectNode) plan).getChild());
  }
```

**VERIFY RED:** FAIL — LogicalPlanner doesn't handle WHERE yet (returns Project(TableScan) without Filter).

**GREEN:** Add WHERE clause handling to `LogicalPlanner`.

**VERIFY GREEN:** PASS.

### Cycle 7c: Add LIMIT support

**RED:**
```java
  @Test
  @DisplayName("SELECT a FROM t LIMIT 10 produces Limit(Project(TableScan))")
  void selectWithLimit() {
    Statement stmt = parser.parse("SELECT a FROM t LIMIT 10");
    DqePlanNode plan = LogicalPlanner.plan(stmt, this::mockTableInfo);
    assertInstanceOf(LimitNode.class, plan);
    assertEquals(10, ((LimitNode) plan).getCount());
  }
```

**VERIFY RED → GREEN → VERIFY GREEN.**

### Cycle 7d: Add GROUP BY support

**RED:**
```java
  @Test
  @DisplayName("SELECT a, COUNT(*) FROM t GROUP BY a produces plan with AggregationNode")
  void selectWithGroupBy() {
    Statement stmt = parser.parse("SELECT a, COUNT(*) FROM t GROUP BY a");
    DqePlanNode plan = LogicalPlanner.plan(stmt, this::mockTableInfo);
    assertInstanceOf(AggregationNode.class, findNode(plan, AggregationNode.class));
  }
```

**VERIFY RED → GREEN → VERIFY GREEN.**

### Cycle 7e: Add ORDER BY support

Same pattern with `SortNode`.

### Step final: VERIFY all tests pass

Run: `./gradlew :dqe:test`
Expected: All tests PASS.

### Commit

```bash
git add dqe/src/
git commit -m "feat(dqe): add LogicalPlanner converting Trino AST to DQE plan nodes"
```

---

## Task 8: Physical Operators — Page-Based Execution

**Files:**
- Test: one test class per operator (written first)
- Create: `Operator.java` interface + operator implementations (written to pass tests)

Each operator is built in its own Red-Green cycle. Start with the simplest (LimitOperator), build up.

### Cycle 8a: Operator interface + LimitOperator

**RED — write test first:**

Create: `dqe/src/test/java/org/opensearch/sql/dqe/operator/LimitOperatorTest.java`

```java
@DisplayName("LimitOperator")
class LimitOperatorTest {

  @Test
  @DisplayName("Limits output to specified number of rows across multiple pages")
  void limitsRows() {
    Operator source = new TestPageSource(List.of(buildPage(5), buildPage(5)));
    LimitOperator limit = new LimitOperator(source, 7);

    List<Page> pages = drainOperator(limit);
    int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(7, totalRows);
  }

  @Test
  @DisplayName("Returns null after limit is reached")
  void returnsNullAfterLimit() {
    Operator source = new TestPageSource(List.of(buildPage(5)));
    LimitOperator limit = new LimitOperator(source, 3);

    Page first = limit.processNextBatch();
    assertNotNull(first);
    assertEquals(3, first.getPositionCount());
    assertNull(limit.processNextBatch());
  }
}
```

`TestPageSource` is a test helper that returns pre-built Pages. `buildPage(n)` creates a Page with `n` rows of dummy bigint data.

**VERIFY RED:** FAIL — `Operator`, `LimitOperator`, `TestPageSource` do not exist.

**GREEN:** Create `Operator.java` interface, `LimitOperator.java`, and `TestPageSource.java` (in test sources).

```java
public interface Operator extends Closeable {
  Page processNextBatch();
  @Override void close();
}
```

```java
public class LimitOperator implements Operator {
  private final Operator source;
  private final long limit;
  private long emitted = 0;

  public LimitOperator(Operator source, long limit) {
    this.source = source;
    this.limit = limit;
  }

  @Override
  public Page processNextBatch() {
    if (emitted >= limit) return null;
    Page page = source.processNextBatch();
    if (page == null) return null;
    long remaining = limit - emitted;
    if (page.getPositionCount() <= remaining) {
      emitted += page.getPositionCount();
      return page;
    }
    Page trimmed = page.getRegion(0, (int) remaining);
    emitted += remaining;
    return trimmed;
  }

  @Override
  public void close() { source.close(); }
}
```

**VERIFY GREEN:** PASS.

### Cycle 8b: ProjectOperator

**RED:** Write `ProjectOperatorTest` — test that it selects/reorders columns.
**VERIFY RED:** FAIL.
**GREEN:** Write `ProjectOperator`.
**VERIFY GREEN:** PASS.

### Cycle 8c: FilterOperator

**RED:** Write `FilterOperatorTest` — test that it removes rows not matching a predicate.
**VERIFY RED:** FAIL.
**GREEN:** Write `FilterOperator`.
**VERIFY GREEN:** PASS.

### Cycle 8d: HashAggregationOperator

**RED:** Write `HashAggregationOperatorTest` — test COUNT, SUM with GROUP BY.
**VERIFY RED:** FAIL.
**GREEN:** Write `HashAggregationOperator` (partial mode).
**VERIFY GREEN:** PASS.

### Cycle 8e: SortOperator

**RED → GREEN → VERIFY** same pattern.

### Final VERIFY

Run: `./gradlew :dqe:test --tests 'org.opensearch.sql.dqe.operator.*'`
Expected: All operator tests PASS.

### Commit

```bash
git add dqe/src/
git commit -m "feat(dqe): add physical operators (limit, project, filter, agg, sort)"
```

---

## Task 9: Local Execution Planner — Plan Nodes to Operator Pipeline

**Files:**
- Test: `dqe/src/test/java/org/opensearch/sql/dqe/shard/executor/LocalExecutionPlannerTest.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/shard/executor/LocalExecutionPlanner.java`

### Step 1: RED

```java
@DisplayName("LocalExecutionPlanner: plan nodes → operator pipeline")
class LocalExecutionPlannerTest {

  @Test
  @DisplayName("TableScanNode produces ScanOperator")
  void tableScanToOperator() {
    TableScanNode scan = new TableScanNode("logs", List.of("a"));
    Operator mockSource = new TestPageSource(List.of(buildPage(3)));
    LocalExecutionPlanner planner =
        new LocalExecutionPlanner(node -> mockSource);

    Operator result = scan.accept(planner, null);
    assertNotNull(result.processNextBatch());
  }

  @Test
  @DisplayName("LimitNode wraps child operator in LimitOperator")
  void limitNodeProducesLimitOperator() {
    TableScanNode scan = new TableScanNode("logs", List.of("a"));
    LimitNode limit = new LimitNode(scan, 5);
    Operator mockSource = new TestPageSource(List.of(buildPage(10)));
    LocalExecutionPlanner planner =
        new LocalExecutionPlanner(node -> mockSource);

    Operator result = limit.accept(planner, null);
    Page page = result.processNextBatch();
    assertEquals(5, page.getPositionCount());
  }
}
```

### Step 2: VERIFY RED — FAIL: `LocalExecutionPlanner` does not exist.

### Step 3: GREEN — Write `LocalExecutionPlanner` as a `DqePlanVisitor<Operator, Void>`.

### Step 4: VERIFY GREEN — PASS.

### Commit

```bash
git add dqe/src/
git commit -m "feat(dqe): add LocalExecutionPlanner mapping plan nodes to operators"
```

---

## Task 10: OpenSearchPageSource — Shard-Local Data Access

**Files:**
- Test: `dqe/src/test/java/org/opensearch/sql/dqe/shard/source/OpenSearchPageSourceTest.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/OpenSearchPageSource.java`

### Step 1: RED

```java
@DisplayName("OpenSearchPageSource: shard-local SearchAction → Trino Pages")
class OpenSearchPageSourceTest {

  @Test
  @DisplayName("SearchRequest uses _only_local preference")
  void usesOnlyLocalPreference() {
    NodeClient mockClient = mock(NodeClient.class);
    // Set up mock to capture the SearchRequest and verify preference
    ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
    when(mockClient.search(captor.capture()))
        .thenReturn(mockSearchResponse(/* 0 hits */));

    OpenSearchPageSource source = new OpenSearchPageSource(
        mockClient, "logs", 0, new SearchSourceBuilder(), List.of(), 1024);
    source.processNextBatch();

    assertEquals("_only_local", captor.getValue().preference());
  }

  @Test
  @DisplayName("Converts SearchHits to Trino Page with correct values")
  void convertsHitsToPage() {
    NodeClient mockClient = mock(NodeClient.class);
    when(mockClient.search(any()))
        .thenReturn(mockSearchResponse(
            Map.of("name", "alice", "age", 30),
            Map.of("name", "bob", "age", 25)));

    List<ColumnHandle> columns = List.of(
        new ColumnHandle("name", VarcharType.VARCHAR),
        new ColumnHandle("age", BigintType.BIGINT));

    OpenSearchPageSource source = new OpenSearchPageSource(
        mockClient, "logs", 0, new SearchSourceBuilder(), columns, 1024);
    Page page = source.processNextBatch();

    assertNotNull(page);
    assertEquals(2, page.getPositionCount());
    assertEquals(30L, BigintType.BIGINT.getLong(page.getBlock(1), 0));
  }

  @Test
  @DisplayName("Returns null when no hits")
  void returnsNullWhenExhausted() {
    NodeClient mockClient = mock(NodeClient.class);
    when(mockClient.search(any()))
        .thenReturn(mockSearchResponse(/* 0 hits */));

    OpenSearchPageSource source = new OpenSearchPageSource(
        mockClient, "logs", 0, new SearchSourceBuilder(), List.of(), 1024);
    assertNull(source.processNextBatch());
  }
}
```

### Step 2: VERIFY RED — FAIL: `OpenSearchPageSource` does not exist.

### Step 3: GREEN — Write `OpenSearchPageSource` implementing `Operator`.

Uses scroll API: first call initiates scroll with `_only_local` preference; subsequent calls scroll next batch. Converts `SearchHit[]` to `Page` via `PageBuilder.build()`.

### Step 4: VERIFY GREEN — PASS.

### Commit

```bash
git add dqe/src/
git commit -m "feat(dqe): add OpenSearchPageSource for shard-local data access"
```

---

## Task 11: Plan Fragmenter — Distribute Plan to Shards

**Files:**
- Test: `dqe/src/test/java/org/opensearch/sql/dqe/coordinator/fragment/PlanFragmenterTest.java`
- Create: `PlanFragment.java`, `PlanFragmenter.java`

### Step 1: RED

```java
@DisplayName("PlanFragmenter: split plan into per-shard fragments")
class PlanFragmenterTest {

  @Test
  @DisplayName("Produces one fragment per shard for a simple scan")
  void oneFragmentPerShard() {
    TableScanNode scan = new TableScanNode("logs", List.of("a"));
    ProjectNode project = new ProjectNode(scan, List.of("a"));

    ClusterState clusterState = mockClusterStateWithShards("logs", 3);

    PlanFragmenter fragmenter = new PlanFragmenter();
    PlanFragmenter.FragmentResult result = fragmenter.fragment(project, clusterState);

    assertEquals(3, result.shardFragments().size());
    for (PlanFragment frag : result.shardFragments()) {
      assertEquals("logs", frag.indexName());
    }
  }

  @Test
  @DisplayName("Splits aggregation into PARTIAL (shard) and FINAL (coordinator)")
  void splitsAggregation() {
    TableScanNode scan = new TableScanNode("logs", List.of("category"));
    AggregationNode agg = new AggregationNode(
        scan, List.of("category"), List.of("COUNT(*)"), AggregationNode.Step.PARTIAL);

    ClusterState clusterState = mockClusterStateWithShards("logs", 2);

    PlanFragmenter fragmenter = new PlanFragmenter();
    PlanFragmenter.FragmentResult result = fragmenter.fragment(agg, clusterState);

    assertEquals(2, result.shardFragments().size());
    // Shard fragments have PARTIAL aggregation
    // Coordinator plan has FINAL aggregation
    assertNotNull(result.coordinatorPlan());
  }
}
```

### Step 2: VERIFY RED — FAIL.

### Step 3: GREEN — Write `PlanFragment` record + `PlanFragmenter`.

### Step 4: VERIFY GREEN — PASS.

### Commit

```bash
git add dqe/src/
git commit -m "feat(dqe): add PlanFragmenter for distributing plan to shards"
```

---

## Task 12: Shard Transport Action

**Files:**
- Test: `dqe/src/test/java/org/opensearch/sql/dqe/shard/transport/ShardExecuteRequestTest.java`
- Test: `dqe/src/test/java/org/opensearch/sql/dqe/shard/transport/ShardExecuteResponseTest.java`
- Create: `ShardExecuteAction.java`, `ShardExecuteRequest.java`, `ShardExecuteResponse.java`, `TransportShardExecuteAction.java`

### Cycle 12a: ShardExecuteRequest serialization

**RED:** Test that `ShardExecuteRequest` round-trips through `StreamOutput`/`StreamInput`.
**VERIFY RED → GREEN → VERIFY GREEN.**

### Cycle 12b: ShardExecuteResponse serialization

**RED:** Test that `ShardExecuteResponse` round-trips (serialized Pages in, Pages out).
**VERIFY RED → GREEN → VERIFY GREEN.**

### Cycle 12c: TransportShardExecuteAction

**RED:** Test that `doExecute` deserializes a fragment, builds an operator pipeline via `LocalExecutionPlanner`, drains pages, and returns them in the response. Use mocked `NodeClient` and a simple plan (TableScan → Limit).

**VERIFY RED → GREEN → VERIFY GREEN.**

### Commit

```bash
git add dqe/src/
git commit -m "feat(dqe): add shard transport action for fragment execution"
```

---

## Task 13: Coordinator Merge

**Files:**
- Test: `dqe/src/test/java/org/opensearch/sql/dqe/coordinator/merge/ResultMergerTest.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/merge/ResultMerger.java`

### Cycle 13a: Passthrough merge

**RED:** Test that `mergePassthrough` concatenates pages from multiple shards.
**VERIFY RED → GREEN → VERIFY GREEN.**

### Cycle 13b: Aggregation merge

**RED:** Test that `mergeAggregation` runs FINAL aggregation over partial results from 2 shards.
**VERIFY RED → GREEN → VERIFY GREEN.**

### Cycle 13c: Sorted merge with limit

**RED:** Test that `mergeSorted` merge-sorts pre-sorted shard results and applies limit.
**VERIFY RED → GREEN → VERIFY GREEN.**

### Commit

```bash
git add dqe/src/
git commit -m "feat(dqe): add ResultMerger for coordinator-side shard result combination"
```

---

## Task 14: REST Endpoint and Coordinator Transport

**Files:**
- Test: `dqe/src/test/java/org/opensearch/sql/dqe/coordinator/transport/TrinoSqlRequestTest.java`
- Test: `dqe/src/test/java/org/opensearch/sql/dqe/coordinator/transport/TrinoSqlResponseTest.java`
- Test: `dqe/src/test/java/org/opensearch/sql/dqe/coordinator/rest/RestTrinoSqlActionTest.java`
- Create: all coordinator transport + REST classes

### Cycle 14a: TrinoSqlRequest/Response serialization

**RED:** Round-trip tests for request (query string + explain flag) and response (result string + content type).
**VERIFY RED → GREEN → VERIFY GREEN.**

### Cycle 14b: RestTrinoSqlAction routes

**RED:** Test that `routes()` returns POST for `/_plugins/_trino_sql` and `/_plugins/_trino_sql/_explain`.
**VERIFY RED → GREEN → VERIFY GREEN.**

### Cycle 14c: TransportTrinoSqlAction coordinator orchestration

**RED:** Test the full coordinator flow with mocked dependencies: parse → plan → fragment → dispatch (mocked) → merge → format. Verify the response contains expected JSON.
**VERIFY RED → GREEN → VERIFY GREEN.**

### Commit

```bash
git add dqe/src/
git commit -m "feat(dqe): add REST endpoint and coordinator transport for /_plugins/_trino_sql"
```

---

## Task 15: Plugin Wiring

**Exception: Declarative configuration — verified by compilation.**

**Files:**
- Modify: `plugin/src/main/java/org/opensearch/sql/plugin/SQLPlugin.java`

### Step 1: Add to getRestHandlers()

```java
new RestTrinoSqlAction()
```

### Step 2: Add to getActions()

```java
new ActionHandler<>(
    new ActionType<>(TrinoSqlAction.NAME, TrinoSqlResponse::new),
    TransportTrinoSqlAction.class),
new ActionHandler<>(
    new ActionType<>(ShardExecuteAction.NAME, ShardExecuteResponse::new),
    TransportShardExecuteAction.class)
```

### Step 3: Add to getExecutorBuilders()

```java
new FixedExecutorBuilder(
    settings,
    DqePlugin.DQE_THREAD_POOL_NAME,
    Math.max(1, OpenSearchExecutors.allocatedProcessors(settings) / 2),
    1000,
    "thread_pool." + DqePlugin.DQE_THREAD_POOL_NAME)
```

Where `DqePlugin.DQE_THREAD_POOL_NAME = "dqe-shard-executor"` (create this constant now if not already in a source file — but only after a test demands it in Task 16).

### Step 4: VERIFY — Compilation

Run: `./gradlew :opensearch-sql-plugin:compileJava`
Expected: BUILD SUCCESSFUL.

### Commit

```bash
git add plugin/src/
git commit -m "feat(dqe): wire DQE REST handler, transport actions, and thread pool into SQLPlugin"
```

---

## Task 16: Cluster Settings

**Files:**
- Test: `dqe/src/test/java/org/opensearch/sql/dqe/common/config/DqeSettingsTest.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/common/config/DqeSettings.java`
- Modify: `plugin/src/main/java/org/opensearch/sql/plugin/SQLPlugin.java`

### Step 1: RED

```java
@DisplayName("DqeSettings")
class DqeSettingsTest {

  @Test
  @DisplayName("DQE_ENABLED defaults to false")
  void enabledDefaultsFalse() {
    assertEquals(false, DqeSettings.DQE_ENABLED.getDefault(Settings.EMPTY));
  }

  @Test
  @DisplayName("QUERY_TIMEOUT defaults to 30 seconds")
  void timeoutDefaults30s() {
    assertEquals(
        TimeValue.timeValueSeconds(30),
        DqeSettings.QUERY_TIMEOUT.getDefault(Settings.EMPTY));
  }

  @Test
  @DisplayName("PAGE_BATCH_SIZE defaults to 1024")
  void batchSizeDefaults1024() {
    assertEquals(1024, (int) DqeSettings.PAGE_BATCH_SIZE.getDefault(Settings.EMPTY));
  }

  @Test
  @DisplayName("settings() returns all three settings")
  void settingsListComplete() {
    assertEquals(3, DqeSettings.settings().size());
  }
}
```

### Step 2: VERIFY RED — FAIL: `DqeSettings` does not exist.

### Step 3: GREEN

```java
public class DqeSettings {
  public static final Setting<Boolean> DQE_ENABLED =
      Setting.boolSetting("plugins.dqe.enabled", false,
          Setting.Property.NodeScope, Setting.Property.Dynamic);

  public static final Setting<TimeValue> QUERY_TIMEOUT =
      Setting.timeSetting("plugins.dqe.query.timeout", TimeValue.timeValueSeconds(30),
          Setting.Property.NodeScope, Setting.Property.Dynamic);

  public static final Setting<Integer> PAGE_BATCH_SIZE =
      Setting.intSetting("plugins.dqe.page.batch_size", 1024, 1,
          Setting.Property.NodeScope, Setting.Property.Dynamic);

  public static List<Setting<?>> settings() {
    return List.of(DQE_ENABLED, QUERY_TIMEOUT, PAGE_BATCH_SIZE);
  }
}
```

### Step 4: VERIFY GREEN — PASS.

### Step 5: Register in SQLPlugin.getSettings()

```java
.addAll(DqeSettings.settings())
```

### Step 6: VERIFY compilation

Run: `./gradlew :opensearch-sql-plugin:compileJava`
Expected: BUILD SUCCESSFUL.

### Commit

```bash
git add dqe/src/ plugin/src/
git commit -m "feat(dqe): add DQE cluster settings (enabled, timeout, batch_size)"
```

---

## Task 17: End-to-End Smoke Test

**Files:**
- Create: `dqe/src/test/java/org/opensearch/sql/dqe/DqeEndToEndTest.java`

This is the capstone test. It wires all components together with mocks for OpenSearch (no real cluster) and validates the full pipeline.

### Step 1: RED

```java
@DisplayName("DQE end-to-end pipeline")
class DqeEndToEndTest {

  @Test
  @DisplayName("Full pipeline: parse → plan → fragment → execute → merge for SELECT with agg")
  void fullPipelineWithAggregation() {
    // 1. Parse
    DqeSqlParser parser = new DqeSqlParser();
    Statement stmt = parser.parse(
        "SELECT category, COUNT(*) FROM logs WHERE status = 200 GROUP BY category LIMIT 10");

    // 2. Plan
    DqePlanNode plan = LogicalPlanner.plan(stmt, this::mockTableInfo);
    assertNotNull(plan);

    // 3. Fragment (mock 2 shards)
    ClusterState mockState = mockClusterStateWithShards("logs", 2);
    PlanFragmenter fragmenter = new PlanFragmenter();
    PlanFragmenter.FragmentResult fragments = fragmenter.fragment(plan, mockState);
    assertEquals(2, fragments.shardFragments().size());

    // 4. Execute each fragment with mock data
    List<List<Page>> shardResults = new ArrayList<>();
    for (PlanFragment frag : fragments.shardFragments()) {
      // Build operator pipeline with mock page source returning test data
      Operator mockSource = new TestPageSource(List.of(buildTestData(frag.shardId())));
      LocalExecutionPlanner execPlanner = new LocalExecutionPlanner(node -> mockSource);
      Operator pipeline = frag.shardPlan().accept(execPlanner, null);
      shardResults.add(drainOperator(pipeline));
    }

    // 5. Merge
    ResultMerger merger = new ResultMerger();
    List<Page> finalResult = merger.mergeAggregation(
        shardResults, (AggregationNode) fragments.coordinatorPlan());

    // 6. Verify merged result has aggregated rows
    int totalRows = finalResult.stream().mapToInt(Page::getPositionCount).sum();
    assertTrue(totalRows > 0, "Expected non-empty result after merge");
  }

  private TableInfo mockTableInfo(String indexName) {
    return new TableInfo(
        indexName,
        List.of(
            new TableInfo.ColumnInfo("category", "keyword", VarcharType.VARCHAR),
            new TableInfo.ColumnInfo("status", "long", BigintType.BIGINT)));
  }
}
```

### Step 2: VERIFY RED

This test should fail if any component in the pipeline is broken or not yet integrated. If it passes, it validates the entire stack.

Run: `./gradlew :dqe:test --tests '*DqeEndToEndTest*'`

### Step 3: GREEN — Fix any integration issues

If the test fails, fix the integration bugs. Do NOT change the test (it describes desired behavior).

### Step 4: VERIFY GREEN

Run: `./gradlew :dqe:test`
Expected: ALL tests PASS.

### Commit

```bash
git add dqe/src/
git commit -m "test(dqe): add end-to-end smoke test for full DQE pipeline"
```

---

## Dependency Resolution Notes

Task 1 uses Trino JARs as Gradle dependencies. Potential conflicts and mitigations:

1. **ANTLR 4.13.2**: Both Trino and PPL use the same version — no conflict expected.
2. **Guava**: Excluded from Trino deps; OpenSearch's `33.3.0-jre` wins.
3. **Jackson**: Excluded from Trino SPI; OpenSearch's version wins.
4. **OpenTelemetry**: Excluded since OpenSearch doesn't use it.
5. **Airlift Slice**: Standalone library (`io.airlift:slice:2.3`), no conflict expected.

If classpath conflicts become blocking, fork the conflicting Trino packages into `dqe/src/main/java` with relocated package paths. The integration code (Tasks 2-17) uses Trino types through interfaces, so swapping JAR deps for forked source changes import paths only.

---

## Summary

| Task | Component | TDD Cycles | Key Classes |
|------|-----------|------------|-------------|
| 1 | Module scaffolding | Config exception | `dqe/build.gradle` |
| 2 | Type mapping | 1 RED-GREEN | `TypeMapping` |
| 3 | Page builder | 1 RED-GREEN | `PageBuilder`, `ColumnHandle` |
| 4 | Metadata adapter | 1 RED-GREEN (3 tests) | `OpenSearchMetadata`, `TableInfo` |
| 5 | SQL parser wrapper | 1 RED-GREEN (4 tests) | `DqeSqlParser` |
| 6 | Plan nodes | 7 RED-GREEN cycles | `DqePlanNode`, all node types |
| 7 | Logical planner | 5 RED-GREEN cycles | `LogicalPlanner` |
| 8 | Physical operators | 5 RED-GREEN cycles | `Operator`, all operator types |
| 9 | Execution planner | 1 RED-GREEN (2 tests) | `LocalExecutionPlanner` |
| 10 | Page source | 1 RED-GREEN (3 tests) | `OpenSearchPageSource` |
| 11 | Fragmenter | 1 RED-GREEN (2 tests) | `PlanFragmenter`, `PlanFragment` |
| 12 | Shard transport | 3 RED-GREEN cycles | `TransportShardExecuteAction` |
| 13 | Result merger | 3 RED-GREEN cycles | `ResultMerger` |
| 14 | REST + coordinator | 3 RED-GREEN cycles | `RestTrinoSqlAction`, `TransportTrinoSqlAction` |
| 15 | Plugin wiring | Config exception | `SQLPlugin` mods |
| 16 | Cluster settings | 1 RED-GREEN (4 tests) | `DqeSettings` |
| 17 | E2E test | 1 integration test | `DqeEndToEndTest` |
