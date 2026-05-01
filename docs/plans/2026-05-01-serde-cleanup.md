# Serde Cleanup: Drop Java Native Serialization from SQL Plugin Plan/Expression Paths

> **For kiro-cli:** Use `executing-plans` skill — tasks are sequential within each phase; phases must ship in order.

**Goal:** Remove `ObjectInputStream` / `ObjectOutputStream` from three serde call sites in the SQL plugin, replacing each with a format that matches the actual payload shape. Simpler code, stable wire formats, fewer moving parts.

**Scope:** three serializers, three phases, one branch (`fix/serder`):

1. **RelJsonSerializer** (Calcite script pushdown) — smallest, already done on this branch.
2. **DefaultExpressionSerializer** (V2 legacy script pushdown) — largest; recommend delete-the-path over migrate-the-path.
3. **PlanSerializer** (cursor pagination) — sealed hierarchy + Jackson Smile.

**Revised ordering:** 1 → 2 → 3. Phase 3 depends on Phase 2 because cursor plans carry `NamedExpression` (an `Expression` subclass), so Phase 2 must define the new Expression serde shape first.

**Tech Stack:** Java 21 (records + sealed), Jackson 2.x (already on classpath), Jackson Smile (binary JSON sub-module of Jackson).

---

## Phase 1 — RelJsonSerializer ✅ DONE (commit `f2eb2f94d`)

Shipped on this branch. Summary for reviewers:

- Wire format changed from `Base64(JavaSerialized(String))` to `Base64(UTF-8(String))`.
- The payload was always a single Calcite JSON string; the Java serialization frame was pure overhead.
- `ObjectOutputStream` / `ObjectInputStream` imports removed.
- All 11 existing round-trip tests still pass; one regression test added (`testDeserializeRejectsJavaSerializedPayload`).

No further work in Phase 1.

---

## Phase 2 — DefaultExpressionSerializer

`DefaultExpressionSerializer` is the compilation path for the legacy "V2" expression scripts used when `plugins.calcite.enabled=false`. Calcite has been the default since OS 3.x; V2 is second-class.

Two strategies, pick one before starting:

### Strategy 2A — Delete V2 (recommended)

**Rationale:** The code is ~5000 LOC spread across `FunctionDSL` and ~15 `*Functions.register(...)` modules, all built around lambda-capturing anonymous `FunctionExpression` subclasses whose only purpose is to survive `ObjectInputStream.readObject()`. If V2 is no longer a supported runtime configuration, this whole subsystem gets deleted wholesale — far less code to maintain than a full Jackson migration of the same surface.

**Precondition:** product signoff that `plugins.calcite.enabled=false` is either unsupported going forward, or falls back to non-pushdown (in-memory) execution instead of V2 scripts.

#### Task 2A-1: Add regression test first

**Files:**
- Modify: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/serde/DefaultExpressionSerializerTest.java`

**Step 1: Add test**

```java
@Test
void deserialize_rejects_javaSerializedPayload() throws Exception {
  ByteArrayOutputStream baos = new ByteArrayOutputStream();
  try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
    oos.writeObject(new java.util.HashMap<String, String>());
  }
  String payload = Base64.getEncoder().encodeToString(baos.toByteArray());
  assertThrows(Exception.class,
      () -> new DefaultExpressionSerializer().deserialize(payload));
}
```

**Step 2: Run — must fail on current code**

Run: `./gradlew :opensearch:test --tests "*DefaultExpressionSerializerTest.deserialize_rejects_javaSerializedPayload"`
Expected: FAIL.

**Step 3: Commit**

```bash
git commit -am "test: regression — ExpressionSerializer rejects Java-serialized payloads"
```

#### Task 2A-2: Make V2 path unreachable

**Files:**
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/serde/DefaultExpressionSerializer.java` — both methods throw `UnsupportedOperationException`.
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/script/CompoundedScriptEngine.java` — delete `V2` case; remove `v2ExpressionScriptEngine` field.
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/scan/OpenSearchIndexScanQueryBuilder.java:58` and `.../OpenSearchIndexScanAggregationBuilder.java:57` — stop constructing `DefaultExpressionSerializer`; gate script pushdown behind `plugins.calcite.enabled`. On non-Calcite path, fall back to non-pushdown execution.

**Step 1: Compile to find all call sites**

Run: `./gradlew :opensearch:compileJava`
Expected: compilation failures list every place still wired to V2. Fix each.

**Step 2: Run full test suite**

Run: `./gradlew :core:test :opensearch:test`
Expected: green. Existing V2-only test methods (5 in `DefaultExpressionSerializerTest`) need to be deleted alongside this commit.

**Step 3: Commit**

```bash
git commit -am "refactor: route script pushdown through Calcite engine only

The legacy V2 path via DefaultExpressionSerializer is no longer
constructed. Queries with plugins.calcite.enabled=false now fall
back to non-pushdown execution instead of V2 scripts."
```

#### Task 2A-3: Delete V2 code

**Files to delete:**
- `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/serde/DefaultExpressionSerializer.java`
- `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/serde/ExpressionSerializer.java`
- `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/script/ExpressionScriptEngine.java`
- `core/src/main/java/org/opensearch/sql/expression/function/SerializableFunction.java` + sibling `SerializableBi/Tri/Quad/NoArgFunction.java`
- Corresponding tests

**Files to modify:**
- `core/src/main/java/org/opensearch/sql/expression/Expression.java` — remove `extends Serializable`.
- `core/src/main/java/org/opensearch/sql/expression/function/FunctionDSL.java` and ~15 `*Functions.java` modules — replace `SerializableFunction<T,R>` with `java.util.function.Function<T,R>`, same for Bi/Tri/Quad/NoArg (use a custom `TriFunction`/`QuadFunction` interface extending `java.util.function.BiFunction` pattern as needed).

**Step 1: Sweep**

Run: `grep -rn "SerializableFunction\|SerializableBiFunction\|SerializableTriFunction\|SerializableQuadFunction\|SerializableNoArgFunction\|implements Serializable\|extends Serializable" core opensearch --include="*.java" | grep -v test | grep -v build/`
Expected: zero matches in the expression and function packages.

**Step 2: Build**

Run: `./gradlew :core:compileJava :opensearch:compileJava`

**Step 3: Full test suite**

Run: `./gradlew :core:test :opensearch:test`

**Step 4: Commit**

```bash
git commit -am "refactor: delete V2 expression serde and Serializable markers

Deletes DefaultExpressionSerializer, ExpressionScriptEngine, the
SerializableFunction/BiFunction/TriFunction/QuadFunction/NoArgFunction
marker interfaces, and 'extends Serializable' from Expression.

Script pushdown now runs exclusively through the Calcite engine
(RelJsonSerializer in Phase 1)."
```

### Strategy 2B — Full Jackson migration (fallback)

Use only if product blocks Strategy 2A.

**Design:**
1. Sealed `SerializableExpr` interface with records: `LiteralExpr`, `ReferenceExpr`, `FunctionCallExpr(String name, List<SerializableExpr> args)`, `CaseExpr`, `WhenExpr`, `AggregatorExpr`, `WindowFnExpr`, `ParseExpr`, `SpanExpr`, `NamedExpr`, `NamedArgExpr`.
2. `FunctionCallExpr.name` is a stable `FunctionName`; on deserialize, look up via `BuiltinFunctionRepository.resolve(functionName)`. Fail closed on unknown name.
3. Refactor `FunctionDSL.impl*(...)` to produce `FunctionCallExpr` with the right name, not anonymous `FunctionExpression` with captured lambdas.
4. Wire format: `Base64(Smile(jsonTree))` with `@t` tag per sealed variant.
5. Test matrix: every `FunctionName` in the registry round-trips through the new format.

**Estimated cost:** 3–4 weeks for one engineer (touches `FunctionDSL` + every `*Functions.register`). Task breakdown deferred until Strategy 2A is rejected.

---

## Phase 3 — PlanSerializer (cursor pagination)

Current wire format: `n:<hex(gzip(Java-serialized bytes))>`. Clients treat the cursor as opaque and echo it back; there is no server-side cursor registry. Stateless roundtrip makes a wire format change low-risk.

New wire format: `n:v2:<base64(smile(jsonTree))>`. `deserialize` rejects anything not starting with `n:v2:`.

### Task 3-1: Round-trip test for production plan types (RED)

**Files:**
- Modify: `core/src/test/java/org/opensearch/sql/executor/pagination/PlanSerializerTest.java`

Context from exploration: `PlanSerializerTest` has 11 methods but zero round-trips for the two real production plan types (`ProjectOperator`, `OpenSearchIndexScan`). All existing tests use a test-only `TestOperator`. Lock in production behavior before changing format.

**Step 1: Add `ProjectOperator(OpenSearchIndexScan)` round-trip test** using real fixtures from `opensearch/src/test` or its `PaginationTest` helpers.

**Step 2: Run on current code, confirm passes.** This becomes the baseline that the new format must also satisfy.

**Step 3: Commit**

```bash
git commit -am "test: ProjectOperator+IndexScan cursor round-trip regression"
```

### Task 3-2: Format-rejection regression (RED)

**Step 1: Add test** that feeds a `n:<hex(gzip(Java-serialized HashMap))>` cursor to `convertToPlan` and expects an `IllegalStateException`.

**Step 2: Run — will fail on current code** (current code reaches `readObject`).

**Step 3: Commit**

```bash
git commit -am "test: cursor deserializer rejects legacy Java-serialized payloads"
```

### Task 3-3: Sealed `SerializablePlanNode` hierarchy

**Files:**
- Create: `core/src/main/java/org/opensearch/sql/executor/pagination/serde/SerializablePlanNode.java`
- Create: `core/src/main/java/org/opensearch/sql/executor/pagination/serde/ProjectNode.java`
- Create: `core/src/main/java/org/opensearch/sql/executor/pagination/serde/IndexScanNode.java`

```java
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@t")
@JsonSubTypes({
    @JsonSubTypes.Type(value = ProjectNode.class,   name = "project"),
    @JsonSubTypes.Type(value = IndexScanNode.class, name = "scan")
})
public sealed interface SerializablePlanNode permits ProjectNode, IndexScanNode {}

public record ProjectNode(List<NamedExpression> projectList,
                          SerializablePlanNode input) implements SerializablePlanNode {}

public record IndexScanNode(byte[] requestBytes, int maxResponseSize)
    implements SerializablePlanNode {}
```

Unknown `@t` throws `InvalidTypeIdException` before any class is constructed.

**`NamedExpression` serde:** uses the Phase-2 Expression serde. Phase 2 must land first.

**Step 1: Commit**

```bash
git commit -am "plan-serde: introduce sealed SerializablePlanNode hierarchy"
```

### Task 3-4: Flattener + rebuilder with post-decode context injection

**Files:**
- Create: `core/src/main/java/org/opensearch/sql/executor/pagination/serde/PlanNodeFlattener.java`
- Create: `core/src/main/java/org/opensearch/sql/executor/pagination/serde/PlanNodeRebuilder.java`
- Create: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/scan/IndexScanFlattener.java`
- Create: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/scan/IndexScanRebuilder.java`

Rebuilder uses pattern matching on the sealed interface (compile-time exhaustive):

```java
public final class PlanNodeRebuilder {
  public static PhysicalPlan rebuild(SerializablePlanNode node, StorageEngine engine) {
    return switch (node) {
      case ProjectNode p -> new ProjectOperator(
          rebuild(p.input(), engine), p.projectList(), List.of());
      case IndexScanNode s -> IndexScanRebuilder.rebuild(s, engine);
    };
  }
}
```

`IndexScanRebuilder` reconstructs `OpenSearchQueryRequest` from the byte payload and re-injects `client` / `engine` from the caller-supplied `StorageEngine`. Replaces the current `"engine"` string-sentinel trick in `CursorDeserializationStream.resolveObject`.

`IndexScanFlattener` calls `request.writeTo(BytesStreamOutput)` (already PIT-mode only, same precondition as today's `OpenSearchIndexScan.writeExternal`).

**Step 1: Commit**

```bash
git commit -am "plan-serde: add flattener + rebuilder with post-decode context injection"
```

### Task 3-5: Rewire `PlanSerializer` to `n:v2:` Smile format (GREEN)

**Files:**
- Modify: `core/src/main/java/org/opensearch/sql/executor/pagination/PlanSerializer.java`

```java
private static final ObjectMapper SMILE_MAPPER = new ObjectMapper(new SmileFactory());

public Cursor convertToCursor(PhysicalPlan plan) {
  try {
    SerializablePlanNode node = PlanNodeFlattener.flatten(plan);
    byte[] smile = SMILE_MAPPER.writeValueAsBytes(node);
    return new Cursor("n:v2:" + Base64.getEncoder().encodeToString(smile));
  } catch (NoCursorException e) {
    return Cursor.None;
  } catch (Exception e) {
    return Cursor.None;
  }
}

public PhysicalPlan convertToPlan(String cursor) {
  if (!cursor.startsWith("n:v2:")) {
    throw new IllegalStateException("Unsupported cursor format");
  }
  byte[] smile = Base64.getDecoder().decode(cursor.substring("n:v2:".length()));
  SerializablePlanNode node = SMILE_MAPPER.readValue(smile, SerializablePlanNode.class);
  return PlanNodeRebuilder.rebuild(node, engine);
}
```

**Step 1: Delete `CursorDeserializationStream` inner class and all `ObjectInputStream` / `Externalizable` imports.**

**Step 2: Run tests, including Task 3-1 and 3-2 regressions.**

Run: `./gradlew :core:test --tests "*PlanSerializerTest"`
Expected: all green.

**Step 3: Commit**

```bash
git commit -am "refactor: switch cursor format to Jackson Smile envelope

Cursor wire format changes from n:<hex(gzip(javaSerialized))> to
n:v2:<base64(smile(jsonTree))>. The new format is schema'd via the
sealed SerializablePlanNode hierarchy; unknown type tags are rejected
by Jackson before any subclass is constructed."
```

### Task 3-6: Delete `SerializablePlan` + `readExternal`/`writeExternal`

**Files:**
- Delete: `core/src/main/java/org/opensearch/sql/planner/SerializablePlan.java`
- Modify: `core/src/main/java/org/opensearch/sql/planner/physical/ProjectOperator.java` — drop `implements SerializablePlan`, delete `readExternal`/`writeExternal`.
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/scan/OpenSearchIndexScan.java` — same.
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/executor/protector/ResourceMonitorPlan.java` — drop `getPlanForSerialization` if orphaned.

**Step 1: Sweep**

Run: `grep -rn "SerializablePlan\|readExternal\|writeExternal" core opensearch --include="*.java" | grep -v build/`
Expected: zero matches in production code.

**Step 2: Build + full test pass (including `OpenSearchIndexScanPaginationTest`).**

**Step 3: Commit**

```bash
git commit -am "refactor: delete SerializablePlan / Externalizable path"
```

---

## Out of scope

- Rolling-upgrade BWC for cursor format (explicitly ignored).
- Base64 size / JSON depth limits as defense-in-depth (separate hardening, not scope of this refactor).
- JVM-wide `jdk.serialFilter` (platform configuration, not code).
- `PredicateAnalyzer` per-predicate `RelJsonSerializer` allocation (pre-existing, unrelated).
