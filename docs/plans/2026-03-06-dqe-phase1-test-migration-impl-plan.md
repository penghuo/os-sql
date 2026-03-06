# DQE Phase 1 Test Migration and TDD Bug Fix — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Migrate 103 phase1 integration tests from trino-v5 into wukong, then fix DQE engine bugs in a TDD loop until all tests pass.

**Architecture:** Copy the Python validate.py test framework and trino-v5 test data into `dqe/integ-test/`, adapt the endpoint to `/_plugins/_trino_sql`, then iteratively run tests and fix Java engine code.

**Tech Stack:** Python 3 (test runner), Bash (data setup), Java 11+/Trino SPI (engine fixes), OpenSearch REST API

---

### Task 1: Clean out old test scaffolding

**Files:**
- Delete: `dqe/integ-test/queries/Q1_simple_select.json` through `Q8_explain.json`
- Delete: `dqe/integ-test/data/dqe_test_logs.json`
- Delete: `dqe/integ-test/run-tests.sh`
- Keep: `dqe/integ-test/README.md`, `dqe/integ-test/reports/`

**Step 1: Remove old files**

```bash
rm dqe/integ-test/queries/Q*.json
rm dqe/integ-test/data/dqe_test_logs.json
rm dqe/integ-test/run-tests.sh
rmdir dqe/integ-test/queries 2>/dev/null || true
```

**Step 2: Commit**

```bash
git add -u dqe/integ-test/
git commit -m "refactor(dqe): remove old integ-test scaffolding"
```

---

### Task 2: Copy test data files (mappings + bulk)

**Files:**
- Create: `dqe/integ-test/data/mappings/*.json` (7 files)
- Create: `dqe/integ-test/data/bulk/*.ndjson` (7 files)

**Step 1: Copy mapping and bulk files from trino-v5**

```bash
mkdir -p dqe/integ-test/data/mappings dqe/integ-test/data/bulk
cp /home/ec2-user/oss/trino-v5/scripts/dqe-test/data/mappings/*.json dqe/integ-test/data/mappings/
cp /home/ec2-user/oss/trino-v5/scripts/dqe-test/data/bulk/*.ndjson dqe/integ-test/data/bulk/
```

**Step 2: Verify files copied**

```bash
ls dqe/integ-test/data/mappings/  # expect 7 JSON files
ls dqe/integ-test/data/bulk/       # expect 7 NDJSON files
```

**Step 3: Commit**

```bash
git add dqe/integ-test/data/
git commit -m "feat(dqe): add phase1 test data (mappings + bulk)"
```

---

### Task 3: Copy setup-data.sh

**Files:**
- Create: `dqe/integ-test/setup-data.sh`

**Step 1: Copy from trino-v5**

```bash
cp /home/ec2-user/oss/trino-v5/scripts/dqe-test/setup-data.sh dqe/integ-test/setup-data.sh
chmod +x dqe/integ-test/setup-data.sh
```

The script uses generic REST API calls and path-relative references, so it works as-is.

**Step 2: Verify it runs against live OpenSearch**

```bash
cd dqe/integ-test && ./setup-data.sh http://localhost:9200
```

Expected: All 7 indices created with correct doc counts.

**Step 3: Commit**

```bash
git add dqe/integ-test/setup-data.sh
git commit -m "feat(dqe): add setup-data.sh for integ-test data loading"
```

---

### Task 4: Copy validate.py and adapt endpoint

**Files:**
- Create: `dqe/integ-test/validate.py`

**Step 1: Copy validate.py from trino-v5**

```bash
cp /home/ec2-user/oss/trino-v5/scripts/dqe-test/validate.py dqe/integ-test/validate.py
```

**Step 2: Modify execute_query to use /_plugins/_trino_sql**

In `dqe/integ-test/validate.py`, change the `execute_query` function:

```python
def execute_query(url: str, query: str) -> dict:
    """Execute a SQL query via the DQE REST API."""
    endpoint = f"{url}/_plugins/_trino_sql"
    body = {"query": query}
```

Remove the `engine` parameter entirely. Remove the `if engine: body["engine"] = engine` lines.

Also remove the engine-check in `validate_test_case` — remove the block that checks `result.get("engine")` since `/_plugins/_trino_sql` does not return an `engine` field.

**Step 3: Commit**

```bash
git add dqe/integ-test/validate.py
git commit -m "feat(dqe): add validate.py test runner adapted for /_plugins/_trino_sql"
```

---

### Task 5: Copy phase1 test cases (excluding error_cases)

**Files:**
- Create: `dqe/integ-test/cases/phase1/basic_select/Q001-Q015.json` (15 files)
- Create: `dqe/integ-test/cases/phase1/where_predicates/Q016-Q040.json` (25 files)
- Create: `dqe/integ-test/cases/phase1/type_specific/Q041-Q060.json` (20 files)
- Create: `dqe/integ-test/cases/phase1/order_by_limit/Q061-Q068.json` (8 files)
- Create: `dqe/integ-test/cases/phase1/multi_shard/Q076-Q085,Q111-Q113.json` (13 files)
- Create: `dqe/integ-test/cases/phase1/expressions/Q086-Q095.json` (10 files)

Total: 103 test cases (excluding error_cases)

**Step 1: Copy all non-error test cases**

```bash
mkdir -p dqe/integ-test/cases/phase1
for dir in basic_select where_predicates type_specific order_by_limit multi_shard expressions; do
  cp -r /home/ec2-user/oss/trino-v5/scripts/dqe-test/cases/phase1/${dir} dqe/integ-test/cases/phase1/
done
```

**Step 2: Verify count**

```bash
find dqe/integ-test/cases/phase1/ -name "*.json" | wc -l  # expect 103
```

**Step 3: Commit**

```bash
git add dqe/integ-test/cases/
git commit -m "feat(dqe): add 103 phase1 test cases (excluding error_cases)"
```

---

### Task 6: Write run-phase1-tests.sh

**Files:**
- Create: `dqe/integ-test/run-phase1-tests.sh`

**Step 1: Copy and adapt from trino-v5**

```bash
cp /home/ec2-user/oss/trino-v5/scripts/dqe-test/run-phase1-tests.sh dqe/integ-test/run-phase1-tests.sh
chmod +x dqe/integ-test/run-phase1-tests.sh
```

Edit the script to:
1. Remove `error_cases` from the subdir loop (line with `for subdir in ...`)
2. Keep only: `basic_select where_predicates type_specific order_by_limit multi_shard expressions`

**Step 2: Commit**

```bash
git add dqe/integ-test/run-phase1-tests.sh
git commit -m "feat(dqe): add run-phase1-tests.sh (excludes error_cases)"
```

---

### Task 7: Run baseline tests and capture initial failures

**Step 1: Load test data**

```bash
cd /home/ec2-user/oss/wukong/dqe/integ-test && ./setup-data.sh http://localhost:9200
```

**Step 2: Run all 103 tests and capture output**

```bash
cd /home/ec2-user/oss/wukong/dqe/integ-test && ./run-phase1-tests.sh http://localhost:9200 2>&1 | tee reports/baseline-report.txt
```

**Step 3: Analyze failures**

Categorize failures into buckets:
- Function calls with implicit coercion (the known ExpressionCompiler bug)
- Other expression evaluation issues
- Type mapping issues
- Data/schema mismatches

Save the analysis as a comment in the report. This informs which engine fixes to prioritize.

---

### Task 8: Fix ExpressionCompiler — implicit cast for function arguments

This is the known root cause bug.

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/function/expression/ExpressionCompiler.java:198-215`
- Modify: `dqe/src/test/java/org/opensearch/sql/dqe/function/expression/ExpressionCompilerTest.java`

**Step 1: Write the failing unit test**

Add to `ExpressionCompilerTest.java` a test that:
1. Registers `abs(DOUBLE)->DOUBLE` in a fresh FunctionRegistry
2. Creates an ExpressionCompiler with a BIGINT column called "status"
3. Compiles `abs(status) = 200`
4. Evaluates against a page with status = [-200, 200, 100]
5. Asserts: position 0 = true (abs(-200)=200), position 1 = true (abs(200)=200), position 2 = false (abs(100)!=200)

Also add needed import: `import io.trino.spi.type.DoubleType;`

**Step 2: Run test to verify it fails**

```bash
./gradlew :dqe:test --tests "*.ExpressionCompilerTest.functionCallWithImplicitCoercion" -i
```

Expected: FAIL — the BIGINT block is passed directly to abs() which reads it as DOUBLE, getting garbage values.

**Step 3: Fix compileFunctionCall in ExpressionCompiler.java**

Replace the `compileFunctionCall` method (lines 198-215). After resolving the function via `registry.resolve()`, compare each resolved parameter type against the actual argument type. For mismatches, wrap the argument in a `CastBlockExpression`:

```java
private BlockExpression compileFunctionCall(FunctionCall func) {
    String funcName = func.getName().toString();
    List<BlockExpression> compiledArgs =
        func.getArguments().stream().map(this::compile).collect(Collectors.toList());
    List<Type> argTypes =
        compiledArgs.stream().map(BlockExpression::getType).collect(Collectors.toList());

    ResolvedFunction resolved = registry.resolve(funcName, argTypes);
    FunctionMetadata metadata = registry.getMetadata(resolved);

    if (metadata.getScalarImplementation() == null) {
      throw new UnsupportedOperationException(
          "Function '" + funcName + "' has no scalar implementation");
    }

    // Insert implicit casts where the resolved parameter type differs from the actual arg type
    List<Type> paramTypes = resolved.getArgumentTypes();
    List<BlockExpression> castArgs = new ArrayList<>();
    for (int i = 0; i < compiledArgs.size(); i++) {
      BlockExpression arg = compiledArgs.get(i);
      if (!arg.getType().equals(paramTypes.get(i))) {
        castArgs.add(new CastBlockExpression(arg, paramTypes.get(i)));
      } else {
        castArgs.add(arg);
      }
    }

    return new ScalarFunctionExpression(
        metadata.getScalarImplementation(), castArgs, metadata.getReturnType());
}
```

**Step 4: Run test to verify it passes**

```bash
./gradlew :dqe:test --tests "*.ExpressionCompilerTest.functionCallWithImplicitCoercion" -i
```

Expected: PASS

**Step 5: Run all ExpressionCompiler tests to check for regressions**

```bash
./gradlew :dqe:test --tests "*.ExpressionCompilerTest" -i
```

Expected: All tests PASS

**Step 6: Commit**

```bash
git add dqe/src/main/java/org/opensearch/sql/dqe/function/expression/ExpressionCompiler.java
git add dqe/src/test/java/org/opensearch/sql/dqe/function/expression/ExpressionCompilerTest.java
git commit -m "fix(dqe): insert implicit casts for function arguments in ExpressionCompiler"
```

---

### Task 9: Build, deploy, and re-run integration tests

**Step 1: Build the DQE module**

```bash
./gradlew :dqe:compileJava
```

Expected: BUILD SUCCESSFUL

**Step 2: Build the full plugin (needed for deploying to OpenSearch)**

```bash
./gradlew assemble -x test -x javadoc
```

**Step 3: Deploy updated plugin to running OpenSearch**

Follow the existing deployment process (restart OpenSearch with updated plugin JARs). The exact commands depend on how the local OpenSearch is set up.

**Step 4: Re-load test data and run tests**

```bash
cd /home/ec2-user/oss/wukong/dqe/integ-test
./setup-data.sh http://localhost:9200
./run-phase1-tests.sh http://localhost:9200 2>&1 | tee reports/post-fix-report.txt
```

**Step 5: Compare with baseline**

Check how many tests now pass vs the baseline. The implicit cast fix should resolve function-related failures. Remaining failures need further investigation.

---

### Task 10+: TDD loop for remaining failures

For each remaining failure category, repeat this cycle:

1. **Identify the failing test and root cause** — read the failure message, trace through the Java code
2. **Write a unit test** that reproduces the failure at the Java level
3. **Run to verify it fails**
4. **Fix the engine code**
5. **Run to verify it passes** (plus no regressions)
6. **Commit the fix**
7. **Rebuild, redeploy, re-run integ tests**

**Expected failure categories** (to be confirmed by baseline run):
- Type mapping differences (e.g., DQE returns LONG where test expects varchar)
- Missing or different column names in schema
- BETWEEN / IN / LIKE evaluation issues
- CASE expression type inference
- Multi-shard ordering differences
- TRY_CAST not implemented
- Arithmetic type promotion edge cases

**Test change policy:** If a test case expected value is wrong for a legitimate reason (e.g., the test was written against Trino which returns a different type than DQE correct behavior), document the reason and get human approval before changing the test case.

**Termination condition:** All 103 tests pass. Save final report to `dqe/integ-test/reports/`.

---

### Task Summary

| Task | Description | Depends on |
|------|-------------|------------|
| 1 | Clean old test scaffolding | — |
| 2 | Copy test data (mappings + bulk) | 1 |
| 3 | Copy setup-data.sh | 2 |
| 4 | Copy and adapt validate.py | 1 |
| 5 | Copy 103 phase1 test cases | 1 |
| 6 | Write run-phase1-tests.sh | 4, 5 |
| 7 | Run baseline tests | 3, 6 |
| 8 | Fix ExpressionCompiler implicit cast | 7 |
| 9 | Build, deploy, re-run tests | 8 |
| 10+ | TDD loop for remaining failures | 9 |
