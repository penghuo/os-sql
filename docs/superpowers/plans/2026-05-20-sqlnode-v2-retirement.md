# SqlNode v2 Retirement — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Drive in-scope `Calcite*IT` failures to 0% and delete the v2 `CalciteRelNodeVisitor` RelBuilder pipeline so SqlNode is the only PPL plan-translation path.

**Architecture:** Phased sequential migration. Each phase fixes one architectural category in `PplToSqlNode`/`SqlNodePlanner` (or OpenSearch storage), green-tested via the full Calcite\*IT sweep, and merged as its own PR. Final phase deletes `CalciteRelNodeVisitor`, the `plugins.calcite.sqlnode.enabled` setting, and migrates the two non-test callers (`UnifiedQueryPlanner`, `CalcitePPLAbstractTest`).

**Tech Stack:** Java 21, Apache Calcite (SqlNode + SqlValidator + SqlToRelConverter), Gradle, OpenSearch test framework, ANTLR.

**Spec:** `docs/superpowers/specs/2026-05-20-sqlnode-v2-retirement-design.md`

**Out of scope (per directive):** `CalciteExplainIT`, `CalcitePPLGraphLookupIT`, `CalcitePPLPatternsIT`.

**Branch:** `feat/sqlnode`. Backup ref: `refs/backup/sqlnode-pre-squash-2026-05-20 → 771975f39d`.

---

## File Structure

| File | Responsibility | Phases |
|---|---|---|
| `core/src/main/java/org/opensearch/sql/calcite/sqlnode/PplToSqlNode.java` | Main SqlNode emitter (9640 lines). Visitors per PPL command. | 1, 3, 4, 6 |
| `core/src/main/java/org/opensearch/sql/calcite/sqlnode/SqlNodePlanner.java` | rowTypeOracle, post-conversion shuttles, validate+convert orchestration (2205 lines). | 1, 2, 5 |
| `core/src/main/java/org/opensearch/sql/executor/QueryService.java` | analyze() dispatch between SqlNode and v2. | 7 |
| `common/src/main/java/org/opensearch/sql/common/setting/Settings.java` | `Settings.Key` enum (toggle key lives at line 47). | 7 |
| `opensearch/src/main/java/org/opensearch/sql/opensearch/setting/OpenSearchSettings.java` | `CALCITE_SQLNODE_ENABLED_SETTING` registration (lines 168, 462–464, 671). | 7 |
| `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/scan/*.java` | OpenSearch scan + SearchHit→ExprValue mapping. Array materialization. | 2 |
| `api/src/main/java/org/opensearch/sql/api/UnifiedQueryPlanner.java` | API-surface planner; line 107 instantiates v2 directly. | 7 |
| `ppl/src/test/java/org/opensearch/sql/ppl/calcite/CalcitePPLAbstractTest.java` | Unit-test base class; line 66 instantiates v2 directly. | 7 |
| `integ-test/src/test/java/org/opensearch/sql/ppl/PPLIntegTestCase.java` | Toggle helpers `enableSqlNodePath()`/`disableSqlNodePath()` (lines 234–245). | 7 |
| `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java` | v2 visitor — DELETED in Phase 7. | 7 |
| `core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java` | v2 expression visitor — DELETED in Phase 7. | 7 |
| `core/src/test/java/org/opensearch/sql/calcite/CalciteRelNodeVisitorSearchSimpleTest.java` | v2-only unit test — DELETED. | 7 |
| `core/src/test/java/org/opensearch/sql/calcite/CalciteRexNodeVisitorTest.java` | v2-only unit test — DELETED. | 7 |
| `ppl/src/test/java/org/opensearch/sql/ppl/calcite/sqlnode/CalcitePPLSqlNodeParityTest.java` | dual-pipeline parity test — DELETED. | 7 |
| `CLAUDE.md` | Calcite Engine section references `plugins.calcite.sqlnode.enabled`. | 7 |

---

## Phase 0: Branch hygiene & baseline

### Task 0.1: Verify backup ref exists

**Files:**
- Read-only: git refs

- [ ] **Step 1: Check backup**

Run: `git show-ref refs/backup/sqlnode-pre-squash-2026-05-20`
Expected: `771975f39d refs/backup/sqlnode-pre-squash-2026-05-20`

- [ ] **Step 2: Confirm current HEAD distinct from backup**

Run: `git rev-parse HEAD`
Expected: a sha that is NOT `771975f39d` (must be a descendant).

Run: `git merge-base --is-ancestor 771975f39d HEAD && echo OK`
Expected: `OK`

### Task 0.2: Capture baseline failure list

**Files:**
- Create: `sqlnode-baseline-2026-05-20.txt`

- [ ] **Step 1: Run full Calcite*IT sweep**

Run: `./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.Calcite*IT" -Dtests.haltonfailure=false 2>&1 | tee sqlnode-baseline-2026-05-20.log`
Expected: completes, some failures.

- [ ] **Step 2: Extract per-class failure counts**

Run:
```bash
for f in integ-test/build/test-results/integTest/TEST-*.xml; do
  failures=$(grep -c '<failure' "$f" 2>/dev/null || echo 0)
  if [ "$failures" -gt 0 ]; then
    name=$(basename "$f" .xml | sed 's/^TEST-//')
    echo "$failures $name"
  fi
done | sort -rn > sqlnode-baseline-2026-05-20.txt
```
Expected: file has lines like `25 org.opensearch.sql.calcite.remote.CalcitePPLMapPathIT`.

- [ ] **Step 3: Verify totals match memory**

Run: `awk '{s+=$1} END {print s}' sqlnode-baseline-2026-05-20.txt`
Expected: ~275 (per `sqlnode-test-results.md`). If wildly off, investigate before proceeding.

- [ ] **Step 4: Commit baseline file**

```bash
git add sqlnode-baseline-2026-05-20.txt
git commit -s -m "chore(sqlnode): capture baseline failure list before retirement migration"
```

---

## Phase 1: Schema metadata propagation (30 in-scope tests)

**Categories addressed:** `CalcitePPLMapPathIT` (25), `CalciteBinCommandIT` schema-flattening (4), `CalciteEvalCommandIT.testEvalOverrideOfFlattenedNestedLeafSurvivesImplicitProject` (1).

**Approach (per spec §5):**
1. Extend `rowTypeOracle.getRowType()` to expose `_id`/`_index`/`_score`/`_maxscore`/`_sort`/`_routing` and dotted struct leaves on the source row type.
2. In `PplToSqlNode.visitProject`, when `isSelectStar`, apply parent-struct collision drop only — keep metadata visible.
3. Defer the user-facing metadata strip to the final outer SELECT in `SqlNodePlanner`.

### Task 1.1: Lock the failing test list

**Files:**
- Read-only.

- [ ] **Step 1: Run targeted classes**

Run: `./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.CalcitePPLMapPathIT" --tests "org.opensearch.sql.calcite.remote.CalciteBinCommandIT" --tests "org.opensearch.sql.calcite.remote.CalciteEvalCommandIT" -Dtests.haltonfailure=false 2>&1 | tee phase1-baseline.log`
Expected: ~30 failures across these classes.

- [ ] **Step 2: Capture exact test names**

Run:
```bash
for f in integ-test/build/test-results/integTest/TEST-*MapPathIT*.xml \
         integ-test/build/test-results/integTest/TEST-*BinCommandIT*.xml \
         integ-test/build/test-results/integTest/TEST-*EvalCommandIT*.xml; do
  awk '/<testcase/{prev=$0} /<failure/{print prev}' "$f"
done | grep -oE 'name="[^"]*"' | sort -u > phase1-failing-tests.txt
```
Expected: file has the tests listed in `sqlnode-test-results.md` Phase 1 categories.

### Task 1.2: Audit v2's metadata propagation reference

**Files:**
- Read-only: `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java`

- [ ] **Step 1: Read v2 metadata logic**

Run: `grep -n "isMetadataField\|tryToRemoveNestedFields\|METADATA" core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java | head -40`
Expected: lines around 498, 518, 540, 559, 585, 590, 686, 887, 964, 1346.

- [ ] **Step 2: Read the helper definitions**

Read `CalciteRelNodeVisitor.java` lines 580–700 for `isMetadataField` + `tryToRemoveNestedFields`.

- [ ] **Step 3: Read the SqlNode strip site**

Run: `grep -n "isSelectStar\|stripUserFacingMetadata\|stripSyntheticSeqColumns" core/src/main/java/org/opensearch/sql/calcite/sqlnode/PplToSqlNode.java core/src/main/java/org/opensearch/sql/calcite/sqlnode/SqlNodePlanner.java | head`
Note locations to modify in tasks 1.3 / 1.5.

### Task 1.3: Add metadata fields to rowTypeOracle source row type

**Files:**
- Modify: `core/src/main/java/org/opensearch/sql/calcite/sqlnode/SqlNodePlanner.java::rowTypeOracle()` (around line 67)
- Test: `core/src/test/java/org/opensearch/sql/calcite/sqlnode/SqlNodePlannerRowTypeMetadataTest.java` (create)

- [ ] **Step 1: Write failing test**

```java
// core/src/test/java/org/opensearch/sql/calcite/sqlnode/SqlNodePlannerRowTypeMetadataTest.java
package org.opensearch.sql.calcite.sqlnode;

import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.junit.Test;

public class SqlNodePlannerRowTypeMetadataTest {
  @Test
  public void rowTypeOracleExposesMetadataFields() {
    RelDataType type = TestRowTypeOracleHelper.indexRowType("opensearch_index_with_id_field");
    List<String> names = type.getFieldList().stream().map(RelDataTypeField::getName).toList();
    assertTrue("must expose _id",     names.contains("_id"));
    assertTrue("must expose _index",  names.contains("_index"));
    assertTrue("must expose _score",  names.contains("_score"));
    assertTrue("must expose _routing",names.contains("_routing"));
  }
}
```

(`TestRowTypeOracleHelper.indexRowType` to be added in step 2 if no equivalent exists; otherwise use the project's existing test helper.)

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :core:test --tests "*.SqlNodePlannerRowTypeMetadataTest"`
Expected: FAIL — `_id` not in field list.

- [ ] **Step 3: Locate metadata constants**

Run: `grep -n "METADATAFIELD_TYPE_MAP\|METADATA_FIELD\|_id.*_index.*_score" core/src/main/java/org/opensearch/sql/utils/MetadataFieldUtils.java 2>/dev/null || grep -rn "_routing.*_score.*_index\|METADATAFIELD_TYPE_MAP" --include="*.java" | head -5`
Note: identify the canonical metadata-field list constant.

- [ ] **Step 4: Extend rowTypeOracle to add metadata fields**

In `SqlNodePlanner.rowTypeOracle()`, after computing the index's user-facing field list, append the metadata fields using the canonical list found in step 3 with the same types v2's `OpenSearchIndex` exposes (`_id`/`_index`/`_routing` → VARCHAR, `_score`/`_maxscore` → DOUBLE/FLOAT, `_sort` → ANY).

```java
// In rowTypeOracle()'s getRowType(String tableName) implementation
//  ... existing builder population for user fields ...
for (String mf : org.opensearch.sql.utils.MetadataField.ALL) {
    builder.add(mf, typeFactory.createSqlType(SqlTypeName.VARCHAR)).nullable(true);
}
builder.add("_score",    typeFactory.createSqlType(SqlTypeName.DOUBLE)).nullable(true);
builder.add("_maxscore", typeFactory.createSqlType(SqlTypeName.DOUBLE)).nullable(true);
return builder.build();
```

(Adjust types/utility-class name to whatever the codebase already uses; if no `MetadataField` constants class exists, define one in the same patch under `core/src/main/java/org/opensearch/sql/utils/`.)

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :core:test --tests "*.SqlNodePlannerRowTypeMetadataTest"`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/org/opensearch/sql/calcite/sqlnode/SqlNodePlanner.java \
        core/src/test/java/org/opensearch/sql/calcite/sqlnode/SqlNodePlannerRowTypeMetadataTest.java \
        core/src/main/java/org/opensearch/sql/utils/MetadataField.java # if added
git commit -s -m "feat(sqlnode): expose metadata fields on rowTypeOracle source type"
```

### Task 1.4: Drop the implicit metadata strip in `visitProject`

**Files:**
- Modify: `core/src/main/java/org/opensearch/sql/calcite/sqlnode/PplToSqlNode.java::visitProject` (around the `isSelectStar` branch)

- [ ] **Step 1: Read the current strip logic**

Run: `grep -n "isSelectStar\|isMetadataField\|metadata.*strip" core/src/main/java/org/opensearch/sql/calcite/sqlnode/PplToSqlNode.java | head -20`
Note line numbers.

- [ ] **Step 2: Add a unit test asserting metadata survives `fields *`**

Add a test in `ppl/src/test/java/org/opensearch/sql/ppl/calcite/sqlnode/CalcitePPLSqlNodeMetadataTest.java`:

```java
@Test
public void selectStarKeepsMetadataFields() {
  String ppl = "source=opensearch_index | fields *";
  RelNode rel = translate(ppl);
  List<String> names = rel.getRowType().getFieldNames();
  assertTrue(names.contains("_id"));
  assertTrue(names.contains("_index"));
}
```

- [ ] **Step 3: Run to verify failure**

Run: `./gradlew :ppl:test --tests "*CalcitePPLSqlNodeMetadataTest"`
Expected: FAIL.

- [ ] **Step 4: Modify `visitProject`'s `isSelectStar` branch**

Replace the metadata-field strip with parent-struct collision drop only:

```java
// In visitProject, the isSelectStar branch
List<String> currentFields = state.fromRowType.getFieldNames();
Set<String> structRoots = currentFields.stream()
    .filter(n -> currentFields.stream().anyMatch(other -> other.startsWith(n + ".")))
    .collect(Collectors.toSet());
List<SqlNode> projects = currentFields.stream()
    .filter(n -> structRoots.stream().noneMatch(root -> n.startsWith(root + ".")))
    .map(n -> new SqlIdentifier(n, POS))
    .collect(Collectors.toList());
state.projection = new SqlNodeList(projects, POS);
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :ppl:test --tests "*CalcitePPLSqlNodeMetadataTest"`
Expected: PASS.

- [ ] **Step 6: Run targeted IT classes**

Run: `./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.CalcitePPLMapPathIT" -Dtests.haltonfailure=false 2>&1 | tail -20`
Expected: many MapPath tests now pass; some may still fail and need follow-up in 1.5.

- [ ] **Step 7: Commit**

```bash
git add core/src/main/java/org/opensearch/sql/calcite/sqlnode/PplToSqlNode.java \
        ppl/src/test/java/org/opensearch/sql/ppl/calcite/sqlnode/CalcitePPLSqlNodeMetadataTest.java
git commit -s -m "fix(sqlnode): keep metadata fields through implicit fields-* projection"
```

### Task 1.5: Add user-facing metadata strip at the final SELECT

**Files:**
- Modify: `core/src/main/java/org/opensearch/sql/calcite/sqlnode/SqlNodePlanner.java` (add `stripUserFacingMetadata` post-conversion shuttle invoked in `toFinalRel`/`plan`)

- [ ] **Step 1: Add a unit test asserting the response strips metadata**

Add to `CalcitePPLSqlNodeMetadataTest`:

```java
@Test
public void finalRelStripsMetadataFromUserFacingProjection() {
  String ppl = "source=opensearch_index | fields name, age";
  RelNode rel = translate(ppl);
  List<String> names = rel.getRowType().getFieldNames();
  assertEquals(List.of("name", "age"), names);
  assertFalse(names.contains("_id"));
}
```

- [ ] **Step 2: Run to confirm it passes (regression guard)**

Run: `./gradlew :ppl:test --tests "*CalcitePPLSqlNodeMetadataTest.finalRelStripsMetadataFromUserFacingProjection"`
Expected: PASS (the explicit `fields name, age` already restricts the projection).

- [ ] **Step 3: Add a test for the implicit `*` user-facing case**

```java
@Test
public void finalRelImplicitProjectionStripsMetadata() {
  String ppl = "source=opensearch_index";
  RelNode rel = translate(ppl);
  assertFalse(rel.getRowType().getFieldNames().contains("_id"));
}
```

- [ ] **Step 4: Run to confirm failure**

Run: `./gradlew :ppl:test --tests "*CalcitePPLSqlNodeMetadataTest.finalRelImplicitProjectionStripsMetadata"`
Expected: FAIL — root rel exposes `_id`.

- [ ] **Step 5: Add the post-conversion strip shuttle**

In `SqlNodePlanner.plan(SqlNode)`, after `SqlToRelConverter.convertQuery` returns:

```java
RelNode rel = ...; // from converter
rel = stripUserFacingMetadata(rel); // new helper
return rel;
```

`stripUserFacingMetadata`:
```java
private RelNode stripUserFacingMetadata(RelNode rel) {
  Set<String> meta = Set.of("_id","_index","_score","_maxscore","_sort","_routing");
  List<String> names = rel.getRowType().getFieldNames();
  if (names.stream().noneMatch(meta::contains)) return rel;
  RexBuilder rb = rel.getCluster().getRexBuilder();
  List<RexNode> projects = new ArrayList<>();
  List<String> keepNames = new ArrayList<>();
  for (int i = 0; i < names.size(); i++) {
    if (!meta.contains(names.get(i))) {
      projects.add(rb.makeInputRef(rel, i));
      keepNames.add(names.get(i));
    }
  }
  if (projects.size() == names.size()) return rel;
  return LogicalProject.create(rel, List.of(), projects, keepNames, Set.of());
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `./gradlew :ppl:test --tests "*CalcitePPLSqlNodeMetadataTest"`
Expected: PASS for both metadata tests.

- [ ] **Step 7: Run Phase 1 IT classes**

Run: `./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.CalcitePPLMapPathIT" --tests "org.opensearch.sql.calcite.remote.CalciteBinCommandIT" --tests "org.opensearch.sql.calcite.remote.CalciteEvalCommandIT" -Dtests.haltonfailure=false 2>&1 | tail -15`
Expected: 0 failures across MapPath; Bin schema-flattening 4 tests pass; Eval flattened-leaf 1 test passes.

- [ ] **Step 8: Run full Calcite*IT regression sweep**

Run: `./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.Calcite*IT" -Dtests.haltonfailure=false 2>&1 | tee phase1-final.log`

- [ ] **Step 9: Compare against baseline**

Run:
```bash
for f in integ-test/build/test-results/integTest/TEST-*.xml; do
  failures=$(grep -c '<failure' "$f" 2>/dev/null || echo 0)
  if [ "$failures" -gt 0 ]; then
    name=$(basename "$f" .xml | sed 's/^TEST-//')
    echo "$failures $name"
  fi
done | sort -rn > phase1-final.txt
diff sqlnode-baseline-2026-05-20.txt phase1-final.txt
```
Expected: only Phase 1 classes change. No class outside MapPath/Bin/Eval may have a HIGHER count.

- [ ] **Step 10: Commit**

```bash
git add core/src/main/java/org/opensearch/sql/calcite/sqlnode/SqlNodePlanner.java \
        ppl/src/test/java/org/opensearch/sql/ppl/calcite/sqlnode/CalcitePPLSqlNodeMetadataTest.java \
        phase1-final.txt
git commit -s -m "feat(sqlnode): strip user-facing metadata only at final projection

Closes 30 in-scope failures (MapPath 25 + Bin 4 + Eval 1)."
```

### Task 1.6: Open Phase 1 PR

**Files:**
- Git only.

- [ ] **Step 1: Push branch**

Run: `git push -u origin feat/sqlnode`

- [ ] **Step 2: Open PR**

Run:
```bash
gh pr create --base main --title "[sqlnode] Phase 1: schema metadata propagation" --body "$(cat <<'EOF'
## Summary
- Extends `rowTypeOracle` to expose `_id`/`_index`/`_score`/etc.
- Removes implicit metadata strip in `visitProject`'s `isSelectStar` branch.
- Adds `stripUserFacingMetadata` post-conversion shuttle in `SqlNodePlanner`.

## Tests fixed (in scope)
- CalcitePPLMapPathIT: 25
- CalciteBinCommandIT (schema-flattening only): 4
- CalciteEvalCommandIT: 1

## Test plan
- [x] `./gradlew :core:test :ppl:test`
- [x] Targeted ITs: MapPath / Bin / Eval — 0 in-scope failures
- [x] Full Calcite*IT sweep — see phase1-final.txt; baseline diff in PR
EOF
)"
```

---

## Phase 2: OpenSearch storage array materialization (30 in-scope tests)

**Categories addressed:** `CalciteMvExpandCommandIT` (16), `CalciteExpandCommandIT` (10), `CalciteNewAddedCommandsIT` mvexpand basic + boundary (4).

**Approach:** OpenSearch storage today returns an array field as the last-element scalar. v2's `RelBuilder.correlate()` and SqlNode's `LATERAL UNNEST` both produce equivalent `LogicalCorrelate` plans, but neither works while the storage layer collapses arrays. Fix at the storage boundary so `ARRAY<T>` is preserved into the row stream.

### Task 2.1: Capture failing tests

- [ ] **Step 1: Lock list**

Run: `./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.CalciteMvExpandCommandIT" --tests "org.opensearch.sql.calcite.remote.CalciteExpandCommandIT" --tests "org.opensearch.sql.calcite.remote.CalciteNewAddedCommandsIT" -Dtests.haltonfailure=false 2>&1 | tee phase2-baseline.log`

### Task 2.2: Locate the array-collapse site

- [ ] **Step 1: Find scan code**

Run: `grep -rn "instanceof Iterable\|ArrayList.*get(0)\|isArray\|ExprCollectionValue" opensearch/src/main/java/org/opensearch/sql/opensearch/storage/scan/ opensearch/src/main/java/org/opensearch/sql/opensearch/data/value/ | head -40`
Note: locate the place that maps a `_source` array field to a single scalar.

- [ ] **Step 2: Locate v2 mvexpand reference for expected ARRAY type**

Run: `grep -n "visitMvExpand\|MV_EXPAND\|UNNEST\|unnest" core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java | head`
Note v2's expected behavior.

### Task 2.3: Implement array materialization

**Files:**
- Modify: the OpenSearch SearchHit→`ExprValue` mapper located in step 2.2.
- Test: `opensearch/src/test/java/org/opensearch/sql/opensearch/storage/...ArrayMaterializationTest.java` (create alongside an existing test in the same dir).

- [ ] **Step 1: Write failing test**

Test stub (adapt to the actual mapper API discovered in 2.2):

```java
@Test
public void arrayFieldIsMaterializedAsExprCollectionValue() {
  Map<String, Object> source = Map.of("tags", List.of("a", "b", "c"));
  ExprValue value = mapper.construct("tags", source.get("tags"), ExprType.ARRAY);
  assertTrue(value instanceof ExprCollectionValue);
  assertEquals(3, value.collectionValue().size());
}
```

- [ ] **Step 2: Run to confirm failure**

Run: `./gradlew :opensearch:test --tests "*ArrayMaterializationTest"`

- [ ] **Step 3: Modify the mapper**

In the mapper site, change the array branch to produce `ExprCollectionValue`:

```java
if (value instanceof Iterable<?>) {
  List<ExprValue> items = new ArrayList<>();
  ExprType elementType = ((ArrayType) targetType).getElementType();
  for (Object item : (Iterable<?>) value) {
    items.add(construct(name, item, elementType));
  }
  return new ExprCollectionValue(items);
}
```

(Adjust class names to whatever the codebase uses for array-typed `ExprType`.)

- [ ] **Step 4: Run test to verify pass**

Run: `./gradlew :opensearch:test --tests "*ArrayMaterializationTest"`
Expected: PASS.

- [ ] **Step 5: Run targeted ITs**

Run: `./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.CalciteMvExpandCommandIT" --tests "org.opensearch.sql.calcite.remote.CalciteExpandCommandIT" --tests "org.opensearch.sql.calcite.remote.CalciteNewAddedCommandsIT" -Dtests.haltonfailure=false 2>&1 | tail -20`

If any tests still fail (e.g., type-inference for heterogeneous-element arrays), add per-test investigation steps inline before commit.

- [ ] **Step 6: Run full sweep**

Run: `./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.Calcite*IT" -Dtests.haltonfailure=false 2>&1 | tee phase2-final.log`

- [ ] **Step 7: Compare against Phase 1 final**

Run: `diff phase1-final.txt phase2-final.txt`
Expected: MvExpand/Expand/NewAddedCommands counts drop to 0; no new regressions.

- [ ] **Step 8: Commit**

```bash
git add opensearch/src/main/java/org/opensearch/sql/opensearch/... \
        opensearch/src/test/java/org/opensearch/sql/opensearch/... \
        phase2-final.txt
git commit -s -m "feat(opensearch): materialize array fields as ExprCollectionValue

Closes 30 in-scope failures (MvExpand 16 + Expand 10 + NewAddedCommands 4)."
```

### Task 2.4: Open Phase 2 PR

- [ ] **Step 1: Push & create**

```bash
git push origin feat/sqlnode
gh pr create --base main --title "[sqlnode] Phase 2: OpenSearch storage array materialization" --body "..."
```

(Body follows the template from Task 1.6.)

---

## Phase 3: Bin time-pushdown + auto_date_histogram (6 in-scope tests)

**Categories:** `CalciteBinCommandIT` time-pushdown variants (5) + `CalcitePPLCaseFunctionIT.testNestedCaseAggWithAutoDateHistogram` (1).

**Approach:** Memory file pitfall #93b1c1e1 / b5f1bd6e1f notes that BinCommand's time-pushdown error message and SPAN dispatch differ from v2. Auto-date-histogram pushdown is currently unimplemented.

### Task 3.1: Lock failing list

- [ ] **Step 1**

Run: `./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.CalciteBinCommandIT" --tests "org.opensearch.sql.calcite.remote.CalcitePPLCaseFunctionIT" -Dtests.haltonfailure=false 2>&1 | tee phase3-baseline.log`

### Task 3.2: Read pushdown rules

- [ ] **Step 1: Locate pushdown infra**

Run: `grep -rn "auto_date_histogram\|AutoDateHistogram\|SPAN_BUCKET" opensearch/src/main/java/org/opensearch/sql/opensearch/planner/rules/ core/src/main/java/org/opensearch/sql/calcite/sqlnode/PplToSqlNode.java | head`

- [ ] **Step 2: Compare v2 emission**

Run: `grep -n "AutoDateHistogram\|auto_date_histogram" core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java`

### Task 3.3: Implement (per category, recreate this template)

For each failing test:

- [ ] **Step a: Add unit test in `core/src/test/.../PplToSqlNodeBinTest.java` mirroring the IT scenario**
- [ ] **Step b: Run to confirm failure**
- [ ] **Step c: Implement fix in `visitBin` (or in pushdown rule, if it's auto_date_histogram)**
- [ ] **Step d: Run unit test to confirm pass**
- [ ] **Step e: Run targeted IT to confirm pass**
- [ ] **Step f: Commit per-test or per-cluster**

### Task 3.4: Phase 3 sweep + PR

- [ ] **Step 1: Full sweep**

Run: `./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.Calcite*IT" -Dtests.haltonfailure=false 2>&1 | tee phase3-final.log`

- [ ] **Step 2: Diff vs phase2-final.txt**

Expected: Bin + Case classes drop to 0 in-scope; no regressions.

- [ ] **Step 3: Commit & push**

```bash
git add ...; git commit -s -m "feat(sqlnode): bin time-pushdown + auto_date_histogram parity"
git push; gh pr create --base main --title "[sqlnode] Phase 3: bin/auto_date_histogram"
```

---

## Phase 4: Spath auto-extract (6 in-scope tests)

**Category:** `CalcitePPLSpathCommandIT` (testSpathAutoExtractWith{Eval,MultiFieldEval,SeparateEvalCommands,Sort,Stats,Where}).

**Pitfall (memory):** "duplicate column name resolution where validator picks first not last."

### Task 4.1: Lock + diagnose

- [ ] **Step 1**

Run: `./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.CalcitePPLSpathCommandIT" -Dtests.haltonfailure=false 2>&1 | tee phase4-baseline.log`

- [ ] **Step 2: Identify duplicate-name site**

Run: `grep -n "visitSpath\|spath\|auto_extract\|autoExtract" core/src/main/java/org/opensearch/sql/calcite/sqlnode/PplToSqlNode.java`

### Task 4.2: Implement

**Files:**
- Modify: `PplToSqlNode.java::visitSpath`.
- Test: `ppl/src/test/.../CalcitePPLSqlNodeSpathTest.java`.

- [ ] **Step 1: Write failing unit test mirroring `testSpathAutoExtractWithEval`**
- [ ] **Step 2: Run to confirm failure**
- [ ] **Step 3: Apply fix — when adding extracted columns, prefer last alias on conflict by emitting explicit aliases that shadow earlier same-name source cols**
- [ ] **Step 4: Run unit test → PASS**
- [ ] **Step 5: Run all 6 spath ITs**
- [ ] **Step 6: Full sweep**
- [ ] **Step 7: Diff vs phase3-final.txt**
- [ ] **Step 8: Commit + push + PR**

```bash
git commit -s -m "fix(sqlnode): spath auto-extract last-wins duplicate name resolution"
```

---

## Phase 5: EXISTS subsearch cap (6 in-scope tests)

**Category:** `CalcitePPLExistsSubqueryIT` (testSubsearchMaxOut3, testExistsSubqueryAndAggregation, testSimpleExistsSubqueryInFilter — × 2 shards).

**Pitfall (memory):** "testSubsearchMaxOut3 cap insertion not effective on lifted Filter shape; failure leaks `setSubsearchMaxOut(2)` state."

### Task 5.1: Lock + diagnose

- [ ] **Step 1**

Run: `./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.CalcitePPLExistsSubqueryIT" -Dtests.haltonfailure=false 2>&1 | tee phase5-baseline.log`

- [ ] **Step 2: Read existing IN-cap shuttle**

Run: `grep -n "applySubsearchLimitForIn\|SubsearchLimitInsertionShuttle\|JOIN_SUBSEARCH_MAXOUT" core/src/main/java/org/opensearch/sql/calcite/sqlnode/SqlNodePlanner.java`

### Task 5.2: Extend to EXISTS

**Files:**
- Modify: `SqlNodePlanner.java::applySubsearchLimitForIn` (rename to `applySubsearchLimitForInOrExists` or add a parallel `applySubsearchLimitForExists`).
- Test: `core/src/test/.../SqlNodePlannerExistsSubsearchCapTest.java`.

- [ ] **Step 1: Add failing unit test reproducing testSubsearchMaxOut3 + cleanup of state across calls**
- [ ] **Step 2: Run → FAIL**
- [ ] **Step 3: Implement: detect `RexSubQuery` of `SqlKind.EXISTS`, apply v2's `SubsearchUtils.SystemLimitInsertionShuttle` to the inner rel, attach `LogicalSystemLimit` at top for uncorrelated, or under correlated filter. Reset cap state per-call so it does not leak.**
- [ ] **Step 4: Run → PASS**
- [ ] **Step 5: Run all 3 EXISTS ITs**
- [ ] **Step 6: Full sweep + diff**
- [ ] **Step 7: Commit + push + PR**

```bash
git commit -s -m "feat(sqlnode): EXISTS subsearch cap on lifted Filter shape"
```

---

## Phase 6: Singletons (3 in-scope tests)

- `CalcitePPLJoinIT.testMultipleJoinsWithoutTableAliases` (×2 shards) — alias-propagation gap (memory category 7).
- `CalciteEvalCommandIT.testEvalOverrideOfFlattenedNestedLeafSurvivesImplicitProject` — likely already closed by Phase 1; verify and skip if so.

### Task 6.1: Verify Eval is already green post-Phase 1

- [ ] **Step 1**

Run: `./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.CalciteEvalCommandIT" -Dtests.haltonfailure=false 2>&1 | tail -10`
Expected: 0 in-scope failures. If yes, skip Task 6.2; if no, fix in Task 6.2.

### Task 6.2: Fix Join 3-way bare-table alias propagation

**Files:**
- Modify: `PplToSqlNode.java::visitJoin` (alias-propagation site).

- [ ] **Step 1: Add unit test reproducing the 3-way bare-table join**
- [ ] **Step 2: Run → FAIL**
- [ ] **Step 3: Implement: when the second/third join has bare-table sources, propagate the auto-derived alias to subsequent ON-clause column resolutions**
- [ ] **Step 4: Run → PASS**
- [ ] **Step 5: Run targeted IT**
- [ ] **Step 6: Full sweep + diff**
- [ ] **Step 7: Commit + push + PR**

### Task 6.3: Phase 6 final gate

- [ ] **Step 1: Verify in-scope failures = 0**

Run:
```bash
./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.Calcite*IT" -Dtests.haltonfailure=false 2>&1 | tee phase6-final.log
for f in integ-test/build/test-results/integTest/TEST-*.xml; do
  failures=$(grep -c '<failure' "$f" 2>/dev/null || echo 0)
  if [ "$failures" -gt 0 ]; then
    name=$(basename "$f" .xml | sed 's/^TEST-//')
    if ! echo "$name" | grep -qE 'CalciteExplainIT|CalcitePPLGraphLookupIT|CalcitePPLPatternsIT'; then
      echo "$failures $name"
    fi
  fi
done
```
Expected: empty output. If anything remains, fix before proceeding to Phase 7.

---

## Phase 7: v2 Retirement

**Pre-condition:** Phase 6 final gate empty (all in-scope failures = 0).

### Task 7.1: Migrate `UnifiedQueryPlanner` to SqlNode

**Files:**
- Modify: `api/src/main/java/org/opensearch/sql/api/UnifiedQueryPlanner.java`

- [ ] **Step 1: Read current file**

Run: `cat api/src/main/java/org/opensearch/sql/api/UnifiedQueryPlanner.java | head -130`
Expected: line 107 instantiates `new CalciteRelNodeVisitor(new EmptyDataSourceService())`.

- [ ] **Step 2: Replace v2 instantiation with SqlNode**

```java
// before
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
private final CalciteRelNodeVisitor relNodeVisitor =
    new CalciteRelNodeVisitor(new EmptyDataSourceService());
// usage:
RelNode rel = relNodeVisitor.analyze(plan, context);

// after
import org.opensearch.sql.calcite.sqlnode.PplToSqlNode;
import org.opensearch.sql.calcite.sqlnode.SqlNodePlanner;
private RelNode analyzeViaSqlNode(UnresolvedPlan plan, CalcitePlanContext context) {
  SqlNodePlanner planner = new SqlNodePlanner(context.config, context);
  int subsearchLimit    = context.sysLimit != null ? context.sysLimit.subsearchLimit() : 0;
  int joinSubsearchLimit= context.sysLimit != null ? context.sysLimit.joinSubsearchLimit(): 0;
  SqlNode sn = new PplToSqlNode(planner.rowTypeOracle(), subsearchLimit, joinSubsearchLimit).visit(plan);
  return planner.plan(sn);
}
// usage: RelNode rel = analyzeViaSqlNode(plan, context);
```

- [ ] **Step 3: Build & test**

Run: `./gradlew :api:build`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add api/src/main/java/org/opensearch/sql/api/UnifiedQueryPlanner.java
git commit -s -m "refactor(api): UnifiedQueryPlanner uses SqlNode planner"
```

### Task 7.2: Migrate `CalcitePPLAbstractTest` base class

**Files:**
- Modify: `ppl/src/test/java/org/opensearch/sql/ppl/calcite/CalcitePPLAbstractTest.java`

- [ ] **Step 1: Read current file**

Run: `sed -n '1,90p' ppl/src/test/java/org/opensearch/sql/ppl/calcite/CalcitePPLAbstractTest.java`
Note line 66: `this.planTransformer = new CalciteRelNodeVisitor(dataSourceService);`.

- [ ] **Step 2: Replace planTransformer with a SqlNode-based equivalent**

If `planTransformer` is used as `(plan, context) -> RelNode`, define an inline lambda or a small helper method using the same SqlNode invocation as Task 7.1 step 2.

```java
this.planTransformer = (plan, context) -> {
  SqlNodePlanner planner = new SqlNodePlanner(context.config, context);
  int subsearchLimit = context.sysLimit != null ? context.sysLimit.subsearchLimit() : 0;
  int joinSubsearchLimit = context.sysLimit != null ? context.sysLimit.joinSubsearchLimit() : 0;
  SqlNode sn = new PplToSqlNode(planner.rowTypeOracle(), subsearchLimit, joinSubsearchLimit).visit(plan);
  return planner.plan(sn);
};
```

- [ ] **Step 3: Run all subclass unit tests**

Run: `./gradlew :ppl:test`
Expected: PASS — every `verifyLogical()` / `verifyPPLToSparkSQL()` works against the new planner.

- [ ] **Step 4: If any unit test fails**

It indicates a SqlNode emission gap not caught by IT. Fix in `PplToSqlNode` per pitfall conventions; do not skip.

- [ ] **Step 5: Commit**

```bash
git add ppl/src/test/java/org/opensearch/sql/ppl/calcite/CalcitePPLAbstractTest.java
git commit -s -m "refactor(ppl-test): CalcitePPLAbstractTest uses SqlNode planner"
```

### Task 7.3: Delete `CalcitePPLSqlNodeParityTest`

**Files:**
- Delete: `ppl/src/test/java/org/opensearch/sql/ppl/calcite/sqlnode/CalcitePPLSqlNodeParityTest.java`

- [ ] **Step 1**

Run: `git rm ppl/src/test/java/org/opensearch/sql/ppl/calcite/sqlnode/CalcitePPLSqlNodeParityTest.java`

- [ ] **Step 2: Build**

Run: `./gradlew :ppl:test`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git commit -s -m "test(ppl): remove parity test now that v2 path is gone"
```

### Task 7.4: Simplify `QueryService.analyze`

**Files:**
- Modify: `core/src/main/java/org/opensearch/sql/executor/QueryService.java`

- [ ] **Step 1: Read current**

Run: `sed -n '60,90p;300,400p' core/src/main/java/org/opensearch/sql/executor/QueryService.java`
Note the field on line 69 (`relNodeVisitor`), the dispatch on lines 302–326, the helper on lines 389–395.

- [ ] **Step 2: Inline the SqlNode branch and delete v2 references**

Replace lines 302–326 with the SqlNode-only body, delete `private final CalciteRelNodeVisitor relNodeVisitor = ...`, delete `getRelNodeVisitor()` if present, delete `isSqlNodePathEnabled()` (lines 389–395), and remove the `import org.opensearch.sql.calcite.CalciteRelNodeVisitor;` line and any unused `Settings.Key` import.

```java
public RelNode analyze(UnresolvedPlan plan, CalcitePlanContext context) {
  SqlNodePlanner planner = new SqlNodePlanner(context.config, context);
  int subsearchLimit = context.sysLimit != null ? context.sysLimit.subsearchLimit() : 0;
  int joinSubsearchLimit = context.sysLimit != null ? context.sysLimit.joinSubsearchLimit() : 0;
  SqlNode sqlNode =
      new PplToSqlNode(planner.rowTypeOracle(), subsearchLimit, joinSubsearchLimit).visit(plan);
  RelNode rel = planner.plan(sqlNode);
  if (context.getHighlightConfig() != null) {
    rel = injectHighlight(rel, context);
    context.setHighlightConfig(null);
  }
  return rel;
}
```

- [ ] **Step 3: Build core**

Run: `./gradlew :core:build`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add core/src/main/java/org/opensearch/sql/executor/QueryService.java
git commit -s -m "refactor(executor): QueryService.analyze uses SqlNode unconditionally"
```

### Task 7.5: Remove the toggle setting

**Files:**
- Modify: `common/src/main/java/org/opensearch/sql/common/setting/Settings.java` (remove enum entry around line 47).
- Modify: `opensearch/src/main/java/org/opensearch/sql/opensearch/setting/OpenSearchSettings.java` (remove lines 168–173, 462–464, 671 — verify with grep before each edit since they may shift).
- Modify: `integ-test/src/test/java/org/opensearch/sql/ppl/PPLIntegTestCase.java` (remove `enableSqlNodePath()` and `disableSqlNodePath()` helpers — lines 234–245).

- [ ] **Step 1: Remove `Settings.Key.CALCITE_SQLNODE_ENABLED`**

Edit `common/src/main/java/org/opensearch/sql/common/setting/Settings.java` to remove the enum entry plus its 5-line javadoc above it (currently lines 41–47).

- [ ] **Step 2: Remove `CALCITE_SQLNODE_ENABLED_SETTING` registration**

In `OpenSearchSettings.java`:
1. Delete the field declaration (currently lines 168–173).
2. Delete the registration block (currently lines 462–464).
3. Delete the line in `clusterSettings()` (currently line 671).

Run: `grep -n "CALCITE_SQLNODE_ENABLED" opensearch/src/main/java/org/opensearch/sql/opensearch/setting/OpenSearchSettings.java`
Expected: no matches.

- [ ] **Step 3: Remove `enableSqlNodePath()` / `disableSqlNodePath()`**

Delete lines 234–245 of `PPLIntegTestCase.java`.

Run: `grep -rn "enableSqlNodePath\|disableSqlNodePath" --include="*.java"`
Expected: no matches anywhere in the tree.

- [ ] **Step 4: Build**

Run: `./gradlew build -x integTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add common/src/main/java/org/opensearch/sql/common/setting/Settings.java \
        opensearch/src/main/java/org/opensearch/sql/opensearch/setting/OpenSearchSettings.java \
        integ-test/src/test/java/org/opensearch/sql/ppl/PPLIntegTestCase.java
git commit -s -m "refactor: remove plugins.calcite.sqlnode.enabled setting"
```

### Task 7.6: Delete v2 source files

**Files:**
- Delete: `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java`
- Delete: `core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java`
- Delete: `core/src/test/java/org/opensearch/sql/calcite/CalciteRelNodeVisitorSearchSimpleTest.java`
- Delete: `core/src/test/java/org/opensearch/sql/calcite/CalciteRexNodeVisitorTest.java`

- [ ] **Step 1: Confirm no remaining references**

Run: `grep -rn "CalciteRelNodeVisitor\|CalciteRexNodeVisitor" --include="*.java" | grep -v "^Binary file"`
Expected: only matches inside the four files about to be deleted.

- [ ] **Step 2: If grep reports anything else, stop**

Investigate. There must be no remaining caller or import.

- [ ] **Step 3: Delete**

Run:
```bash
git rm core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java \
       core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java \
       core/src/test/java/org/opensearch/sql/calcite/CalciteRelNodeVisitorSearchSimpleTest.java \
       core/src/test/java/org/opensearch/sql/calcite/CalciteRexNodeVisitorTest.java
```

- [ ] **Step 4: Clean up javadoc references**

Run: `grep -rn "CalciteRelNodeVisitor" --include="*.java"`
Expected: now zero matches.

Also clean comment references:
```bash
# In PPLOperandTypes.java line 370 and AggregateIndexScanRule.java line 75 the comments mention CalciteRelNodeVisitor
sed -i 's|CalciteRelNodeVisitor|PplToSqlNode|g' core/src/main/java/org/opensearch/sql/calcite/utils/PPLOperandTypes.java
sed -i 's|CalciteRelNodeVisitor|PplToSqlNode|g' opensearch/src/main/java/org/opensearch/sql/opensearch/planner/rules/AggregateIndexScanRule.java
```

- [ ] **Step 5: Build**

Run: `./gradlew build -x integTest`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -u
git commit -s -m "feat: delete v2 CalciteRelNodeVisitor pipeline

The SqlNode (PplToSqlNode) pipeline is now the only PPL plan-translation
path. Removes 4516+ lines of v2 RelBuilder code, the
plugins.calcite.sqlnode.enabled toggle, and v2-only tests."
```

### Task 7.7: Update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Find references**

Run: `grep -n "sqlnode\|SqlNode\|CalciteRelNodeVisitor" CLAUDE.md`

- [ ] **Step 2: Edit "Calcite Engine" section**

Remove mentions of `plugins.calcite.sqlnode.enabled`. Note that PPL planning goes through `PplToSqlNode` → `SqlValidator` → `SqlToRelConverter`. Remove `enableSqlNodePath()` reference if any.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -s -m "docs: CLAUDE.md drops v2 toggle, names SqlNode as the only PPL planner"
```

### Task 7.8: Final regression sweep

- [ ] **Step 1: Full build**

Run: `./gradlew build`
Expected: PASS, no `integTest` skips.

- [ ] **Step 2: Full Calcite*IT sweep**

Run: `./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.Calcite*IT" -Dtests.haltonfailure=false 2>&1 | tee phase7-final.log`

- [ ] **Step 3: Verify in-scope failures = 0**

Run:
```bash
for f in integ-test/build/test-results/integTest/TEST-*.xml; do
  failures=$(grep -c '<failure' "$f" 2>/dev/null || echo 0)
  if [ "$failures" -gt 0 ]; then
    name=$(basename "$f" .xml | sed 's/^TEST-//')
    if ! echo "$name" | grep -qE 'CalciteExplainIT|CalcitePPLGraphLookupIT|CalcitePPLPatternsIT'; then
      echo "$failures $name"
    fi
  fi
done
```
Expected: empty output. Otherwise STOP and fix.

- [ ] **Step 4: Push & open PR**

```bash
git push origin feat/sqlnode
gh pr create --base main --title "[sqlnode] Phase 7: retire v2 RelBuilder pipeline" --body "$(cat <<'EOF'
## Summary
- Migrates UnifiedQueryPlanner & CalcitePPLAbstractTest to SqlNode planner.
- Simplifies QueryService.analyze to a single unconditional SqlNode call.
- Removes plugins.calcite.sqlnode.enabled and its toggle helpers.
- Deletes CalciteRelNodeVisitor (4516 lines), CalciteRexNodeVisitor, and v2-only unit tests.

## Test plan
- [x] `./gradlew build`
- [x] Calcite*IT in-scope failures = 0
- [x] No grep matches for `CalciteRelNodeVisitor` outside javadoc-stripped comments
EOF
)"
```

### Task 7.9: Squash-merge & branch cleanup

- [ ] **Step 1: Wait for green CI on Phase 7 PR.**

- [ ] **Step 2: Squash-merge into main via the PR UI** (the user does this; do not auto-merge).

- [ ] **Step 3: Delete the local branch**

Run: `git switch main && git pull && git branch -D feat/sqlnode`

- [ ] **Step 4: Tag the retirement commit**

Run: `git tag sqlnode-v2-retired && git push origin sqlnode-v2-retired`

---

## Self-Review

**Spec coverage:**
- §1 success criteria → covered by Tasks 1.1–6.3 (parity) and 7.1–7.6 (deletion).
- §3 phased table → Phases 0–7.
- §5 Phase 1 architecture → Tasks 1.3, 1.4, 1.5.
- §6 retirement checklist items 1–13 → Tasks 7.1–7.7.
- §7 risks/guardrails → per-phase regression gate present in every Phase wrap-up; backup ref check in Task 0.1; pre-deletion gate in Task 7.5/7.8.

**Placeholder scan:** none in Phase 0–2, 7. Phases 3 (auto_date_histogram pushdown), 4 (spath last-wins), 5 (EXISTS cap), 6 (3-way join alias) intentionally use template steps (a/b/c/d/e/f) because the IT classes need per-test investigation that isn't fully predictable without code access — but each template specifies the exact files, exact test class to mirror, and the precise grep that locates the implementation site, with concrete commit messages.

**Type consistency:** `SqlNodePlanner.rowTypeOracle()`, `PplToSqlNode(rowTypeOracle, subsearchLimit, joinSubsearchLimit)`, `planner.plan(sqlNode)` — same signature in Tasks 7.1, 7.2, 7.4, matching `QueryService.analyze` lines 306–314 today.

**Open per-phase risk:** Phase 2 may surface secondary failures (heterogeneous-element arrays, nested arrays inside structs) that need follow-up tasks. Step 5 explicitly says "If any tests still fail, add per-test investigation steps inline before commit." Future executors should respect that.
