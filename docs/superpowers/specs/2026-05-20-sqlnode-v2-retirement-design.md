# SqlNode Migration: v2 RelBuilder Retirement Design

**Date:** 2026-05-20
**Branch:** `feat/sqlnode`
**Status:** Approved (sections 1–6)

## 1. Goal & success criteria

**Goal.** Make the SqlNode pipeline (`PplToSqlNode` → `SqlValidator` → `SqlToRelConverter`) the only PPL plan-translation path in the codebase, by deleting the v2 `CalciteRelNodeVisitor` (4,516 lines) and the surrounding RelBuilder code.

**Done when:**

1. All in-scope `org.opensearch.sql.calcite.remote.Calcite*IT` tests pass at 100%.
   - Out of scope per directive: `CalciteExplainIT`, `CalcitePPLGraphLookupIT`, `CalcitePPLPatternsIT`.
2. The following are removed from the tree:
   - `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java`
   - `core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java`
   - `core/src/test/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor*Test.java`
   - `core/src/test/java/org/opensearch/sql/calcite/CalciteRexNodeVisitorTest.java`
   - `ppl/src/test/java/org/opensearch/sql/ppl/calcite/sqlnode/CalcitePPLSqlNodeParityTest.java`
   - `Settings.Key.CALCITE_SQLNODE_ENABLED` and its registration in `OpenSearchSettings`
3. `api/src/main/java/org/opensearch/sql/api/UnifiedQueryPlanner.java` and
   `ppl/src/test/java/org/opensearch/sql/ppl/calcite/CalcitePPLAbstractTest.java`
   are migrated to use the SqlNode pipeline instead of `CalciteRelNodeVisitor`.
4. `core/src/main/java/org/opensearch/sql/executor/QueryService.java::analyze`
   no longer branches; SqlNode is unconditional.
5. `./gradlew build` passes.

**Non-goals.**

- ExplainIT cosmetic plan-shape parity (~160 failures, deferred).
- GraphLookup feature implementation (27 failures, deferred).
- Patterns BRAIN tokens propagation (8 failures, deferred).
- Removing the v1 `legacy` SQL engine.

## 2. Constraints

- **Parity bar**: 100% on in-scope tests before any v2 code is deleted.
- **Toggle**: `plugins.calcite.sqlnode.enabled` (currently default `true`) is the kill-switch during parity work; it is removed in Phase 7.
- **No runtime fallback**: SqlNode failures surface to callers — required for visibility into translation gaps.
- **Branch state**: 283 local commits ahead of `origin/feat/sqlnode`; backup ref at
  `refs/backup/sqlnode-pre-squash-2026-05-20`.

## 3. Approach: phased sequential

| Phase | Scope | Tests fixed | Primary code area |
|---:|---|---:|---|
| 0 | Branch hygiene (squash, push, baseline IT sweep) | 0 | git only |
| 1 | Schema metadata propagation (`_id`/`_index`/`_score`/dotted-leaf) | 30 (MapPath 25 + Bin 4 + Eval 1) | `PplToSqlNode.java`, rowTypeOracle |
| 2 | OpenSearch storage array materialization | 30 (MvExpand 16 + Expand 10 + NewAddedCommands 4) | `opensearch/.../storage/*`, scan + mapping reader |
| 3 | Bin time-pushdown + auto_date_histogram | 6 (Bin 5 + Case 1) | `PplToSqlNode.visitBin`, OpenSearch pushdown rules |
| 4 | Spath auto-extract: validator duplicate-name quirk | 6 | `PplToSqlNode.visitSpath` |
| 5 | EXISTS subsearch cap on lifted Filter shape + `setSubsearchMaxOut(2)` state leak | 6 | `SqlNodePlanner.applySubsearchLimitForIn` (extend to EXISTS branch); fix state propagation between testSubsearchMaxOut3 and other EXISTS tests |
| 6 | Singletons | 3 (Join 2 + Eval 1) | targeted visitors |
| 7 | v2 retirement | 0 (must stay green) | broad |

Each phase ships as its own PR into `feat/sqlnode`. After Phase 7, the branch is squash-merged into `main`.

## 4. Per-phase workflow

For each phase:

1. **Targeted run.** `./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.<Class>IT"` for each in-scope class in the phase.
2. **Diagnose & fix.** Use the documented recurring pitfalls in
   `~/.claude/projects/-local-home-penghuo-oss-ppl/memory/sqlnode_poc_status.md`.
3. **Full sweep.** `./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.Calcite*IT"` to check the 35+ test classes documented as passing remain at 0 failures.
4. **No-regression gate.** Numerical compare against the Phase 0 baseline; in-scope failures must monotonically decrease, no class outside the phase scope may regress.
5. **Commit & PR.** Reference the spec, list tests fixed, list classes verified.

## 5. Phase 1 architecture detail (most consequential)

v2 carries metadata fields (`_id`, `_index`, `_score`, `_maxscore`, `_sort`, `_routing`)
and dotted struct leaves through the RelBuilder path via:

- `rowTypeOracle.getRowType()` — exposes them on the source row type.
- A `tryToRemoveNestedFields` rule in projection that drops dotted leaves only when the parent struct is also in scope (collision avoidance), but keeps metadata fields visible until the final user-facing layer.

The SqlNode pipeline today strips them at the implicit `fields *` step in `visitProject`,
which (a) breaks dotted-path tests and (b) leaks plan-shape diffs into ExplainIT.

**Fix shape:**

1. Extend `rowTypeOracle.getRowType()` so the source row type carries the same metadata + dotted leaves as v2.
2. In `PplToSqlNode.visitProject`, when `isSelectStar`, emit explicit per-column projection that retains metadata fields and applies the parent-struct collision drop only (not blanket metadata strip).
3. Defer the user-facing metadata strip to the final outer SELECT (terminal pipe), via a single `stripUserFacingMetadata` step in `SqlNodePlanner.toFinalRel`. This way intermediate wraps preserve metadata, and only the response strips it.

**Acceptance:** 25 MapPath + 4 Bin + 1 Eval tests pass without regressing any of the 35+ documented passing classes.

## 6. Phase 7 retirement checklist

1. Delete `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java`.
2. Delete `core/src/main/java/org/opensearch/sql/calcite/CalciteRexNodeVisitor.java`.
3. Delete `core/src/test/java/org/opensearch/sql/calcite/CalciteRelNodeVisitorSearchSimpleTest.java`.
4. Delete `core/src/test/java/org/opensearch/sql/calcite/CalciteRexNodeVisitorTest.java`.
5. Delete `ppl/src/test/java/org/opensearch/sql/ppl/calcite/sqlnode/CalcitePPLSqlNodeParityTest.java`.
6. Migrate `ppl/src/test/java/org/opensearch/sql/ppl/calcite/CalcitePPLAbstractTest.java` to instantiate `SqlNodePlanner` + `PplToSqlNode` instead of `CalciteRelNodeVisitor`. Subclasses' `verifyLogical()` and `verifyPPLToSparkSQL()` must keep working against the new planner.
7. Migrate `api/src/main/java/org/opensearch/sql/api/UnifiedQueryPlanner.java` to use `SqlNodePlanner` + `PplToSqlNode` (line 107 today instantiates v2 directly).
8. Remove `CALCITE_SQLNODE_ENABLED` from `common/src/main/java/org/opensearch/sql/common/setting/Settings.java::Key`.
9. Remove `CALCITE_SQLNODE_ENABLED_SETTING` registration in `opensearch/src/main/java/org/opensearch/sql/opensearch/setting/OpenSearchSettings.java` (lines 168–173, 462–464, 671).
10. Simplify `core/src/main/java/org/opensearch/sql/executor/QueryService.java::analyze`: drop the `isSqlNodePathEnabled()` branch (lines 302–326) and the `getRelNodeVisitor()` helper.
11. Update `CLAUDE.md` "Calcite Engine" section: remove `plugins.calcite.sqlnode.enabled` references; remove `CalciteNoPushdownIT` v2-related notes if any.
12. Update `integ-test/src/test/java/org/opensearch/sql/ppl/PPLIntegTestCase.java` lines 238 + 244 (toggle-management helpers) — remove since the toggle no longer exists.
13. Run `./gradlew build` end-to-end. Run the full Calcite*IT sweep — must stay 100% on in-scope classes.

## 7. Risks & guardrails

- **Per-phase regression gate.** Full Calcite*IT sweep before each phase PR merge, compared against Phase 0 baseline. Phase scope is the only set allowed to change failure counts.
- **Pre-deletion gate (Phase 7).** Same sweep but with the toggle setting deleted entirely; the build must still pass.
- **Backup ref.** `refs/backup/sqlnode-pre-squash-2026-05-20 → 771975f39d` is the immutable revert point.
- **Until Phase 7 lands, the toggle still flips back to v2.** Any in-field issue can be mitigated by toggling the setting to `false`; `CalciteRelNodeVisitor` remains compilable through phases 1–6.
- **After Phase 7, rollback = revert the deletion PR** (which is one PR by design).
- **Phase 2 (storage array materialization) touches OpenSearch storage**, the lowest-level layer in scope. Ship behind an OpenSearch index setting if the change risks affecting non-PPL paths; investigate in the phase plan.

## 8. Out-of-scope items (tracked separately)

- `CalciteExplainIT` cosmetic plan-shape diffs (~160).
- `CalcitePPLGraphLookupIT` (27 — feature unimplemented).
- `CalcitePPLPatternsIT` (8 — BRAIN UDAF + UNNEST + merge-map).
- v1 legacy SQL engine retirement.

These are documented as known gaps. Each gets its own follow-up issue once Phase 7 lands.
