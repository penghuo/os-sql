# SqlNodePipeline IT Status

Tracker for running every `Calcite*IT` integration-test class one-by-one against the stripped-down `SqlNodePipeline` (no bypasses, no IP rewrite, no aggregate-hint reattach, no float/varchar literal wrappers, no FilterMerge pre-pass).

## Run policy

- One IT class per `./gradlew :integ-test:integTest -Dtests.class="*<ClassName>"` invocation.
- **Stop on first failure** in each phase for triage; record the first failing test method + error excerpt below, then resume after fix/skip decision.
- Pushdown ON column = default settings (production path). Pushdown OFF column = re-runs via `CalciteNoPushdownIT` at the end (it re-bundles existing classes with `plugins.calcite.pushdown.enabled=false`).
- Each row reflects the **most recent** run. Re-run after code changes; update STATUS + NOTES.

## Status legend

| Symbol | Meaning |
|---|---|
| `⏳` | Not yet run |
| `✅` | Passed |
| `❌` | Failed (note in column) |
| `⚠️` | Passed with warnings / partial |
| `⏭️` | Skipped (env-blocked, e.g. cross-cluster) |

## Run command

```bash
./gradlew :integ-test:integTest -Dtests.class="*<ClassName>" --info 2>&1 | tail -100
```

For pushdown OFF (last phase only):

```bash
./gradlew :integ-test:integTest -Dtests.class="*CalciteNoPushdownIT" --info
```

---

## Phase 1 — Smoke baseline (lowest risk, basic commands)

Goal: prove the round-trip works at all on plans that touch none of the removed bypasses.

| # | Class | Pushdown ON | Notes |
|---|---|---|---|
| 1 | CalcitePPLBasicIT | ✅ | All 42 pass after Layers 1+3 + PredicateAnalyzer.expectMapCall + wrapFloatLiteralsForRoundTrip. (Layers 2 + 4 turned out misplaced — Layer 2 was redundant after Layer 3, Layer 4 belonged in PredicateAnalyzer.) |
| 2 | CalcitePPLPluginIT | ✅ | 10/10 pass. |
| 3 | CalciteFieldsCommandIT | ✅ | 78/78 pass. |
| 4 | CalciteHeadCommandIT | ✅ | 12/12 pass. |
| 5 | CalcitePPLRenameIT | ✅ | 48/48 pass. |
| 6 | CalciteRenameCommandIT | ✅ | 4/4 pass. |
| 7 | CalciteWhereCommandIT | ✅ | 82/82 pass after adding SqlLibrary.POSTGRESQL (for ILIKE). |
| 8 | CalciteSearchCommandIT | ✅ | 104/104 pass. |
| 9 | CalciteDescribeCommandIT | ✅ | 6/6 pass. |
| 10 | CalciteShowDataSourcesCommandIT | ⏭️ | Excluded by build exclude rules (datasource IT). |
| 11 | CalciteInformationSchemaCommandIT | ⏭️ | Excluded by build exclude rules. |
| 12 | CalciteSettingsIT | ✅ | 4/4 pass. |
| 13 | CalciteResourceMonitorIT | ❌ | 1/2 fail (`queryExceedResourceLimitShouldFail`). Test sets memory_limit=1% and expects 500 "Insufficient resources"; query succeeds. Runtime-engine concern, not round-trip. **Deferred.** |
| 14 | CalciteErrorReportStageIT | ✅ | 7/7 pass. |

## Phase 2 — IP comparisons (expected REGRESSION: rewriteIpComparisons removed)

| # | Class | Pushdown ON | Notes |
|---|---|---|---|
| 15 | CalciteIPComparisonIT | ✅ | 12/12 pass after two fixes: (a) drop SqlSyntax.BINARY override in CompareIpFunction (was unparsing as `host EQUALS_IP ip` which Babel parser rejects); (b) compute valid SqlOperandCountRange + checkOperandTypes from allowedParamTypes in UDFOperandMetadata.UDTOperandMetadata (validator path needs them; visitor path tolerated null). |
| 16 | CalciteIPFunctionsIT | ✅ | 2/2 pass. |
| 17 | CalcitePPLIPFunctionIT | ✅ | 2/2 pass. |

## Phase 3 — Float / Varchar literal handling (expected REGRESSION: wrap*ForRoundTrip removed)

| # | Class | Pushdown ON | Notes |
|---|---|---|---|
| 18 | CalcitePPLCaseFunctionIT | ⚠️ | 8/9 pass after two fixes: (a) `withIdentifierExpansion(true)` in SqlValidator config (resolves `RexInputRef out of range` on CASE-as-GROUP-BY-key — matches Calcite's own SqlToRelFixture default); (b) `wrapVarcharCaseBranchesForRoundTrip` (Spark-style VARCHAR for CASE branches; without it, ANSI CHAR semantics pad `'low'` → `'low '`, verified against Calcite's own SqlOperatorTest.testCase). Remaining 1 fail (`testNestedCaseAggWithAutoDateHistogram`): `Windowed aggregate expression is illegal in GROUP BY clause` — separate WIDTH_BUCKET-with-window shape, deferred. |
| 19 | CalcitePPLCastFunctionIT | ✅ | 42/42 after refactoring ExprIPType to extend ExprSqlType(VARCHAR). `cast(ip as STRING)` is now a trivial identity cast; no custom UDF needed. IP comparisons still dispatch via EQUALS_IP/LESS_IP (matched by class identity). |
| 20 | CalcitePPLConditionBuiltinFunctionIT | ✅ | 24/24 pass after two R3 fixes: (a) EnhancedCoalesceFunction `getOperandMetadata` was `null`, validator's overload-filter called `SqlOperator.getOperandCountRange()` which throws by default. Provided permissive variadic metadata accepting 1+ operands. (b) STRING_TIMESTAMP was `family(CHARACTER, TIMESTAMP)`; EXPR_TIMESTAMP UDT reports as VARCHAR/CHARACTER not TIMESTAMP, so EARLIEST/LATEST validator rejected the call. Switched to `wrapUDT(List.of(STRING_T, TIMESTAMP_UDT))`. |
| 21 | CalciteMathematicalFunctionIT | ⏳ | |
| 22 | CalciteOperatorIT | ⏳ | |

## Phase 4 — Aggregate hints (expected REGRESSION: reapplyAggregateHints removed)

Pushdown shape may change for non-nullable group keys. Some assertions may flip from `terms` to `composite_buckets`.

| # | Class | Pushdown ON | Notes |
|---|---|---|---|
| 23 | CalcitePPLAggregationIT | ⏳ | |
| 24 | CalcitePPLAggregationPaginatingIT | ⏳ | |
| 25 | CalcitePPLNestedAggregationIT | ⏳ | |
| 26 | CalciteStatsCommandIT | ⏳ | |
| 27 | CalciteTimechartCommandIT | ⏳ | |
| 28 | CalciteTimechartPerFunctionIT | ⏳ | |

## Phase 5 — Joins / Subqueries (was bypassed: containsJoinOrCorrelate)

Most likely failure mode: `RelToSqlConverter` emits JOIN syntax that the Babel parser rejects ("Join expression encountered in illegal context").

| # | Class | Pushdown ON | Notes |
|---|---|---|---|
| 29 | CalcitePPLJoinIT | ⏳ | |
| 30 | CalcitePPLLookupIT | ⏳ | |
| 31 | CalcitePPLInSubqueryIT | ⏳ | |
| 32 | CalcitePPLExistsSubqueryIT | ⏳ | |
| 33 | CalcitePPLScalarSubqueryIT | ⏳ | |

## Phase 6 — Spath / Bin / Eventstats / Trendline (was bypassed for various RelToSql limits)

| # | Class | Pushdown ON | Notes |
|---|---|---|---|
| 34 | CalcitePPLSpathCommandIT | ⏳ | |
| 35 | CalciteBinCommandIT | ⏳ | |
| 36 | CalciteBinChartNullIT | ⏳ | |
| 37 | CalcitePPLEventstatsIT | ⏳ | |
| 38 | CalcitePPLTrendlineIT | ⏳ | |
| 39 | CalciteTrendlineCommandIT | ⏳ | |

## Phase 7 — Relevance / Highlight (was bypassed: containsMapValueConstructor / containsHighlightAugmentedScan)

| # | Class | Pushdown ON | Notes |
|---|---|---|---|
| 40 | CalciteMatchIT | ⏳ | |
| 41 | CalciteMatchPhraseIT | ⏳ | |
| 42 | CalciteMatchPhrasePrefixIT | ⏳ | |
| 43 | CalciteMatchBoolPrefixIT | ⏳ | |
| 44 | CalciteMultiMatchIT | ⏳ | |
| 45 | CalciteQueryStringIT | ⏳ | |
| 46 | CalciteSimpleQueryStringIT | ⏳ | |
| 47 | CalciteRelevanceFunctionIT | ⏳ | |
| 48 | CalciteHighlightIT | ⏳ | |

## Phase 8 — Append / Multisearch / GraphLookup / Union (was bypassed: containsUnion / containsGraphLookup)

| # | Class | Pushdown ON | Notes |
|---|---|---|---|
| 49 | CalcitePPLAppendCommandIT | ⏳ | |
| 50 | CalcitePPLAppendcolIT | ⏳ | |
| 51 | CalcitePPLAppendPipeCommandIT | ⏳ | |
| 52 | CalciteMultisearchCommandIT | ⏳ | |
| 53 | CalciteUnionCommandIT | ⏳ | |
| 54 | CalcitePPLGraphLookupIT | ⏳ | |

## Phase 9 — Lambda collection UDFs (was bypassed: containsLambdaCollectionUdf)

| # | Class | Pushdown ON | Notes |
|---|---|---|---|
| 55 | CalciteArrayFunctionIT | ⏳ | |
| 56 | CalciteMVAppendFunctionIT | ⏳ | |
| 57 | CalciteMvCombineCommandIT | ⏳ | |
| 58 | CalciteMvExpandCommandIT | ⏳ | |
| 59 | CalciteMultiValueStatsIT | ⏳ | |
| 60 | CalciteNoMvCommandIT | ⏳ | |

## Phase 10 — Date/time / parse / rex / dedup / fillnull / sort / eval

| # | Class | Pushdown ON | Notes |
|---|---|---|---|
| 61 | CalciteDateTimeFunctionIT | ⏳ | |
| 62 | CalciteDateTimeComparisonIT | ⏳ | |
| 63 | CalciteDateTimeImplementationIT | ⏳ | |
| 64 | CalciteConvertTZFunctionIT | ⏳ | |
| 65 | CalciteNowLikeFunctionIT | ⏳ | |
| 66 | CalcitePPLDateTimeBuiltinFunctionIT | ⏳ | |
| 67 | CalcitePPLBuiltinDatetimeFunctionInvalidIT | ⏳ | |
| 68 | CalciteParseCommandIT | ⏳ | |
| 69 | CalcitePPLParseIT | ⏳ | |
| 70 | CalciteRexCommandIT | ⏳ | |
| 71 | CalciteDedupCommandIT | ⏳ | |
| 72 | CalcitePPLDedupIT | ⏳ | |
| 73 | CalciteFillNullCommandIT | ⏳ | |
| 74 | CalcitePPLFillnullIT | ⏳ | |
| 75 | CalciteSortCommandIT | ⏳ | |
| 76 | CalcitePPLSortIT | ⏳ | |
| 77 | CalciteEvalCommandIT | ⏳ | |
| 78 | CalcitePPLEvalMaxMinFunctionIT | ⏳ | |
| 79 | CalcitePPLEnhancedCoalesceIT | ⏳ | |

## Phase 11 — JSON / strings / crypto / system / builtin

| # | Class | Pushdown ON | Notes |
|---|---|---|---|
| 80 | CalciteJsonFunctionsIT | ⏳ | |
| 81 | CalcitePPLJsonBuiltinFunctionIT | ⏳ | |
| 82 | CalcitePPLStringBuiltinFunctionIT | ⏳ | |
| 83 | CalciteTextFunctionIT | ⏳ | |
| 84 | CalciteSystemFunctionIT | ⏳ | |
| 85 | CalcitePPLCryptographicFunctionIT | ⏳ | |
| 86 | CalcitePPLBuiltinFunctionIT | ⏳ | |
| 87 | CalcitePPLBuiltinFunctionsNullIT | ⏳ | |

## Phase 12 — Misc commands / formats / data types

| # | Class | Pushdown ON | Notes |
|---|---|---|---|
| 88 | CalciteAddColTotalsCommandIT | ⏳ | |
| 89 | CalciteAddTotalsCommandIT | ⏳ | |
| 90 | CalciteAliasFieldAggregationIT | ⏳ | |
| 91 | CalciteChartCommandIT | ⏳ | |
| 92 | CalciteConvertCommandIT | ⏳ | |
| 93 | CalciteCsvFormatIT | ⏳ | |
| 94 | CalciteDataTypeIT | ⏳ | |
| 95 | CalciteExpandCommandIT | ⏳ | |
| 96 | CalciteExplainIT | ⏳ | |
| 97 | CalcitePPLExplainIT | ⏳ | |
| 98 | CalciteFieldFormatCommandIT | ⏳ | |
| 99 | CalciteFlattenCommandIT | ⏳ | |
| 100 | CalciteFlattenDocValueIT | ⏳ | |
| 101 | CalciteGeoIpFunctionsIT | ⏳ | |
| 102 | CalciteGeoPointFormatsIT | ⏳ | |
| 103 | CalciteLegacyAPICompatibilityIT | ⏳ | |
| 104 | CalciteLikeQueryIT | ⏳ | |
| 105 | CalciteMixedFieldTypeIT | ⏳ | |
| 106 | CalciteNewAddedCommandsIT | ⏳ | |
| 107 | CalciteNotInNullFilterIT | ⏳ | |
| 108 | CalciteNotLikeNullIT | ⏳ | |
| 109 | CalciteObjectFieldOperateIT | ⏳ | |
| 110 | CalcitePPLGrokIT | ⏳ | |
| 111 | CalcitePPLMapPathIT | ⏳ | |
| 112 | CalcitePPLPatternsIT | ⏳ | |
| 113 | CalcitePrometheusDataSourceCommandsIT | ⏳ | |
| 114 | CalciteQueryAnalysisIT | ⏳ | |
| 115 | CalciteRareCommandIT | ⏳ | |
| 116 | CalciteRegexCommandIT | ⏳ | |
| 117 | CalciteReplaceCommandIT | ⏳ | |
| 118 | CalciteReverseCommandIT | ⏳ | |
| 119 | CalciteStreamstatsCommandIT | ⏳ | |
| 120 | CalciteTopCommandIT | ⏳ | |
| 121 | CalciteTransposeCommandIT | ⏳ | |
| 122 | CalciteVisualizationFormatIT | ⏳ | |

## Phase 13 — Benchmarks (long-running, run last)

| # | Class | Pushdown ON | Notes |
|---|---|---|---|
| 123 | CalcitePPLBig5IT | ⏳ | |
| 124 | CalcitePPLClickBenchIT | ⏳ | |
| 125 | CalcitePPLTpchIT | ⏳ | |
| 126 | CalcitePPLTpchPaginatingIT | ⏳ | |

## Phase 14 — No-pushdown re-run (pushdown OFF)

| # | Class | Pushdown OFF | Notes |
|---|---|---|---|
| 127 | CalciteNoPushdownIT | ⏳ | Re-runs the suite registered in `@SuiteClasses` with `plugins.calcite.pushdown.enabled=false`. |

## Phase 15 — Cross-cluster (env-dependent, may not be runnable locally)

| # | Class | Pushdown ON | Notes |
|---|---|---|---|
| 128 | CalciteCrossClusterSearchIT | ⏳ | Likely needs multi-cluster fixture; mark `⏭️` if env can't run it. |

---

## Failure log

Append entries here as failures occur. Format:

```
### <ClassName> — <YYYY-MM-DD>
- First failing method: <method>
- Symptom: <one-line summary of error>
- Likely cause: <bypass removed | dialect | other>
- Disposition: skip / fix / defer
- Reproducer: <gradle command + filtered tail of error>
```

### CalcitePPLBasicIT — 2026-05-27
- Run: `./gradlew :integ-test:integTest -DignorePrometheus=true -Dtests.class="*CalcitePPLBasicIT"`
- Counts: 42 tests, 5 failed.
- Failing methods (each surfaces a distinct symptom):
  - `testMultipleTablesAndFilters_WithIndexPattern` — HTTP 400, "No match found for function signature MAP(<CHARACTER>, <CHARACTER>)" at validation. Visitor emits a `MAP_VALUE_CONSTRUCTOR` somewhere in the index-pattern path; `RelToSql` unparses as bare `MAP(...)`, validator can't resolve. (Was guarded by removed `containsMapValueConstructor`.)
  - `testMultipleTablesAndFilters_SameTable` — same MAP signature failure.
  - `testFilterQueryWithOr2` — same MAP signature failure.
  - `testRegexpFilter` — HTTP 400, "No match found for function signature REGEXP(<CHARACTER>, <CHARACTER>)". `REGEXP` operator name not resolved on round-trip — likely the PPL operator is registered under a different name than what the unparser emits (e.g. `RLIKE` vs `REGEXP`).
  - `testNumericLiteral` — `AssertionError`, schema differs: round-tripped query reports different column metadata (likely `decimalLiteral` widened or renamed). Possibly a side-effect of `wrapFloatLiteralsForRoundTrip` removal or DECIMAL inference shift.
- Likely causes:
  - 3 of 5 failures: removed `containsMapValueConstructor` bypass; index-pattern + OR-filter rewrites generate a `MAP_VALUE_CONSTRUCTOR`.
  - 1 failure: `REGEXP` operator name mapping in `OpenSearchSparkSqlDialect` / `PPLBuiltinOperators` lookup.
  - 1 failure: literal-type drift on round-trip.
- Disposition: STOP. Triage the 3 distinct root causes before proceeding to Phase 1 #2.
- First-cause priority: MAP_VALUE_CONSTRUCTOR — affects 3/5 failing tests here and many later phases.

### CalcitePPLBasicIT — 2026-05-27 (after Layers 1+2+3 fix)
- Re-run after fixes:
  - **Layer 1**: added `SqlLibrary.SPARK` to `buildOperatorTable` (one line in `SqlNodePipeline.java`). Fixes "No match found for function signature MAP(<CHARACTER>, <CHARACTER>)".
  - **Layer 2**: re-added a narrow `wrapMapConstructorOperands` pre-pass in `SqlNodePipeline.relToSql` that wraps each VARCHAR/CHAR operand of `MAP_VALUE_CONSTRUCTOR` with `CAST(... AS VARCHAR)` so re-parse types stay VARCHAR. Fixes the `(CHAR(5), CHAR(57)) MAP` → `(VARCHAR, VARCHAR) MAP` drift.
  - **Layer 3**: replaced the broken `family(MAP×14, optional[1..13]).or(family(MAP×25, optional[1..24]))` operand metadata in `RelevanceQueryFunction.getOperandMetadata` with an OR-chain of strictly-sized `family(MAP×k)` checkers for k = 1..25. Calcite's `FamilyOperandTypeChecker.checkOperandTypes` requires exact arity match; the previous `optional` predicate was only honoured by `getOperandCountRange`, not by the strict check.
- Counts now: 42 tests, 4 failed.
- `testRegexpFilter` is now passing (free win — REGEXP-name resolution apparently came through too).
- Remaining failing methods:
  - `testMultipleTablesAndFilters_WithIndexPattern`, `testMultipleTablesAndFilters_SameTable`, `testFilterQueryWithOr2` — now fail at the **execution** stage with HTTP 500: "all shards failed", inner: `QueryShardException: Failed to compile inline script ... UnsupportedOperationException[Relevance search query functions are only supported when they are pushed down]`. The serialised script JSON shows the `query_string` call's child operator is named **MAP** (Spark library function, FUNCTION syntax) instead of the visitor-built **MAP_VALUE_CONSTRUCTOR** (SPECIAL syntax). The OpenSearch push-down analyzer pattern-matches on the original operator and doesn't recognise the swapped form, so push-down doesn't fire, the call falls through to enumerable codegen, and `RelevanceQueryFunction.implement` throws because relevance UDFs are push-down-only. Call this **Layer 4**: round-trip swaps the inner operator identity from `MAP_VALUE_CONSTRUCTOR` to Spark `MAP`.
  - `testNumericLiteral` — unchanged AssertionError, unrelated to MAP.
- Disposition: continuing with Layer 4 (operator swap).

### CalcitePPLBasicIT — 2026-05-27 (after Layer 4 fix)
- **Layer 4** added: `restoreMapValueConstructor` post-pass in `SqlNodePipeline.revalidate` walks the validated RelNode and replaces every `SqlLibraryOperators.MAP` RexCall with `SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR`. Restores the operator identity the OpenSearch push-down analyzer pattern-matches on.
- Counts: 42 tests, 1 failed.
- All 3 MAP-related tests now pass.
- Only `testNumericLiteral` remains failing — unrelated to MAP. Schema assertion expects `decimalLiteral=double, doubleLiteral=double, floatLiteral=float` but the actual round-tripped types differ. This is the pre-flagged literal-type-drift issue (FLOAT/REAL re-parses as DOUBLE/DECIMAL after `wrapFloatLiteralsForRoundTrip` was removed).

### CalcitePPLBasicIT — 2026-05-27 (after wrapFloatLiteralsForRoundTrip restored)
- Restored `wrapFloatLiteralsForRoundTrip` pre-pass in `SqlNodePipeline.relToSql`: wraps every FLOAT/REAL `RexLiteral` with `makeAbstractCast(literalType, literal)`. The unparser then emits `CAST(6E-2 AS REAL)` instead of bare `6E-2`, preserving the FLOAT type identity through the round trip.
- Root cause confirmed via direct trace test: visitor produces `RexLiteral(0.06, FLOAT)`. Unparser writes bare `6E-2`. Babel parser re-parses it as `SqlNumericLiteral` with `typeName=DOUBLE` (Calcite's parser types every exponent-bearing literal as DOUBLE because SQL textual literal syntax has no FLOAT/REAL marker). Validator then types `MINUS(DOUBLE, DOUBLE)` as DOUBLE. The CAST workaround is the only mechanism that lets a FLOAT RexLiteral survive a round-trip.
- All 42 tests pass.
