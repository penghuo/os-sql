# SqlNode POC — Integration Test Status

**Date:** 2026-05-20
**Branch:** `feat/sqlnode`
**Sweep target:** `org.opensearch.sql.calcite.remote.Calcite*IT`
**Sweep duration:** 19m 02s
**Result files:** 210 (105 IT classes × 2 shards each)

## Totals

| Metric | Count | Pct |
|---|---:|---:|
| Total tests | 4617 | — |
| **Pass** | **4161** | **90.1%** |
| Failures | 275 | 5.96% |
| Errors | 0 | 0% |
| Skipped | 181 | 3.92% |

## Per-class failures

Sum across 2 shards each. Excluded from active-fix scope per directive: `CalciteExplainIT`, `CalcitePPLGraphLookupIT`.

| Failures | Class | Category |
|---:|---|---|
| 158 | CalciteExplainIT | cosmetic plan-shape (RexBuilder vs SqlValidator) — out of scope |
| 27 | CalcitePPLGraphLookupIT | graphlookup unimplemented — out of scope |
| 25 | CalcitePPLMapPathIT | dotted-path schema flattening |
| 16 | CalciteMvExpandCommandIT | OpenSearch storage array materialization |
| 10 | CalciteExpandCommandIT | UNNEST + storage layer |
| 9 | CalciteBinCommandIT | schema flattening + auto_date_histogram pushdown |
| 8 | CalcitePPLPatternsIT | BRAIN UDAF + UNNEST + merge-map agg |
| 6 | CalcitePPLSpathCommandIT | auto-extract / duplicate-name validator quirk |
| 6 | CalcitePPLExistsSubqueryIT | testSubsearchMaxOut3 cap leak + flake |
| 6 | CalciteNewAddedCommandsIT | mvexpand basic + graphlookup |
| 2 | CalcitePPLJoinIT | testMultipleJoinsWithoutTableAliases |
| 1 | CalcitePPLCaseFunctionIT | testNestedCaseAggWithAutoDateHistogram |
| 1 | CalciteEvalCommandIT | testEvalOverrideOfFlattenedNestedLeafSurvivesImplicitProject |

**In-scope failures (excluding ExplainIT + GraphLookupIT):** 90 across 11 classes.

## Failing test names (in-scope only)

### CalcitePPLMapPathIT (25)
testAddtotalsOnMapPath, testChartOnMapPath, testDedupOnMapPath, testEvalOnMapPath, testEventstatsOnMapPath, testFieldsExclusionOnMapPath, testFieldsExclusionWildcardOnMapPath, testFillnullOnMapPath, testJoinOnMapPath, testLookupOnMapPath, testMultipleCommandsOnSameMapPath, testMvcombineOnMapPath, testParseOnMapPath, testPatternsOnMapPath, testRareByOnMapPath, testRenameOnMapPath, testReplaceOnMapPath, testRexOnMapPath, testSortOnMapPath, testStatsOnMapPath, testStreamstatsByMapPath, testStreamstatsGlobalWindowByMapPath, testTopOnMapPath, testTrendlineOnMapPath, testWhereOnMapPath

### CalciteMvExpandCommandIT (8 unique × 2 shards)
testMvexpandFlattenedSchemaPresence, testMvexpandHappyMultipleElements, testMvexpandLargeArrayElements, testMvexpandLimitParameter, testMvexpandMixedShapesKeepsAllElements, testMvexpandMultiDocumentLimitParameter, testMvexpandPartialElementMissingName, testMvexpandTypeInferenceForHeterogeneousSubfields

### CalciteExpandCommandIT (5 unique × 2 shards)
testExpandEmptyArray, testExpandOnNested, testExpandOnNullField, testExpandWithAlias, testExpandWithEval

### CalciteBinCommandIT (5 unique, mostly × 2)
testBinsOnTimeFieldWithPushdownDisabled_ShouldFail, testBinWithEvalCreatedDottedFieldName, testBinWithNestedFieldWithoutExplicitProjection, testStatsWithBinsOnTimeAndTermField_Avg, testStatsWithBinsOnTimeAndTermField_Count, testStatsWithBinsOnTimeField_Avg, testStatsWithBinsOnTimeField_Count

### CalcitePPLPatternsIT (4 × 2 shards)
testBrainAggregationMode_ShowNumberedToken, testBrainAggregationModeWithGroupByClause_ShowNumberedToken, testBrainLabelMode_NotShowNumberedToken, testBrainLabelMode_ShowNumberedToken

### CalcitePPLSpathCommandIT (6, single shard)
testSpathAutoExtractWithEval, testSpathAutoExtractWithMultiFieldEval, testSpathAutoExtractWithSeparateEvalCommands, testSpathAutoExtractWithSort, testSpathAutoExtractWithStats, testSpathAutoExtractWithWhere

### CalcitePPLExistsSubqueryIT (3 × 2 shards)
testExistsSubqueryAndAggregation, testSimpleExistsSubqueryInFilter, testSubsearchMaxOut3

### CalciteNewAddedCommandsIT (2-4 unique × 2 shards)
testGraphLookup, testGraphLookupTopLevel, testMvExpandCommandBasicExpansion, testMvExpandCommandLimitBoundary

### CalcitePPLJoinIT (1 × 2 shards)
testMultipleJoinsWithoutTableAliases

### CalcitePPLCaseFunctionIT (1)
testNestedCaseAggWithAutoDateHistogram

### CalciteEvalCommandIT (1)
testEvalOverrideOfFlattenedNestedLeafSurvivesImplicitProject

## Architectural-defer categories

All in-scope failures fall into well-documented categories that require system-level changes beyond surgical post-conversion shuttles:

1. **Schema metadata propagation** (MapPath 25, Bin 4, Eval 1) — would require carrying `_id`/`_index`/`_score` and dotted struct leaves through the SqlNode pipeline like v2's RelBuilder path
2. **OpenSearch storage array materialization** (MvExpand 16, Expand 10, NewAddedCommands 4) — array fields surface as scalars to the SQL layer; UNNEST/LATERAL works but storage doesn't return the array
3. **BRAIN UDAF + UNNEST + merge-map** (Patterns 8) — needs custom merge-map aggregator over `MAP<token, ARRAY<value>>`
4. **Spath auto-extract validator quirk** (Spath 6) — duplicate column name resolution where validator picks first not last
5. **EXISTS subsearch cap threading** (Exists 3+flakes) — testSubsearchMaxOut3 cap insertion not effective on lifted Filter shape; failure leaks `setSubsearchMaxOut(2)` state
6. **graphlookup unimplemented** (NewAddedCommands 2) — out of scope per directive
7. **3-way bare-table joins** (Join 1) — alias propagation gap
8. **auto_date_histogram pushdown** (Case 1)
9. **flattened nested-leaf eval override** (Eval 1)

## Branch state

- Local commits ahead of `origin/feat/sqlnode`: 283 (about to be squashed into 1)
- Total tests fixed across all sessions: ~313 net (vs. session-2 baseline of 588 leftover → 275 leftover)
- Backup ref: `refs/backup/sqlnode-pre-squash-2026-05-20` → `771975f39d`
