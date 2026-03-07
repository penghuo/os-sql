# ClickBench Correctness Baseline - 2026-03-07

## Summary
- PASS: 0/43
- FAIL: 43/43
- SKIP: 0/43

All 43 ClickBench queries fail against the DQE `_plugins/_trino_sql` endpoint. Failures are categorized below.

## Category 1: Unsupported Aggregate Functions (no scalar implementation)

These queries fail because aggregate functions (`COUNT`, `SUM`, `AVG`) lack shard-level scalar implementations in DQE.

| Query | Error |
|-------|-------|
| Q1    | `Function 'count' has no scalar implementation` — `SELECT COUNT(*) FROM hits` |
| Q2    | `Function 'count' has no scalar implementation` — `SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0` |
| Q3    | `Function 'sum' has no scalar implementation` — `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits` |
| Q4    | `Function 'avg' has no scalar implementation` — `SELECT AVG(UserID) FROM hits` |
| Q5    | `Function 'count' has no scalar implementation` — `SELECT COUNT(DISTINCT UserID) FROM hits` |
| Q6    | `Function 'count' has no scalar implementation` — `SELECT COUNT(DISTINCT SearchPhrase) FROM hits` |
| Q21   | `Function 'count' has no scalar implementation` — `SELECT COUNT(*) FROM hits WHERE ...` |
| Q30   | `Function 'sum' has no scalar implementation` — `SELECT SUM(ResolutionWidth), SUM(ResolutionHeight) ...` |

## Category 2: Column Reference / Planning Errors

These fail because the DQE planner cannot resolve column references after GROUP BY, ORDER BY, or with aggregate aliases.

| Query | Error |
|-------|-------|
| Q7    | `No matching overload for min with argument types [timestamp(3)]` |
| Q8    | `Column 'COUNT(*)' not found in columns: [AdvEngineID, count(*)]` — case mismatch in agg column name |
| Q9    | `Column 'u' not found in columns: [RegionID, count([UserID])]` — alias 'u' not resolved |
| Q10   | `Column '[AdvEngineID]' not found in columns: [RegionID]` — column from HAVING not in GROUP BY projection |
| Q11   | `Column 'u' not found in columns: [MobilePhoneModel, count([UserID])]` |
| Q12   | `Column 'u' not found in columns: [MobilePhone, MobilePhoneModel, count([UserID])]` |
| Q13   | `Column 'c' not found in columns: [SearchPhrase, count(*)]` — alias 'c' not resolved |
| Q14   | `Column 'u' not found in columns: [SearchPhrase, count([UserID])]` |
| Q15   | `Column 'c' not found in columns: [SearchEngineID, SearchPhrase, count(*)]` |
| Q16   | `Column 'COUNT(*)' not found in columns: [UserID, count(*)]` |
| Q17   | `Column 'COUNT(*)' not found in columns: [UserID, SearchPhrase, count(*)]` |
| Q19   | `Column 'EXTRACT(MINUTE FROM EventTime)' not found` — EXTRACT in GROUP BY not resolved |
| Q22   | `Column '[URL]' not found in columns: [SearchPhrase, URL]` |
| Q23   | `Column '[URL]' not found in columns: [SearchPhrase, Title, URL]` |
| Q28   | `Column '[length(URL)]' not found` — function call in ORDER BY not resolved |
| Q29   | `Column 'REGEXP_REPLACE(...)' not found` — function in GROUP BY not resolved |
| Q31   | `Column '[IsRefresh]' not found in columns: [ClientIP, SearchEngineID, SearchPhrase]` |
| Q32   | `Column '[IsRefresh]' not found in columns: [ClientIP, SearchPhrase, WatchID]` |
| Q33   | `Column '[IsRefresh]' not found in columns: [ClientIP, WatchID]` |
| Q34   | `Column 'c' not found in columns: [URL, count(*)]` — alias not resolved |
| Q35   | `Column '1' not found in columns: [URL]` — positional GROUP BY not supported |
| Q36   | `Column '(ClientIP - 1)' not found` — arithmetic expression in projection not resolved |

## Category 3: Unsupported Expression Types

These fail because DQE cannot handle `GenericLiteral` expressions (date/timestamp literals).

| Query | Error |
|-------|-------|
| Q37   | `Unsupported expression type: GenericLiteral` — `DATE '2013-07-01'` |
| Q38   | `Unsupported expression type: GenericLiteral` |
| Q39   | `Unsupported expression type: GenericLiteral` |
| Q40   | `Unsupported expression type: GenericLiteral` |
| Q41   | `Unsupported expression type: GenericLiteral` |
| Q42   | `Unsupported expression type: GenericLiteral` |
| Q43   | `Unsupported expression type: GenericLiteral` |

## Category 4: Timeout / Execution Hangs

These queries did not return within 30s. No error logged — may be executing extremely slowly or hanging.

| Query | Description |
|-------|-------------|
| Q18   | `SELECT UserID, SearchPhrase, COUNT(*) ... GROUP BY ... LIMIT 10` — high-cardinality GROUP BY |
| Q20   | `SELECT UserID FROM hits WHERE UserID = ...` — simple filter but scans 100M docs |
| Q24   | `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10` |
| Q25   | `SELECT SearchPhrase ... WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10` |
| Q26   | `SELECT SearchPhrase ... WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10` |
| Q27   | `SELECT SearchPhrase ... ORDER BY EventTime, SearchPhrase LIMIT 10` |

## Root Cause Summary

| Category | Count | Root Cause |
|----------|-------|------------|
| Missing aggregate scalar impls | 8 | `count`, `sum`, `avg` need shard-level scalar implementations |
| Column reference / planning | 22 | Alias resolution, GROUP BY projection, HAVING, positional refs |
| Unsupported expression types | 7 | `GenericLiteral` (DATE/TIMESTAMP literals) not supported |
| Timeout / hang | 6 | Execution too slow or stuck on 100M docs |
| **Total** | **43** | |
