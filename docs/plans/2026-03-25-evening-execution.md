# Execution Plan — 2026-03-25 Evening Session

**Goal:** Maximize queries within 2x of ClickHouse-Parquet baseline. Currently 24/43.

**Time budget:** ~3-4 hours remaining tonight.

**Dev loop per change:** compile (~5s) → reload-plugin (~3min) → correctness (~2min) → benchmark target queries (~1min each with targeted warmup)

---

## Priority Order (by ROI)

### P1: Category A — COUNT(DISTINCT) Fast-Path (6 queries, ~2h)

**Queries:** Q04(5.7x), Q05(5.8x), Q08(4.1x), Q09(8.0x), Q11(13.1x), Q13(7.8x)

**Root cause:** DQE has optimized COUNT(DISTINCT) paths (LongOpenHashSet, parallel segment scanning) but pattern detection in `TransportShardExecuteAction.java` doesn't match these query shapes.

**Query shapes:**
- Q04: `SELECT COUNT(DISTINCT UserID) FROM hits` — scalar COUNT(DISTINCT) on BIGINT
- Q05: `SELECT COUNT(DISTINCT SearchPhrase) FROM hits` — scalar COUNT(DISTINCT) on VARCHAR
- Q08: `GROUP BY RegionID` + `COUNT(DISTINCT UserID)` — single numeric key + CD
- Q09: `GROUP BY RegionID` + `SUM, COUNT(*), AVG, COUNT(DISTINCT UserID)` — mixed aggs + CD
- Q11: `GROUP BY MobilePhone, MobilePhoneModel` + `COUNT(DISTINCT UserID)` + WHERE — 2 keys + CD + filter
- Q13: `GROUP BY SearchPhrase` + `COUNT(DISTINCT UserID)` + WHERE — varchar key + CD + filter

**Steps:**
1. Read dispatch logic in `TransportShardExecuteAction.java` (~lines 246-360) to understand current fast-path patterns
2. For each query, identify which pattern it should match and why it doesn't
3. Widen detection patterns (likely 2-3 code changes)
4. Compile → reload → correctness → benchmark all 6

**Expected outcome:** 4-6 queries flip to within 2x. Even partial wins (e.g., Q08 from 4.1x, Q04 from 5.7x) are valuable.

---

### P2: Category D — Borderline/Filtered (6 queries, ~1.5h)

**Queries:** Q14(2.1x), Q28(3.2x), Q30(4.5x), Q35(4.0x), Q36(5.0x), Q37(4.5x)

**Quick wins first:**
- **Q14 (2.1x):** Nearly there. Check filter pushdown for `SearchPhrase <> ''` + 2-key GROUP BY.
- **Q28 (3.2x):** REGEXP_REPLACE in GROUP BY. Cache compiled `Pattern` object.
- **Q35 (4.0x):** `GROUP BY ClientIP, ClientIP-1, ClientIP-2, ClientIP-3` — expression fusion.
- **Q36/Q37 (5.0x/4.5x):** VARCHAR GROUP BY on filtered subset. Ordinal pre-aggregation.
- **Q30 (4.5x):** Filtered 2-key GROUP BY.

**Steps:**
1. Profile each query to identify bottleneck
2. Implement targeted fix per query
3. Compile → reload → correctness → benchmark

---

### P3: Category C — Scalar Agg (2 queries, ~30min)

**Queries:** Q02(4.1x), Q29(3.0x)

- Q02: Check if `tryFlatArrayPath` is used. If not, identify why.
- Q29: Already has algebraic shortcut. Gap is DocValues throughput.

---

### P4: Category B — Q15 and Q32 (stretch goal)

- Q15: 29.5x after multi-pass. Needs parallel scanning + capacity tuning.
- Q32: 2.5x, close to threshold. Check merge overhead.

### P5: Category E — Q39 (stretch goal)

- Q39: 27x, 5-key GROUP BY with CASE+VARCHAR. Hardest query.

---

## Tonight's Realistic Target

| Scenario | Queries within 2x | What it takes |
|----------|-------------------|---------------|
| Conservative | 28/43 | P1 partial (4 of 6 CD queries) |
| Moderate | 32/43 | P1 full + P2 partial (Q14, Q28) |
| Optimistic | 36/43 | P1 + P2 + P3 |

## Execution Order

```
1. [P1] Read TransportShardExecuteAction dispatch logic
2. [P1] Identify detection gaps for all 6 CD queries
3. [P1] Implement + compile + reload + correctness + benchmark
4. [P2] Profile Q14 (closest to 2x) → targeted fix → benchmark
5. [P2] Profile Q28 (regex caching) → fix → benchmark
6. [P2] Profile Q35/Q36/Q37 → fix → benchmark
7. [P3] Check Q02/Q29 flat path eligibility → fix → benchmark
8. Run full benchmark to get final score
```
