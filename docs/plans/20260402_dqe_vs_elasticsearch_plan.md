# DQE vs Elasticsearch — Status & Next Steps

## Current State (2026-04-01, r5.4xlarge, 100M hits, 4 shards)

**DQE faster than ES: 30/43** (was 29/43 on Mar 26)
**DQE within 2x of CH-Parquet: 31/43** (was 25/43 on Mar 26)

## DQE vs Elasticsearch — Full Breakdown

### DQE Faster Than ES (30 queries) ✅

| Query | DQE | ES | DQE/ES | Pattern |
|-------|-----|-----|--------|---------|
| Q02 | 0.010s | 0.587s | 0.02x | Scalar agg |
| Q03 | 0.009s | 0.272s | 0.03x | Scalar agg |
| Q06 | 0.010s | 0.302s | 0.03x | MIN/MAX |
| Q07 | 0.132s | 0.862s | 0.15x | Filtered scan |
| Q08 | 2.172s | 8.773s | 0.25x | Filtered GROUP BY |
| Q09 | 3.027s | 12.709s | 0.24x | Filtered GROUP BY |
| Q10 | 0.221s | 1.142s | 0.19x | GROUP BY + filter |
| Q12 | 0.356s | 4.215s | 0.08x | GROUP BY + filter |
| Q13 | 0.642s | 4.211s | 0.15x | VARCHAR GROUP BY + COUNT(DISTINCT) |
| Q14 | 1.962s | 4.430s | 0.44x | Single-key GROUP BY |
| Q17 | 0.013s | 8.546s | 0.00x | Cached/trivial |
| Q20 | 1.366s | 14.498s | 0.09x | Multi-key GROUP BY |
| Q21 | 1.375s | 17.073s | 0.08x | Multi-key GROUP BY |
| Q22 | 2.154s | 26.757s | 0.08x | Multi-key GROUP BY |
| Q23 | 1.359s | 14.489s | 0.09x | Multi-key GROUP BY |
| Q24 | 0.014s | 0.776s | 0.02x | Cached/trivial |
| Q25 | 0.392s | 1.622s | 0.24x | GROUP BY + filter |
| Q26 | 0.018s | 0.775s | 0.02x | Cached/trivial |
| Q27 | 1.719s | 85.607s | 0.02x | Multi-key GROUP BY |
| Q29 | 0.011s | 120.908s | 0.00x | Scan optimization |
| Q30 | 0.955s | 1.627s | 0.59x | Filtered 2-key GROUP BY |
| Q31 | 1.143s | 1.679s | 0.68x | Filtered 2-key GROUP BY |
| Q33 | 0.839s | 12.715s | 0.07x | 2-key GROUP BY |
| Q34 | 0.883s | 12.604s | 0.07x | 2-key GROUP BY |
| Q35 | 0.991s | 23.306s | 0.04x | Derived single-key GROUP BY |
| Q36 | 0.297s | 8.196s | 0.04x | Small GROUP BY |
| Q37 | 0.101s | 6.447s | 0.02x | Small GROUP BY |
| Q38 | 0.039s | 8.095s | 0.00x | Small GROUP BY |
| Q39 | 2.039s | 12.760s | 0.16x | VARCHAR GROUP BY + CASE WHEN |
| Q42 | 0.024s | 0.164s | 0.15x | Small filtered |

### DQE Slower Than ES (13 queries) ❌

| Priority | Query | DQE | ES | DQE/ES | Gap to 1x | Pattern | Feasibility |
|----------|-------|-----|-----|--------|-----------|---------|-------------|
| 1 | Q11 | 1.932s | 1.522s | 1.27x | -21% | 2-key GROUP BY + COUNT(DISTINCT) | Medium — similar to Q13 but 2-key path |
| 2 | Q05 | 0.694s | 0.517s | 1.34x | -26% | COUNT(DISTINCT SearchPhrase) | Medium — varchar distinct path |
| 3 | Q15 | 1.089s | 0.805s | 1.35x | -26% | GROUP BY UserID (17M unique) | Hard — cache-limited |
| 4 | Q32 | 8.104s | 5.623s | 1.44x | -31% | 2-key GROUP BY (WatchID, ClientIP) | Medium — profile merge overhead |
| 5 | Q16 | 12.273s | 8.333s | 1.47x | -32% | 2-key GROUP BY (UserID, SearchPhrase) | Hard — high cardinality |
| 6 | Q18 | 31.092s | 19.957s | 1.56x | -37% | 3-key GROUP BY | Hard — structural |
| 7 | Q04 | 1.448s | 0.287s | 5.05x | -80% | COUNT(DISTINCT UserID) | Hard — ES uses HLL |
| — | Q00 | 0.010s | 0.002s | 5.00x | noise | Sub-ms overhead | Skip |
| — | Q01 | 0.011s | 0.004s | 2.75x | noise | Sub-ms overhead | Skip |
| — | Q19 | 0.012s | 0.003s | 4.00x | noise | Sub-ms overhead | Skip |
| — | Q40 | 0.080s | 0.026s | 3.08x | noise | Small filtered | Skip |
| — | Q41 | 0.092s | 0.021s | 4.38x | noise | Small filtered | Skip |
| — | Q28 | 15.734s | N/A | N/A | — | ES timeout | N/A |

### Queries Losing to BOTH CH-Parquet and ES (5 remaining)

Q04, Q11, Q15, Q16, Q18

## What We Shipped Today (Apr 1)

| Commit | Change | Impact |
|--------|--------|--------|
| `54c75cad4` | Revert hash-partitioned COUNT(DISTINCT) | Q04: 539s → 1.4s |
| `fcf677a7b` | High-bit hash partitioning for GROUP BY | Collision clustering fix |
| `6288d60f7` | Q13 shard-level top-N pruning + revert hash-partitioned GROUP BY | Q13: 7.28s → 0.64s, Q15: 20.9s → 1.1s |

Key lesson: hash-partitioned parallelism (each worker scans ALL docs, filters by hash) causes N× work amplification on high-cardinality data. Doc-range splitting (each worker scans totalDocs/N) is always better for scan-heavy workloads.

## Next Steps — Beating ES on Remaining 7 Actionable Queries

### Phase 1: Low-Hanging Fruit (Q11, Q05, Q32)

**Q11 (1.27x ES)** — `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) FROM hits WHERE MobilePhoneModel <> '' GROUP BY ... ORDER BY u DESC LIMIT 10`
- 2-key GROUP BY + COUNT(DISTINCT). Same shard-level top-N pruning pattern as Q13 but needs to be applied to the `executeNKeyCountDistinctWithHashSets` or `executeMixedTypeCountDistinctWithHashSets` path.
- Expected: apply Q13's inline single-value + shard pruning → should cross <1x ES.

**Q05 (1.34x ES)** — `SELECT COUNT(DISTINCT SearchPhrase) FROM hits WHERE SearchPhrase <> ''`
- Scalar COUNT(DISTINCT) on VARCHAR. Currently uses ordinal-based dedup (FixedBitSet).
- Profile to find bottleneck — likely the ordinal map construction or bitset allocation.
- ES uses HyperLogLog (approximate) — DQE returns exact. May need to accept this gap or add optional HLL mode.

**Q32 (1.44x ES)** — `SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY ... ORDER BY c DESC LIMIT 10`
- 2-key GROUP BY with multiple aggregates. Profile to find if it's hash map overhead, merge cost, or scan cost.
- Potential: batch DocValues reads, composite key encoding.

### Phase 2: Harder Targets (Q15, Q16, Q18)

**Q15 (1.35x ES)** — `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10`
- 17M unique UserIDs. Hash table exceeds L3 cache. ES likely uses global ordinals + top-N aggregation.
- Potential: ordinal-based counting for numeric keys (if index is sorted by UserID), or shard-level top-N pruning to avoid shipping all 17M groups.

**Q16 (1.47x ES)** — `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY ... ORDER BY COUNT(*) DESC LIMIT 10`
- 2-key GROUP BY with high cardinality. Similar structural challenge to Q15.
- Potential: composite key encoding, shard-level top-N.

**Q18 (1.56x ES)** — `SELECT extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10`
- 3-key GROUP BY. Largest absolute gap (31s vs 20s ES).
- Potential: shard-level top-N pruning (only ship top-K groups instead of all).

### Phase 3: Accept or Approximate (Q04)

**Q04 (5.05x ES)** — `SELECT COUNT(DISTINCT UserID) FROM hits`
- ES uses HyperLogLog (approximate, ~2% error). DQE returns exact count.
- Options: (a) accept the gap as a correctness trade-off, (b) add optional HLL mode behind a setting, (c) optimize exact path further (currently 1.4s, hard to get to 0.3s without HLL).

### Skip (Sub-ms Noise)

Q00, Q01, Q19, Q40, Q41 — absolute times are <100ms. The DQE/ES ratio is high but the absolute difference is negligible (8-70ms). These are dominated by per-query overhead (parsing, planning, shard dispatch). Not worth optimizing unless we reduce fixed overhead across all queries.
