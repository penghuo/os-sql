status: WORKING
iteration: 24

## Current State
- Score: 28/43 within 2x (up from 26/43 in iter23)
- Correctness: 39/43 PASS (no regression)
- Machine: r5.4xlarge (16 vCPU, 124GB RAM), 48GB heap, 4 shards, 4 segments/shard
- Q05 flipped: 2.47s → 0.80s (3.58x → 1.16x) — hash-based COUNT(DISTINCT varchar)
- Q19 fixed: was all None, now 0.009s (0.08x) — works in benchmark
- Q14 improved: 1.74s → 1.67s (2.37x → 2.27x) — still above 2x

## Queries Above 2x (15)
Q02(4.18x) Q04(3.94x) Q08(4.58x) Q09(5.89x) Q11(6.82x)
Q13(8.28x) Q14(2.27x) Q15(3.70x) Q16(7.89x) Q18(9.91x)
Q28(2.44x) Q29(2.16x) Q32(3.25x) Q35(3.94x) Q39(14.66x)

## What Was Done
1. Implemented hash-based COUNT(DISTINCT varchar) — FNV-1a hash on raw BytesRef bytes
2. Avoids 6M+ String allocations per shard by hashing ordinal bytes directly
3. Added NOT-EMPTY filter detection for WHERE col <> '' pattern
4. Parallel ordinal hashing with worker threads
5. Coordinator merges LongOpenHashSet instead of Set<String>

## Next Steps
1. Q14 (2.27x) and Q29 (2.16x) are closest to flipping
2. Q28 (2.44x) — REGEXP_REPLACE Pattern caching already implemented, bottleneck is string encoding
3. All remaining above-2x queries hit optimized fused paths — bottleneck is Lucene DocValues overhead
4. Performance target (≥38/43) NOT achievable on r5.4xlarge with code optimizations alone

## Evidence
- Full benchmark: /tmp/full_iter24/r5.4xlarge.json (28/43 within 2x)
- Correctness: 39/43 PASS (/tmp/correctness_iter24.log)
- Build: BUILD SUCCESSFUL
