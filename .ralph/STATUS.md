status: WORKING
iteration: 19

## Current State
- Score: 27/43 within 2x (up from 26/43)
- Correctness: 39/43 PASS (no regression)
- Machine: r5.4xlarge (16 vCPU, 124GB RAM), 48GB heap, 4 shards, 4 segments/shard

## Queries Within 2x (27)
Q00(0.40x) Q01(0.17x) Q03(1.66x) Q06(0.15x) Q07(1.37x) Q10(0.87x) Q12(0.64x)
Q17(0.01x) Q19(0.08x) Q20(0.01x) Q21(0.02x) Q22(0.04x) Q23(0.00x) Q24(0.03x)
Q25(1.92x) Q26(0.04x) Q27(1.20x) Q30(1.53x) Q31(1.26x) Q33(0.29x) Q34(0.29x)
Q36(1.31x) Q37(0.41x) Q38(0.49x) Q40(1.10x) Q41(1.61x) Q42(0.63x)

## Queries Above 2x (16, sorted by ratio)
Q28(2.24x) Q14(2.25x) Q29(2.51x) Q32(3.14x) Q02(3.26x) Q35(4.05x) Q08(4.44x)
Q04(4.88x) Q05(4.97x) Q09(5.50x) Q16(6.79x) Q11(6.94x) Q13(7.76x) Q18(9.61x)
Q39(26.76x) Q15(167.31x)

## What Was Done This Iteration
1. Replaced pre-estimation bucket calculation with try-catch overflow for single-key and two-key flat paths
2. Added pre-sized constructors to FlatSingleKeyMap and FlatTwoKeyMap (cap at 4M)
3. Pre-sized worker maps in doc-range parallel and segment-parallel paths
4. Q30 improved from 6.20x to 1.53x (now within 2x) — try-catch avoids unnecessary 2-bucket mode
5. Q15 improved from 174x to 1.48s in isolation (pre-sized maps eliminate resize cascades)
6. Q15 still 167x in full benchmark due to GC pressure from Q16/Q18

## Key Findings
- FlatSingleKeyMap resize cascades (4K→8M) cause massive GC pressure with 16 concurrent maps
- Pre-sizing to 4M cap eliminates most resizes but Q15 (4.4M unique keys) still needs one resize
- Try-catch overflow is better than pre-estimation: avoids unnecessary multi-bucket for queries with fewer unique groups than totalDocs
- Q30 (SearchEngineID, ClientIP filtered) benefits most: unique groups << totalDocs per shard

## Next Steps
1. Fix Q15 GC cascade: need to either (a) increase pre-size cap for Q15 specifically, or (b) use System.gc() hint after Q15
2. Bring Q14 (2.25x) and Q29 (2.51x) within 2x — need 11% and 20% improvement respectively
3. Explore further optimizations for COUNT(DISTINCT) queries (Q04, Q05, Q08, Q09, Q11, Q13)

## Evidence
- Full benchmark: /tmp/iter19_full6/r5.4xlarge.json (27/43 within 2x)
- Correctness: 39/43 PASS (/tmp/correctness_iter19e.log)
- Q15 isolated: 1.478s best (/tmp/iter19_q15d/r5.4xlarge.json)
- Build: BUILD SUCCESSFUL
