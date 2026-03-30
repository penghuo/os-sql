status: WORKING
iteration: 22

## Current State
- Score: 27/43 within 2x (baseline), Q15 improved 106x→3.94x but GC cascade masks gains
- Correctness: 39/43 PASS (no regression)
- Machine: r5.4xlarge (16 vCPU, 124GB RAM), 48GB heap, 4 shards, 4 segments/shard

## Key Change This Iteration
- Sequential scan for high-cardinality MatchAll COUNT(*) GROUP BY (totalDocs > 10M)
- Eliminates 16 concurrent 128MB hash maps that thrash L3 cache
- Q15: 55.5s → 2.05s (96% improvement, 106x → 3.94x)

## Queries Above 2x (16 in clean run)
Q14(2.28x) Q29(2.28x) Q28(2.45x) Q32(3.19x) Q02(4.13x) Q35(3.95x)
Q05(4.26x) Q08(4.61x) Q04(5.08x) Q09(5.54x) Q11(6.42x) Q16(7.19x)
Q13(8.02x) Q18(10.41x) Q39(13.82x) Q15(3.94x — was 106x)

## Next Steps
1. Run another full benchmark to get clean results (Q18 GC cascade affected first run)
2. Focus on borderline queries: Q14(2.28x), Q29(2.28x)
3. Consider further Q15 optimization (3.94x → need 2x)

## Evidence
- Full benchmark: /tmp/iter22_opt1/r5.4xlarge.json (22/43 due to GC cascade)
- Q15 isolated: /tmp/q16_opt/r5.4xlarge.json (1.88s best)
- Correctness: 39/43 PASS (/tmp/correctness_iter22.log)
- Build: BUILD SUCCESSFUL
