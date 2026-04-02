# Ralph Planning Logs

Append-only log of planning rationale and decisions.

## Plan 1 — 2026-04-02

### Research Phase
Spawned 3 parallel subagents:
1. **explorer** → COUNT(DISTINCT) dispatch patterns, method signatures, line numbers
2. **explorer** → ShardExecuteResponse structure, transport mode, BigArrays usage
3. **librarian** → HyperLogLogPlusPlus API, CardinalityAggregator patterns, MurmurHash3 usage

### Key Findings
- Dispatch: L269 numeric (`isBareSingleNumericColumnScan` → `executeDistinctValuesScanWithRawSet`), L277 varchar (`isBareSingleVarcharColumnScan` → `executeDistinctValuesScanVarcharWithRawSet`)
- Coordinator merge: L655 numeric (`mergeCountDistinctValuesViaRawSets`), L659 varchar (`mergeCountDistinctVarcharViaRawSets`)
- ShardExecuteResponse: 4 transient fields for distinct sets, all in-process only
- BigArrays not used in DQE — safe to use NON_RECYCLING_INSTANCE
- HLL API: `collect(bucket, hash)`, `merge(thisBucket, other, otherBucket)`, `cardinality(bucket)`, `close()`
- Hashing: `BitMixer.mix64(long)` for numeric, `MurmurHash3.hash128(bytes)` for varchar
- Existing HLL in codebase: only in Calcite-based `DistinctCountApproxAggFunction`, not in DQE

### Planning Decisions
1. **4 tasks, dependency chain**: T1 (shard numeric) → T2 (coordinator merge) → T3 (varchar) → T4 (verification)
2. **No config flag**: HLL always-on for scalar COUNT(DISTINCT) — matches ES behavior
3. **Transient field**: No serialization needed since all execution is in-process
4. **Ordinal→hash cache for varchar**: Pre-hash all ordinals to avoid redundant MurmurHash3 calls
5. **Keep exact paths as dead code**: No deletion, preserves fallback option

## Plan #1 produced 4 tasks — Thu Apr  2 16:24:19 UTC 2026
