# GC Hint & Inter-Query Cleanup in TransportTrinoSqlAction.java

## File
`dqe/src/main/java/org/opensearch/sql/dqe/coordinator/transport/TransportTrinoSqlAction.java`

## Query Entry Points

| Method | Line | Purpose |
|--------|------|---------|
| `executeDirect()` | L156 | Direct invocation (non-transport) |
| `doExecute()` | L162 | Transport action entry (Task + ActionRequest) |
| `executeInternal()` | L168 | Shared implementation — all queries funnel here |

Both entry points delegate to `executeInternal(queryStr, isExplain, listener)`.

## System.gc() Call Sites — 3 in Coordinator

### 1. PRE-QUERY GC Barrier (L172-178) — Coordinator
```java
// L172-178 in executeInternal()
Runtime rt = Runtime.getRuntime();
long used = rt.totalMemory() - rt.freeMemory();
if (used > rt.maxMemory() * 7 / 10) {   // 70% heap threshold
    System.gc();
    Thread.sleep(50);  // wait for G1GC cycle
}
```
- **Condition**: heap usage > 70% of max
- **Fires**: BEFORE any query work (before plan cache lookup, before merge buffer allocation)
- **Includes 50ms sleep** to let G1GC complete

### 2. POST-QUERY GC Hint — Transport Path (L508-511)
```java
// L506: listener.onResponse(new TrinoSqlResponse(responseJson));
int totalMergedRows = 0;
for (Page p : mergedPages) totalMergedRows += p.getPositionCount();
if (totalMergedRows > 10000) {
    System.gc();  // L511
}
```
- **Condition**: totalMergedRows > 10,000
- **Fires**: AFTER `listener.onResponse()` (response already sent)
- **No sleep** — fire-and-forget hint

### 3. POST-QUERY GC Hint — All-Local Path (L834-837)
```java
// L832: listener.onResponse(new TrinoSqlResponse(responseJson));
int totalMergedRows = 0;
for (Page p : mergedPages) totalMergedRows += p.getPositionCount();
if (totalMergedRows > 10000) {
    System.gc();  // L837
}
```
- Same logic as #2, duplicated for the all-local execution shortcut (L535: `if (allLocal && shardAction != null)`)
- **No sleep** — fire-and-forget hint

## Shard-Level GC (TransportShardExecuteAction.java)

### Pre-Query Barrier (L196-205)
Same pattern as coordinator: 70% heap → `System.gc()` + 50ms sleep.
Comment explicitly mentions Q14→Q15 cascade problem.

### Post-Query Hints (L2850, L2920)
Same `totalRows > 10000` pattern, no sleep.

## Key Problem: Post-Query GC Has No Sleep

The pre-query barriers (coordinator L177, shard L204) include `Thread.sleep(50)` to let G1GC complete.
The post-query hints (coordinator L511/L837, shard L2850/L2920) do **NOT** sleep.

This means:
1. Query N finishes, calls `System.gc()` (no sleep), returns immediately
2. Query N+1 arrives before GC completes
3. Pre-query barrier checks heap — if GC hasn't finished, heap still looks >70%, triggers another `System.gc()` + 50ms sleep
4. But if heap is between say 60-70% (GC partially done), the pre-query barrier **doesn't fire**, and Q(N+1) starts with stale garbage still present

**The 170x vs 2.84x discrepancy**: In full benchmark, rapid sequential queries hit the window where post-query GC hasn't completed and pre-query barrier threshold isn't met. In isolation, there's no prior garbage.

## No Explicit Buffer Release

- `mergedPages` (List<Page>) is not explicitly cleared/nulled after response — relies entirely on GC
- No `close()` or `release()` on merge results
- Query plan cache (`QUERY_PLAN_CACHE`, L105-106) is a ConcurrentHashMap capped at 128 entries (L290-291)

## Memory Monitoring

Only `Runtime.getRuntime()` checks at L174-176. No:
- OpenSearch circuit breaker integration
- MemoryMXBean usage
- Continuous heap monitoring
- Memory pool tracking
