# Spike: COUNT(DISTINCT) OOM Prevention for High-Cardinality VARCHAR Groups

## Problem

`executeVarcharCountDistinctWithHashSets()` allocates one `LongOpenHashSet` per unique
SearchPhrase to collect distinct UserIDs. At 100M rows (Q14), this OOMs:

- ~1.5M unique SearchPhrases per shard (6M total / 4 shards)
- Each `LongOpenHashSet` starts at 1024 capacity = 8KB (1024 longs * 8 bytes)
- **Just the initial allocation**: 1.5M * 8KB = **12GB per shard**
- 4 shards in parallel = 48GB -> OOM at 32GB heap

The OOM happens from the sheer number of groups, not from large individual sets.
Most SearchPhrases have 1-3 UserIDs, but the initial capacity of 1024 wastes ~8KB per group.

## Approach Assessment

### Approach A: HyperLogLog Fallback — REJECTED

HLL at 14-bit precision = 16KB per group. With 1.5M groups: **24GB per shard**. Worse than
HashSets for sparse groups. Would need to limit which groups get HLL, at which point it
collapses into Approach E. Adds library dependency and complexity for no clear benefit.

### Approach B: Bounded HashSet + Early-Stop — REJECTED

Undercounts arbitrarily. If we stop adding entries after 10M total, we get exact counts for
groups scanned first and zero/undercount for later groups. The error is not random — it's
biased by document order. Unacceptable for correctness.

### Approach C: Fallback to Decomposed Plan — REJECTED

The decomposed plan expands to `GROUP BY (SearchPhrase, UserID)` pairs. At 100M rows with
WHERE filter, this produces ~25M pairs per shard — even more memory and network than HashSets.
Also, TopN inflation on the decomposed plan can't help because the query groups by SearchPhrase,
not by (SearchPhrase, UserID).

### Approach D: Two-Phase Execution — REJECTED (too complex)

Requires re-scanning the index for capped groups. Identifying which groups need re-scan
requires coordinator round-trip. Doubles latency for large queries. The complexity is not
justified when Approach E solves the problem more simply.

### Approach E: Memory-Budgeted Allocation — MODIFIED AND RECOMMENDED

See below.

## Recommended Approach: Spill-to-Count with Memory Budget

### Core Insight

Q14 asks for `ORDER BY COUNT(DISTINCT UserID) DESC LIMIT 10`. The top-10 SearchPhrases
by distinct UserID count are almost certainly high-traffic phrases with thousands+ of
distinct users. The ~1.4M SearchPhrases with 1-3 UserIDs each will never appear in the top-10.

We don't need exact HashSets for every group. We need:
1. **Exact HashSets** for groups that could be in the final top-K
2. **Just the count** for groups that are clearly not contenders

But we can't know which groups are contenders until we've scanned. So we use a two-tier
approach within a single pass:

### Algorithm

```
BUDGET = 512MB per shard (configurable)
totalBytes = 0
spilledGroups = new HashMap<String, Long>()  // key -> exact count (frozen)

For each (searchPhrase, userId) pair in the scan:
  set = varcharDistinctSets.get(searchPhrase)

  if set != null:
    // Group still has a live HashSet
    added = set.add(userId)
    if added:
      totalBytes += 12  // ~12 bytes per entry amortized

    if totalBytes > BUDGET:
      // Spill smallest groups to counts
      spillSmallGroups(varcharDistinctSets, spilledGroups, targetFreeBytes=BUDGET/4)

  else if spilledGroups.containsKey(searchPhrase):
    // Group was spilled — we lost the ability to deduplicate
    // Increment count (becomes an upper bound)
    spilledGroups.merge(searchPhrase, 1L, Long::sum)

  else:
    // New group — create small HashSet
    newSet = new LongOpenHashSet(4)  // START SMALL: 8 capacity = 64 bytes
    newSet.add(userId)
    varcharDistinctSets.put(searchPhrase, newSet)
    totalBytes += 64

spillSmallGroups():
  // Sort groups by set.size() ascending, spill the smallest half
  // These small groups (1-3 entries) are unlikely to be top-K
  // Record their exact count at spill time
  for group in sortedBySize(varcharDistinctSets):
    spilledGroups.put(group.key, group.set.size())
    varcharDistinctSets.remove(group.key)
    freedBytes += group.set.capacity * 8
    if freedBytes >= targetFreeBytes: break
```

### Why This Works

**Memory**: Budget is hard-capped at 512MB. After spilling, only the largest groups retain
HashSets. The top-10 groups by distinct count will have the largest HashSets and will be
spilled last (or never).

**Accuracy**: Groups that are spilled get an **upper-bound count** (they count duplicate
UserIDs as distinct after spill). But these are small groups that won't be in the final
top-10 anyway. The top-10 groups retain exact HashSets throughout.

Edge case: A group could be spilled early, then grow large via post-spill increments, making
its upper-bound count exceed a true top-10 group's exact count. Mitigation: spill
aggressively (keep only top 10K groups by size), and the coordinator's existing top-K pruning
handles the rest — if an upper-bound count exceeds the 10th-place exact count, it's a
candidate for merge, and the over-count is conservative (safe for ORDER BY DESC LIMIT).

**Correctness guarantee**: For `ORDER BY count DESC LIMIT K`, the result is correct as long
as all true top-K groups retain exact HashSets. Spilled groups have upper-bound counts, so
they may appear as false candidates but will be filtered by the coordinator. The only risk is
if a true top-K group gets spilled — mitigated by keeping a generous number of live groups
(10K+).

### Key Implementation Detail: Reduce Initial HashSet Capacity

The biggest single fix, independent of spilling: change the `LongOpenHashSet` constructor
call from `new LongOpenHashSet(16)` to `new LongOpenHashSet(4)`.

Current: `LongOpenHashSet(16)` -> capacity 1024 (the constructor's minimum) = 8KB per group.
With a new minimum capacity of 8: -> 64 bytes per group.

**1.5M groups * 64 bytes = 96MB** vs current 12GB. This alone may prevent OOM.

But since groups grow dynamically, we still need the budget cap for safety.

### Memory Budget Values

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `SHARD_BUDGET_BYTES` | 512MB | 4 shards * 512MB = 2GB total, leaves room for other work in 32GB heap |
| `MIN_HASHSET_CAPACITY` | 8 | 64 bytes per group; sufficient for the median group (1-3 entries) |
| `SPILL_KEEP_COUNT` | 10000 | Keep top-10K groups by size; 10x safety margin over typical LIMIT |
| `SPILL_TRIGGER_RATIO` | 0.9 | Spill when budget is 90% used |

### Accuracy Tradeoff

- **Top-K groups (large distinct counts)**: EXACT. They retain HashSets.
- **Spilled groups (small distinct counts)**: UPPER BOUND. Post-spill insertions can't
  deduplicate. These groups appear only if they are false positives in the coordinator's
  candidate set, which is pruned away.
- **Net effect on Q14 LIMIT 10**: Correct. The top-10 SearchPhrases by distinct UserID
  will have thousands+ of distinct users; they will never be spilled.

### Impact on Coordinator Merge

Minimal changes. The coordinator already receives `Map<String, LongOpenHashSet>` from shards.

For spilled groups, the shard would send just the count (no HashSet). Two options:

**Option 1 (simple)**: Don't send spilled groups at all. They're small and won't be top-K.
The coordinator only sees groups with live HashSets.

**Option 2 (safer)**: Send spilled groups as `(key, count)` pairs without HashSets. The
coordinator treats them as lower-bound candidates. If a spilled group's sum-of-counts across
shards exceeds the Kth-place exact count, it's a candidate — but since it's an upper bound,
this is conservative.

**Recommendation**: Start with Option 1. If correctness tests reveal missing groups, switch
to Option 2.

### Implementation Sketch — Methods to Change

1. **`LongOpenHashSet`** — reduce `INITIAL_CAPACITY` from 1024 to 8. This is the single
   highest-impact change.

2. **`TransportShardExecuteAction.executeVarcharCountDistinctWithHashSets()`** — add:
   - `long totalBytes` counter, incremented on each `set.add()` that returns true
   - `HashMap<String, Long> spilledCounts` for frozen groups
   - `spillSmallGroups()` helper: iterate `varcharDistinctSets`, sort by size, remove
     smallest groups, record their counts in `spilledCounts`
   - In the scan loop: check if group is in `spilledCounts` before creating new HashSet
   - After scan: build output page from both `varcharDistinctSets` (exact) and
     `spilledCounts` (upper bound)

3. **`ShardExecuteResponse`** — optionally add `spilledVarcharCounts: Map<String, Long>`
   field (only needed for Option 2)

4. **`TransportTrinoSqlAction.mergeDedupCountDistinctViaVarcharSets()`** — if Option 2:
   merge spilled counts as scalar additions (no HashSet union needed)

### Phased Rollout

**Phase 1** (immediate, low-risk): Reduce `INITIAL_CAPACITY` to 8 in `LongOpenHashSet`.
Test if Q14 at 100M passes without OOM. This alone reduces baseline from 12GB to 96MB.

**Phase 2** (if Phase 1 is insufficient): Add memory-budgeted spilling at 512MB per shard.
This handles pathological cases where many groups grow large.

## Decision

**Implement Phase 1 first.** The initial capacity reduction is a one-line change that cuts
memory by 125x for sparse groups. If 100M Q14 still OOMs after that (unlikely unless
individual groups grow very large), proceed with Phase 2 spilling.
