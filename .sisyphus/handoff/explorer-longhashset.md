# LongOpenHashSet Implementation Analysis

## File
`dqe/src/main/java/org/opensearch/sql/dqe/operator/LongOpenHashSet.java` (208 lines)

## Architecture
- Open-addressing hash set for **primitive long** values (no boxing)
- Linear probing with Murmur3 64-bit finalizer
- Sentinel-based: `EMPTY = Long.MIN_VALUE` marks empty slots (no separate `boolean[] occupied`)
- Special-case tracking for value `0` (`hasZero`) and value `Long.MIN_VALUE` (`hasSentinel`)

## Constants
```java
INITIAL_CAPACITY = 8
LOAD_FACTOR = 0.65f
EMPTY = Long.MIN_VALUE  // sentinel for empty slots
```

## Fields & Memory Layout
```java
long[] keys;        // capacity * 8 bytes
int size;           // 4 bytes
int capacity;       // 4 bytes (always power-of-2)
int threshold;      // 4 bytes = (int)(capacity * 0.65)
boolean hasZero;    // 1 byte
boolean hasSentinel; // 1 byte
```

## Constructor — Pre-sizing Logic (line 52)
```java
public LongOpenHashSet(int expectedElements) {
    int rawCapacity = Math.max(INITIAL_CAPACITY, (int)(expectedElements / LOAD_FACTOR) + 1);
    this.capacity = Integer.highestOneBit(rawCapacity - 1) << 1;  // round up to power-of-2
    this.keys = new long[capacity];
    java.util.Arrays.fill(this.keys, EMPTY);
    this.threshold = (int)(capacity * LOAD_FACTOR);
}
```
**Key insight for Q04 waste**: `new LongOpenHashSet(25_000_000)` →
- rawCapacity = (int)(25M / 0.65) + 1 = 38,461,539
- capacity = nextPow2(38,461,539) = **67,108,864** (64Mi slots)
- Memory = 64Mi × 8 bytes = **512 MB** per set
- With only 4.25M distinct values → **6.1% fill**, 93.9% wasted

## add() — Hot Path (line 70)
```java
public boolean add(long value) {
    if (value == 0) { /* hasZero path */ }
    if (value == EMPTY) { /* hasSentinel path */ }
    int mask = capacity - 1;
    // Murmur3 64-bit finalizer
    long h = value;
    h ^= h >>> 33;
    h *= 0xff51afd7ed558ccdL;
    h ^= h >>> 33;
    h *= 0xc4ceb9fe1a85ec53L;
    h ^= h >>> 33;
    int slot = (int) h & mask;
    // Linear probing
    while ((k = keys[slot]) != EMPTY) {
        if (k == value) return false;  // duplicate
        slot = (slot + 1) & mask;
    }
    keys[slot] = value;
    size++;
    if (size > threshold) resize();  // doubles capacity
    return true;
}
```

## resize() — Doubles Capacity (line 189)
```java
private void resize() {
    int newCapacity = capacity * 2;
    long[] newKeys = new long[newCapacity];
    Arrays.fill(newKeys, EMPTY);
    // Re-hash all existing keys into new array
    // ... same Murmur3 finalizer + linear probing
    this.capacity = newCapacity;
    this.threshold = (int)(newCapacity * LOAD_FACTOR);
}
```

## ensureCapacity() — Pre-allocate for Merge (line 163)
```java
public void ensureCapacity(int minCapacity) {
    int needed = (int) Math.ceil(minCapacity / LOAD_FACTOR);
    if (needed > capacity) {
        int newCap = Integer.highestOneBit(needed - 1) << 1;
        // Allocate + rehash into new array
    }
}
```

## size() (line 127)
```java
public int size() { return size; }
```

## Memory Footprint Formula
```
Total bytes = capacity × 8  +  ~22 bytes overhead
            = nextPow2(ceil(expectedElements / 0.65)) × 8
```

For N distinct values, optimal pre-size is N (not totalDocs):
- `new LongOpenHashSet(N)` → capacity = nextPow2(N/0.65) → ~1.54N slots → 12.3N bytes

## Pre-sizing Usage in Codebase (the Q04 problem)

| Location | Pre-size | Notes |
|---|---|---|
| `FusedScanAggregate.java:1587` | `Math.min(totalDocs, 32_000_000)` | **THE PROBLEM** — uses totalDocs not distinct estimate |
| `FusedScanAggregate.java:1684` | `Math.min(totalDocs, 32_000_000)` | Same pattern, second code path |
| `TransportShardExecuteAction.java:1171` | `1024` | Conservative, will resize as needed |
| `TransportTrinoSqlAction.java:2000` | `totalValues` | Uses actual total, same problem |
| `FusedGroupByAggregate.java:14293` | `16` | Tiny initial, resizes on demand |
| `HashAggregationOperator.java:734` | default (8) | No pre-sizing |

## Q04 Waste Calculation
- totalDocs = 25M, distinct = 4.25M
- Current: `LongOpenHashSet(25_000_000)` → 64Mi slots → **512 MB**
- Optimal: `LongOpenHashSet(4_250_000)` → 8Mi slots → **64 MB**
- Savings: **448 MB per COUNT(DISTINCT) per shard**
