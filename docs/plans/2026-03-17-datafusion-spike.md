# DataFusion Integration Spike

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Prove DataFusion can replace Java hash aggregation in DQE, targeting ≤1.1x ClickHouse on all ClickBench queries.

**Architecture:** Lucene handles storage + filtering (unchanged). DocValues bulk-read into Arrow-compatible flat arrays on the Java side. JNI bridge passes buffer pointers to a Rust shared library that uses DataFusion for vectorized GROUP BY + aggregation. Results returned as flat arrays via JNI. Correctness failures (14 queries) addressed as a separate concern (mostly ORDER BY tie-breaking).

**Tech Stack:** Rust + DataFusion 44 + Arrow + jni crate. Java JNI bindings. Gradle build integration for native library.

**Current State:**
- 42 queries benchmarked, 40/42 within 2x CH, OS/CH aggregate ratio = 0.73x (OS faster overall)
- 11 queries exceed 1.1x CH target: Q4(1.5x) Q5(1.4x) Q6(1.1x) Q9(1.9x) Q10(2.9x) Q14(4.3x) Q15(1.3x) Q19(1.4x) Q36(1.2x) Q40(1.2x) Q43(1.1x)
- 14 correctness failures (mostly ORDER BY tie-breaking)
- Rust not installed on this machine

**Spike Questions to Answer:**
1. Build: Can we compile a Rust .so and load it via JNI inside OpenSearch's plugin classloader?
2. Memory: What's the overhead of DocValues → flat array → DataFusion → result extraction?
3. Performance: Does DataFusion aggregation beat the Java hot path on real ClickBench data?
4. Scope: Which query patterns benefit most? Which need custom DataFusion operators?

---

## Build/Reload/Verify Commands

```bash
# Build Rust native library
cd /home/ec2-user/oss/wukong/dqe/native
cargo build --release
cp target/release/libdqe_datafusion.so /home/ec2-user/oss/wukong/dqe/src/main/resources/

# Build Java + bundle plugin
cd /home/ec2-user/oss/wukong
./gradlew :opensearch-sql-plugin:bundlePlugin -x test -x javadoc

# Reload OpenSearch
sudo kill $(cat /opt/opensearch/opensearch.pid 2>/dev/null) 2>/dev/null
sleep 2
sudo /opt/opensearch/bin/opensearch-plugin remove opensearch-sql
sudo /opt/opensearch/bin/opensearch-plugin install --batch \
  "file:///home/ec2-user/oss/wukong/plugin/build/distributions/opensearch-sql-3.6.0.0-SNAPSHOT.zip"
sudo -u opensearch /opt/opensearch/bin/opensearch -d -p /opt/opensearch/opensearch.pid
for i in $(seq 1 30); do
  curl -s -o /dev/null -w '' "http://localhost:9200/_cluster/health" 2>/dev/null \
    && echo "Ready after ${i}s" && break
  sleep 1
done

# Quick Q9 timing
Q9='SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits_1m GROUP BY RegionID ORDER BY u DESC LIMIT 10'
BODY="{\"query\": $(echo "$Q9" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read().strip()))')}"
for i in 1 2 3 4 5; do curl -s -o /dev/null -H 'Content-Type: application/json' \
  "http://localhost:9200/_plugins/_trino_sql" -d "$BODY"; done
for i in 1 2 3; do
  START=$(date +%s%N)
  curl -s -o /dev/null -H 'Content-Type: application/json' \
    "http://localhost:9200/_plugins/_trino_sql" -d "$BODY"
  END=$(date +%s%N)
  echo "Q9 try $i: $(( (END - START) / 1000000 ))ms"
done

# Full correctness + perf
cd /home/ec2-user/oss/wukong/benchmarks/clickbench
bash run/run_all.sh correctness
bash run/run_all.sh perf-lite
```

---

### Task 1: Install Rust Toolchain

**Step 1: Install rustup + stable toolchain**

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.cargo/env
rustc --version
cargo --version
```

Expected: `rustc 1.XX.0` and `cargo 1.XX.0`

**Step 2: Verify target**

```bash
rustup target list --installed
```

Expected: `x86_64-unknown-linux-gnu` in the list.

---

### Task 2: Create Rust Native Library Project

**Step 1: Create cargo project**

```bash
mkdir -p /home/ec2-user/oss/wukong/dqe/native
cd /home/ec2-user/oss/wukong/dqe/native
cargo init --lib --name dqe_datafusion
```

**Step 2: Set up Cargo.toml**

Replace `Cargo.toml` with:

```toml
[package]
name = "dqe_datafusion"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]  # Shared library for JNI

[dependencies]
datafusion = { version = "44", default-features = false, features = ["default_catalog"] }
arrow = { version = "54", default-features = false, features = ["ffi"] }
tokio = { version = "1", features = ["rt"] }
jni = "0.21"

[profile.release]
opt-level = 3
lto = "thin"
codegen-units = 1
```

**Step 3: Build to verify dependencies resolve**

```bash
cargo build --release 2>&1 | tail -5
```

Expected: `Finished release [optimized] target(s)` (may take 3-5 minutes first time for DataFusion compilation).

**Step 4: Verify .so is produced**

```bash
ls -la target/release/libdqe_datafusion.so
```

Expected: Shared library file exists (~10-30MB).

---

### Task 3: Implement JNI Entry Point — groupByCountDistinct

This is the simplest case: single numeric GROUP BY key + COUNT(DISTINCT numeric) (Q9 pattern).

**Files:**
- Modify: `dqe/native/src/lib.rs`

**Step 1: Implement the Rust JNI function**

Replace `src/lib.rs` with:

```rust
use jni::JNIEnv;
use jni::objects::JClass;
use jni::sys::{jlong, jint, jlongArray};

use std::collections::HashMap;
use std::sync::OnceLock;

static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime")
    })
}

/// JNI entry point: GROUP BY key (long[]) with COUNT(DISTINCT value) (long[]).
/// Returns a flat long array: [key0, count0, key1, count1, ...] for all groups,
/// sorted by count DESC, limited to topN groups (0 = all).
///
/// This uses DataFusion's hash aggregate operator for vectorized execution.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_sql_dqe_native_DataFusionBridge_groupByCountDistinct(
    mut env: JNIEnv,
    _class: JClass,
    keys_ptr: jlong,
    values_ptr: jlong,
    num_rows: jint,
    top_n: jint,
) -> jlongArray {
    let result = group_by_count_distinct_impl(keys_ptr, values_ptr, num_rows, top_n);
    match result {
        Ok(data) => {
            let arr = env.new_long_array(data.len() as i32).unwrap();
            env.set_long_array_region(&arr, 0, &data).unwrap();
            arr.into_raw()
        }
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", format!("DataFusion error: {}", e));
            std::ptr::null_mut()
        }
    }
}

fn group_by_count_distinct_impl(
    keys_ptr: jlong,
    values_ptr: jlong,
    num_rows: jint,
    top_n: jint,
) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let n = num_rows as usize;
    if n == 0 {
        return Ok(vec![]);
    }

    // Read input arrays from JVM memory (zero-copy: just pointer cast)
    let keys: &[i64] = unsafe { std::slice::from_raw_parts(keys_ptr as *const i64, n) };
    let values: &[i64] = unsafe { std::slice::from_raw_parts(values_ptr as *const i64, n) };

    // Build Arrow arrays
    use arrow::array::{Int64Array, ArrayRef};
    use arrow::record_batch::RecordBatch;
    use arrow::datatypes::{Schema, Field, DataType};
    use datafusion::prelude::*;
    use datafusion::functions_aggregate::count::count_distinct;

    let key_array: ArrayRef = std::sync::Arc::new(Int64Array::from(keys.to_vec()));
    let val_array: ArrayRef = std::sync::Arc::new(Int64Array::from(values.to_vec()));

    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(schema, vec![key_array, val_array])?;

    // Execute GROUP BY + COUNT(DISTINCT) via DataFusion
    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch)?;

    let result_df = runtime().block_on(async {
        let grouped = df.aggregate(
            vec![col("key")],
            vec![count_distinct(col("val")).alias("cnt")],
        )?;

        let sorted = grouped.sort(vec![
            col("cnt").sort(false, true),  // DESC NULLS LAST
        ])?;

        let limited = if top_n > 0 {
            sorted.limit(0, Some(top_n as usize))?
        } else {
            sorted
        };

        limited.collect().await
    })?;

    // Extract results into flat [key, count, key, count, ...] array
    let mut output = Vec::new();
    for batch in &result_df {
        let key_col = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let cnt_col = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..batch.num_rows() {
            output.push(key_col.value(i));
            output.push(cnt_col.value(i));
        }
    }

    Ok(output)
}

/// JNI entry point: Mixed aggregation — GROUP BY key (long[]) with multiple agg columns.
/// aggTypes: 0=SUM, 1=COUNT_STAR, 2=AVG, 3=COUNT_DISTINCT
/// Returns flat array: [key, agg0, agg1, ..., key, agg0, agg1, ...] for top_n groups.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_sql_dqe_native_DataFusionBridge_groupByMixedAgg(
    mut env: JNIEnv,
    _class: JClass,
    keys_ptr: jlong,
    num_rows: jint,
    agg_ptrs: jlongArray,
    agg_types: jlongArray,  // using long[] for simplicity; values are 0-3
    num_aggs: jint,
    top_n: jint,
    sort_agg_idx: jint,
) -> jlongArray {
    // Read agg buffer pointers and types from Java arrays
    let n_aggs = num_aggs as usize;
    let mut agg_ptr_vec = vec![0i64; n_aggs];
    let mut agg_type_vec = vec![0i64; n_aggs];
    env.get_long_array_region(&unsafe { jni::objects::JLongArray::from_raw(agg_ptrs) }, 0, &mut agg_ptr_vec).unwrap();
    env.get_long_array_region(&unsafe { jni::objects::JLongArray::from_raw(agg_types) }, 0, &mut agg_type_vec).unwrap();

    let result = group_by_mixed_agg_impl(
        keys_ptr, num_rows, &agg_ptr_vec, &agg_type_vec, top_n, sort_agg_idx,
    );
    match result {
        Ok(data) => {
            let arr = env.new_long_array(data.len() as i32).unwrap();
            env.set_long_array_region(&arr, 0, &data).unwrap();
            arr.into_raw()
        }
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", format!("DataFusion error: {}", e));
            std::ptr::null_mut()
        }
    }
}

fn group_by_mixed_agg_impl(
    keys_ptr: jlong,
    num_rows: jint,
    agg_ptrs: &[i64],
    agg_types: &[i64],
    top_n: jint,
    sort_agg_idx: jint,
) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let n = num_rows as usize;
    let n_aggs = agg_ptrs.len();
    if n == 0 {
        return Ok(vec![]);
    }

    use arrow::array::{Int64Array, Float64Array, ArrayRef};
    use arrow::record_batch::RecordBatch;
    use arrow::datatypes::{Schema, Field, DataType};
    use datafusion::prelude::*;
    use datafusion::functions_aggregate::count::count_distinct;
    use datafusion::functions_aggregate::sum::sum;
    use datafusion::functions_aggregate::average::avg;

    // Build schema and arrays
    let keys: &[i64] = unsafe { std::slice::from_raw_parts(keys_ptr as *const i64, n) };
    let key_array: ArrayRef = std::sync::Arc::new(Int64Array::from(keys.to_vec()));

    let mut fields = vec![Field::new("key", DataType::Int64, false)];
    let mut columns: Vec<ArrayRef> = vec![key_array];

    for (i, &ptr) in agg_ptrs.iter().enumerate() {
        let col_data: &[i64] = unsafe { std::slice::from_raw_parts(ptr as *const i64, n) };
        let col_name = format!("c{}", i);
        fields.push(Field::new(&col_name, DataType::Int64, false));
        columns.push(std::sync::Arc::new(Int64Array::from(col_data.to_vec())));
    }

    let schema = std::sync::Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns)?;

    // Build aggregate expressions
    let mut agg_exprs = Vec::new();
    for (i, &agg_type) in agg_types.iter().enumerate() {
        let col_ref = col(&format!("c{}", i));
        let expr = match agg_type {
            0 => sum(col_ref).alias(&format!("a{}", i)),           // SUM
            1 => datafusion::functions_aggregate::count::count(lit(1i64)).alias(&format!("a{}", i)), // COUNT(*)
            2 => avg(col_ref).alias(&format!("a{}", i)),           // AVG
            3 => count_distinct(col_ref).alias(&format!("a{}", i)), // COUNT(DISTINCT)
            _ => return Err("Unknown agg type".into()),
        };
        agg_exprs.push(expr);
    }

    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch)?;

    let result_df = runtime().block_on(async {
        let grouped = df.aggregate(vec![col("key")], agg_exprs)?;

        let sort_col = if sort_agg_idx >= 0 {
            format!("a{}", sort_agg_idx)
        } else {
            "key".to_string()
        };
        let sorted = grouped.sort(vec![col(&sort_col).sort(false, true)])?;

        let limited = if top_n > 0 {
            sorted.limit(0, Some(top_n as usize))?
        } else {
            sorted
        };

        limited.collect().await
    })?;

    // Extract results: [key, a0, a1, ..., key, a0, a1, ...]
    // All values as i64 (AVG is converted to millionths to preserve precision)
    let mut output = Vec::new();
    for batch in &result_df {
        let key_col = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let num_rows = batch.num_rows();
        for row in 0..num_rows {
            output.push(key_col.value(row));
            for (i, &agg_type) in agg_types.iter().enumerate() {
                let col = batch.column(1 + i);
                if agg_type == 2 {
                    // AVG returns Float64 — convert to fixed-point (multiply by 1_000_000)
                    let f_col = col.as_any().downcast_ref::<Float64Array>().unwrap();
                    output.push((f_col.value(row) * 1_000_000.0) as i64);
                } else {
                    let i_col = col.as_any().downcast_ref::<Int64Array>().unwrap();
                    output.push(i_col.value(row));
                }
            }
        }
    }

    Ok(output)
}
```

**Step 2: Build**

```bash
cd /home/ec2-user/oss/wukong/dqe/native
cargo build --release 2>&1 | tail -5
```

Expected: BUILD SUCCESSFUL

**Step 3: Commit**

```bash
cd /home/ec2-user/oss/wukong
git add dqe/native/
git commit -m "feat(dqe): add Rust DataFusion native library for vectorized aggregation

Initial spike: JNI bridge for GROUP BY + COUNT(DISTINCT) and mixed
aggregation via DataFusion. Exposes two JNI functions:
- groupByCountDistinct: single key + COUNT(DISTINCT) (Q9 pattern)
- groupByMixedAgg: single key + mixed SUM/COUNT/AVG/COUNT(DISTINCT) (Q10)"
```

---

### Task 4: Create Java JNI Bridge Class

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/native/DataFusionBridge.java`

**Step 1: Create the Java bridge class**

```java
package org.opensearch.sql.dqe.native;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import sun.misc.Unsafe;

/**
 * JNI bridge to DataFusion (Rust) for vectorized GROUP BY + aggregation.
 * Uses Unsafe to get direct memory addresses of long[] arrays for zero-copy
 * handoff to native code.
 */
public final class DataFusionBridge {

    private static final Unsafe UNSAFE;
    private static final long LONG_ARRAY_OFFSET;
    private static volatile boolean loaded = false;

    static {
        try {
            java.lang.reflect.Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
            LONG_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access Unsafe", e);
        }
    }

    /** Load the native library from the plugin's classpath resources. */
    public static synchronized void ensureLoaded() {
        if (loaded) return;
        try {
            // Try loading from java.library.path first
            System.loadLibrary("dqe_datafusion");
            loaded = true;
        } catch (UnsatisfiedLinkError e1) {
            // Fall back: extract from resources to temp file
            try (InputStream is = DataFusionBridge.class.getResourceAsStream("/libdqe_datafusion.so")) {
                if (is == null) throw new RuntimeException("libdqe_datafusion.so not found in resources");
                Path tmp = Files.createTempFile("libdqe_datafusion", ".so");
                Files.copy(is, tmp, StandardCopyOption.REPLACE_EXISTING);
                System.load(tmp.toAbsolutePath().toString());
                tmp.toFile().deleteOnExit();
                loaded = true;
            } catch (Exception e2) {
                throw new RuntimeException("Failed to load DataFusion native library", e2);
            }
        }
    }

    /** Get the direct memory address of a long[] array's data (past the object header). */
    public static long arrayAddress(long[] arr) {
        return UNSAFE.getLong(arr, 0L) + LONG_ARRAY_OFFSET;  // This is wrong — see below
    }

    /**
     * Pass a long[] to native code by address. The native code reads directly from JVM heap.
     * IMPORTANT: The array must not be GC-moved during the native call. Since we call
     * native synchronously and the array is pinned on the stack, this is safe for short calls.
     * For production, use DirectByteBuffer or Panama MemorySegment.
     *
     * Actually, JNI GetLongArrayElements with isCopy=false gives the direct pointer.
     * But for the spike, we pass the array itself and let JNI handle pinning.
     */

    // Native method: GROUP BY key + COUNT(DISTINCT value)
    // keys and values are direct memory addresses of long[] array data
    // Returns flat long[]: [key0, count0, key1, count1, ...]
    public static native long[] groupByCountDistinct(long keysPtr, long valuesPtr, int numRows, int topN);

    // Native method: GROUP BY key + mixed aggregations
    // aggPtrs[i] = address of the i-th aggregate column's long[] data
    // aggTypes[i] = 0:SUM, 1:COUNT_STAR, 2:AVG, 3:COUNT_DISTINCT
    // Returns flat long[]: [key, a0, a1, ..., key, a0, a1, ...]
    public static native long[] groupByMixedAgg(
        long keysPtr, int numRows, long[] aggPtrs, long[] aggTypes, int numAggs, int topN, int sortAggIdx);

    /**
     * High-level API: GROUP BY + COUNT(DISTINCT) on Java long[] arrays.
     * Handles memory addressing internally.
     */
    public static long[] countDistinct(long[] keys, long[] values, int topN) {
        ensureLoaded();
        // For the spike: pass arrays through JNI. The Rust side uses
        // GetLongArrayElements to access the data.
        // We'll refine the memory model in a follow-up.
        return groupByCountDistinct(
            addressOf(keys), addressOf(values), keys.length, topN);
    }

    /**
     * Get the native memory address of a Java long[] array's data region.
     * Uses Unsafe to compute: object address + array base offset.
     */
    private static long addressOf(long[] arr) {
        // Object address via Unsafe (compressed oops: need to decompress)
        // This is fragile. For production, use JNI GetPrimitiveArrayCritical or Panama.
        // For the spike, we'll pass array refs through JNI and let Rust call
        // GetLongArrayElements.
        return 0; // placeholder — actual implementation passes arrays via JNI
    }
}
```

**NOTE:** The address-passing approach is fragile due to GC. For the spike, we'll modify the Rust side to accept Java arrays directly via JNI's `GetLongArrayElements` (which pins the array). This is the safer approach.

**Step 2: Update the Rust JNI to accept Java long[] arrays instead of raw pointers**

Replace the JNI function signatures in `lib.rs` to accept `jlongArray` instead of `jlong` pointers:

```rust
#[no_mangle]
pub extern "system" fn Java_org_opensearch_sql_dqe_native_DataFusionBridge_groupByCountDistinct(
    mut env: JNIEnv,
    _class: JClass,
    keys_array: jlongArray,
    values_array: jlongArray,
    num_rows: jint,
    top_n: jint,
) -> jlongArray {
    // Copy Java arrays to Rust Vec (JNI pinning + copy)
    let n = num_rows as usize;
    let mut keys = vec![0i64; n];
    let mut values = vec![0i64; n];
    env.get_long_array_region(
        &unsafe { jni::objects::JLongArray::from_raw(keys_array) }, 0, &mut keys).unwrap();
    env.get_long_array_region(
        &unsafe { jni::objects::JLongArray::from_raw(values_array) }, 0, &mut values).unwrap();

    let result = group_by_count_distinct_impl_vec(keys, values, top_n);
    // ... same result handling as before
}
```

This approach copies data from JVM to Rust (not zero-copy), but it's safe and correct. We'll measure the copy overhead to answer the memory question.

**Step 3: Compile Java**

```bash
cd /home/ec2-user/oss/wukong
./gradlew :dqe:compileJava
```

Expected: BUILD SUCCESSFUL

**Step 4: Commit**

```bash
git add dqe/src/main/java/org/opensearch/sql/dqe/native/DataFusionBridge.java
git commit -m "feat(dqe): add Java JNI bridge for DataFusion native aggregation"
```

---

### Task 5: Wire DataFusion into Shard Execution for Q9

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java`

The existing `executeCountDistinctWithHashSets` reads DocValues into an open-addressing hash map. We replace the hash map logic with:
1. Bulk-read DocValues into two `long[]` arrays (keys + values)
2. Call `DataFusionBridge.groupByCountDistinct(keys, values, topN)`
3. Parse the result into the shard response format

**Step 1: Add a DataFusion-accelerated path**

In `executeCountDistinctWithHashSets`, after the DocValues reading loop (which populates `grpKeys` + `grpSets`), add an alternative path that calls DataFusion. Initially, gate it behind a system property for A/B testing:

```java
boolean useDataFusion = Boolean.getBoolean("dqe.datafusion.enabled");
if (useDataFusion) {
    // Bulk-read all docs into flat arrays
    long[] allKeys = new long[totalDocs];
    long[] allValues = new long[totalDocs];
    // ... read from DocValues ...
    long[] result = DataFusionBridge.countDistinct(allKeys, allValues, 0);
    // Parse result into response format
}
```

**Step 2: Build, reload, test**

Enable with: `-Ddqe.datafusion.enabled=true` in OpenSearch JVM options.

Run Q9, compare with/without DataFusion.

**Step 3: Commit**

---

### Task 6: Standalone Benchmark (bypass OpenSearch HTTP)

To isolate DataFusion's raw performance from HTTP/framework overhead, write a standalone Java benchmark that:
1. Reads DocValues from Lucene index directly
2. Calls DataFusion for aggregation
3. Measures just the aggregation time

This answers the core spike question: **how fast is DataFusion on this data?**

**Files:**
- Create: `dqe/src/test/java/org/opensearch/sql/dqe/native/DataFusionBenchmark.java`

**Step 1: Implement benchmark**

```java
public class DataFusionBenchmark {
    public static void main(String[] args) throws Exception {
        // Open Lucene index directly
        Path indexPath = Path.of("/opt/opensearch/data/nodes/0/indices/...");
        // Read RegionID and UserID DocValues into long[] arrays
        // Time the DataFusion call
        // Compare with Java HashMap baseline
    }
}
```

**Step 2: Run and record results**

Capture: DataFusion time vs Java time for Q9, Q10, Q14 patterns.

**Step 3: Commit results**

---

### Task 7: Expand to Q10 Pattern (Mixed Aggregation)

Wire `groupByMixedAgg` for Q10's pattern: GROUP BY RegionID with SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth), COUNT(DISTINCT UserID).

This tests whether DataFusion handles mixed aggs efficiently and whether the JNI overhead is acceptable for more complex queries.

---

### Task 8: Measure and Document Spike Results

**Step 1: Run full perf-lite benchmark with DataFusion enabled**

**Step 2: Document findings**

```markdown
## Spike Results

### Build
- [ ] Rust .so loads inside OpenSearch plugin classloader?
- [ ] Gradle integration for native build?

### Memory
- [ ] DocValues → long[] copy overhead: Xms for N rows
- [ ] JNI array copy overhead: Xms for N elements
- [ ] DataFusion internal allocation: XMB peak

### Performance (Q9)
- Java path: Xms shard + Xms coord = Xms total
- DataFusion path: Xms shard + Xms coord = Xms total
- Speedup: X.Xx

### Performance (Q10)
- Java path: Xms shard + Xms coord = Xms total
- DataFusion path: Xms shard + Xms coord = Xms total
- Speedup: X.Xx

### Scope Assessment
- Queries that benefit: Q9, Q10, Q14, ...
- Queries that don't benefit: ...
- Patterns needing custom operators: ...
```

**Step 3: Commit report**

```bash
git add docs/plans/2026-03-17-datafusion-spike-results.md
git commit -m "docs: DataFusion integration spike results"
```
