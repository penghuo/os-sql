use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::functions_aggregate::count::count_distinct;
use datafusion::prelude::*;

fn main() {
    // Production scale: 25M rows per shard, 4.25M distinct users
    let num_rows: usize = 25_000_000;
    let num_regions: i64 = 400;
    let num_users: i64 = 4_250_000;

    println!("=== Scaled DataFusion Benchmark (production sizes) ===");
    println!("Rows: {}, Regions: {}, Users: {}", num_rows, num_regions, num_users);

    let mut keys = Vec::with_capacity(num_rows);
    let mut values = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        keys.push((i as i64) % num_regions);
        values.push((i as i64 * 7 + 13) % num_users);
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let ctx = SessionContext::new();

    // Test 1: Scalar COUNT(DISTINCT) — Q04 pattern (no GROUP BY)
    println!("\n--- Scalar COUNT(DISTINCT) [Q04 pattern] ---");
    bench("DF scalar COUNT(DISTINCT)", 2, 5, || {
        let batch = make_batch_1col(&values);
        let df = ctx.read_batch(batch).unwrap();
        rt.block_on(async {
            df.aggregate(vec![], vec![count_distinct(col("val")).alias("cnt")])
                .unwrap()
                .collect().await.unwrap()
        });
    });

    bench("HashMap scalar COUNT(DISTINCT)", 2, 5, || {
        let mut set = HashSet::with_capacity(4_000_000);
        for &v in &values {
            set.insert(v);
        }
        std::hint::black_box(set.len());
    });

    // Test 2: GROUP BY + COUNT(DISTINCT) — Q08 pattern
    println!("\n--- GROUP BY + COUNT(DISTINCT) top-10 [Q08 pattern] ---");
    bench("DF GROUP BY + COUNT(DISTINCT) top-10", 2, 5, || {
        let batch = make_batch_2col(&keys, &values);
        let df = ctx.read_batch(batch).unwrap();
        rt.block_on(async {
            df.aggregate(vec![col("key")], vec![count_distinct(col("val")).alias("cnt")])
                .unwrap()
                .sort(vec![col("cnt").sort(false, true)])
                .unwrap()
                .limit(0, Some(10))
                .unwrap()
                .collect().await.unwrap()
        });
    });

    bench("HashMap GROUP BY + COUNT(DISTINCT) top-10", 2, 5, || {
        hashmap_count_distinct(&keys, &values, 10);
    });

    // Test 3: Data materialization cost at scale
    println!("\n--- Data materialization overhead ---");
    bench("Arrow array creation (2 cols, 25M rows)", 2, 5, || {
        let _ = make_batch_2col(&keys, &values);
    });

    bench("Vec copy only (2 cols, 25M rows)", 2, 5, || {
        let k = keys.clone();
        let v = values.clone();
        std::hint::black_box((k, v));
    });

    println!("\n=== Done ===");
}

fn make_batch_1col(values: &[i64]) -> RecordBatch {
    let va: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
    let schema = Arc::new(Schema::new(vec![
        Field::new("val", DataType::Int64, false),
    ]));
    RecordBatch::try_new(schema, vec![va]).unwrap()
}

fn make_batch_2col(keys: &[i64], values: &[i64]) -> RecordBatch {
    let ka: ArrayRef = Arc::new(Int64Array::from(keys.to_vec()));
    let va: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));
    RecordBatch::try_new(schema, vec![ka, va]).unwrap()
}

fn bench(name: &str, warmup: usize, iters: usize, mut f: impl FnMut()) {
    for _ in 0..warmup {
        f();
    }
    let mut times = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        f();
        times.push(start.elapsed().as_millis());
    }
    let mut sorted = times.clone();
    sorted.sort();
    let med = sorted[sorted.len() / 2];
    let min = sorted[0];
    println!(
        "{}: median={}ms min={}ms  [{:?}]",
        name, med, min,
        times.iter().map(|t| format!("{}ms", t)).collect::<Vec<_>>().join(", ")
    );
}

fn hashmap_count_distinct(keys: &[i64], values: &[i64], top_n: usize) {
    let mut groups: HashMap<i64, HashSet<i64>> = HashMap::new();
    for i in 0..keys.len() {
        groups.entry(keys[i]).or_default().insert(values[i]);
    }
    let mut result: Vec<(i64, i64)> = groups.iter().map(|(&k, v)| (k, v.len() as i64)).collect();
    result.sort_by(|a, b| b.1.cmp(&a.1));
    result.truncate(top_n);
    std::hint::black_box(result);
}
