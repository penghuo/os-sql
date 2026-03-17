use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::functions_aggregate::count::count_distinct;
use datafusion::functions_aggregate::sum::sum;
use datafusion::functions_aggregate::average::avg;
use datafusion::prelude::*;

fn main() {
    let num_rows: usize = 125_000; // per shard (1M / 8)
    let num_regions: i64 = 400;
    let num_users: i64 = 100_000;

    println!("=== DataFusion Aggregation Benchmark ===");
    println!("Rows: {}, Regions: {}, Users: {}", num_rows, num_regions, num_users);

    let mut keys = Vec::with_capacity(num_rows);
    let mut values = Vec::with_capacity(num_rows);
    let mut extra = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        keys.push((i as i64) % num_regions);
        values.push((i as i64 * 7 + 13) % num_users);
        extra.push(i as i64 * 3 + 1); // for SUM/AVG columns
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // === Test 1: DataFusion with fresh SessionContext each time ===
    bench("DF fresh-ctx COUNT(DISTINCT) top-10", 5, 10, || {
        let ctx = SessionContext::new();
        let batch = make_batch_2col(&keys, &values);
        let df = ctx.read_batch(batch).unwrap();
        rt.block_on(async {
            df.aggregate(vec![col("key")], vec![count_distinct(col("val")).alias("cnt")])
                .unwrap()
                .sort(vec![col("cnt").sort(false, true)])
                .unwrap()
                .limit(0, Some(10))
                .unwrap()
                .collect()
                .await
                .unwrap()
        });
    });

    // === Test 2: DataFusion with reused SessionContext ===
    let ctx = SessionContext::new();
    bench("DF reuse-ctx COUNT(DISTINCT) top-10", 5, 10, || {
        let batch = make_batch_2col(&keys, &values);
        let df = ctx.read_batch(batch).unwrap();
        rt.block_on(async {
            df.aggregate(vec![col("key")], vec![count_distinct(col("val")).alias("cnt")])
                .unwrap()
                .sort(vec![col("cnt").sort(false, true)])
                .unwrap()
                .limit(0, Some(10))
                .unwrap()
                .collect()
                .await
                .unwrap()
        });
    });

    // === Test 3: DataFusion COUNT(DISTINCT) all groups ===
    bench("DF reuse-ctx COUNT(DISTINCT) ALL", 5, 10, || {
        let batch = make_batch_2col(&keys, &values);
        let df = ctx.read_batch(batch).unwrap();
        rt.block_on(async {
            df.aggregate(vec![col("key")], vec![count_distinct(col("val")).alias("cnt")])
                .unwrap()
                .collect()
                .await
                .unwrap()
        });
    });

    // === Test 4: Rust HashMap baseline ===
    bench("HashMap COUNT(DISTINCT) top-10", 5, 10, || {
        hashmap_count_distinct(&keys, &values, 10);
    });

    // === Test 5: DataFusion mixed agg (Q10 pattern) ===
    // SUM(extra) + COUNT(*) + AVG(extra) + COUNT(DISTINCT val)
    bench("DF mixed agg (SUM+COUNT+AVG+CD) top-10", 5, 10, || {
        let batch = make_batch_3col(&keys, &values, &extra);
        let df = ctx.read_batch(batch).unwrap();
        rt.block_on(async {
            df.aggregate(
                vec![col("key")],
                vec![
                    sum(col("extra")).alias("s"),
                    datafusion::functions_aggregate::count::count(col("key")).alias("c"),
                    avg(col("extra")).alias("a"),
                    count_distinct(col("val")).alias("cd"),
                ],
            )
            .unwrap()
            .sort(vec![col("cd").sort(false, true)])
            .unwrap()
            .limit(0, Some(10))
            .unwrap()
            .collect()
            .await
            .unwrap()
        });
    });

    // === Test 6: Rust HashMap mixed agg baseline ===
    bench("HashMap mixed agg (SUM+COUNT+AVG+CD) top-10", 5, 10, || {
        hashmap_mixed_agg(&keys, &values, &extra, 10);
    });

    // === Test 7: DataFusion via SQL (measure planning overhead) ===
    bench("DF SQL COUNT(DISTINCT) top-10", 5, 10, || {
        let batch = make_batch_2col(&keys, &values);
        let ctx2 = SessionContext::new();
        rt.block_on(async {
            ctx2.register_batch("t", batch).unwrap();
            ctx2.sql("SELECT key, COUNT(DISTINCT val) as cnt FROM t GROUP BY key ORDER BY cnt DESC LIMIT 10")
                .await.unwrap()
                .collect().await.unwrap()
        });
    });

    // === Test 8: Data materialization overhead ===
    bench("Arrow array creation only (2 cols)", 5, 10, || {
        let _ = make_batch_2col(&keys, &values);
    });

    println!("\n=== Done ===");
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

fn make_batch_3col(keys: &[i64], values: &[i64], extra: &[i64]) -> RecordBatch {
    let ka: ArrayRef = Arc::new(Int64Array::from(keys.to_vec()));
    let va: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
    let ea: ArrayRef = Arc::new(Int64Array::from(extra.to_vec()));
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
        Field::new("extra", DataType::Int64, false),
    ]));
    RecordBatch::try_new(schema, vec![ka, va, ea]).unwrap()
}

fn bench(name: &str, warmup: usize, iters: usize, mut f: impl FnMut()) {
    for _ in 0..warmup {
        f();
    }
    let mut times = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        f();
        times.push(start.elapsed().as_micros());
    }
    let mut sorted = times.clone();
    sorted.sort();
    let med = sorted[sorted.len() / 2];
    let min = sorted[0];
    println!(
        "\n{}: median={:.1}ms min={:.1}ms  [{:?}]",
        name,
        med as f64 / 1000.0,
        min as f64 / 1000.0,
        times.iter().map(|t| format!("{:.1}", *t as f64 / 1000.0)).collect::<Vec<_>>().join(", ")
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

fn hashmap_mixed_agg(keys: &[i64], values: &[i64], extra: &[i64], top_n: usize) {
    struct Acc {
        sum: i64,
        count: i64,
        distinct: HashSet<i64>,
    }
    let mut groups: HashMap<i64, Acc> = HashMap::new();
    for i in 0..keys.len() {
        let acc = groups.entry(keys[i]).or_insert_with(|| Acc {
            sum: 0,
            count: 0,
            distinct: HashSet::new(),
        });
        acc.sum += extra[i];
        acc.count += 1;
        acc.distinct.insert(values[i]);
    }
    let mut result: Vec<(i64, i64, i64, f64, i64)> = groups
        .iter()
        .map(|(&k, a)| (k, a.sum, a.count, a.sum as f64 / a.count as f64, a.distinct.len() as i64))
        .collect();
    result.sort_by(|a, b| b.4.cmp(&a.4));
    result.truncate(top_n);
    std::hint::black_box(result);
}
