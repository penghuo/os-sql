use jni::JNIEnv;
use jni::objects::{JClass, JLongArray};
use jni::sys::{jint, jlongArray};

use std::sync::{Arc, OnceLock};

static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime")
    })
}

/// JNI: GROUP BY key (long[]) with COUNT(DISTINCT value) (long[]).
/// Returns flat long[]: [key0, count0, key1, count1, ...] sorted by count DESC, limited to topN.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_sql_dqe_datafusion_DataFusionBridge_groupByCountDistinct(
    mut env: JNIEnv,
    _class: JClass,
    keys_array: jlongArray,
    values_array: jlongArray,
    num_rows: jint,
    top_n: jint,
) -> jlongArray {
    let n = num_rows as usize;
    let mut keys = vec![0i64; n];
    let mut values = vec![0i64; n];

    let keys_ref = unsafe { &JLongArray::from_raw(keys_array) };
    let vals_ref = unsafe { &JLongArray::from_raw(values_array) };
    env.get_long_array_region(keys_ref, 0, &mut keys).unwrap();
    env.get_long_array_region(vals_ref, 0, &mut values).unwrap();

    let result = group_by_count_distinct_impl(keys, values, top_n);
    match result {
        Ok(data) => {
            let arr = env.new_long_array(data.len() as i32).unwrap();
            env.set_long_array_region(&arr, 0, &data).unwrap();
            arr.into_raw()
        }
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("DataFusion error: {}", e),
            );
            std::ptr::null_mut()
        }
    }
}

fn group_by_count_distinct_impl(
    keys: Vec<i64>,
    values: Vec<i64>,
    top_n: jint,
) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    if keys.is_empty() {
        return Ok(vec![]);
    }

    use datafusion::arrow::array::{ArrayRef, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::functions_aggregate::count::count_distinct;
    use datafusion::prelude::*;

    let key_array: ArrayRef = Arc::new(Int64Array::from(keys));
    let val_array: ArrayRef = Arc::new(Int64Array::from(values));

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(schema, vec![key_array, val_array])?;

    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch)?;

    let result_df = runtime().block_on(async {
        let grouped = df.aggregate(
            vec![col("key")],
            vec![count_distinct(col("val")).alias("cnt")],
        )?;

        let sorted = grouped.sort(vec![col("cnt").sort(false, true)])?;

        let limited = if top_n > 0 {
            sorted.limit(0, Some(top_n as usize))?
        } else {
            sorted
        };

        limited.collect().await
    })?;

    let mut output = Vec::new();
    for batch in &result_df {
        let key_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let cnt_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            output.push(key_col.value(i));
            output.push(cnt_col.value(i));
        }
    }

    Ok(output)
}

/// JNI: Mixed aggregation — GROUP BY key (long[]) with multiple agg columns.
/// aggTypes: 0=SUM, 1=COUNT_STAR, 2=AVG, 3=COUNT_DISTINCT
/// Returns flat long[]: [key, a0, a1, ..., key, a0, a1, ...] for topN groups.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_sql_dqe_datafusion_DataFusionBridge_groupByMixedAgg(
    mut env: JNIEnv,
    _class: JClass,
    keys_array: jlongArray,
    num_rows: jint,
    agg_ptrs_array: jlongArray,
    agg_types_array: jlongArray,
    num_aggs: jint,
    top_n: jint,
    sort_agg_idx: jint,
) -> jlongArray {
    let n = num_rows as usize;
    let n_aggs = num_aggs as usize;

    let mut keys = vec![0i64; n];
    let keys_ref = unsafe { &JLongArray::from_raw(keys_array) };
    env.get_long_array_region(keys_ref, 0, &mut keys).unwrap();

    // Read aggregate column pointers and types
    let mut agg_ptr_vec = vec![0i64; n_aggs];
    let mut agg_type_vec = vec![0i64; n_aggs];
    let ptrs_ref = unsafe { &JLongArray::from_raw(agg_ptrs_array) };
    let types_ref = unsafe { &JLongArray::from_raw(agg_types_array) };
    env.get_long_array_region(ptrs_ref, 0, &mut agg_ptr_vec).unwrap();
    env.get_long_array_region(types_ref, 0, &mut agg_type_vec).unwrap();

    // Read each aggregate column's data from the JVM array pointers
    // Note: agg_ptrs contains raw jlongArray references, not memory addresses
    let mut agg_columns: Vec<Vec<i64>> = Vec::with_capacity(n_aggs);
    for &ptr in &agg_ptr_vec {
        let mut col_data = vec![0i64; n];
        let arr_ref = unsafe { &JLongArray::from_raw(ptr as jlongArray) };
        env.get_long_array_region(arr_ref, 0, &mut col_data).unwrap();
        agg_columns.push(col_data);
    }

    let result = group_by_mixed_agg_impl(keys, agg_columns, &agg_type_vec, top_n, sort_agg_idx);
    match result {
        Ok(data) => {
            let arr = env.new_long_array(data.len() as i32).unwrap();
            env.set_long_array_region(&arr, 0, &data).unwrap();
            arr.into_raw()
        }
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("DataFusion error: {}", e),
            );
            std::ptr::null_mut()
        }
    }
}

fn group_by_mixed_agg_impl(
    keys: Vec<i64>,
    agg_columns: Vec<Vec<i64>>,
    agg_types: &[i64],
    top_n: jint,
    sort_agg_idx: jint,
) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    if keys.is_empty() {
        return Ok(vec![]);
    }

    use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::functions_aggregate::average::avg;
    use datafusion::functions_aggregate::count::count_distinct;
    use datafusion::functions_aggregate::sum::sum;
    use datafusion::prelude::*;

    let key_array: ArrayRef = Arc::new(Int64Array::from(keys));

    let mut fields = vec![Field::new("key", DataType::Int64, false)];
    let mut columns: Vec<ArrayRef> = vec![key_array];

    for (i, col_data) in agg_columns.into_iter().enumerate() {
        let col_name = format!("c{}", i);
        fields.push(Field::new(&col_name, DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(col_data)));
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns)?;

    // Build aggregate expressions
    let mut agg_exprs = Vec::new();
    for (i, &agg_type) in agg_types.iter().enumerate() {
        let col_ref = col(&format!("c{}", i));
        let expr = match agg_type {
            0 => sum(col_ref).alias(&format!("a{}", i)),
            1 => datafusion::functions_aggregate::count::count(lit(1i64))
                .alias(&format!("a{}", i)),
            2 => avg(col_ref).alias(&format!("a{}", i)),
            3 => count_distinct(col_ref).alias(&format!("a{}", i)),
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

    // Extract: [key, a0, a1, ..., key, a0, a1, ...]
    // AVG returns Float64 — convert to fixed-point (×1_000_000)
    let mut output = Vec::new();
    for batch in &result_df {
        let key_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            output.push(key_col.value(row));
            for (i, &agg_type) in agg_types.iter().enumerate() {
                let col = batch.column(1 + i);
                if agg_type == 2 {
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
