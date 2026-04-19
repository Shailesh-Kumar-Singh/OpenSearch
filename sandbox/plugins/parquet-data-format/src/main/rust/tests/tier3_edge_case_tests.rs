/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Targeted test for TIER 3 binary search edge case:
//! When lo+1 == hi and the loop doesn't execute, verify output is still correctly sorted.

use std::fs::File;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use opensearch_parquet_format::merge::merge_sorted;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::tempdir;

fn write_parquet(path: &str, batch: &RecordBatch) {
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

fn read_all_int64(path: &str, col: &str) -> Vec<i64> {
    let file = File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
    let mut vals = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let idx = batch.schema().index_of(col).unwrap();
        let arr = batch.column(idx).as_primitive::<arrow::datatypes::Int64Type>();
        for i in 0..arr.len() {
            vals.push(arr.value(i));
        }
    }
    vals
}

/// File A has 2 rows where the boundary falls between them (lo+1 == hi, loop doesn't run).
/// File B provides the heap_top that sits between A's two values.
/// This exercises the exact edge case in TIER 3.
#[test]
fn test_tier3_two_row_batch_boundary() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));

    // File A: [1, 5] — boundary will fall between these
    // File B: [3]    — heap_top = 3, so row 0 (1) fits, row 1 (5) doesn't
    let batches = vec![
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 5]))]).unwrap(),
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![3]))]).unwrap(),
    ];

    let tmp = tempdir().unwrap();
    let files: Vec<String> = batches.iter().enumerate().map(|(i, b)| {
        let p = tmp.path().join(format!("input_{}.parquet", i));
        let s = p.to_string_lossy().to_string();
        write_parquet(&s, b);
        s
    }).collect();

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(&files, &output, "test", &["v".into()], &[false], &[false]).unwrap();

    let vals = read_all_int64(&output, "v");
    assert_eq!(vals, vec![1, 3, 5], "Output must be globally sorted");
}

/// 3 files designed so that after TIER 3 processes file A and advances,
/// the cursor lands on the second-to-last row, then the next inner loop
/// iteration hits TIER 3 again with run_start == batch_h - 2.
#[test]
fn test_tier3_boundary_at_second_to_last_row() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));

    // File A: [1, 3, 5] — TIER 3 will first emit [1], advance to row 1 (val 3).
    //   3 <= heap_top(4), so loop continues. TIER 2: last=5 > 4. TIER 3 again.
    //   Now run_start=1, batch_h=3, hi=2. lo+1 < hi → 2 < 2 → false.
    //   run_end=1. Emits row 1 (val 3). Advances to row 2 (val 5). 5 > 4 → push back.
    // File B: [2, 4]
    // File C: [6]
    let batches = vec![
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 3, 5]))]).unwrap(),
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![2, 4]))]).unwrap(),
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![6]))]).unwrap(),
    ];

    let tmp = tempdir().unwrap();
    let files: Vec<String> = batches.iter().enumerate().map(|(i, b)| {
        let p = tmp.path().join(format!("input_{}.parquet", i));
        let s = p.to_string_lossy().to_string();
        write_parquet(&s, b);
        s
    }).collect();

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(&files, &output, "test", &["v".into()], &[false], &[false]).unwrap();

    let vals = read_all_int64(&output, "v");
    assert_eq!(vals, vec![1, 2, 3, 4, 5, 6], "Output must be globally sorted");
}

/// Stress test: many interleaved values across 3 files to exercise
/// TIER 2/3 transitions repeatedly.
#[test]
fn test_tier3_many_interleaved() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));

    // File A: odd numbers [1,3,5,7,9,11,13,15,17,19]
    // File B: even numbers [2,4,6,8,10,12,14,16,18,20]
    // File C: [0, 21]
    let a: Vec<i64> = (0..10).map(|i| i * 2 + 1).collect();
    let b: Vec<i64> = (1..=10).map(|i| i * 2).collect();
    let c: Vec<i64> = vec![0, 21];

    let batches = vec![
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(a))]).unwrap(),
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(b))]).unwrap(),
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(c))]).unwrap(),
    ];

    let tmp = tempdir().unwrap();
    let files: Vec<String> = batches.iter().enumerate().map(|(i, b)| {
        let p = tmp.path().join(format!("input_{}.parquet", i));
        let s = p.to_string_lossy().to_string();
        write_parquet(&s, b);
        s
    }).collect();

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(&files, &output, "test", &["v".into()], &[false], &[false]).unwrap();

    let vals = read_all_int64(&output, "v");
    let expected: Vec<i64> = (0..=21).collect();
    assert_eq!(vals, expected, "Output must be globally sorted 0..=21");
}
