use crate::error::DataGeneratorError;
use std::error::Error;
use std::fs::{self, File};
use std::path::Path;

use polars::prelude::*;
use std::io::BufWriter;

pub fn write_dataframe_to_single_parquet(
    df: &mut DataFrame,
    file_path: &Path,
) -> Result<(), DataGeneratorError> {
    let file = File::create(file_path)?;
    let writer = BufWriter::new(file);
    ParquetWriter::new(writer).finish(df)?;
    Ok(())
}

pub fn cleanup_dataset_parquet_files(dataset_dir: &Path) -> Result<(), Box<dyn Error>> {
    if dataset_dir.exists() {
        for entry in fs::read_dir(dataset_dir)? {
            let path = entry?.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                fs::remove_file(path)?;
            }
        }
    }

    Ok(())
}

pub fn write_dataframe_chunk_to_parquet(
    df_chunk: &mut DataFrame,
    dataset_id: &str,
    base_dir: &Path,
    part_number: usize,
) -> Result<(), Box<dyn Error>> {
    let dataset_dir = base_dir.join(format!("dataset={}", dataset_id));

    if !dataset_dir.exists() {
        fs::create_dir_all(&dataset_dir)?;
    }
    let file_path = dataset_dir.join(format!("part-{:05}.parquet", part_number));

    let file = File::create(&file_path)?;
    let writer = BufWriter::new(file);
    ParquetWriter::new(writer).finish(df_chunk)?;
    Ok(())
}

pub fn write_dataframe_to_multi_parquet(
    df: &DataFrame,
    dataset_id: &str,
    base_dir: &Path,
    chunk_size: usize,
) -> Result<DataFrame, DataGeneratorError> {
    // Ensure the base directory and dataset directory exist
    let dataset_dir = base_dir.join(format!("dataset={}", dataset_id));

    // create dataset directory if not exist, else clean up
    if !dataset_dir.exists() {
        fs::create_dir_all(&dataset_dir)?;
    } else {
        cleanup_dataset_parquet_files(&dataset_dir)?;
    }

    let n_rows = df.height();
    let mut part_number = 0;

    for start in (0..n_rows).step_by(chunk_size) {
        let end = std::cmp::min(start + chunk_size, n_rows);
        let chunk = df.slice(start as i64, end - start);

        // Convert chunk to mutable for writing
        let mut chunk_mut = chunk.clone();

        // write the chunk
        write_dataframe_chunk_to_parquet(&mut chunk_mut, dataset_id, base_dir, part_number)?;
        part_number += 1;
    }
    Ok(df.clone())
}
