use crate::error::DataGeneratorError;
use std::error::Error;
use std::fs::{self, File};
use std::path::Path;

use polars::prelude::*;

pub fn read_single_parquet_file(file_path: &Path) -> Result<DataFrame, DataGeneratorError> {
    let file = File::open(file_path)?;
    let df = ParquetReader::new(file).finish()?;
    Ok(df)
}

pub fn read_partitioned_parquet(base_dir: &Path) -> Result<DataFrame, DataGeneratorError> {
    let mut dataframes: Vec<DataFrame> = Vec::new();

    fn read_parquet_files(
        path: &Path,
        dataframes: &mut Vec<DataFrame>,
    ) -> Result<(), Box<dyn Error>> {
        if path.is_dir() {
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    // Recursively read nested directories
                    read_parquet_files(&path, dataframes)?;
                } else if path.is_file()
                    && path.extension().and_then(|s| s.to_str()) == Some("parquet")
                {
                    let df = ParquetReader::new(File::open(path)?).finish()?;
                    dataframes.push(df);
                }
            }
        }
        Ok(())
    }

    let base_path = Path::new(base_dir);
    read_parquet_files(base_path, &mut dataframes)?;

    // Iteratively vstack DataFrames
    let mut combined_df = match dataframes.get(0) {
        Some(df) => df.clone(),
        None => return Err(DataGeneratorError::Other("No dataframes found".to_string())),
    };

    for df in dataframes.iter().skip(1) {
        combined_df = combined_df.vstack(df)?;
    }

    Ok(combined_df)
}
