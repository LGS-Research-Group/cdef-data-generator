pub mod mappings;
pub mod pnr;
pub mod recnum;
pub mod utils;

use crate::error::DataGeneratorError;
use crate::registers::*;
use crate::write::write_dataframe_to_single_parquet;
use polars::prelude::*;
use std::path::{Path, PathBuf};

pub use self::utils::*;

pub fn generate_data(
    registers: &[String],
    no_rows: usize,
    years: &[i32],
    output_dir: &Path,
) -> Result<(), DataGeneratorError> {
    for register in registers {
        println!("Generating data for register: {}", register);
        generate_from_json(register, no_rows, years, output_dir)?;
    }
    Ok(())
}

pub fn generate_from_json(
    register: &str,
    no_rows: usize,
    years: &[i32],
    output_dir: &Path,
) -> Result<(), DataGeneratorError> {
    let schema_dir = PathBuf::from("schemas");
    let json_file = schema_dir.join(format!("{}.json", register));

    if !json_file.exists() {
        return Err(DataGeneratorError::Other(format!(
            "Schema file for register '{}' not found at path: {}",
            register,
            json_file.display()
        )));
    }

    let json = load_json(&json_file).map_err(|e| {
        DataGeneratorError::Other(format!(
            "Failed to load JSON for register '{}': {}",
            register, e
        ))
    })?;

    // Create directory for the register
    let register_dir = output_dir.join(register);
    std::fs::create_dir_all(&register_dir).map_err(|e| {
        DataGeneratorError::Other(format!(
            "Failed to create directory for register '{}': {}",
            register, e
        ))
    })?;

    for &year in years {
        let mut columns = Vec::new();

        if let Some(columns_def) = json.get("columns").and_then(|c| c.as_array()) {
            for col_def in columns_def {
                let col_name = col_def
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or_default();

                let series = match register {
                    "akm" => create_akm_series(col_name, no_rows),
                    "idan" => create_idan_series(col_name, no_rows),
                    "ind" => create_ind_series(col_name, no_rows),
                    "uddf" => create_uddf_series(col_name, no_rows),
                    "lpr3_diagnoser" => create_lpr3_diagnoser_series(col_name, no_rows),
                    "lpr3_kontakter" => create_lpr3_kontakter_series(col_name, no_rows, year),
                    "lpr_adm" => create_lpr_adm_series(col_name, no_rows, year),
                    "lpr_bes" => create_lpr_bes_series(col_name, no_rows, year),
                    "lpr_diag" => create_lpr_diag_series(col_name, no_rows, year),
                    _ => create_bef_series(col_name, no_rows, year),
                };
                columns.push(series);
            }
        }

        let df = DataFrame::new(columns)?;

        // Write DataFrame to a Parquet file
        let file_name = if register == "bef" {
            format!("{}12.parquet", year)
        } else {
            format!("{}.parquet", year)
        };
        let file_path = register_dir.join(file_name);
        write_dataframe_to_single_parquet(&mut df.clone(), &file_path)?;
        println!("Generated data for register '{}' year {}", register, year);
    }

    Ok(())
}
