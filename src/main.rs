mod cli;
mod config;
mod error;
mod generate;
mod read;
mod registers;
mod write;

use polars::prelude::DataFrame;
use std::env;
use std::path::Path;
use std::time::Instant;

use config::Config;
use error::DataGeneratorError;
use generate::generate_data;
use read::{read_partitioned_parquet, read_single_parquet_file};
use write::{write_dataframe_to_multi_parquet, write_dataframe_to_single_parquet};

fn main() -> Result<(), DataGeneratorError> {
    let cli = cli::Cli::parse_args()?;
    let config = Config::new(&cli)?;

    env::set_var("RAYON_NUM_THREADS", config.threads.to_string());

    if let Some(input_path) = &config.input {
        process_input(&config, input_path)?;
    } else {
        generate_output(&config)?;
    }

    Ok(())
}

fn process_input(config: &Config, input_path: &Path) -> Result<(), DataGeneratorError> {
    let start_time = Instant::now();

    let df = if input_path.is_dir() {
        read_partitioned_parquet(input_path)?
    } else if input_path.is_file() {
        read_single_parquet_file(input_path)?
    } else {
        return Err(DataGeneratorError::InvalidInput(format!(
            "Input path \"{}\" is neither a file nor a directory",
            input_path.display()
        )));
    };

    let elapsed = start_time.elapsed().as_secs_f64();
    println!("{:?}", df);
    println!("Time taken to read from Parquet: {:.3} seconds", elapsed);

    if let Some(output_path) = &config.output {
        write_output(&df, output_path, config.rows, config.threads)?;
    }

    Ok(())
}

fn generate_output(config: &Config) -> Result<(), DataGeneratorError> {
    let start_time = Instant::now();
    let output_dir = config
        .output
        .as_ref()
        .map(Path::new)
        .unwrap_or_else(|| Path::new("output"));

    let years = (config.years.0..=config.years.1).collect::<Vec<i32>>();
    generate_data(&config.registers, config.rows, &years, output_dir)?;

    let elapsed = start_time.elapsed().as_secs_f64();
    println!(
        "Time taken to generate {} rows for years {:?} for {} registers using {} threads:",
        config.rows,
        years,
        config.registers.len(),
        config.threads
    );
    println!("--- {:.3} seconds ---", elapsed);

    Ok(())
}

fn write_output(
    df: &DataFrame,
    output_path: &Path,
    rows: usize,
    threads: usize,
) -> Result<(), DataGeneratorError> {
    let is_partitioned = output_path.to_str().unwrap_or("").contains('/');

    if is_partitioned {
        let base_path = output_path.with_file_name("");
        if base_path.exists() && base_path.is_file() {
            return Err(DataGeneratorError::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!(
                    "A file with the name '{}' already exists.",
                    base_path.display()
                ),
            )));
        }
    }

    let start_time = Instant::now();

    if is_partitioned {
        println!(
            "Output directory for multi-parquet file data: {}",
            output_path.display()
        );
        let dataset_id = "0";
        let chunk_size = rows / threads;
        write_dataframe_to_multi_parquet(df, dataset_id, output_path, chunk_size)?;
    } else {
        println!(
            "Output file for single-parquet file data: {}",
            output_path.display()
        );
        write_dataframe_to_single_parquet(&mut df.clone(), output_path)?;
    }

    let elapsed = start_time.elapsed().as_secs_f64();
    println!("Time taken to write to Parquet: {:.3} seconds", elapsed);

    Ok(())
}
