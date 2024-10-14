use std::env;
use std::path::{Path, PathBuf};
use std::time::Instant;

use polars::frame::DataFrame;

mod cli;
mod error;
mod generate;
mod read;
mod write;

use cli::Cli;
use error::DataGeneratorError;
use generate::generate_data;
use read::{read_partitioned_parquet, read_single_parquet_file};
use write::{write_dataframe_to_multi_parquet, write_dataframe_to_single_parquet};

fn main() -> Result<(), DataGeneratorError> {
    let cli = Cli::parse_args()?;

    env::set_var("RAYON_NUM_THREADS", cli.threads.to_string());
    let (start_year, end_year) = cli.parse_years()?;
    let years = (start_year..=end_year).collect::<Vec<i32>>();

    if let Some(input_path) = &cli.input {
        let start_time = Instant::now();
        let path = input_path.as_path();

        let df = if path.is_dir() {
            read_partitioned_parquet(path)?
        } else if path.is_file() {
            read_single_parquet_file(path)?
        } else {
            return Err(DataGeneratorError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Input path \"{}\" is neither a file nor a directory",
                    input_path.display()
                ),
            )));
        };

        let elapsed = start_time.elapsed().as_secs_f64();
        println!("{:?}", df);
        println!("Time taken to read from Parquet: {:.3} seconds", elapsed);

        // Handle output writing if needed
        if let Some(output_path) = &cli.output {
            write_output(&df, output_path, cli.rows, cli.threads)?;
        }
    } else {
        let start_time = Instant::now();
        let output_dir = cli
            .output
            .as_ref()
            .map(PathBuf::as_path)
            .unwrap_or_else(|| Path::new("output"));

        generate_data(&cli.registers, cli.rows, &years, output_dir)?;

        let elapsed = start_time.elapsed().as_secs_f64();
        println!(
            "Time taken to generate {} rows for years {:?} for {} registers using {} threads:",
            cli.rows,
            years,
            cli.registers.len(),
            cli.threads
        );
        println!("--- {:.3} seconds ---", elapsed);
    }

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
