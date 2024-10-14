use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "cdef-data-generator")]
#[command(author = "Tobias Kragholm <tkragholm@gmail.com>")]
#[command(version = "1.0")]
#[command(about = "Generates fake data based on the provided schema file", long_about = None)]
pub struct Cli {
    /// Registers to generate data for (can be specified multiple times)
    #[arg(
        long,
        env = "CDEF_REGISTERS",
        required = true,
        num_args = 1..,
    )]
    pub registers: Vec<String>,

    /// Range of years to generate data for (format: START-END)
    #[arg(long, env = "CDEF_YEARS", required = true)]
    pub years: String,

    /// Number of rows to generate per year
    #[arg(short, long, env = "CDEF_NUM_ROWS", default_value_t = 10000)]
    pub rows: usize,

    /// Number of threads to use
    #[arg(short, long, env = "RAYON_NUM_THREADS", default_value_t = 1)]
    pub threads: usize,

    /// Output path to write to
    #[arg(short, long, env = "CDEF_OUTPUT_PATH")]
    pub output: Option<PathBuf>,

    /// Input path to read from
    #[arg(short, long, env = "CDEF_INPUT_PATH")]
    pub input: Option<PathBuf>,
}

impl Cli {
    pub fn parse_args() -> Result<Self, crate::error::DataGeneratorError> {
        Ok(Self::parse())
    }

    pub fn parse_years(&self) -> Result<(i32, i32), crate::error::DataGeneratorError> {
        let parts: Vec<&str> = self.years.split('-').collect();
        if parts.len() != 2 {
            return Err(crate::error::DataGeneratorError::Other(
                "Invalid year range format. Use START-END".to_string(),
            ));
        }
        let start_year = parts[0].parse::<i32>().map_err(|_| {
            crate::error::DataGeneratorError::Other("Invalid start year".to_string())
        })?;
        let end_year = parts[1]
            .parse::<i32>()
            .map_err(|_| crate::error::DataGeneratorError::Other("Invalid end year".to_string()))?;
        if start_year > end_year {
            return Err(crate::error::DataGeneratorError::Other(
                "Start year must be less than or equal to end year".to_string(),
            ));
        }
        Ok((start_year, end_year))
    }
}
