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

    /// Start year for data generation
    #[arg(long, env = "CDEF_START_YEAR", required = true)]
    pub start_year: i32,

    /// End year for data generation (optional, if not provided, generates only for start year)
    #[arg(long, env = "CDEF_END_YEAR")]
    pub end_year: Option<i32>,

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

    pub fn get_years(&self) -> Result<(i32, i32), crate::error::DataGeneratorError> {
        let end_year = self.end_year.unwrap_or(self.start_year);
        if self.start_year > end_year {
            return Err(crate::error::DataGeneratorError::Other(
                "Start year must be less than or equal to end year".to_string(),
            ));
        }
        Ok((self.start_year, end_year))
    }
}
