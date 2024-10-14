use std::path::PathBuf;

pub struct Config {
    pub registers: Vec<String>,
    pub years: (i32, i32),
    pub rows: usize,
    pub threads: usize,
    pub output: Option<PathBuf>,
    pub input: Option<PathBuf>,
}

impl Config {
    pub fn new(cli: &crate::cli::Cli) -> Result<Self, crate::error::DataGeneratorError> {
        let (start_year, end_year) = cli.get_years()?;
        Ok(Self {
            registers: cli.registers.clone(),
            years: (start_year, end_year),
            rows: cli.rows,
            threads: cli.threads,
            output: cli.output.clone(),
            input: cli.input.clone(),
        })
    }
}
