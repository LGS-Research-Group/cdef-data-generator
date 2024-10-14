use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataGeneratorError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON parsing error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Polars error: {0}")]
    Polars(#[from] polars::error::PolarsError),
    #[error("CLI argument error: {0}")]
    Cli(#[from] clap::Error),
    #[error("Other error: {0}")]
    Other(String),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
}

impl From<Box<dyn std::error::Error>> for DataGeneratorError {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        DataGeneratorError::Other(err.to_string())
    }
}
