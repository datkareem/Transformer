use arrow_schema::ArrowError;

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("I/O Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Parquet Error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Arrow Error: {0}")]
    Arrow(#[from] ArrowError),
    #[error("Data Error: {0}")]
    Data(String),
    #[error("CSV Error: {0}")]
    Csv(#[from] csv::Error),
    #[error("JSON Error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, PipelineError>;
