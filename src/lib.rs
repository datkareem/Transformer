pub mod error;
pub mod load;
pub mod structs;
pub mod transform;

// Re-export public API
pub use error::{PipelineError, Result};
pub use load::{write_csv, write_json, write_parquet};
pub use structs::{Record, SimpleLogger, TemperatureUnit, TransformConfig};
pub use transform::process_data;
