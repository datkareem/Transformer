use log::{Log, Metadata, Record as LogRecord};
use serde::{Deserialize, Serialize};

/// Simple logger implementation
pub struct SimpleLogger;

impl Log for SimpleLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &LogRecord) {
        println!("[{}] {}", record.level(), record.args());
    }

    fn flush(&self) {}
}

/// Weather data record with comprehensive statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub country: String,
    pub year: i32,
    pub month: u32,
    pub avg_temp: f64,
    pub min_temp: f64,
    pub max_temp: f64,
    pub std_dev: f64,
    pub median_temp: f64,
    pub count: u32,
    pub percentile_25: f64,
    pub percentile_75: f64,
    pub percentile_90: f64,
    pub percentile_95: f64,
}

/// Configuration for data transformation
#[derive(Debug, Clone)]
pub struct TransformConfig {
    pub unit: TemperatureUnit,
    pub threshold: Option<f64>,
    pub aggregate: bool,
}

/// Temperature unit conversion
#[derive(Debug, Clone, clap::ValueEnum)]
pub enum TemperatureUnit {
    Celsius,
    Fahrenheit,
    Kelvin,
}

impl Default for TransformConfig {
    fn default() -> Self {
        Self {
            unit: TemperatureUnit::Celsius,
            threshold: Some(3.0),
            aggregate: false,
        }
    }
}
