use crate::error::{PipelineError, Result};
use crate::structs::{Record, TemperatureUnit, TransformConfig};
use arrow_array::{Float64Array, RecordBatch, StringArray};
use chrono::{Datelike, NaiveDate};
use log::debug;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;
use std::{collections::HashMap, fs::File, path::Path};

const DATE_FORMAT: &str = "%Y-%m-%d";

/// Processes weather data from a Parquet file with comprehensive statistical analysis.
///
/// This function reads weather data from a Parquet file, applies filtering based on
/// countries and date ranges, performs data cleaning and validation, and calculates
/// comprehensive statistics including percentiles, outlier detection, and temperature
/// unit conversions.
///
/// # Arguments
///
/// * `file_path` - Path to the input Parquet file containing weather data
/// * `target_countries` - Slice of country alpha-2 codes to filter data by (empty slice means all countries)
/// * `start_year` - Inclusive start year for date filtering
/// * `end_year` - Inclusive end year for date filtering  
/// * `config` - Transform configuration containing unit preferences, outlier thresholds, and aggregation settings
///
/// # Returns
///
/// Returns a `Result<Vec<Record>>` containing weather statistics records, one per country-month combination.
/// Each record includes comprehensive statistics: mean, min, max, standard deviation, median, and percentiles.
///
/// # Errors
///
/// Returns `PipelineError` if:
/// - File cannot be opened or read
/// - Parquet file is malformed or missing required columns
/// - Arrow array operations fail
/// ```
pub fn process_data(
    file_path: &Path,
    target_countries: &[String],
    start_year: i32,
    end_year: i32,
    config: &TransformConfig,
) -> Result<Vec<Record>> {
    debug!("Reading Parquet file: {}", file_path.display());
    let mut monthly_data: HashMap<(String, i32, u32), Vec<f64>> = HashMap::new();
    let file = File::open(file_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut total_rows = 0;
    let mut filtered_rows = 0;

    // Extract and collect raw data
    println!("Starting data extraction from Parquet batches");
    for batch_result in reader {
        let batch = batch_result.map_err(PipelineError::Arrow)?;
        let date_col = get_column_str(&batch, "date")?;
        let country_col = get_column_str(&batch, "country_alpha2")?;
        let temp_col = get_column_f64(&batch, "temp_mean_c_approx")?;

        total_rows += batch.num_rows();

        for i in 0..batch.num_rows() {
            let date_str = date_col.value(i);
            let country = country_col.value(i);
            let temp = temp_col.value(i);

            // Parse date
            let date = match NaiveDate::parse_from_str(date_str, DATE_FORMAT) {
                Ok(dt) => dt,
                Err(_) => continue,
            };
            let year = date.year();
            let month = date.month();

            // Apply filters - efficient: check countries only if list is not empty
            let country_match =
                target_countries.is_empty() || target_countries.contains(&country.to_string());
            if country_match && year >= start_year && year <= end_year {
                filtered_rows += 1;
                // Data validation and cleaning
                let cleaned_temp = clean_temp(temp, config);
                if let Some(valid_temp) = cleaned_temp {
                    let converted_temp = convert_temp(valid_temp, &config.unit);
                    let key = if config.aggregate {
                        let countries_key = if target_countries.is_empty() {
                            "ALL".to_string()
                        } else {
                            target_countries.join(",")
                        };
                        (countries_key, year, month)
                    } else {
                        (country.to_string(), year, month)
                    };
                    monthly_data.entry(key).or_default().push(converted_temp);
                }
            }
        }
    }

    println!(
        "Processed {} total rows, {} matched filters",
        total_rows, filtered_rows
    );
    debug!(
        "Found {} unique country-month combinations",
        monthly_data.len()
    );

    if config.threshold.is_some() {
        debug!(
            "Outlier detection enabled with threshold: {:?}",
            config.threshold
        );
    }

    // Transform data with comprehensive statistics (parallelized)
    println!("Starting statistical analysis");

    let entries: Vec<_> = monthly_data.into_iter().collect();
    let mut results: Vec<Record> = entries
        .into_par_iter()
        .filter_map(|((country, year, month), temps)| {
            if temps.is_empty() {
                return None;
            }

            // Apply outlier detection if enabled
            let cleaned_temps = if let Some(threshold) = config.threshold {
                let original_count = temps.len();
                let cleaned = remove_outliers(&temps, threshold);
                let removed = original_count - cleaned.len();
                if removed > 0 {
                    debug!(
                        "Removed {} outliers for {}/{}/{}",
                        removed, country, year, month
                    );
                }
                cleaned
            } else {
                temps
            };

            if cleaned_temps.is_empty() {
                return None;
            }

            Some(analyze_temps(country, year, month, &cleaned_temps))
        })
        .collect();

    if config.threshold.is_some() {
        debug!("Outlier detection completed");
    }

    // Sort by country, then year, then month
    debug!("Sorting {} results", results.len());
    results.sort_by(|a, b| {
        a.country
            .cmp(&b.country)
            .then_with(|| a.year.cmp(&b.year))
            .then_with(|| a.month.cmp(&b.month))
    });

    debug!("Transform processing completed successfully");
    Ok(results)
}

/// Extracts a Float64 column from an Arrow RecordBatch by name.
///
/// This function safely retrieves a column from a RecordBatch and downcasts it to a Float64Array.
/// It's used for accessing numeric temperature data from the Parquet file.
///
/// # Arguments
///
/// * `batch` - Reference to the Arrow RecordBatch containing the data
/// * `name` - Name of the column to extract
///
/// # Returns
///
/// Returns a `Result<&Float64Array>` containing a reference to the typed array.
///
/// # Errors
///
/// Returns `PipelineError::Data` if:
/// - Column with the specified name doesn't exist
/// - Column exists but is not of Float64 type
/// ```
fn get_column_f64<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Float64Array> {
    batch
        .column_by_name(name)
        .ok_or_else(|| PipelineError::Data(format!("Column not found: {}", name)))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| PipelineError::Data(format!("Column {} is not Float64", name)))
}

/// Extracts a String column from an Arrow RecordBatch by name.
///
/// This function safely retrieves a column from a RecordBatch and downcasts it to a StringArray.
/// It's used for accessing text data like country codes and date strings from the Parquet file.
///
/// # Arguments
///
/// * `batch` - Reference to the Arrow RecordBatch containing the data
/// * `name` - Name of the column to extract
///
/// # Returns
///
/// Returns a `Result<&StringArray>` containing a reference to the typed array.
///
/// # Errors
///
/// Returns `PipelineError::Data` if:
/// - Column with the specified name doesn't exist
/// - Column exists but is not of String/Utf8 type
/// ```
fn get_column_str<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .ok_or_else(|| PipelineError::Data(format!("Column not found: {}", name)))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PipelineError::Data(format!("Column {} is not Utf8/String", name)))
}

/// Cleans and validates temperature data by applying quality control checks.
///
/// This function performs data quality validation on temperature readings to filter out
/// invalid, unrealistic, or corrupted values. It checks for reasonable temperature bounds
/// and ensures the value is finite (not NaN or infinite).
///
/// # Arguments
///
/// * `temp` - Raw temperature value in Celsius to validate
/// * `_config` - Transform configuration
///
/// # Returns
///
/// Returns `Some(f64)` if the temperature passes all validation checks, `None` if it should be filtered out.
///
/// # Validation Rules
///
/// - Temperature must be finite (not NaN or infinity)
/// - Temperature must be within reasonable bounds (-100°C to 70°C)
/// - These bounds cover extreme Earth temperatures from Antarctica to Death Valley
/// ```
fn clean_temp(temp: f64, _config: &TransformConfig) -> Option<f64> {
    // Check for reasonable temperature bounds (e.g., -100°C to 70°C)
    if !(-100.0..=70.0).contains(&temp) {
        return None;
    }

    // Check for NaN or infinite values
    if !temp.is_finite() {
        return None;
    }

    Some(temp)
}

/// Converts temperature from Celsius to the specified unit.
///
/// This function performs temperature unit conversion from Celsius (the source unit in the dataset)
/// to the target unit specified in the configuration. All calculations use standard conversion formulas.
///
/// # Arguments
///
/// * `temp_celsius` - Temperature value in Celsius degrees
/// * `unit` - Target temperature unit for conversion
///
/// # Returns
///
/// Returns the converted temperature value as `f64` in the target unit.
///
/// # Conversion Formulas
///
/// - **Celsius**: No conversion (identity)
/// - **Fahrenheit**: °F = (°C × 9/5) + 32
/// - **Kelvin**: K = °C + 273.15
/// ```
fn convert_temp(temp_celsius: f64, unit: &TemperatureUnit) -> f64 {
    match unit {
        TemperatureUnit::Celsius => temp_celsius,
        TemperatureUnit::Fahrenheit => temp_celsius * 9.0 / 5.0 + 32.0,
        TemperatureUnit::Kelvin => temp_celsius + 273.15,
    }
}

/// Removes statistical outliers from temperature data using standard deviation method.
///
/// This function identifies and filters out outliers based on how many standard deviations
/// they are from the mean. Values beyond the threshold are considered outliers and removed.
///
/// # Arguments
///
/// * `data` - Slice of temperature values to filter
/// * `threshold` - Number of standard deviations beyond which values are considered outliers
///
/// # Returns
///
/// Returns a `Vec<f64>` containing only the values that fall within the threshold.
/// If input has fewer than 2 values, returns all values unchanged.
///
/// Common threshold values:
/// - 1.0: Removes ~32% of data (aggressive)
/// - 2.0: Removes ~5% of data (moderate)
/// - 3.0: Removes ~0.3% of data (conservative)
fn remove_outliers(data: &[f64], threshold: f64) -> Vec<f64> {
    if data.len() < 2 {
        return data.to_vec();
    }

    let mean_val = data.iter().sum::<f64>() / data.len() as f64;
    let variance =
        data.iter().map(|x| (x - mean_val).powi(2)).sum::<f64>() / (data.len() - 1) as f64;
    let std_dev = variance.sqrt();

    data.iter()
        .filter(|&&x| (x - mean_val).abs() <= threshold * std_dev)
        .copied()
        .collect()
}

/// Calculates comprehensive statistical analysis of temperature data for a specific location and time.
///
/// This function computes a full suite of descriptive statistics for temperature measurements
///
/// # Arguments
///
/// * `country` - Country identifier (alpha-2 code or aggregated name)
/// * `year` - Year of the measurements
/// * `month` - Month of the measurements (1-12)
/// * `temps` - Slice of temperature values in the target unit
///
/// # Returns
///
/// Returns a `Record` struct containing:
/// - **Basic statistics**: count, mean, min, max, standard deviation
/// - **Percentiles**: 25th, 50th (median), 75th, 90th, 95th
/// - **Metadata**: country, year, month identifiers
///
/// # Statistical Methods
///
/// - **Mean**: Arithmetic average of all values
/// - **Standard Deviation**: Sample standard deviation (N-1 denominator)
/// - **Percentiles**: Linear interpolation method for precise quantile calculation
/// - **Min/Max**: Extreme values in the dataset
fn analyze_temps(country: String, year: i32, month: u32, temps: &[f64]) -> Record {
    let count = temps.len() as u32;
    let avg_temp = if temps.is_empty() {
        0.0
    } else {
        temps.iter().sum::<f64>() / temps.len() as f64
    };
    let min_temp = temps.iter().fold(f64::INFINITY, |a, &b| a.min(b));
    let max_temp = temps.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));

    let std_dev = if temps.len() > 1 {
        let variance =
            temps.iter().map(|x| (x - avg_temp).powi(2)).sum::<f64>() / (temps.len() - 1) as f64;
        variance.sqrt()
    } else {
        0.0
    };

    let median_temp = calculate_median(temps);
    let percentile_25 = calculate_percentile(temps, 25.0);
    let percentile_75 = calculate_percentile(temps, 75.0);
    let percentile_90 = calculate_percentile(temps, 90.0);
    let percentile_95 = calculate_percentile(temps, 95.0);

    Record {
        country,
        year,
        month,
        avg_temp,
        min_temp,
        max_temp,
        std_dev,
        median_temp,
        count,
        percentile_25,
        percentile_75,
        percentile_90,
        percentile_95,
    }
}

/// Calculates the median (50th percentile) from temperature data.
///
/// This function computes the middle value of a dataset when values are arranged in
/// ascending order.
///
/// # Arguments
///
/// * `data` - Slice of temperature values to analyze
///
/// # Returns
///
/// Returns the median value as `f64`. Returns 0.0 for empty datasets.
fn calculate_median(data: &[f64]) -> f64 {
    if data.is_empty() {
        return 0.0;
    }

    let mut sorted_data = data.to_vec();
    sorted_data.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let len = sorted_data.len();
    if len.is_multiple_of(2) {
        (sorted_data[len / 2 - 1] + sorted_data[len / 2]) / 2.0
    } else {
        sorted_data[len / 2]
    }
}

/// Calculates a specific percentile from temperature data using linear interpolation.
///
/// This function computes the value below which a given percentage of observations fall.
///
/// # Arguments
///
/// * `data` - Slice of temperature values to analyze
/// * `percentile` - Desired percentile as a percentage (0.0 to 100.0)
///
/// # Returns
///
/// Returns the calculated percentile value as `f64`. Returns 0.0 for empty datasets.
fn calculate_percentile(data: &[f64], percentile: f64) -> f64 {
    if data.is_empty() {
        return 0.0;
    }

    let mut sorted_data = data.to_vec();
    sorted_data.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let index = (percentile / 100.0) * (sorted_data.len() - 1) as f64;
    let lower = index.floor() as usize;
    let upper = index.ceil() as usize;

    if lower == upper {
        sorted_data[lower]
    } else {
        let weight = index - lower as f64;
        sorted_data[lower] * (1.0 - weight) + sorted_data[upper] * weight
    }
}
