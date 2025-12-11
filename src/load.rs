use crate::error::Result;
use crate::structs::Record;
use arrow_array::{Float64Array, Int32Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use csv::Writer;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::{fs::File, path::Path, sync::Arc};

/// Writes weather statistics to a CSV file with formatted numeric values.
///
/// # Arguments
/// * `results` - Slice of Record structs containing weather statistics
/// * `output_path` - Path where the CSV file will be created
///
/// # Returns
/// Returns `Ok(())` on success.
///
/// # Errors
/// Returns error if file cannot be created or written to.
pub fn write_csv(results: &[Record], output_path: &Path) -> Result<()> {
    let file = File::create(output_path)?;
    let mut writer = Writer::from_writer(file);

    writer.write_record([
        "Country",
        "Year",
        "Month",
        "Avg_Temp",
        "Min_Temp",
        "Max_Temp",
        "Std_Dev",
        "Median_Temp",
        "Count",
        "Percentile_25",
        "Percentile_75",
        "Percentile_90",
        "Percentile_95",
    ])?;

    for stats in results {
        writer.write_record(&[
            stats.country.to_string(),
            stats.year.to_string(),
            stats.month.to_string(),
            format!("{:.2}", stats.avg_temp),
            format!("{:.2}", stats.min_temp),
            format!("{:.2}", stats.max_temp),
            format!("{:.2}", stats.std_dev),
            format!("{:.2}", stats.median_temp),
            stats.count.to_string(),
            format!("{:.2}", stats.percentile_25),
            format!("{:.2}", stats.percentile_75),
            format!("{:.2}", stats.percentile_90),
            format!("{:.2}", stats.percentile_95),
        ])?;
    }

    writer.flush()?;
    Ok(())
}

/// Writes weather statistics to a pretty-formatted JSON file.
///
/// # Arguments
/// * `results` - Slice of Record structs containing weather statistics
/// * `output_path` - Path where the JSON file will be created
///
/// # Returns
/// Returns `Ok(())` on success.
///
/// # Errors
/// Returns error if file cannot be created or serialization fails.
pub fn write_json(results: &[Record], output_path: &Path) -> Result<()> {
    let file = File::create(output_path)?;
    serde_json::to_writer_pretty(file, results)?;
    Ok(())
}

/// Writes weather statistics to a columnar Parquet file using Arrow format.
///
/// Creates an optimized Parquet file.
///
/// # Arguments
/// * `results` - Slice of Record structs containing weather statistics
/// * `output_path` - Path where the Parquet file will be created
///
/// # Returns
/// Returns `Ok(())` on success.
///
/// # Errors
/// Returns error if file cannot be created, schema is invalid, or Arrow operations fail.
pub fn write_parquet(results: &[Record], output_path: &Path) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("country", DataType::Utf8, false),
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::UInt32, false),
        Field::new("avg_temp", DataType::Float64, false),
        Field::new("min_temp", DataType::Float64, false),
        Field::new("max_temp", DataType::Float64, false),
        Field::new("std_dev", DataType::Float64, false),
        Field::new("median_temp", DataType::Float64, false),
        Field::new("count", DataType::UInt32, false),
        Field::new("percentile_25", DataType::Float64, false),
        Field::new("percentile_75", DataType::Float64, false),
        Field::new("percentile_90", DataType::Float64, false),
        Field::new("percentile_95", DataType::Float64, false),
    ]));

    let countries: StringArray =
        StringArray::from_iter_values(results.iter().map(|r| r.country.as_str()));
    let years: Int32Array = results.iter().map(|r| r.year).collect();
    let months: UInt32Array = results.iter().map(|r| r.month).collect();
    let avg_temps: Float64Array = results.iter().map(|r| r.avg_temp).collect();
    let min_temps: Float64Array = results.iter().map(|r| r.min_temp).collect();
    let max_temps: Float64Array = results.iter().map(|r| r.max_temp).collect();
    let std_devs: Float64Array = results.iter().map(|r| r.std_dev).collect();
    let median_temps: Float64Array = results.iter().map(|r| r.median_temp).collect();
    let counts: UInt32Array = results.iter().map(|r| r.count).collect();
    let percentile_25: Float64Array = results.iter().map(|r| r.percentile_25).collect();
    let percentile_75: Float64Array = results.iter().map(|r| r.percentile_75).collect();
    let percentile_90: Float64Array = results.iter().map(|r| r.percentile_90).collect();
    let percentile_95: Float64Array = results.iter().map(|r| r.percentile_95).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(countries),
            Arc::new(years),
            Arc::new(months),
            Arc::new(avg_temps),
            Arc::new(min_temps),
            Arc::new(max_temps),
            Arc::new(std_devs),
            Arc::new(median_temps),
            Arc::new(counts),
            Arc::new(percentile_25),
            Arc::new(percentile_75),
            Arc::new(percentile_90),
            Arc::new(percentile_95),
        ],
    )?;

    let file = File::create(output_path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
