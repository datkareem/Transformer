use clap::Parser;
use lib::{
    PipelineError, SimpleLogger, TemperatureUnit, TransformConfig, process_data, write_csv,
    write_json, write_parquet,
};
use log::debug;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

static LOGGER: SimpleLogger = SimpleLogger;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// input Parquet file (project root dir)
    #[arg(short, long)]
    input_file: PathBuf,

    /// Output base name (will create dir containing .csv, .json, and .parquet files)
    #[arg(short, long, default_value = "output")]
    output: String,

    /// Country alpha-2 codes to filter data by (e.g., US,FR,CA). If not specified, processes all countries.
    #[arg(short, long, value_delimiter = ',')]
    countries: Vec<String>,

    /// Start year (inclusive) for filtering (optional)
    #[arg(long)]
    start_year: Option<i32>,

    /// End year (inclusive) for filtering (optional)
    #[arg(long)]
    end_year: Option<i32>,

    /// Temperature unit for output
    #[arg(long, default_value = "celsius")]
    unit: TemperatureUnit,

    /// Outlier detection threshold (standard deviations)
    #[arg(long)]
    threshold: Option<f64>,

    /// Aggregate all countries together instead of keeping them separate
    #[arg(long, default_value_t = false)]
    aggregate: bool,

    /// Log level for output
    #[arg(long, default_value = "false")]
    debug: bool,
}

fn main() -> Result<(), PipelineError> {
    // Initialize timer and logger
    let total_start = Instant::now();
    log::set_logger(&LOGGER).unwrap();

    // Acquire CLI args
    let args = Args::parse();
    let start_year = args.start_year.unwrap_or(1980);
    let end_year = args.end_year.unwrap_or(2024);
    if args.debug {
        log::set_max_level(log::LevelFilter::Debug);
    } else {
        log::set_max_level(log::LevelFilter::Info);
    }
    let countries_display = if args.countries.is_empty() {
        "ALL".to_string()
    } else {
        args.countries.join(",")
    };

    // UI
    println!("Transformer! Rust Weather Data Pipeline");
    debug!(
        "Input file: {} | Countries: {}",
        args.input_file.display(),
        countries_display
    );
    debug!(
        "Date range: {}-{} | Temperature unit: {:?}",
        start_year, end_year, args.unit
    );
    if let Some(threshold) = args.threshold {
        debug!("Outlier threshold: {} std devs", threshold);
    }
    if args.aggregate {
        debug!("Aggregating countries together");
    }

    debug!(
        "Processing {} for {} ({}-{})",
        args.input_file.display(),
        countries_display,
        start_year,
        end_year
    );

    // Create transformation config
    debug!(
        "Creating transformation configuration | Unit={:?}, Threshold={:?}, Aggregate={}",
        args.unit, args.threshold, args.aggregate
    );
    let config = TransformConfig {
        unit: args.unit,
        threshold: args.threshold,
        aggregate: args.aggregate,
    };

    // Process data with comprehensive statistics
    println!("Starting data processing...");
    let processing_start = Instant::now();
    let results = process_data(
        &args.input_file,
        &args.countries,
        start_year,
        end_year,
        &config,
    )?;
    let processing_time = processing_start.elapsed();
    println!(
        "Data processing completed in {:.2?} | Processed {} records",
        processing_time,
        results.len()
    );

    // Create output directory
    let output_dir = PathBuf::from(format!("./output/{}", args.output));
    fs::create_dir_all(&output_dir)?;
    println!(
        "Created output directory: {} | Writing output files...",
        output_dir.display()
    );
    let io_start = Instant::now();

    // Extract just the directory name for the file names (remove path separators)
    let output_name = args
        .output
        .split(['/', '\\'])
        .next_back()
        .unwrap_or(&args.output);
    let csv_path = output_dir.join(format!("{}.csv", output_name));
    let json_path = output_dir.join(format!("{}.json", output_name));
    let parquet_path = output_dir.join(format!("{}.parquet", output_name));

    let csv_start = Instant::now();
    write_csv(&results, &csv_path)?;
    println!("CSV write took {:.2?}", csv_start.elapsed());

    let json_start = Instant::now();
    write_json(&results, &json_path)?;
    println!("JSON write took {:.2?}", json_start.elapsed());

    let parquet_start = Instant::now();
    write_parquet(&results, &parquet_path)?;
    println!("Parquet write took {:.2?}", parquet_start.elapsed());

    let io_time = io_start.elapsed();
    println!("All files took {:.2?}", io_time);
    println!("\nWrote files to directory: {}", output_dir.display());
    debug!("  - {}", csv_path.display());
    debug!("  - {}", json_path.display());
    debug!("  - {}", parquet_path.display());

    // Show summary
    println!("\nProcessed {} records", results.len());
    if let Some(first) = results.first() {
        debug!(
            "Sample: {} {}/{} avg={:.1}°C count={}",
            first.country, first.year, first.month, first.avg_temp, first.count
        );
    }

    let total_time = total_start.elapsed();
    println!("Pipeline completed successfully in {:.2?}", total_time);
    debug!(
        "Performance breakdown: Processing={:.1}%, IO={:.1}%",
        (processing_time.as_secs_f64() / total_time.as_secs_f64()) * 100.0,
        (io_time.as_secs_f64() / total_time.as_secs_f64()) * 100.0
    );

    println!("\nTotal runtime: {:.2?}", total_time);
    Ok(())
}
