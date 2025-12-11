# Transformer: Weather Data Processing Pipeline
### Kareem Taha | 3 Dec 25

A high-performance weather data processing tool designed to demonstrate efficient data engineering patterns and performance.

## Overview
Transformer is a fast data conversion and analysis tool that processes weather station data from a Parquet file. The dataset was curated from Kaggle and contains daily weather readings for 94 major countries, from 1980 - 2024. 

Transformer performs ETL and outputs to 3 file formats: (csv, json, parquet). 

The project implements identical functionality in Python, allowing for direct performance comparisons between the two languages for data processing workloads. 

### Key Features
- **Multi-format Output**: CSV, JSON, and Parquet export capabilities
- **Statistical Analysis**: Comprehensive weather statistics including percentiles, outlier detection, and temperature conversions
- **Country Filtering**: Process data for specific countries or aggregate across all nations
- **Performance Monitoring**: Built-in timing and logging for performance analysis
- **Cross-language Implementation**: Identical functionality in Rust and Python for benchmarking

### Data Processing
- **Temperature Conversion**: Automatic Celsius to Fahrenheit or Kelvin conversion
- **Statistical Calculations**: Min, max, mean, median, and multiple percentiles (25th, 75th, 90th, 95th)
- **Outlier Detection**: Identifies temperature readings outside normal ranges
- **Country Aggregation**: Group statistics by country or process all countries together

### Output Formats
- **CSV**: Human-readable tabular format with headers
- **JSON**: Structured data format for APIs and web applications  
- **Parquet**: Columnar format optimized for analytics and big data workflows

### Performance Features
- **Benchmarking**: Built-in timing measurements and Python equivalent for performance comparison
- **Parallelism**: Rayon-driven parallel processing for data analysis operations

### Installation and Usage
1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd Transformer
   ```

2. **Prepare Input Data**: Ensure you have the Parquet file of weather station data. The schema found in the code is based off of the included file.

3. **Rust Setup:**
   ```bash
   cargo build --release
   ./target/release/Transformer [--args]
   ```

3.5. **Python Setup:**
   ```bash
   pip install -r requirements.txt
   python3 Transformer.py [--args]
   ```

4. **Benchmark Setup:**
   ```bash
   ./benchmark.sh
   ```

5. **Review Output**: Processed data will be saved in the `output/` directory with filenames indicating the format and timestamp.

## Arguments
Both Rust and Python implementations share identical command-line interfaces:

### Required Arguments
- `input_file`: Name of input Parquet file containing weather data (must exist in project root)

### Optional Arguments
- `--output`: str = Name of the output directory/files [default: `output`]
- `--countries`: str = Comma-separated list of countries to filter (e.g., "US,CA,MX")
- `--aggregate`: bool = Aggregate all countries into single record [flag]
- `--start-year`: int = Start year of data analysis
- `--end-year`: int = End year of data analysis
- `--unit`: str = Unit of temperature (Celsius, Fahrenheit, Kelvin)
- `--threshold`: float = Threshold for outlier detection (measured in std. deviations; default = 3.0)
- `--debug`: bool = Extra debug logging
For bool arguments, no value is needed, just pass the flag.

### Examples

```bash
# Basic CSV output
./target/release/Transformer --input-file input.parquet

# Kelvin output for all countries
./target/release/Transformer --input-file input.parquet --output test --start-year 2000 --end-year 2022 --unit kelvin

# Aggregated (average) record per month of a subset of countries
./target/release/Transformer --input-file input.parquet --countries DE,GE,JP --start-year 2000 --end-year 2022 --aggregate

# Python equivalent
python Transformer.py --input-file input.parquet --output output --countries "US,CA"
```

### Project Structure

```
Transformer/
├── src/                    # Rust source code
│   ├── error.rs           # Error handler
│   ├── main.rs            # CLI interface
│   ├── lib.rs             # Lib exports
│   ├── structs.rs         # Data structures
│   ├── transform.rs       # Core processing logic
│   └── load.rs            # Output
├── input.parquet          # Input data
├── output/                # Output files
├── Transformer.py         # Python version (for benchmark)
├── requirements.txt       # Python dependencies
├── benchmark.sh          # Performance comparison benchmark
└── Cargo.toml            # Rust dependencies
```

## Benchmark Comparison

The project includes both Rust and Python implementations to demonstrate performance differences between the two languages for data processing tasks.

Use the provided benchmark script to compare performance:

```bash
# Make script executable (Linux/macOS)
chmod +x benchmark.sh
./benchmark.sh

# Windows PowerShell
./benchmark.sh
```

The script measures:
- Total execution time
- Execution time per step (ETL)
- Total records

### Results

Based on testing with weather station data:
- **Rust**: ~10x faster data processing time
- **Memory**: Rust uses ~40-60% less peak memory
- **File IO**: Rust writes files ~6x faster, except for a 10x slowdown for JSON (likely due to Python's advantage natively storing data as dict/JSON) 
- **Output**: Identical results between implementations
- **Startup**: Rust has faster cold start times

## Conclusion

There were many small takeaways from this project. For instance, Apache Arrow requires careful attention to data type conversions and array construction methods. While Rust's ownership system initially creates complexity, it prevents entire classes of memory safety issues common in data processing pipelines. 

Some challenges I faced included learning the proper methods for StringArray creation and handling nullable data types in the Arrow format. Also implementing custom error handling that provides useful feedback. The biggest challenge was dealing with the small memory ownership details of the language.

In the end, the Rust implementation required more upfront development time but delivered superior runtime performance and memory efficiency. In both file IO and CPU operations, Rust saw a 5-10x speedup. For small queries this was not impactful, but greater magnitudes of records (10k+) saw meaningful performances improvements. 

---

## AI Disclaimer

The only AI used in this project was Github Copilot Auto-Line Complete, which helped me maintain correct logic/syntax.

## License

This project is licensed under the Apache License. See the LICENSE file for details.