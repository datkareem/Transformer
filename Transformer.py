#!/usr/bin/env python3
import argparse
import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class Record:
    """Weather statistics record matching Rust struct"""
    
    def __init__(self, country: str, year: int, month: int, temps: List[float]):
        self.country = country
        self.year = year
        self.month = month
        
        if not temps:
            self._init_empty()
            return
            
        temps_array = np.array(temps)
        self.count = len(temps)
        self.avg_temp = np.mean(temps_array)
        self.min_temp = np.min(temps_array)
        self.max_temp = np.max(temps_array)
        self.std_dev = np.std(temps_array, ddof=1) if len(temps) > 1 else 0.0
        self.median_temp = np.median(temps_array)
        self.percentile_25 = np.percentile(temps_array, 25)
        self.percentile_75 = np.percentile(temps_array, 75)
        self.percentile_90 = np.percentile(temps_array, 90)
        self.percentile_95 = np.percentile(temps_array, 95)
    
    def _init_empty(self):
        """Initialize with empty values"""
        self.count = 0
        self.avg_temp = 0.0
        self.min_temp = 0.0
        self.max_temp = 0.0
        self.std_dev = 0.0
        self.median_temp = 0.0
        self.percentile_25 = 0.0
        self.percentile_75 = 0.0
        self.percentile_90 = 0.0
        self.percentile_95 = 0.0
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON/CSV output"""
        return {
            'country': self.country,
            'year': self.year,
            'month': self.month,
            'avg_temp': self.avg_temp,
            'min_temp': self.min_temp,
            'max_temp': self.max_temp,
            'std_dev': self.std_dev,
            'median_temp': self.median_temp,
            'count': self.count,
            'percentile_25': self.percentile_25,
            'percentile_75': self.percentile_75,
            'percentile_90': self.percentile_90,
            'percentile_95': self.percentile_95
        }


class TemperatureConverter:
    """Temperature unit conversion utilities"""
    
    @staticmethod
    def convert(temp_celsius: float, unit: str) -> float:
        """Convert temperature from Celsius to specified unit"""
        if unit.lower() == 'celsius':
            return temp_celsius
        elif unit.lower() == 'fahrenheit':
            return temp_celsius * 9.0 / 5.0 + 32.0
        elif unit.lower() == 'kelvin':
            return temp_celsius + 273.15
        else:
            raise ValueError(f"Unsupported temperature unit: {unit}")


class OutlierDetector:
    """Statistical outlier detection"""
    
    @staticmethod
    def remove_outliers(data: List[float], threshold: float) -> List[float]:
        """Remove outliers using standard deviation threshold"""
        if len(data) < 2:
            return data
            
        data_array = np.array(data)
        mean_val = np.mean(data_array)
        std_val = np.std(data_array, ddof=1)
        
        mask = np.abs(data_array - mean_val) <= threshold * std_val
        return data_array[mask].tolist()


class Transformer:
    """Main weather data transformation pipeline"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def clean_temp(self, temp: float) -> Optional[float]:
        """Clean and validate temperature data"""
        if not np.isfinite(temp):
            return None
        if not (-100.0 <= temp <= 70.0):  # Reasonable temperature bounds
            return None
        return temp
    
    def process_data(self, 
                    input_file: Path,
                    countries: List[str],
                    start_year: int,
                    end_year: int,
                    unit: str = 'celsius',
                    threshold: Optional[float] = None,
                    aggregate: bool = False) -> List[Record]:
        """Process weather data with comprehensive statistics"""
        
        self.logger.debug(f"Reading Parquet file: {input_file}")
        
        # Read Parquet file
        try:
            df = pd.read_parquet(input_file)
        except Exception as e:
            raise RuntimeError(f"Failed to read Parquet file: {e}")
        
        self.logger.debug("Starting data extraction from Parquet data")
        
        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        
        # Apply filters
        mask = (
            df['country_alpha2'].isin(countries) &
            (df['year'] >= start_year) &
            (df['year'] <= end_year)
        )
        filtered_df = df[mask].copy()
        
        total_rows = len(df)
        filtered_rows = len(filtered_df)
        print(f"Processed {total_rows} total rows, {filtered_rows} matched filters")
        
        # Clean temperature data
        filtered_df['temp_cleaned'] = filtered_df['temp_mean_c_approx'].apply(self.clean_temp)
        filtered_df = filtered_df.dropna(subset=['temp_cleaned'])
        
        # Convert temperature units
        filtered_df['temp_converted'] = filtered_df['temp_cleaned'].apply(
            lambda x: TemperatureConverter.convert(x, unit)
        )
        
        # Group data
        if aggregate:
            country_key = ','.join(countries)
            filtered_df['country_key'] = country_key
        else:
            filtered_df['country_key'] = filtered_df['country_alpha2']
        
        # Group by country-year-month
        grouped = filtered_df.groupby(['country_key', 'year', 'month'])['temp_converted'].apply(list)
        
        self.logger.debug(f"Found {len(grouped)} unique country-month combinations")
        
        if threshold:
            self.logger.debug(f"Outlier detection enabled with threshold: {threshold}")
        
        # Calculate statistics
        print("Starting statistical analysis")
        results = []
        outliers_removed = 0
        
        for (country, year, month), temps in grouped.items():
            if not temps:
                continue
                
            # Apply outlier removal if enabled
            if threshold:
                original_count = len(temps)
                temps = OutlierDetector.remove_outliers(temps, threshold)
                removed = original_count - len(temps)
                outliers_removed += removed
                if removed > 0:
                    self.logger.debug(f"Removed {removed} outliers for {country}/{year}/{month}")
            
            if temps:  # Check if we still have data after outlier removal
                stats = Record(country, int(year), int(month), temps)
                results.append(stats)
        
        if threshold and outliers_removed > 0:
            self.logger.debug(f"Removed {outliers_removed} total outliers across all records")
        
        # Sort results
        print(f"Sorting {len(results)} results")
        results.sort(key=lambda x: (x.country, x.year, x.month))
        
        self.logger.debug("Transform processing completed successfully")
        return results
    
    def write_csv(self, results: List[Record], output_path: Path):
        """Write results to CSV file"""
        df = pd.DataFrame([record.to_dict() for record in results])
        df.to_csv(output_path, index=False)
    
    def write_json(self, results: List[Record], output_path: Path):
        """Write results to JSON file"""
        data = [record.to_dict() for record in results]
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    def write_parquet(self, results: List[Record], output_path: Path):
        """Write results to Parquet file"""
        df = pd.DataFrame([record.to_dict() for record in results])
        df.to_parquet(output_path, index=False)


def setup_logging(debug=False):
    """Setup logging configuration"""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format='[%(levelname)s] %(message)s',
        handlers=[logging.StreamHandler()]
    )


def main():
    """Main application entry point"""
    parser = argparse.ArgumentParser(
        description="Python Weather Data Transformer - Performance comparison with Rust version"
    )
    
    parser.add_argument('-i', '--input-file', type=Path, required=True,
                       help='Path to the input Parquet file')
    parser.add_argument('-o', '--output', type=str, default='output',
                       help='Output base name (creates directory with CSV, JSON, and Parquet files)')
    parser.add_argument('-c', '--countries', type=str, required=True,
                       help='Country alpha-2 codes to filter data by (e.g., US,DE,FR)')
    parser.add_argument('--start-year', type=int,
                       help='Start year (inclusive) for filtering')
    parser.add_argument('--end-year', type=int, 
                       help='End year (inclusive) for filtering')
    parser.add_argument('--unit', choices=['celsius', 'fahrenheit', 'kelvin'], 
                       default='celsius', help='Temperature unit for output')
    parser.add_argument('--threshold', type=float,
                       help='Outlier detection threshold (standard deviations)')
    parser.add_argument('--aggregate', action='store_true',
                       help='Aggregate all countries together instead of keeping them separate')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging (default: info level)')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(debug=args.debug)
    logger = logging.getLogger(__name__)
    
    # Record start time
    total_start = time.time()
    
    # Parse comma-separated countries
    countries = [c.strip() for c in args.countries.split(',')]
    
    # Set defaults
    start_year = args.start_year or 1980
    end_year = args.end_year or 2024
    
    print("Transformer! Python Weather Data Pipeline")
    logger.debug(f"Input file: {args.input_file} | Countries: {','.join(countries)}")
    logger.debug(f"Date range: {start_year}-{end_year} | Temperature unit: {args.unit}")
    
    if args.threshold:
        logger.debug(f"Outlier threshold: {args.threshold} std devs")
    if args.aggregate:
        logger.debug("Aggregating countries together")
    
    print(f"Processing {args.input_file} for {','.join(countries)} ({start_year}-{end_year})")
    
    # Create transformer
    transformer = Transformer()
    
    # Configuration logging
    logger.debug(f"Creating transformation configuration | Unit={args.unit}, "
               f"Threshold={args.threshold}, Aggregate={args.aggregate}")
    
    # Process data
    print("Starting data processing...")
    processing_start = time.time()
    
    results = transformer.process_data(
        input_file=Path(f"./{args.input_file}"),
        countries=countries,
        start_year=start_year,
        end_year=end_year,
        unit=args.unit,
        threshold=args.threshold,
        aggregate=args.aggregate
    )
    
    processing_time = time.time() - processing_start
    print(f"Data processing completed in {processing_time:.2f}s | Processed {len(results)} records")
    
    # Create output directory
    output_dir = Path(f"./output/{args.output}")
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Created output directory: {output_dir} | Writing output files...")
    
    # Write output files
    io_start = time.time()
    
    csv_path = output_dir / f"{args.output}.csv"
    json_path = output_dir / f"{args.output}.json"
    parquet_path = output_dir / f"{args.output}.parquet"
    
    # Write CSV
    csv_start = time.time()
    transformer.write_csv(results, csv_path)
    csv_time = time.time() - csv_start
    print(f"CSV write took {csv_time:.2f}s")
    
    # Write JSON
    json_start = time.time()
    transformer.write_json(results, json_path)
    json_time = time.time() - json_start
    print(f"JSON write took {json_time:.2f}s")
    
    # Write Parquet
    parquet_start = time.time()
    transformer.write_parquet(results, parquet_path)
    parquet_time = time.time() - parquet_start
    print(f"Parquet write took {parquet_time:.2f}s")
    
    io_time = time.time() - io_start
    print(f"All files took {io_time:.2f}s")
    
    print(f"Wrote files to directory: {output_dir}")
    print(f"  - {csv_path}")
    print(f"  - {json_path}")
    print(f"  - {parquet_path}")
    
    # Show summary
    print(f"\nProcessed {len(results)} records")
    if results:
        first = results[0]
        print(f"Sample: {first.year}/{first.month} avg={first.avg_temp:.1f}°C count={first.count}")
    
    # Performance summary
    total_time = time.time() - total_start
    print(f"Pipeline completed successfully in {total_time:.2f}s")
    logger.debug(f"Performance breakdown: Processing={processing_time/total_time*100:.1f}%, "
               f"IO={io_time/total_time*100:.1f}%")
    
    print(f"\nTotal runtime: {total_time:.2f}s")


if __name__ == "__main__":
    main()