#!/bin/bash

# Performance Comparison Script
# Compares Rust vs Python weather data transformer performance

echo "=== Rust vs Python Performance Comparison ==="
echo

# Build Rust version (optimized)
echo "Building optimized Rust version..."
cargo build --release
echo

# Test parameters
INPUT_FILE="input.parquet"
OUTPUT_RUST="benchmark/rust"
OUTPUT_PYTHON="benchmark/python"
START_YEAR="1980"
END_YEAR="2019"
UNIT="kelvin"

echo "Test Parameters:"
echo "  Input: $INPUT_FILE"
echo "  Countries: ALL"
echo "  Years: $START_YEAR-$END_YEAR"
echo "  Unit: $UNIT"
echo

# Run Rust version
echo "=== Running Rust Version ==="
# Create nested directories properly
mkdir -p "./output/benchmark/rust"
{ time target/release/Transformer \
  --input-file "$INPUT_FILE" \
  --output "$OUTPUT_RUST" \
  --start-year $START_YEAR \
  --end-year $END_YEAR \
  --unit $UNIT; } > "./output/benchmark/rust/exc.log" 2> "./output/benchmark/rust/time.log"

echo
echo "=== Running Python Version ==="
mkdir -p "./output/benchmark/python"
{ time python Transformer.py \
  --input-file "$INPUT_FILE" \
  --output "$OUTPUT_PYTHON" \
  --start-year $START_YEAR \
  --end-year $END_YEAR \
  --unit $UNIT; } > "./output/benchmark/python/exc.log" 2> "./output/benchmark/python/time.log"

echo
echo "=== Comparison Complete ==="
echo "Check ./output/$OUTPUT_RUST/ and ./output/$OUTPUT_PYTHON/ for output files"
echo "Logs saved:"
echo "  Rust: ./output/benchmark/rust/exc.log and time.log"
echo "  Python: ./output/benchmark/python/exc.log and time.log"