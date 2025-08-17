#!/bin/bash
set -e

# Paths inside the Airflow container
CPP_DIR="/opt/airflow/dags/email_validation/cpp"
CPP_FILE="$CPP_DIR/validator.cpp"
OUTPUT_BIN="$CPP_DIR/validator"

# Input / Output CSV files (passed from Python DAG)
INPUT_FILE="$1"
OUTPUT_FILE="$2"

# Check args
if [ $# -ne 2 ]; then
  echo "Usage: run_validator.sh <input_csv> <output_csv>"
  exit 1
fi

# Compile if binary does not exist OR source is newer
if [ ! -f "$OUTPUT_BIN" ] || [ "$CPP_FILE" -nt "$OUTPUT_BIN" ]; then
  echo "Compiling validator.cpp..."
  g++ -std=c++11 "$CPP_FILE" -o "$OUTPUT_BIN"
fi

# Run validator binary
echo "Running email validator..."
"$OUTPUT_BIN" "$INPUT_FILE" "$OUTPUT_FILE"
