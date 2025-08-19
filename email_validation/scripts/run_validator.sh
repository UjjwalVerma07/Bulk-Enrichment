#!/bin/bash
set -e

CPP_DIR="/opt/airflow/dags/email_validation/cpp"
CPP_FILE="$CPP_DIR/validator.cpp"
OUTPUT_BIN="$CPP_DIR/validator"


INPUT_FILE="$1"
OUTPUT_FILE="$2"

if [ $# -ne 2 ]; then
  echo "Usage: run_validator.sh <input_csv> <output_csv>"
  exit 1
fi

if [ ! -f "$OUTPUT_BIN" ] || [ "$CPP_FILE" -nt "$OUTPUT_BIN" ]; then
  echo "Compiling validator.cpp..."
  g++ -std=c++11 "$CPP_FILE" -o "$OUTPUT_BIN"
fi

echo "Running email validator..."
"$OUTPUT_BIN" "$INPUT_FILE" "$OUTPUT_FILE"
