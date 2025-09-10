#!/bin/bash
set -euo pipefail

# Prefer current workspace path; fallback to airflow mount if present
BASE_DIR="/workspace/email_validation/cpp"
if [ -d "/opt/airflow/dags/email_validation/cpp" ] && [ -w "/opt/airflow/dags" ]; then
	BASE_DIR="/opt/airflow/dags/email_validation/cpp"
fi

CPP_DIR="$BASE_DIR"
BUILD_DIR="$CPP_DIR/.gtest_build"
GTEST_SRC_DIR="$CPP_DIR/.deps/googletest"
BIN_DIR="$CPP_DIR/.bin"

mkdir -p "$BUILD_DIR" "$GTEST_SRC_DIR" "$BIN_DIR"

if [ ! -d "$GTEST_SRC_DIR/.git" ]; then
	rm -rf "$GTEST_SRC_DIR"
	git clone --depth=1 https://github.com/google/googletest "$GTEST_SRC_DIR"
fi

# Build gtest from source with cmake locally
cmake -S "$GTEST_SRC_DIR" -B "$BUILD_DIR" >/dev/null
cmake --build "$BUILD_DIR" -j >/dev/null

# Discover lib paths produced by build
GTEST_LIB=$(find "$BUILD_DIR" -name "libgtest.a" | head -n1)
GTEST_MAIN_LIB=$(find "$BUILD_DIR" -name "libgtest_main.a" | head -n1)
GTEST_INCLUDE_DIR="$GTEST_SRC_DIR/googletest/include"

if [ -z "${GTEST_LIB}" ] || [ -z "${GTEST_MAIN_LIB}" ]; then
	echo "Failed to locate GoogleTest static libraries." >&2
	exit 1
fi

# Compile tests
GTEST_BIN="$BIN_DIR/validator_tests"

g++ -std=c++17 -I"$CPP_DIR" -I"$GTEST_INCLUDE_DIR" \
	"$CPP_DIR/validator_lib.cpp" "$CPP_DIR/validator_tests.cpp" \
	"$GTEST_LIB" "$GTEST_MAIN_LIB" -pthread -o "$GTEST_BIN"

echo "Running Google Tests..."
"$GTEST_BIN" | cat

