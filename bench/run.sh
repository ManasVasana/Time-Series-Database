#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/../build"
OUT_DIR="${BUILD_DIR}/benchmark_results"

mkdir -p "${OUT_DIR}"

echo "=== KronosDB Benchmark Suite ==="
echo "Build dir: ${BUILD_DIR}"
echo "Output dir: ${OUT_DIR}"
echo ""

if [[ ! -d "${BUILD_DIR}" ]]; then
    echo "Build directory not found. Run cmake first:"
    echo "  cmake -B build -DCMAKE_BUILD_TYPE=Release -G Ninja"
    echo "  cmake --build build"
    exit 1
fi

TIMESTAMP=$(date +%Y%m%d_%H%M%S)

run_bench() {
    local name="$1"
    local bin="${BUILD_DIR}/${name}"
    local out="${OUT_DIR}/${name}_${TIMESTAMP}.json"
    if [[ ! -x "${bin}" ]]; then
        echo "SKIP: ${name} not found"
        return
    fi
    echo "Running: ${name}"
    "${bin}" --benchmark_format=json --benchmark_out="${out}" 2>&1 | grep -E "(BM_|Run|Time|Throughput)" || true
    echo "  -> ${out}"
}

run_bench bench_ingest
run_bench bench_scan
run_bench bench_recovery
run_bench bench_io_backend

echo ""
echo "All benchmarks complete. Results in: ${OUT_DIR}"
