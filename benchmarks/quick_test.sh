#!/bin/bash
# Quick test of benchmark suite with short duration

set -e

cd "$(dirname "$0")"

echo "=================================="
echo "Quick Benchmark Test (5 seconds)"
echo "=================================="
echo ""

python run_benchmark.py --runs 1 --duration 5 --min-size 524288 --max-size 1048576

echo ""
echo "Test complete!"
