#!/usr/bin/env python3
"""
Run benchmarks comparing STDIO vs Shared Memory performance.
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from typing import List, Dict
from pathlib import Path

MYDIR = Path(__file__).resolve().parent

def run_stdio_variant(variant_flag: str, duration: float, min_size: int, max_size: int) -> Dict:
    """Run STDIO benchmark for the given CLI flag."""

    producer_cmd = [
        sys.executable,
        'producer.py',
        variant_flag,
        '--duration', str(duration),
        '--min-size', str(min_size),
        '--max-size', str(max_size),
        '--quiet',
    ]

    consumer_cmd = [
        sys.executable,
        'consumer.py',
        variant_flag,
        '--quiet',
    ]
    
    # tried unbuffered mode, but it was slightly worse
    # stdio_env = os.environ.copy()
    # stdio_env['PYTHONUNBUFFERED'] = '1'

    producer = subprocess.Popen(
        producer_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=os.path.dirname(__file__),
        #env=stdio_env,
    )

    consumer = subprocess.Popen(
        consumer_cmd,
        stdin=producer.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=os.path.dirname(__file__),
        env=stdio_env,
    )

    producer.stdout.close()

    consumer_stdout, consumer_stderr = consumer.communicate()
    producer_stdout, producer_stderr = producer.communicate()

    producer_stats = parse_stats(producer_stderr.decode())
    consumer_stats = parse_stats(consumer_stderr.decode())

    return {
        'producer_throughput_mbps': producer_stats['throughput_mbps'],
        'consumer_throughput_mbps': consumer_stats['throughput_mbps'],
        'producer_bytes': producer_stats['bytes'],
        'consumer_bytes': consumer_stats['bytes'],
        'producer_messages': producer_stats['messages'],
        'consumer_messages': consumer_stats['messages'],
    }


def run_stdio_raw_benchmark(duration: float, min_size: int, max_size: int) -> Dict:
    return run_stdio_variant('--stdio', duration, min_size, max_size)


def run_stdio_api_benchmark(duration: float, min_size: int, max_size: int) -> Dict:
    return run_stdio_variant('--stdio-api', duration, min_size, max_size)


def run_shm_benchmark(duration: float, min_size: int, max_size: int) -> Dict:
    """
    Run shared memory benchmark.
    
    Args:
        duration: Duration in seconds
        min_size: Minimum payload size
        max_size: Maximum payload size
        
    Returns:
        Dict with throughput_mbps
    """
    shm_name = f"bench_{os.getpid()}_{int(time.time() * 1000)}"
    
    producer_cmd = [
        sys.executable,
        'producer.py',
        '--shm', shm_name,
        '--duration', str(duration),
        '--min-size', str(min_size),
        '--max-size', str(max_size),
        '--quiet',
    ]
    
    consumer_cmd = [
        sys.executable,
        'consumer.py',
        '--shm', shm_name,
        '--quiet',
    ]
    
    # Start consumer first (it will wait for shared memory)
    consumer = subprocess.Popen(
        consumer_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=os.path.dirname(__file__),
    )
    
    # Small delay then start producer
    time.sleep(0.1)
    
    producer = subprocess.Popen(
        producer_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=os.path.dirname(__file__),
    )
    
    # Wait for both to finish
    producer_stdout, producer_stderr = producer.communicate()
    consumer_stdout, consumer_stderr = consumer.communicate()
    
    # Parse throughput from stderr output
    producer_stats = parse_stats(producer_stderr.decode())
    consumer_stats = parse_stats(consumer_stderr.decode())
    
    return {
        'producer_throughput_mbps': producer_stats['throughput_mbps'],
        'consumer_throughput_mbps': consumer_stats['throughput_mbps'],
        'producer_bytes': producer_stats['bytes'],
        'consumer_bytes': consumer_stats['bytes'],
        'producer_messages': producer_stats['messages'],
        'consumer_messages': consumer_stats['messages'],
    }


def parse_stats(stderr_output: str) -> Dict:
    """
    Parse statistics from stderr output.
    
    Args:
        stderr_output: String output from stderr
        
    Returns:
        Dict with bytes, messages, throughput_mbps
    """
    stats = {
        'bytes': 0,
        'messages': 0,
        'throughput_mbps': 0.0,
    }
    
    for line in stderr_output.split('\n'):
        if 'Bytes sent' in line or 'Bytes received' in line:
            # Extract first number (with commas removed)
            parts = line.split(':', 1)[1].strip().split()
            stats['bytes'] = int(parts[0].replace(',', ''))
        elif 'Messages sent' in line or 'Messages received' in line:
            parts = line.split(':', 1)[1].strip().split()
            stats['messages'] = int(parts[0].replace(',', ''))
        elif 'Throughput:' in line:
            parts = line.split(':', 1)[1].strip().split()
            stats['throughput_mbps'] = float(parts[0])
    
    return stats


def safe_stdev(values: List[float]) -> float:
    return statistics.stdev(values) if len(values) > 1 else 0.0


def summarize_variant(name: str, results: List[Dict]) -> Dict[str, float]:
    producer_throughputs = [r['producer_throughput_mbps'] for r in results]
    consumer_throughputs = [r['consumer_throughput_mbps'] for r in results]

    producer_mean = statistics.mean(producer_throughputs)
    consumer_mean = statistics.mean(consumer_throughputs)
    producer_stddev = safe_stdev(producer_throughputs)
    consumer_stddev = safe_stdev(consumer_throughputs)

    print(f"\n{name}:")
    print(f"  Producer Throughput:")
    print(f"    Mean:   {producer_mean:8.2f} MB/s")
    print(f"    Stddev: {producer_stddev:8.2f} MB/s")
    print(f"  Consumer Throughput:")
    print(f"    Mean:   {consumer_mean:8.2f} MB/s")
    print(f"    Stddev: {consumer_stddev:8.2f} MB/s")

    return {
        'producer_mean': producer_mean,
        'producer_stddev': producer_stddev,
        'consumer_mean': consumer_mean,
        'consumer_stddev': consumer_stddev,
    }


def main():
    parser = argparse.ArgumentParser(description='Run IPC benchmarks')
    parser.add_argument('--runs', type=int, default=3,
                        help='Number of runs per benchmark (default: 3)')
    parser.add_argument('--duration', type=float, default=20.0,
                        help='Duration per run in seconds (default: 60)')
    parser.add_argument('--min-size', type=int, default=512 * 1024,
                        help='Minimum payload size in bytes (default: 512KB)')
    parser.add_argument('--max-size', type=int, default=5 * 1024 * 1024,
                        help='Maximum payload size in bytes (default: 5MB)')
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("IPC BENCHMARK: STDIO vs Shared Memory")
    print("=" * 80)
    print(f"Configuration:")
    print(f"  Runs per benchmark: {args.runs}")
    print(f"  Duration per run: {args.duration}s")
    print(f"  Payload size range: {args.min_size / 1024:.0f}KB - {args.max_size / (1024**2):.1f}MB")
    print()

    # STDIO raw benchmarks
    print("Running STDIO benchmarks (raw)...")
    stdio_raw_results = []
    for i in range(args.runs):
        print(f"  Run {i+1}/{args.runs}...", end=' ', flush=True)
        result = run_stdio_raw_benchmark(args.duration, args.min_size, args.max_size)
        stdio_raw_results.append(result)
        print(f"Producer: {result['producer_throughput_mbps']:.2f} MB/s, "
              f"Consumer: {result['consumer_throughput_mbps']:.2f} MB/s")

    print()

    # STDIO API benchmarks
    print("Running STDIO benchmarks (API)...")
    stdio_api_results = []
    for i in range(args.runs):
        print(f"  Run {i+1}/{args.runs}...", end=' ', flush=True)
        result = run_stdio_api_benchmark(args.duration, args.min_size, args.max_size)
        stdio_api_results.append(result)
        print(f"Producer: {result['producer_throughput_mbps']:.2f} MB/s, "
              f"Consumer: {result['consumer_throughput_mbps']:.2f} MB/s")

    print()

    # Shared Memory Benchmarks
    print("Running Shared Memory benchmarks...")
    shm_results = []
    for i in range(args.runs):
        print(f"  Run {i+1}/{args.runs}...", end=' ', flush=True)
        result = run_shm_benchmark(args.duration, args.min_size, args.max_size)
        shm_results.append(result)
        print(f"Producer: {result['producer_throughput_mbps']:.2f} MB/s, "
              f"Consumer: {result['consumer_throughput_mbps']:.2f} MB/s")

    print()
    print("=" * 80)
    print("RESULTS")
    print("=" * 80)

    stdio_raw_stats = summarize_variant("STDIO (raw)", stdio_raw_results)
    stdio_api_stats = summarize_variant("STDIO (API)", stdio_api_results)
    shm_stats = summarize_variant("Shared Memory", shm_results)

    speedup_vs_raw = (
        shm_stats['consumer_mean'] / stdio_raw_stats['consumer_mean']
        if stdio_raw_stats['consumer_mean'] > 0 else 0
    )
    speedup_vs_api = (
        shm_stats['consumer_mean'] / stdio_api_stats['consumer_mean']
        if stdio_api_stats['consumer_mean'] > 0 else 0
    )

    print(f"\nSpeedup vs STDIO (raw): {speedup_vs_raw:.2f}x")
    print(f"  (Shared Memory is {speedup_vs_raw:.2f}x "
          f"{'faster' if speedup_vs_raw > 1 else 'slower'} than STDIO (raw))")
    print(f"Speedup vs STDIO (API): {speedup_vs_api:.2f}x")
    print(f"  (Shared Memory is {speedup_vs_api:.2f}x "
          f"{'faster' if speedup_vs_api > 1 else 'slower'} than STDIO (API))")

    print()

    # Save detailed results to JSON
    results = {
        'config': {
            'runs': args.runs,
            'duration': args.duration,
            'min_size': args.min_size,
            'max_size': args.max_size,
        },
        'stdio_raw': {
            'runs': stdio_raw_results,
            'producer_mean': stdio_raw_stats['producer_mean'],
            'producer_stddev': stdio_raw_stats['producer_stddev'],
            'consumer_mean': stdio_raw_stats['consumer_mean'],
            'consumer_stddev': stdio_raw_stats['consumer_stddev'],
        },
        'stdio_api': {
            'runs': stdio_api_results,
            'producer_mean': stdio_api_stats['producer_mean'],
            'producer_stddev': stdio_api_stats['producer_stddev'],
            'consumer_mean': stdio_api_stats['consumer_mean'],
            'consumer_stddev': stdio_api_stats['consumer_stddev'],
        },
        'shm': {
            'runs': shm_results,
            'producer_mean': shm_stats['producer_mean'],
            'producer_stddev': shm_stats['producer_stddev'],
            'consumer_mean': shm_stats['consumer_mean'],
            'consumer_stddev': shm_stats['consumer_stddev'],
        },
        'speedup_vs_stdio_raw': speedup_vs_raw,
        'speedup_vs_stdio_api': speedup_vs_api,
    }
    
    output_file = MYDIR / f"benchmark_results_{int(time.time())}.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"Detailed results saved to: {output_file}")


if __name__ == '__main__':
    main()
