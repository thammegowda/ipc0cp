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

def run_stdio_benchmark(duration: float, min_size: int, max_size: int) -> Dict:
    """
    Run STDIO benchmark by piping producer to consumer.
    
    Args:
        duration: Duration in seconds
        min_size: Minimum payload size
        max_size: Maximum payload size
        
    Returns:
        Dict with throughput_mbps
    """
    producer_cmd = [
        sys.executable,
        'producer.py',
        '--stdio',
        '--duration', str(duration),
        '--min-size', str(min_size),
        '--max-size', str(max_size),
        '--quiet',
    ]
    
    consumer_cmd = [
        sys.executable,
        'consumer.py',
        '--stdio',
        '--quiet',
    ]
    
    # Start producer and consumer with pipe
    producer = subprocess.Popen(
        producer_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=os.path.dirname(__file__),
    )
    
    consumer = subprocess.Popen(
        consumer_cmd,
        stdin=producer.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=os.path.dirname(__file__),
    )
    
    # Close producer stdout in parent to allow consumer to receive EOF
    producer.stdout.close()
    
    # Wait for both to finish
    consumer_stdout, consumer_stderr = consumer.communicate()
    producer_stdout, producer_stderr = producer.communicate()
    
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
        if 'Bytes sent:' in line or 'Bytes received:' in line:
            # Extract first number (with commas removed)
            parts = line.split(':')[1].strip().split()
            stats['bytes'] = int(parts[0].replace(',', ''))
        elif 'Messages sent:' in line or 'Messages received:' in line:
            parts = line.split(':')[1].strip().split()
            stats['messages'] = int(parts[0].replace(',', ''))
        elif 'Throughput:' in line:
            parts = line.split(':')[1].strip().split()
            stats['throughput_mbps'] = float(parts[0])
    
    return stats


def main():
    parser = argparse.ArgumentParser(description='Run IPC benchmarks')
    parser.add_argument('--runs', type=int, default=3,
                        help='Number of runs per benchmark (default: 3)')
    parser.add_argument('--duration', type=float, default=60.0,
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
    
    # STDIO Benchmarks
    print("Running STDIO benchmarks...")
    stdio_results = []
    for i in range(args.runs):
        print(f"  Run {i+1}/{args.runs}...", end=' ', flush=True)
        result = run_stdio_benchmark(args.duration, args.min_size, args.max_size)
        stdio_results.append(result)
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
    
    # Calculate statistics for STDIO
    stdio_producer_throughputs = [r['producer_throughput_mbps'] for r in stdio_results]
    stdio_consumer_throughputs = [r['consumer_throughput_mbps'] for r in stdio_results]
    
    print("\nSTDIO (baseline):")
    print(f"  Producer Throughput:")
    print(f"    Mean:   {statistics.mean(stdio_producer_throughputs):8.2f} MB/s")
    print(f"    Stddev: {statistics.stdev(stdio_producer_throughputs) if len(stdio_producer_throughputs) > 1 else 0:8.2f} MB/s")
    print(f"  Consumer Throughput:")
    print(f"    Mean:   {statistics.mean(stdio_consumer_throughputs):8.2f} MB/s")
    print(f"    Stddev: {statistics.stdev(stdio_consumer_throughputs) if len(stdio_consumer_throughputs) > 1 else 0:8.2f} MB/s")
    
    # Calculate statistics for Shared Memory
    shm_producer_throughputs = [r['producer_throughput_mbps'] for r in shm_results]
    shm_consumer_throughputs = [r['consumer_throughput_mbps'] for r in shm_results]
    
    print("\nShared Memory:")
    print(f"  Producer Throughput:")
    print(f"    Mean:   {statistics.mean(shm_producer_throughputs):8.2f} MB/s")
    print(f"    Stddev: {statistics.stdev(shm_producer_throughputs) if len(shm_producer_throughputs) > 1 else 0:8.2f} MB/s")
    print(f"  Consumer Throughput:")
    print(f"    Mean:   {statistics.mean(shm_consumer_throughputs):8.2f} MB/s")
    print(f"    Stddev: {statistics.stdev(shm_consumer_throughputs) if len(shm_consumer_throughputs) > 1 else 0:8.2f} MB/s")
    
    # Calculate speedup
    stdio_mean = statistics.mean(stdio_consumer_throughputs)
    shm_mean = statistics.mean(shm_consumer_throughputs)
    speedup = shm_mean / stdio_mean if stdio_mean > 0 else 0
    
    print(f"\nSpeedup: {speedup:.2f}x")
    print(f"  (Shared Memory is {speedup:.2f}x {'faster' if speedup > 1 else 'slower'} than STDIO)")
    
    print()
    
    # Save detailed results to JSON
    results = {
        'config': {
            'runs': args.runs,
            'duration': args.duration,
            'min_size': args.min_size,
            'max_size': args.max_size,
        },
        'stdio': {
            'runs': stdio_results,
            'producer_mean': statistics.mean(stdio_producer_throughputs),
            'producer_stddev': statistics.stdev(stdio_producer_throughputs) if len(stdio_producer_throughputs) > 1 else 0,
            'consumer_mean': statistics.mean(stdio_consumer_throughputs),
            'consumer_stddev': statistics.stdev(stdio_consumer_throughputs) if len(stdio_consumer_throughputs) > 1 else 0,
        },
        'shm': {
            'runs': shm_results,
            'producer_mean': statistics.mean(shm_producer_throughputs),
            'producer_stddev': statistics.stdev(shm_producer_throughputs) if len(shm_producer_throughputs) > 1 else 0,
            'consumer_mean': statistics.mean(shm_consumer_throughputs),
            'consumer_stddev': statistics.stdev(shm_consumer_throughputs) if len(shm_consumer_throughputs) > 1 else 0,
        },
        'speedup': speedup,
    }
    
    output_file = MYDIR / f"benchmark_results_{int(time.time())}.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"Detailed results saved to: {output_file}")


if __name__ == '__main__':
    main()
