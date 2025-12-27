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
from dataclasses import dataclass
from typing import List, Dict, Optional, Callable
from pathlib import Path
from datetime import datetime

MYDIR = Path(__file__).resolve().parent
CPP_BIN_DIR = MYDIR / 'build' / 'bin'

PROCESS_TIMEOUT_GRACE_S = 10.0


def _terminate_process(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=1.0)
    except Exception:
        pass
    if proc.poll() is None:
        try:
            proc.kill()
            proc.wait(timeout=1.0)
        except Exception:
            pass


def _run_piped(
    *,
    producer_cmd: List[str],
    consumer_cmd: List[str],
    timeout_s: float,
    cwd: Path,
    producer_env: Optional[Dict[str, str]] = None,
) -> tuple[str, str]:
    """Run producer->consumer with stdout piped into stdin.

    Returns:
        (producer_stderr, consumer_stderr)
    """

    producer = subprocess.Popen(
        producer_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(cwd),
        env=producer_env,
    )

    consumer = subprocess.Popen(
        consumer_cmd,
        stdin=producer.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(cwd),
    )

    assert producer.stdout is not None
    producer.stdout.close()

    try:
        _consumer_stdout, consumer_stderr_b = consumer.communicate(timeout=timeout_s)
        _producer_stdout, producer_stderr_b = producer.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        _terminate_process(producer)
        _terminate_process(consumer)
        raise

    return producer_stderr_b.decode(), consumer_stderr_b.decode()


def _run_two_processes(
    *,
    consumer_cmd: List[str],
    producer_cmd: List[str],
    timeout_s: float,
    cwd: Path,
    consumer_first: bool = True,
    startup_delay_s: float = 0.1,
) -> tuple[str, str]:
    """Run consumer and producer as separate processes (no piping).

    Returns:
        (producer_stderr, consumer_stderr)
    """

    if consumer_first:
        consumer = subprocess.Popen(
            consumer_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(cwd),
        )
        time.sleep(startup_delay_s)
        producer = subprocess.Popen(
            producer_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(cwd),
        )
    else:
        producer = subprocess.Popen(
            producer_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(cwd),
        )
        time.sleep(startup_delay_s)
        consumer = subprocess.Popen(
            consumer_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(cwd),
        )

    try:
        _producer_stdout, producer_stderr_b = producer.communicate(timeout=timeout_s)
        _consumer_stdout, consumer_stderr_b = consumer.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        _terminate_process(producer)
        _terminate_process(consumer)
        raise

    return producer_stderr_b.decode(), consumer_stderr_b.decode()


def _stats_from_stderr(producer_stderr: str, consumer_stderr: str) -> Dict:
    producer_stats = parse_stats(producer_stderr)
    consumer_stats = parse_stats(consumer_stderr)
    return {
        'producer_throughput_mbps': producer_stats['throughput_mbps'],
        'consumer_throughput_mbps': consumer_stats['throughput_mbps'],
        'producer_bytes': producer_stats['bytes'],
        'consumer_bytes': consumer_stats['bytes'],
        'producer_messages': producer_stats['messages'],
        'consumer_messages': consumer_stats['messages'],
    }


@dataclass(frozen=True)
class Variant:
    key: str
    label: str
    run_once: Callable[[], Dict]


def get_cpp_executable(name: str) -> Path:
    """Get the requested C++ benchmark binary, raising if it is missing."""

    exe = CPP_BIN_DIR / name
    if sys.platform == 'win32':
        exe = exe.with_suffix('.exe')
    if not exe.exists():
        raise FileNotFoundError(
            f"C++ benchmark binary '{name}' not found. "
            f"Build it with `cmake -S {MYDIR} -B {MYDIR / 'build'}`."
        )
    return exe
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
    timeout_s = duration + PROCESS_TIMEOUT_GRACE_S
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
    
    # Wait for both to finish (bounded)
    try:
        producer_stdout, producer_stderr = producer.communicate(timeout=timeout_s)
        consumer_stdout, consumer_stderr = consumer.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        _terminate_process(producer)
        _terminate_process(consumer)
        raise
    
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

def _build_variants(
    *,
    duration: float,
    min_size: int,
    max_size: int,
    include_cpp: bool,
) -> List[Variant]:
    timeout_s = duration + PROCESS_TIMEOUT_GRACE_S

    stdio_env = os.environ.copy()
    stdio_env['PYTHONUNBUFFERED'] = '1'

    variants: List[Variant] = []

    # Python -> Python
    def py_piped(flag: str) -> Dict:
        producer_stderr, consumer_stderr = _run_piped(
            producer_cmd=[
                sys.executable, 'producer.py', flag,
                '--duration', str(duration),
                '--min-size', str(min_size),
                '--max-size', str(max_size),
                '--quiet',
            ],
            consumer_cmd=[
                sys.executable, 'consumer.py', flag,
                '--quiet',
            ],
            timeout_s=timeout_s,
            cwd=MYDIR,
            producer_env=stdio_env,
        )
        return _stats_from_stderr(producer_stderr, consumer_stderr)

    variants.append(Variant('stdio_raw', 'Python STDIO (raw)', lambda: py_piped('--stdio')))
    variants.append(Variant('stdio_api', 'Python STDIO (API)', lambda: py_piped('--stdio-api')))

    def py_shm() -> Dict:
        shm_name = f"bench_{os.getpid()}_{int(time.time() * 1000)}"
        producer_stderr, consumer_stderr = _run_two_processes(
            consumer_cmd=[
                sys.executable, 'consumer.py',
                '--shm', shm_name,
                '--quiet',
            ],
            producer_cmd=[
                sys.executable, 'producer.py',
                '--shm', shm_name,
                '--duration', str(duration),
                '--min-size', str(min_size),
                '--max-size', str(max_size),
                '--quiet',
            ],
            timeout_s=timeout_s,
            cwd=MYDIR,
            consumer_first=True,
            startup_delay_s=0.1,
        )
        return _stats_from_stderr(producer_stderr, consumer_stderr)

    variants.append(Variant('shm', 'Python Shared Memory', py_shm))

    if not include_cpp:
        return variants

    cpp_producer = get_cpp_executable('cpp_producer')
    cpp_consumer = get_cpp_executable('cpp_consumer')

    # C++ -> C++
    def cpp_stdio() -> Dict:
        producer_stderr, consumer_stderr = _run_piped(
            producer_cmd=[
                str(cpp_producer), '--stdio',
                '--duration', str(duration),
                '--min-size', str(min_size),
                '--max-size', str(max_size),
                '--quiet',
            ],
            consumer_cmd=[
                str(cpp_consumer), '--stdio',
                '--quiet',
            ],
            timeout_s=timeout_s,
            cwd=MYDIR,
        )
        return _stats_from_stderr(producer_stderr, consumer_stderr)

    variants.append(Variant('cpp_stdio', 'C++ STDIO (API)', cpp_stdio))

    def cpp_shm() -> Dict:
        shm_name = f"bench_cpp_{os.getpid()}_{int(time.time() * 1000)}"
        producer_stderr, consumer_stderr = _run_two_processes(
            consumer_cmd=[
                str(cpp_consumer), '--shm', shm_name,
                '--quiet',
            ],
            producer_cmd=[
                str(cpp_producer), '--shm', shm_name,
                '--duration', str(duration),
                '--min-size', str(min_size),
                '--max-size', str(max_size),
                '--quiet',
            ],
            timeout_s=timeout_s,
            cwd=MYDIR,
            consumer_first=True,
            startup_delay_s=0.1,
        )
        return _stats_from_stderr(producer_stderr, consumer_stderr)

    variants.append(Variant('cpp_shm', 'C++ Shared Memory', cpp_shm))

    # Python -> C++
    def py_cpp_stdio() -> Dict:
        producer_stderr, consumer_stderr = _run_piped(
            producer_cmd=[
                sys.executable, 'producer.py', '--stdio-api',
                '--duration', str(duration),
                '--min-size', str(min_size),
                '--max-size', str(max_size),
                '--quiet',
            ],
            consumer_cmd=[
                str(cpp_consumer), '--stdio', '--quiet',
            ],
            timeout_s=timeout_s,
            cwd=MYDIR,
            producer_env=stdio_env,
        )
        return _stats_from_stderr(producer_stderr, consumer_stderr)

    variants.append(Variant('py_cpp_stdio', 'Python -> C++ STDIO (API)', py_cpp_stdio))

    def py_cpp_shm() -> Dict:
        shm_name = f"bench_py_cpp_{os.getpid()}_{int(time.time() * 1000)}"
        producer_stderr, consumer_stderr = _run_two_processes(
            consumer_cmd=[
                str(cpp_consumer), '--shm', shm_name, '--quiet',
            ],
            producer_cmd=[
                sys.executable, 'producer.py',
                '--shm', shm_name,
                '--duration', str(duration),
                '--min-size', str(min_size),
                '--max-size', str(max_size),
                '--quiet',
            ],
            timeout_s=timeout_s,
            cwd=MYDIR,
            consumer_first=True,
            startup_delay_s=0.1,
        )
        return _stats_from_stderr(producer_stderr, consumer_stderr)

    variants.append(Variant('py_cpp_shm', 'Python -> C++ Shared Memory', py_cpp_shm))

    return variants


def main() -> None:
    parser = argparse.ArgumentParser(description='Run IPC benchmarks')
    parser.add_argument('--runs', type=int, default=3,
                        help='Number of runs per benchmark (default: 3)')
    parser.add_argument('--duration', type=float, default=20.0,
                        help='Duration per run in seconds (default: 20)')
    parser.add_argument('--min-size', type=int, default=512 * 1024,
                        help='Minimum payload size in bytes (default: 512KB)')
    parser.add_argument('--max-size', type=int, default=5 * 1024 * 1024,
                        help='Maximum payload size in bytes (default: 5MB)')
    # C++ benchmarks are enabled by default.
    parser.add_argument('--no-cpp', action='store_true',
                        help='Disable C++ benchmarks (Python-only)')
    # Backward-compat alias; has no effect because C++ is on by default.
    parser.add_argument('--include-cpp', action='store_true', help=argparse.SUPPRESS)

    args = parser.parse_args()

    print("=" * 80)
    print("IPC BENCHMARK: STDIO vs Shared Memory")
    print("=" * 80)
    print(f"  Runs: {args.runs}")
    print(f"  Duration: {args.duration:.1f}s")
    print(f"  Payload size range: {args.min_size / 1024:.0f}KB - {args.max_size / (1024**2):.1f}MB")
    include_cpp = not bool(args.no_cpp)
    print(f"  Include C++: {include_cpp}")
    print()

    try:
        variants = _build_variants(
            duration=args.duration,
            min_size=args.min_size,
            max_size=args.max_size,
            include_cpp=include_cpp,
        )
    except FileNotFoundError as exc:
        print(f"Warning: C++ benchmark binaries missing; running Python-only. ({exc})", file=sys.stderr)
        variants = _build_variants(
            duration=args.duration,
            min_size=args.min_size,
            max_size=args.max_size,
            include_cpp=False,
        )

    results_by_key: Dict[str, List[Dict]] = {v.key: [] for v in variants}
    for variant in variants:
        print(f"Running {variant.label} benchmarks...")
        for i in range(args.runs):
            print(f"  Run {i+1}/{args.runs}...", end=' ', flush=True)
            result = variant.run_once()
            results_by_key[variant.key].append(result)
            print(
                f"Producer: {result['producer_throughput_mbps']:.2f} MB/s, "
                f"Consumer: {result['consumer_throughput_mbps']:.2f} MB/s"
            )
        print()

    # Summary
    stats_by_key: Dict[str, Dict[str, float]] = {}
    for variant in variants:
        stats_by_key[variant.key] = summarize_variant(variant.label, results_by_key[variant.key])

    stdio_raw_stats = stats_by_key.get('stdio_raw')
    stdio_api_stats = stats_by_key.get('stdio_api')
    shm_stats = stats_by_key.get('shm')

    speedup_vs_raw = 0.0
    speedup_vs_api = 0.0
    if shm_stats and stdio_raw_stats and stdio_raw_stats['consumer_mean'] > 0:
        speedup_vs_raw = shm_stats['consumer_mean'] / stdio_raw_stats['consumer_mean']
    if shm_stats and stdio_api_stats and stdio_api_stats['consumer_mean'] > 0:
        speedup_vs_api = shm_stats['consumer_mean'] / stdio_api_stats['consumer_mean']

    print("\nSpeedups:")
    print(f"  SHM vs STDIO (raw): {speedup_vs_raw:.2f}x")
    print(f"  SHM vs STDIO (API): {speedup_vs_api:.2f}x")

    # JSON output
    def pack_entry(key: str) -> Dict:
        runs = results_by_key.get(key, [])
        entry: Dict = {
            'runs': runs,
            'available': bool(runs),
        }
        stats = stats_by_key.get(key)
        if stats:
            entry.update({
                'producer_mean': stats['producer_mean'],
                'producer_stddev': stats['producer_stddev'],
                'consumer_mean': stats['consumer_mean'],
                'consumer_stddev': stats['consumer_stddev'],
            })
        return entry

    results = {
        'config': {
            'runs': args.runs,
            'duration': args.duration,
            'min_size': args.min_size,
            'max_size': args.max_size,
            'include_cpp': include_cpp,
        },
        'stdio_raw': pack_entry('stdio_raw'),
        'stdio_api': pack_entry('stdio_api'),
        'shm': pack_entry('shm'),
        'cpp_stdio': pack_entry('cpp_stdio'),
        'cpp_shm': pack_entry('cpp_shm'),
        'py_cpp_stdio': pack_entry('py_cpp_stdio'),
        'py_cpp_shm': pack_entry('py_cpp_shm'),
        'speedup_vs_stdio_raw': speedup_vs_raw,
        'speedup_vs_stdio_api': speedup_vs_api,
    }

    timestamp = datetime.now().strftime("%y%m%d-%H%M%S")
    output_file = MYDIR / f"benchmark_results_{timestamp}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)

    print(f"\nSaved results to {output_file}")


if __name__ == '__main__':
    main()
