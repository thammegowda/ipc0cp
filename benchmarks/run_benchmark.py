#!/usr/bin/env python3
"""
Run benchmarks comparing STDIO vs Shared Memory performance.
"""

import argparse
import fnmatch
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

# All available benchmark settings (used for -s/--settings matching)
ALL_SETTINGS = [
    # Python baselines (single producer, single consumer)
    'py-stdio-raw-p1-c1',
    'py-stdio-api-p1-c1',
    'py-shm-p1-c1',
    
    # C++ baselines (single producer, single consumer)
    'cpp-stdio-p1-c1',
    'cpp-shm-p1-c1',
    
    # Cross-language (single producer, single consumer)
    'py-cpp-stdio-p1-c1',
    'py-cpp-shm-p1-c1',
    
    # MPMC variants generated dynamically: {lang}-shm-p{P}-c{C}
]


def _parse_mpmc_scenarios(spec: str) -> List[tuple[int, int]]:
    """Parse MPMC scenario list.

    Format: comma-separated entries, each either "P×C" (e.g. "2x2") or "P:C".
    """
    scenarios: List[tuple[int, int]] = []
    spec = (spec or "").strip()
    if not spec:
        return scenarios

    for raw in spec.split(','):
        item = raw.strip().lower()
        if not item:
            continue
        if 'x' in item:
            left, right = item.split('x', 1)
        elif ':' in item:
            left, right = item.split(':', 1)
        else:
            raise ValueError(f"Invalid MPMC scenario '{raw}'. Use like '2x2' or '2:2'.")

        producers = int(left.strip())
        consumers = int(right.strip())
        if producers <= 0 or consumers <= 0:
            raise ValueError(f"Invalid MPMC scenario '{raw}': counts must be > 0")
        scenarios.append((producers, consumers))

    return scenarios


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

    if producer.returncode != 0:
        raise RuntimeError(
            f"Producer failed (exit {producer.returncode}). Stderr:\n{producer_stderr_b.decode()}"
        )
    if consumer.returncode != 0:
        raise RuntimeError(
            f"Consumer failed (exit {consumer.returncode}). Stderr:\n{consumer_stderr_b.decode()}"
        )

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

    if producer.returncode != 0:
        raise RuntimeError(
            f"Producer failed (exit {producer.returncode}). Stderr:\n{producer_stderr_b.decode()}"
        )
    if consumer.returncode != 0:
        raise RuntimeError(
            f"Consumer failed (exit {consumer.returncode}). Stderr:\n{consumer_stderr_b.decode()}"
        )

    return producer_stderr_b.decode(), consumer_stderr_b.decode()


def _run_many_processes(
    *,
    consumer_cmds: List[List[str]],
    producer_cmds: List[List[str]],
    timeout_s: float,
    cwd: Path,
    consumer_first: bool = True,
    startup_delay_s: float = 0.1,
) -> tuple[List[str], List[str]]:
    """Run multiple consumers and producers as separate processes.

    Returns:
        (producer_stderrs, consumer_stderrs)
    """

    def _start(cmd: List[str]) -> subprocess.Popen:
        return subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(cwd),
        )

    consumers: List[subprocess.Popen] = []
    producers: List[subprocess.Popen] = []

    try:
        if consumer_first:
            consumers = [_start(cmd) for cmd in consumer_cmds]
            time.sleep(startup_delay_s)
            producers = [_start(cmd) for cmd in producer_cmds]
        else:
            producers = [_start(cmd) for cmd in producer_cmds]
            time.sleep(startup_delay_s)
            consumers = [_start(cmd) for cmd in consumer_cmds]

        producer_stderrs: List[str] = []
        consumer_stderrs: List[str] = []

        for p in producers:
            _out, err = p.communicate(timeout=timeout_s)
            producer_stderrs.append(err.decode())
            if p.returncode != 0:
                raise RuntimeError(
                    f"Producer failed (exit {p.returncode}). Stderr:\n{producer_stderrs[-1]}"
                )
        for c in consumers:
            _out, err = c.communicate(timeout=timeout_s)
            consumer_stderrs.append(err.decode())
            if c.returncode != 0:
                raise RuntimeError(
                    f"Consumer failed (exit {c.returncode}). Stderr:\n{consumer_stderrs[-1]}"
                )

        return producer_stderrs, consumer_stderrs
    except subprocess.TimeoutExpired:
        for p in producers:
            _terminate_process(p)
        for c in consumers:
            _terminate_process(c)
        raise


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
        'producer_elapsed_s': producer_stats.get('elapsed_s', 0.0),
        'consumer_elapsed_s': consumer_stats.get('elapsed_s', 0.0),
    }


def _aggregate_stats_from_stderr_list(stderrs: List[str]) -> Dict[str, float]:
    parsed = [parse_stats(s) for s in stderrs]
    total_bytes = sum(p.get('bytes', 0) for p in parsed)
    total_messages = sum(p.get('messages', 0) for p in parsed)
    elapsed_s = max((p.get('elapsed_s', 0.0) for p in parsed), default=0.0)
    throughput_mbps = (total_bytes / (1024.0 * 1024.0)) / elapsed_s if elapsed_s > 0 else 0.0
    return {
        'bytes': float(total_bytes),
        'messages': float(total_messages),
        'elapsed_s': float(elapsed_s),
        'throughput_mbps': float(throughput_mbps),
    }


def _stats_from_many_stderr(producer_stderrs: List[str], consumer_stderrs: List[str]) -> Dict:
    producer = _aggregate_stats_from_stderr_list(producer_stderrs)
    consumer = _aggregate_stats_from_stderr_list(consumer_stderrs)
    return {
        'producer_throughput_mbps': producer['throughput_mbps'],
        'consumer_throughput_mbps': consumer['throughput_mbps'],
        'producer_bytes': int(producer['bytes']),
        'consumer_bytes': int(consumer['bytes']),
        'producer_messages': int(producer['messages']),
        'consumer_messages': int(consumer['messages']),
        'producer_elapsed_s': producer['elapsed_s'],
        'consumer_elapsed_s': consumer['elapsed_s'],
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
        'elapsed_s': 0.0,
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
        elif 'Elapsed time:' in line:
            parts = line.split(':', 1)[1].strip().split()
            # Usually: "<seconds> seconds"
            try:
                stats['elapsed_s'] = float(parts[0])
            except Exception:
                pass
    
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
    mpmc_scenarios: List[tuple[int, int]],
    skip_cpp: bool = False,
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

    variants.append(Variant('py-stdio-raw-p1-c1', 'Python STDIO (raw)', lambda: py_piped('--stdio')))
    variants.append(Variant('py-stdio-api-p1-c1', 'Python STDIO (API)', lambda: py_piped('--stdio-api')))

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

    variants.append(Variant('py-shm-p1-c1', 'Python Shared Memory', py_shm))

    # Python SHM MPMC scenarios (separate processes, aggregate stats)
    for producers, consumers in mpmc_scenarios:
        key = f"py-shm-p{producers}-c{consumers}"
        label = f"Python Shared Memory MPMC (P={producers}, C={consumers})"

        def _mk_py_mpmc_run(p: int, c: int) -> Callable[[], Dict]:
            def _run() -> Dict:
                shm_name = f"bench_mpmc_py_p{p}_c{c}_{os.getpid()}_{int(time.time() * 1000)}"
                prod_stderrs, cons_stderrs = _run_many_processes(
                    consumer_cmds=[
                        [sys.executable, 'consumer.py', '--shm', shm_name, '--quiet']
                        for _ in range(c)
                    ],
                    producer_cmds=[
                        [
                            sys.executable,
                            'producer.py',
                            '--shm',
                            shm_name,
                            '--duration',
                            str(duration),
                            '--min-size',
                            str(min_size),
                            '--max-size',
                            str(max_size),
                            '--quiet',
                        ]
                        for _ in range(p)
                    ],
                    timeout_s=timeout_s,
                    cwd=MYDIR,
                    consumer_first=True,
                    startup_delay_s=0.1,
                )
                return _stats_from_many_stderr(prod_stderrs, cons_stderrs)

            return _run

        variants.append(Variant(key, label, _mk_py_mpmc_run(producers, consumers)))

    if skip_cpp:
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

    variants.append(Variant('cpp-stdio-p1-c1', 'C++ STDIO (API)', cpp_stdio))

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

    variants.append(Variant('cpp-shm-p1-c1', 'C++ Shared Memory', cpp_shm))

    # C++ SHM MPMC scenarios
    for producers, consumers in mpmc_scenarios:
        key = f"cpp-shm-p{producers}-c{consumers}"
        label = f"C++ Shared Memory MPMC (P={producers}, C={consumers})"

        def _mk_cpp_mpmc_run(p: int, c: int) -> Callable[[], Dict]:
            def _run() -> Dict:
                shm_name = f"bench_mpmc_cpp_p{p}_c{c}_{os.getpid()}_{int(time.time() * 1000)}"
                prod_stderrs, cons_stderrs = _run_many_processes(
                    consumer_cmds=[
                        [str(cpp_consumer), '--shm', shm_name, '--quiet']
                        for _ in range(c)
                    ],
                    producer_cmds=[
                        [
                            str(cpp_producer),
                            '--shm',
                            shm_name,
                            '--duration',
                            str(duration),
                            '--min-size',
                            str(min_size),
                            '--max-size',
                            str(max_size),
                            '--quiet',
                        ]
                        for _ in range(p)
                    ],
                    timeout_s=timeout_s,
                    cwd=MYDIR,
                    consumer_first=True,
                    startup_delay_s=0.1,
                )
                return _stats_from_many_stderr(prod_stderrs, cons_stderrs)

            return _run

        variants.append(Variant(key, label, _mk_cpp_mpmc_run(producers, consumers)))

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

    variants.append(Variant('py-cpp-stdio-p1-c1', 'Python -> C++ STDIO (API)', py_cpp_stdio))

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

    variants.append(Variant('py-cpp-shm-p1-c1', 'Python -> C++ Shared Memory', py_cpp_shm))

    # Python (multi-producer) -> C++ (single consumer) over SHM
    # Enabled when MPMC scenarios are requested (mpmc_scenarios non-empty).
    if mpmc_scenarios:
        def py_cpp_shm_p4_c1() -> Dict:
            shm_name = f"bench_py_cpp_mpmc_p4_c1_{os.getpid()}_{int(time.time() * 1000)}"
            prod_stderrs, cons_stderrs = _run_many_processes(
                consumer_cmds=[
                    [str(cpp_consumer), '--shm', shm_name, '--quiet'],
                ],
                producer_cmds=[
                    [
                        sys.executable,
                        'producer.py',
                        '--shm',
                        shm_name,
                        '--duration',
                        str(duration),
                        '--min-size',
                        str(min_size),
                        '--max-size',
                        str(max_size),
                        '--quiet',
                    ]
                    for _ in range(4)
                ],
                timeout_s=timeout_s,
                cwd=MYDIR,
                consumer_first=True,
                startup_delay_s=0.1,
            )
            return _stats_from_many_stderr(prod_stderrs, cons_stderrs)

        variants.append(
            Variant(
                'py-cpp-shm-p4-c1',
                'Python -> C++ Shared Memory MPMC (P=4, C=1)',
                py_cpp_shm_p4_c1,
            )
        )

    return variants



def mem_size(str) -> int:
    """Parse memory size string with optional suffixes (K, M, G)."""
    str = str.strip().upper()
    if str.endswith('G'):
        return int(float(str[:-1]) * 1024 * 1024 * 1024)
    elif str.endswith('M'):
        return int(float(str[:-1]) * 1024 * 1024)
    elif str.endswith('K'):
        return int(float(str[:-1]) * 1024)
    else:
        try:
            return int(str)
        except ValueError:
            raise ValueError(f"Invalid memory size: {str}; expect integer with optional K, M, G suffix.")

def main() -> None:
    epilog = (
        "Available settings:\n"
        + "\n".join(f"  {s}" for s in ALL_SETTINGS)
        + "\n\nUse glob patterns to select: -s 'py-*' or -s 'cpp-*' or -s 'mpmc-*'"
        + "\nMPMC variants are generated dynamically based on --scenarios."
    )
    
    parser = argparse.ArgumentParser(
        description='Run IPC benchmarks',
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--runs', type=int, default=3,
                        help='Number of runs per benchmark (default: 3)')
    parser.add_argument('--duration', type=float, default=20.0,
                        help='Duration per run in seconds (default: 20)')
    parser.add_argument('--min-size', type=mem_size, default=512 * 1024,
                        help='Minimum payload size in bytes (default: 512KB); supports K, M, G suffixes')
    parser.add_argument('--max-size', type=mem_size, default=5 * 1024 * 1024,
                        help='Maximum payload size in bytes (default: 5MB); supports K, M, G suffixes')

    parser.add_argument(
        '-s',
        '--settings',
        nargs='+',
        default=['all'],
        help=(
            "Which benchmark settings to run (multi-value, glob patterns supported). "
            "Default: all. Examples: -s py-* cpp-* | -s xlang-* | -s mpmc-*"
        ),
    )

    parser.add_argument(
        '--scenarios',
        type=str,
        default='2x2,4x4',
        help="MPMC scenarios like '2x2,4x4' (used when settings include 'mpmc' / '*-mpmc')",
    )

    args = parser.parse_args()

    print("=" * 80)
    print("IPC BENCHMARK: STDIO vs Shared Memory")
    print("=" * 80)
    print(f"  Runs: {args.runs}")
    print(f"  Duration: {args.duration:.1f}s")
    print(f"  Payload size range: {args.min_size / 1024:.0f}KB - {args.max_size / (1024**2):.1f}MB")

    settings_patterns = list(args.settings or ['all'])
    if settings_patterns != ['all']:
        print(f"  Settings: {', '.join(settings_patterns)}")
    
    # Always parse scenarios - MPMC variants will be built if scenarios are specified
    mpmc_scenarios: List[tuple[int, int]] = _parse_mpmc_scenarios(args.scenarios)
    if mpmc_scenarios:
        print(f"  Scenarios: {args.scenarios}")
    print()

    try:
        variants = _build_variants(
            duration=args.duration,
            min_size=args.min_size,
            max_size=args.max_size,
            mpmc_scenarios=mpmc_scenarios,
        )
    except FileNotFoundError as exc:
        print(f"Warning: C++ benchmark binaries missing; running Python-only. ({exc})", file=sys.stderr)
        variants = _build_variants(
            duration=args.duration,
            min_size=args.min_size,
            max_size=args.max_size,
            mpmc_scenarios=mpmc_scenarios,
            skip_cpp=True,
        )

    # Build the actual settings list from generated variants (includes dynamic MPMC tags)
    available_settings = sorted({v.key for v in variants})

    # Expand patterns: 'all' means all, otherwise use fnmatch
    if settings_patterns == ['all'] or 'all' in settings_patterns:
        selected_settings = set(available_settings)
    else:
        selected_settings: set[str] = set()
        for pat in settings_patterns:
            matches = {s for s in available_settings if fnmatch.fnmatch(s, pat)}
            if not matches:
                raise SystemExit(
                    f"No settings match pattern '{pat}'.\n\nAvailable settings:\n  "
                    + "\n  ".join(available_settings)
                )
            selected_settings |= matches

    variants = [v for v in variants if v.key in selected_settings]
    if not variants:
        raise SystemExit(
            "No benchmarks selected.\n\nAvailable settings:\n  "
            + "\n  ".join(available_settings)
        )

    # Display matched settings
    print("Matched settings:")
    for setting in sorted(selected_settings):
        print(f"  {setting}")
    print()

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

    stdio_raw_stats = stats_by_key.get('py-stdio-raw-p1-c1')
    stdio_api_stats = stats_by_key.get('py-stdio-api-p1-c1')
    shm_stats = stats_by_key.get('py-shm-p1-c1')

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
            'settings': settings_patterns,
            'scenarios': args.scenarios if mpmc_scenarios else '',
        },
        'py-stdio-raw-p1-c1': pack_entry('py-stdio-raw-p1-c1'),
        'py-stdio-api-p1-c1': pack_entry('py-stdio-api-p1-c1'),
        'py-shm-p1-c1': pack_entry('py-shm-p1-c1'),
        'cpp-stdio-p1-c1': pack_entry('cpp-stdio-p1-c1'),
        'cpp-shm-p1-c1': pack_entry('cpp-shm-p1-c1'),
        'py-cpp-stdio-p1-c1': pack_entry('py-cpp-stdio-p1-c1'),
        'py-cpp-shm-p1-c1': pack_entry('py-cpp-shm-p1-c1'),
        'mpmc': {
            k: pack_entry(k)
            for k in results_by_key.keys()
            if '-p' in k and '-c' in k and not k.endswith('-p1-c1')
        },
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
