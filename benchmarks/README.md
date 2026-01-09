# Benchmarks

This directory contains a benchmark harness to compare:

- **Shared memory (SHM)**: the “0CP” transport (data crosses processes via POSIX shared memory)
- **STDIO**: not zero-copy; included as a baseline

The benchmarks are intentionally simple: they send **random byte payloads** (no compression, no application logic) and report throughput.

## What’s being measured (and what isn’t)

- The reported throughput is **payload bytes only** (it excludes metadata and framing overhead).
- SHM avoids stdin/stdout piping and is meant to approximate a “zero-copy transport” baseline across processes.
- STDIO is not zero-copy and includes kernel buffering/copies; it’s here as a convenient baseline.

This benchmark does not attempt to model end-to-end application behavior (e.g., preprocessing, model inference). It’s a transport comparison.

## Files

- `producer.py`: produces random bytes over STDIO or SHM
- `consumer.py`: consumes random bytes over STDIO or SHM
- `run_benchmark.py`: orchestrates multiple variants and aggregates results
- `quick_test.sh`: quick sanity check run

## Quick start

```bash
cd benchmarks
python run_benchmark.py
```

Customize duration/runs/payload sizes:

```bash
python run_benchmark.py --runs 3 --duration 10
python run_benchmark.py --min-size 1048576 --max-size 10485760
```

Disable C++ variants (Python-only):

```bash
python run_benchmark.py --no-cpp
```

Enable additional multi-producer / multi-consumer (MPMC) SHM scenarios:

```bash
python run_benchmark.py --mpmc
python run_benchmark.py --mpmc --mpmc-scenarios 2x2,4x4
```

## Benchmark variants

When C++ binaries are available, the harness runs:

- **Python STDIO (raw)**: raw framing only (`[uint64 length][payload]`)
- **Python STDIO (API)**: uses `ipc0cp.StdioProducer` / `ipc0cp.StdioConsumer` (JSON metadata + payload framing)
- **Python Shared Memory**: uses `ipc0cp.SharedRingBufferProducer` / `ipc0cp.SharedRingBufferConsumer`
- **C++ STDIO (API)**: C++ producer/consumer using the same framed format
- **C++ Shared Memory**: C++ producer/consumer using the SHM ring buffer
- **Python -> C++ STDIO (API)**: Python producer piped into C++ consumer
- **Python -> C++ Shared Memory**: Python producer + C++ consumer via SHM

If the C++ binaries are missing, `run_benchmark.py` prints a warning and automatically falls back to Python-only.

## Building the C++ benchmark binaries

```bash
cd benchmarks
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

The harness expects binaries in `benchmarks/build/bin/`.

## Manual runs

Raw STDIO (Python → Python):

```bash
python producer.py --stdio --duration 10 | python consumer.py --stdio
```

Framed STDIO API (Python → Python):

```bash
python producer.py --stdio-api --duration 10 | python consumer.py --stdio-api
```

Shared memory (two processes):

```bash
# Terminal 1
python consumer.py --shm test_bench

# Terminal 2
python producer.py --shm test_bench --duration 10
```

## Output

Results are printed to the console and saved as JSON:

```
benchmark_results_<timestamp>.json
```

The JSON includes:

- config (runs, duration, min/max sizes, include_cpp)
- per-variant per-run stats (producer/consumer bytes/messages/throughput)
- summary stats (mean/stddev)
- speedups (SHM vs STDIO raw and SHM vs STDIO API)

## Notes and gotchas

- SHM uses a large shared memory segment (2GB by default in the benchmark scripts). Ensure you have enough shared memory available.
- If a run crashes, you may need to clean up leftover segments under `/dev/shm/` (Linux).

Lifecycle note (MPMC):

- Producers do not unlink shared memory; cleanup is owned by the last consumer.
