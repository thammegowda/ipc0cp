# Benchmarks

This directory contains benchmarking tools to compare the performance of shared memory IPC against STDIN/STDOUT baseline.

## Features

- **End-of-Stream Signaling**: Producer explicitly signals completion with EOS marker (payload_size=0)
- **Exception-Based Error Handling**: Consumer throws `RingBufferException` for errors, returns `None` only for clean EOS
- **Pure Payload Measurement**: Throughput measures only actual data, excluding metadata/sentinels/overhead
- **Statistical Analysis**: Multiple runs with mean/stddev reporting

## Files

- **producer.py**: Sends random bytes via STDIO or shared memory
- **consumer.py**: Receives random bytes via STDIO or shared memory  
- **run_benchmark.py**: Orchestrates benchmark runs and reports statistics
- **quick_test.sh**: Quick 5-second test to verify everything works

## Usage

### Quick Test

Verify the benchmark suite works (5 second test):

```bash
cd benchmarks
chmod +x quick_test.sh
./quick_test.sh
```

### Quick Start

Run full benchmark suite (3 runs of 60 seconds each for both STDIO and shared memory):

```bash
cd benchmarks
python run_benchmark.py
```

### Custom Configuration

```bash
# Run 5 times with 30-second duration per run
python run_benchmark.py --runs 5 --duration 30

# Use different payload sizes (1MB to 10MB)
python run_benchmark.py --min-size 1048576 --max-size 10485760

# Short test run (1 run, 10 seconds)
python run_benchmark.py --runs 1 --duration 10
```

### Manual Testing

Test STDIO mode:
```bash
python producer.py --stdio --duration 10 | python consumer.py --stdio --duration 15
```

Test shared memory mode:
```bash
# Terminal 1 (consumer)
python consumer.py --shm test_bench --duration 15

# Terminal 2 (producer)
python producer.py --shm test_bench --duration 10
```

## Metrics

The benchmark measures:

- **Throughput (MB/s)**: Data transfer rate excluding metadata overhead
- **Bytes transferred**: Total payload bytes (no headers, sentinels, metadata)
- **Messages**: Number of individual payloads sent/received
- **Mean and StdDev**: Statistical summary across multiple runs
- **Speedup**: How much faster shared memory is compared to STDIO

## Default Parameters

- Payload size: 512KB to 5MB (randomized per message)
- Buffer size: 2GB shared memory
- Duration: 60 seconds per run
- Runs: 3 iterations per benchmark
- Timeout: 5 seconds for blocking operations

## Output

Results are displayed on screen and saved to JSON:
```
benchmark_results_<timestamp>.json
```

The JSON file contains detailed statistics for all runs, making it easy to analyze or visualize the data later.
