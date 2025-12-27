# ipc0cp

Zero-copy (0CP) inter-process communication (IPC): A library for exchanging data between Python and C++ using shared memory.

## Overview

`ipc0cp` is a library that enables efficient data exchange between processes (in Python and C++) using shared memory for zero-copy inter-process communication. This approach minimizes overhead and maximizes performance when transferring data between processes written in different languages. 
Also included STDIO (not zero-copy) for comparison and convenience.


## Features

- **Zero-copy data transfer**: Uses shared memory to avoid expensive data copying
- **Lock-free ring buffer**: Single-producer/single-consumer design for high performance
- **Variable-size slots**: Efficient memory usage with support for objects of different sizes
- **Generic object support**: Exchange NumPy arrays, PIL Images, text, JSON, and raw bytes
- **Blocking/non-blocking modes**: Configurable wait behavior for producer and consumer
- **Simple API**: Easy-to-use Python interface with C++20 consumer implementation
- **Cross-language IPC**: Python producer can communicate with C++ consumer and vice versa


### Benchmarks:
See `benchmarks/` dir for more info.
```
python benchmarks/run_benchmark.py --duration 10
================================================================================
IPC BENCHMARK: STDIO vs Shared Memory
================================================================================
  Runs: 3
  Duration: 10.0s
  Payload size range: 512KB - 5.0MB
  Include C++: True

Running Python STDIO (raw) benchmarks...
  Run 1/3... Producer: 247.72 MB/s, Consumer: 247.89 MB/s
  Run 2/3... Producer: 369.54 MB/s, Consumer: 369.76 MB/s
  Run 3/3... Producer: 328.05 MB/s, Consumer: 328.20 MB/s

Running Python STDIO (API) benchmarks...
  Run 1/3... Producer: 329.89 MB/s, Consumer: 329.63 MB/s
  Run 2/3... Producer: 379.15 MB/s, Consumer: 379.56 MB/s
  Run 3/3... Producer: 301.04 MB/s, Consumer: 301.27 MB/s

Running Python Shared Memory benchmarks...
  Run 1/3... Producer: 437.53 MB/s, Consumer: 451.92 MB/s
  Run 2/3... Producer: 521.08 MB/s, Consumer: 538.17 MB/s
  Run 3/3... Producer: 439.14 MB/s, Consumer: 454.27 MB/s

Running C++ STDIO (API) benchmarks...
  Run 1/3... Producer: 696.05 MB/s, Consumer: 696.05 MB/s
  Run 2/3... Producer: 702.02 MB/s, Consumer: 702.03 MB/s
  Run 3/3... Producer: 699.80 MB/s, Consumer: 699.79 MB/s

Running C++ Shared Memory benchmarks...
  Run 1/3... Producer: 2108.69 MB/s, Consumer: 2072.49 MB/s
  Run 2/3... Producer: 2251.24 MB/s, Consumer: 2212.95 MB/s
  Run 3/3... Producer: 2218.87 MB/s, Consumer: 2181.21 MB/s

Running Python -> C++ STDIO (API) benchmarks...
  Run 1/3... Producer: 318.34 MB/s, Consumer: 314.96 MB/s
  Run 2/3... Producer: 320.44 MB/s, Consumer: 317.51 MB/s
  Run 3/3... Producer: 373.52 MB/s, Consumer: 317.95 MB/s

Running Python -> C++ Shared Memory benchmarks...
  Run 1/3... Producer: 439.56 MB/s, Consumer: 427.48 MB/s
  Run 2/3... Producer: 429.56 MB/s, Consumer: 417.03 MB/s
  Run 3/3... Producer: 514.84 MB/s, Consumer: 431.06 MB/s
```


## Python Ring Buffer

The Python implementation provides a `SharedRingBuffer` class for inter-process communication:

### Supported Object Types

- **NumPy arrays** - Multi-dimensional arrays for images, tensors, scientific data
- **PIL Images** - Python Imaging Library images  
- **Text strings** - UTF-8 encoded text
- **JSON objects** - Dictionaries, lists, and primitive types
- **Raw bytes** - Binary data

### Quick Start

**Producer (Process 1):**
```python
from ipc0cp import SharedRingBuffer
import numpy as np

# Create ring buffer
buffer = SharedRingBuffer(
    shm_name="my_buffer",
    total_data_bytes=100 * 1024 * 1024,  # 100 MB
    create=True
)

# Push different types of objects
buffer.push(np.random.rand(480, 640, 3))  # NumPy array
buffer.push("Hello, World!")               # Text
buffer.push({"frame": 1, "data": [1,2,3]}) # JSON
buffer.push(b"Binary data")                # Bytes

buffer.close()
```

**Consumer (Process 2):**
```python
from ipc0cp import SharedRingBuffer

# Attach to existing buffer
buffer = SharedRingBuffer(
    shm_name="my_buffer",
    total_data_bytes=100 * 1024 * 1024,
    create=False
)

# Pop objects
while True:
    obj = buffer.pop(timeout=5.0)
    if obj is None:
        break
    print(f"Received: {type(obj)}")

buffer.close()
buffer.unlink()  # Cleanup
```

### Architecture

The ring buffer uses a hybrid linked-list design with data integrity checking:
- **Header (24 bytes)**: Contains `write_pos`, `read_pos`, and `total_data_bytes` for O(1) space checking
- **Variable slots**: Each slot contains:
  - **Slot header (20 bytes)**: `next_pos` (8 bytes), `metadata_size` (4 bytes), `payload_size` (8 bytes)
  - **Metadata**: JSON metadata (max 1024 bytes)
  - **Start sentinel (1 byte)**: Null byte (0x00) for integrity checking
  - **Payload**: Binary data
  - **End sentinel (1 byte)**: Null byte (0x00) for integrity checking
- **Sentinel bytes**: Null bytes before and after payload detect buffer overruns and data corruption
- **Circular buffer**: Automatic wraparound for continuous operation
- **Lock-free**: Single producer and single consumer operate without locks

### Memory Layout

```
[Header: write_pos | read_pos | total_data_bytes]
[Data Region: Slot₀ → Slot₁ → Slot₂ → ...]

Each Slot:
  next_pos (8 bytes)
  metadata_size (4 bytes)  
  payload_size (8 bytes)
  metadata_json (up to 1024 bytes)
  payload (variable size)
```

## Project Structure

```
ipc0cp/
├── python/
│   └── ipc0cp/              # Python package
│       ├── __init__.py      # Package initialization
│       ├── ring_buffer.py   # Lock-free ring buffer implementation
│       └ tests/           # Test suite
│         └── test_ring_buffer.py
├── cpp/
│   └── ipc0cp/              # C++ library (future)
│       ├── ipc.hpp          # C++ header files
│       └── ipc.cpp          # C++ implementation
├── CMakeLists.txt           # CMake build configuration
├── pyproject.toml           # Python project configuration
├── README.md                # This file
└── LICENSE                  # Apache 2.0 License
```

## Installation

### Python Package

```bash
# Install with dependencies
pip install -e .

# Or install with dev dependencies (for testing)
pip install -e ".[dev]"
```

### Dependencies

- **Python 3.8+**
- **NumPy** >= 1.20.0 - For array operations
- **Pillow** >= 9.0.0 - For image handling
- **pytest** >= 7.0.0 (dev) - For testing

## Testing

Run the test suite:

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run all tests
pytest python/ipc0cp/tests/

# Run with coverage
pytest python/ipc0cp/tests/ --cov=ipc0cp --cov-report=html

# Run specific test class
pytest python/ipc0cp/tests/test_ring_buffer.py::TestSharedRingBufferGenericObjects -v
```

## Logging

Both Python and C++ implementations include optional logging for debugging:

### Python

```python
import ipc0cp

# Enable INFO level logging to stderr
ipc0cp.enable_logging()

# Set specific log level
ipc0cp.set_log_level('DEBUG')  # DEBUG, INFO, WARNING, ERROR, CRITICAL

# Disable logging
ipc0cp.disable_logging()
```

### C++

```cpp
#include "ipc0cp/logger.hpp"

// Enable logging to stderr
ipc0cp::enable_logging();

// Disable logging  
ipc0cp::disable_logging();
```

**Note**: Logging is **disabled by default** to avoid polluting stdout/stderr in production use.

## Examples

See the `examples/` directory for complete working examples:

```bash
# Terminal 1
python examples/consumer_example.py

# Terminal 2  
python examples/producer_example.py
```

## Performance

The lock-free design achieves high throughput:
- **~1000+ objects/second** for mixed workloads
- **Sub-millisecond latency** for small objects
- **Efficient memory usage** with variable-size slots
- **No data copying** within shared memory region

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Authors

- Thamme Gowda

## Acknowledgments

This project aims to provide a simple yet efficient solution for inter-process communication between Python and C++ applications.
