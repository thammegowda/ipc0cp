# ipc0cp

Zero-copy (0CP) inter-process communication (IPC): A library for exchanging data between Python and C++ using shared memory.

## Overview

`ipc0cp` is a library that enables efficient data exchange between Python and C++ processes using shared memory for zero-copy inter-process communication. This approach minimizes overhead and maximizes performance when transferring data between processes written in different languages.

## Features

- **Zero-copy data transfer**: Uses shared memory to avoid expensive data copying
- **Lock-free ring buffer**: Single-producer/single-consumer design for high performance
- **Variable-size slots**: Efficient memory usage with support for objects of different sizes
- **Generic object support**: Exchange NumPy arrays, PIL Images, text, JSON, and raw bytes
- **Blocking/non-blocking modes**: Configurable wait behavior for producer and consumer
- **Simple API**: Easy-to-use Python interface with C++20 consumer implementation
- **Cross-language IPC**: Python producer can communicate with C++ consumer and vice versa

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
│       └── tests/           # Test suite
│           └── test_ring_buffer.py
├── cpp/
│   └── ipc0cp/              # C++ library (future)
│       ├── ipc.hpp          # C++ header files
│       └── ipc.cpp          # C++ implementation
├── examples/                # Example scripts
│   ├── producer_example.py
│   ├── consumer_example.py
│   └── README.md
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
