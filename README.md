# ipc0cp

Zero-copy (0CP) inter-process communication (IPC): A library for exchanging data between Python and C++ using shared memory.

## Overview

`ipc0cp` is an IPC library focused on fast, practical data exchange between processes in **Python** and **C++**.

It ships two transports:

- **Shared memory ring buffer (SHM)**: the high-performance path. Payloads are exchanged via POSIX shared memory.
  This is the “0CP” transport (no stdin/stdout piping, no pipe-buffer copies).
- **STDIO framed transport**: *not* zero-copy. Included as a baseline and for convenience/portability.

## Features

- **SPSC lock-free design**: single producer + single consumer.
- **Variable-size slots**: payload sizes can vary per message.
- **Unified wire format**: both transports use JSON metadata + binary payload framing.
- **Generic object support**: bytes, UTF-8 text, JSON, NumPy arrays, PIL images, and heterogeneous lists via the `ListData` format (bytes, text, JSON, arrays, nested lists, etc.).
- **EOS marker**: producers can signal a clean end-of-stream.
- **Blocking / non-blocking**: optional timeouts for push/pop.
- **Data integrity checks**: sentinel bytes around payloads detect corruption/overruns.

For a deeper dive into the memory layout, see `ARCHITECTURE.md`.

## List payloads & custom serializers

- **List payloads**: Pushing a Python `list` (or building a C++ `ListData`) creates a `metadata` entry like `{"type": "list", "version": "1.0", "count": 3, "items": [ ... ]}`. Each child `items` record keeps the nested metadata plus a `payload_size`, while the slot payload is the concatenation of each serialized payload. Lists support mixed payloads (bytes, text, JSON, numpy, nested lists) but are capped at 10 items and 10 levels deep to keep buffer traversal predictable. Deserialization unpacks them back into native objects automatically, so consumers see a `list` of bytes/dicts/str/... just like the sender pushed.
- **Custom serializers**: Once registered, the `TypeRegistry` dispatches both directions.
  - Python: import `ipc0cp.type_registry.register_type` and provide a callable `(metadata: Dict[str, str], payload: bytes) -> SerializableObject`. The registry is used whenever metadata carries the matching `type` (and optional `version`).
  - C++: call `ipc0cp::TypeRegistry::instance().register_type("MyType", my_deserializer, "1.0")` where `my_deserializer` accepts the metadata map and payload bytes. The registry is seeded with the built-in handlers for `bytes`, `text`, `json`, `image`, `ndarray`, and `list`, so you can override or extend without breaking the wire format.

Unknown types simply log a warning and fall back to raw `BytesData`, ensuring the consumer never blocks even if a newer producer introduces an unrecognized type.

## Python API

The current public API is split into producer/consumer roles:

- SHM: `SharedRingBufferProducer` / `SharedRingBufferConsumer`
- STDIO: `StdioProducer` / `StdioConsumer`

### Shared memory quickstart

Producer (process 1):

```python
import numpy as np
from ipc0cp import SharedRingBufferProducer

producer = SharedRingBufferProducer(
    shm_name="my_buffer",
    total_data_bytes=256 * 1024 * 1024,
    blocking=True,
)

producer.push(np.random.randint(0, 255, (480, 640, 3), dtype=np.uint8))
producer.push({"msg": "hello", "id": 123})
producer.push(b"raw bytes")

producer.close()   # sends EOS
producer.unlink()  # delete shared memory segment
```

Consumer (process 2):

```python
from ipc0cp import SharedRingBufferConsumer

consumer = SharedRingBufferConsumer(
    shm_name="my_buffer",
    total_data_bytes=256 * 1024 * 1024,
    blocking=True,
    auto_attach=True,
    auto_unlink=False,
)

while True:
    obj = consumer.pop(timeout=5.0)
    if obj is None:  # EOS
        break
    print(type(obj))

consumer.close()
consumer.unlink()  # optional cleanup
```

### STDIO quickstart

```bash
python -c 'from ipc0cp import StdioProducer; p=StdioProducer(); p.push({"k": "v"}); p.close()' \
  | python -c 'from ipc0cp import StdioConsumer; c=StdioConsumer(); print(c.pop())'
```

## C++ API

The C++ library provides the same transport concepts and object model.

Consumer example (SHM):

```cpp
#include "ipc0cp/ring_buffer.hpp"
#include <chrono>

int main() {
  ipc0cp::SharedRingBufferConsumer consumer("my_buffer", 256ULL * 1024 * 1024);

  while (true) {
    auto obj = consumer.pop(std::chrono::milliseconds(5000));
    if (!obj) break; // EOS
    // obj->data is a polymorphic ipc0cp::SerializableObject
  }
}
```

## Installation

### Python

```bash
pip install -e .
pip install -e ".[dev]"
```

### C++ (library + tests)

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build
```

## Examples and tests

Python example scripts live in `python/tests/`:

```bash
# Terminal 1
python python/tests/consumer_example.py -s example_buffer --buffer-size 100

# Terminal 2
python python/tests/producer_example.py -s example_buffer -n 100 --buffer-size 100
```

## Benchmarks

The benchmark harness compares shared memory (SHM, “0CP”) vs STDIO baseline.
See `benchmarks/README.md` for the full methodology and the different variants.

```bash
python benchmarks/run_benchmark.py --duration 10
```

Example results (3 runs × 10s; payloads 512KB–5MB; payload-throughput only) from [benchmarks/benchmark_results_251226-235421.json](benchmarks/benchmark_results_251226-235421.json):
Benchmark results (3 runs; 30.0s; payloads 512KB–5MB; payload-throughput only):

| Variant | Producer mean (MB/s) | Consumer mean (MB/s) | Speedup |
|---|---:|---:|---:|
| Python STDIO (raw framing) | 342.45 | 342.38 | — |
| Python STDIO (API framing) | 339.90 | 339.86 | — |
| Python Shared Memory (SHM) | 497.20 | 502.49 | 1.48× |
| C++ STDIO (API framing) | 677.26 | 677.26 | — |
| C++ Shared Memory (SHM) | 2312.92 | 2299.72 | 3.40× |
| Python → C++ STDIO (API) | 329.39 | 311.55 | — |
| Python → C++ Shared Memory (SHM) | 351.53 | 464.10 | 1.49× |

Global speedups (consumer throughput): SHM vs STDIO (raw) **1.47×**, SHM vs STDIO (API) **1.48×**.

## Logging

```python
import ipc0cp
ipc0cp.enable_logging()
ipc0cp.set_log_level("INFO")
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Authors

- Thamme Gowda

## Acknowledgments

This project aims to provide a simple yet efficient solution for inter-process communication between Python and C++ applications.

## Future Work

- Evaluate integrating https://github.com/google/flatbuffers/ to add schema-driven, zero-copy serialization alongside the existing metadata-driven payloads.
