# Cross-Language IPC Test: Python Producer → C++ Consumer

This directory contains a test demonstrating inter-process communication between Python and C++.

## Overview

- **Python Producer**: Creates a shared memory ring buffer and pushes various object types
- **C++ Consumer**: Attaches to the shared memory and consumes objects

## Supported Object Types

The test demonstrates all supported object types:

1. **NumPy Arrays** - Multi-dimensional numerical data
2. **Text Strings** - UTF-8 encoded text
3. **JSON Objects** - Dictionaries, lists, and primitive types
4. **PIL Images** - Python Imaging Library images
5. **Raw Bytes** - Binary data

## Building the C++ Consumer

```bash
cd /home/tg/work/me/ipc0cp
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON
make
```

This will create the `test_consumer` executable in the build directory.

## Running the Test

### Terminal 1: Start the C++ Consumer

```bash
cd /home/tg/work/me/ipc0cp/build
./test_consumer --shm test_py_cpp_ipc --buffer-size 50
```

The consumer will wait for the producer to create the shared memory.

### Terminal 2: Start the Python Producer

```bash
cd /home/tg/work/me/ipc0cp
python python/ipc0cp/tests/test_py_producer_cpp_consumer.py --shm test_py_cpp_ipc -n 20 --buffer-size 50
```

## Command Line Arguments

### C++ Consumer (`test_consumer`)

- `-s, --shm <name>`: Shared memory segment name (default: `test_py_cpp_ipc`)
- `--buffer-size <MB>`: Buffer size in megabytes (default: 50)

### Python Producer (`test_py_producer_cpp_consumer.py`)

- `-s, --shm <name>`: Shared memory segment name (default: `test_py_cpp_ipc`)
- `-n, --num-objects <count>`: Number of objects to send (default: 20)
- `--buffer-size <MB>`: Buffer size in megabytes (default: 50)

## Expected Output

### C++ Consumer

```
C++ Consumer starting...
Shared memory name: test_py_cpp_ipc
Buffer size: 50 MB
Waiting for shared memory to be created by producer...
Successfully attached to shared memory!
Waiting for objects...

Received object:
  Type: 0
  Payload size: 120000 bytes
  Metadata:
    type: ndarray
    shape: (100, 100, 3)
    dtype: float32
  NumPy array: shape=(100, 100, 3), dtype=float32

Received object:
  Type: 2
  Payload size: 28 bytes
  Metadata:
    type: text
  Content: Hello from Python! Message #1

...

Consumed 20 objects in 234 ms
Average: 11.70 ms/object
```

### Python Producer

```
Python Producer starting...
Shared memory name: test_py_cpp_ipc
Buffer size: 50 MB
Number of objects: 20
Created shared memory
Pushing 20 objects...

[1/20] Pushed: NumPy array (100, 100, 3)
[2/20] Pushed: Text: Hello from Python! Message #1
[3/20] Pushed: JSON: {'message': 'Object #2', 'timestamp': 1703516789.123, 'data': [1, 2, 3, 4, 5]}
[4/20] Pushed: PIL Image (200, 150) mode=RGB
[5/20] Pushed: Bytes: 1000 bytes

...

Pushed 20 objects in 0.45 seconds
Average: 22.50 ms/object
```

## Implementation Details

### C++20 Features Used

- `std::optional<T>` - For nullable return values
- `std::variant<T, E>` - For error handling
- `std::chrono` - For timeouts and timing
- `std::map` and modern standard library containers
- Structured bindings - Convenient unpacking of key-value pairs
- Range-based for loops with structured bindings

### Memory Layout Compatibility

Both Python and C++ implementations use identical memory layouts:

- **Header (24 bytes)**: `write_pos | read_pos | total_data_bytes`
- **Slot Structure**: `next_pos(8) | metadata_size(4) | payload_size(8) | metadata_json | payload`
- **Little-endian**: All multi-byte integers stored in little-endian format

### Thread Safety

- Single producer / single consumer design (lock-free)
- Atomic-like updates via proper position management
- No explicit synchronization primitives needed

## Troubleshooting

### "Failed to attach to shared memory"

- Make sure the Python producer is running first, or
- Start the C++ consumer first (it will wait up to 10 seconds for the producer)

### Buffer size mismatch errors

- Ensure both producer and consumer use the same `--buffer-size` value

### Shared memory cleanup

If tests crash, you may need to manually clean up shared memory:

```bash
# List shared memory segments
ls -la /dev/shm/

# Remove specific segment
rm /dev/shm/test_py_cpp_ipc
```

## Performance Notes

- Zero-copy transfer: Data is written once to shared memory
- Typical latency: 10-20 ms per object (including serialization)
- Throughput depends on object size and system memory bandwidth
- No copying between processes - only pointer arithmetic and metadata parsing
