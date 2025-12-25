# SharedRingBuffer Test Suite

This directory contains the comprehensive test suite for the SharedRingBuffer IPC library.

## Test Organization

The tests are organized into themed modules for better maintainability:

### `test_basic.py` (5 tests)
Basic functionality tests:
- `TestSharedRingBufferBasic` (4 tests):
  - `test_create_and_attach`: Creating and attaching to shared memory
  - `test_push_pop_single_image`: Simple push/pop operations
  - `test_grayscale_image`: Grayscale (2D) image handling
  - `test_multiple_dtypes`: Different NumPy dtypes (uint8, int8, float32, etc.)

- `TestSharedRingBufferStats` (1 test):
  - `test_get_stats`: Buffer statistics and monitoring

### `test_variable_size.py` (3 tests)
Variable-size slot tests:
- `TestSharedRingBufferVariableSize`:
  - `test_mixed_size_images`: Images of different sizes (64x64 to 1024x1024)
  - `test_large_image`: Large images close to max_slot_size (~8 MB)
  - `test_oversized_image_rejected`: Rejection of oversized objects

### `test_circular.py` (2 tests)
Circular buffer wraparound tests:
- `TestSharedRingBufferCircular`:
  - `test_circular_wraparound`: Circular buffer wraparound behavior
  - `test_wraparound_during_write`: Mid-slot wraparound handling

### `test_blocking.py` (3 tests)
Blocking/non-blocking behavior tests:
- `TestSharedRingBufferBlocking`:
  - `test_buffer_full_blocking`: Producer blocking when buffer is full
  - `test_buffer_empty_nonblocking`: Consumer returns None when empty (non-blocking)
  - `test_buffer_empty_blocking_timeout`: Consumer timeout when empty (blocking)

### `test_object_types.py` (10 tests)
Generic object type tests:
- `TestSharedRingBufferGenericObjects` (6 tests):
  - `test_text_strings`: Text string handling (unicode, multi-byte)
  - `test_json_objects`: JSON-serializable objects (dict, list, primitives)
  - `test_raw_bytes`: Raw bytes handling
  - `test_pil_images`: PIL Image objects (RGB, grayscale)
  - `test_mixed_object_types`: Mixed object types in sequence
  - `test_metadata_size_limit`: Metadata size limit (1024 bytes)

- `TestSharedRingBufferPillow` (2 tests):
  - `test_pillow_random_images`: Random Pillow-generated images
  - `test_pillow_grayscale`: Pillow grayscale images

### `test_multiprocess.py` (3 tests)
Multiprocess and subprocess IPC tests:
- `TestSharedRingBufferMultiprocess` (2 tests):
  - `test_separate_processes`: Producer/consumer in separate processes (multiprocessing)
  - `test_high_throughput`: High throughput test (100 images)

- `TestSharedRingBufferSubprocessIPC` (1 test):
  - `test_subprocess_producer_consumer`: Real subprocess IPC using producer_example.py and consumer_example.py

## Example Scripts

The `tests/` directory also contains example scripts that can be run standalone or used in subprocess tests:

### `producer_example.py`
Producer script demonstrating IPC usage.

**Usage:**
```bash
python producer_example.py -s <shm_name> [-n NUM_OBJECTS] [--buffer-size MB]
```

**Arguments:**
- `-s, --shm`: Shared memory name (required)
- `-n, --num-objects`: Number of objects to send (default: 100)
- `--buffer-size`: Buffer size in MB (default: 100)

**Example:**
```bash
python producer_example.py -s example_buffer -n 50 --buffer-size 100
```

### `consumer_example.py`
Consumer script demonstrating IPC usage.

**Usage:**
```bash
python consumer_example.py -s <shm_name> [--buffer-size MB] [--wait-time SEC]
```

**Arguments:**
- `-s, --shm`: Shared memory name (required)
- `--buffer-size`: Buffer size in MB (default: 100)
- `--wait-time`: Wait time after last object in seconds (default: 0.5)

**Example:**
```bash
python consumer_example.py -s example_buffer --buffer-size 100 --wait-time 0.5
```

## Running Tests

### Run all tests:
```bash
pytest python/ipc0cp/tests/ -v
```

### Run specific test file:
```bash
pytest python/ipc0cp/tests/test_basic.py -v
```

### Run specific test:
```bash
pytest python/ipc0cp/tests/test_basic.py::TestSharedRingBufferBasic::test_push_pop_single_image -v
```

### Run with coverage:
```bash
pytest python/ipc0cp/tests/ --cov=python/ipc0cp --cov-report=html
```

## Test Configuration

- **Timeout**: 10 seconds per test (configured in `pytest.ini`)
- **Special timeout**: The subprocess IPC test has a 30-second timeout due to subprocess spawning overhead
- **Coverage**: Tests achieve ~90% code coverage

## Test Results

```
24 tests total
- Basic functionality: 5 tests
- Variable size: 3 tests  
- Circular buffer: 2 tests
- Blocking behavior: 3 tests
- Object types: 10 tests
- Multiprocess IPC: 3 tests

All tests passing ✓
Execution time: ~15 seconds
```

## Notes

- Tests use unique shared memory names to avoid conflicts
- Each test cleans up shared memory after completion
- Subprocess tests validate real IPC between separate Python processes
- Tests cover all supported object types: NumPy arrays, PIL Images, text, JSON, bytes
