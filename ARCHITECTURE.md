# Architecture Overview

## High-Level Design

```
┌──────────────────────────────────────────────────────────────┐
│                     Shared Memory Region                     │
│                                                              │
│  ┌───────────────────────────────────────────────────────┐   │
│  │                 Header (64 bytes)                      │  │
│  │                                                       │   │
│  │  write_pos (uint64)   read_pos (uint64)   total_bytes  │  │
│  │  active_prod (uint32) active_cons (uint32) reserved    │  │
│  └───────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌───────────────────────────────────────────────────────┐   │
│  │              Data Region (Variable)                    │  │
│  │                                                       │   │
│  │   ┌─────────┐      ┌─────────┐      ┌─────────┐        │  │
│  │   │ Slot 0  │ ───> │ Slot 1  │ ───> │ Slot 2  │        │  │
│  │   └─────────┘      └─────────┘      └─────────┘        │  │
│  │        │                │                │             │  │
│  │   ┌────▼────────────────▼────────────────▼─────────┐   │  │
│  │   │   Circular linked list of variable slots       │   │  │
│  │   └───────────────────────────────────────────────┘   │   │
│  └───────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘

Producer (Process 1)                    Consumer (Process 2)
    │                                          │
    │  Writes at write_pos                     │  Reads at read_pos
    │  Updates write_pos ──────────────────────│───> Observes write_pos
    │                                          │  Updates read_pos
    │  Observes read_pos <─────────────────────│
    └──────────────────────────────────────────┘
```

## Slot Structure

```
┌──────────────────────────────────────────────────────┐
│                    Single Slot                        │
├──────────────────────────────────────────────────────┤
│  next_pos        (8 bytes)  ─────> Next slot position│
│  metadata_size   (4 bytes)  ─────> JSON metadata len │
│  payload_size    (8 bytes)  ─────> Actual data len   │
│  ┌────────────────────────────────────────────────┐  │
│  │    metadata_json (up to 1024 bytes)           │  │
│  │    {                                           │  │
│  │      "type": "ndarray",                        │  │
│  │      "shape": [480, 640, 3],                   │  │
│  │      "dtype": "<u1"                            │  │
│  │    }                                           │  │
│  └────────────────────────────────────────────────┘  │
│  start_sentinel  (1 byte)   ─────> 0x00 for integrity│
│  ┌────────────────────────────────────────────────┐  │
│  │    payload (variable size)                     │  │
│  │    [ raw bytes of the object ]                 │  │
│  └────────────────────────────────────────────────┘  │
│  end_sentinel    (1 byte)   ─────> 0x00 for integrity│
└──────────────────────────────────────────────────────┘

Sentinel bytes (0x00) before and after payload detect
buffer overruns and data corruption during read.
```

## End-of-Stream Signaling

The ring buffer uses an *implicit* end-of-stream (EOS) mechanism based on an
`active_producers` counter in the header.

**EOS behavior:**
- Producers increment `active_producers` on attach and decrement it on `close()`.
- Consumers treat EOS as: buffer is empty **and** `active_producers == 0`.
- `pop()` returns `None` only for EOS; timeouts/corruption/etc raise exceptions.

**Producer Side:**
```python
# Python
producer = SharedRingBufferProducer("buffer", 1024*1024*100)
producer.push(data)
# ... send more data ...
producer.close()  # Decrements active_producers
```

```cpp
// C++
auto producer = SharedRingBufferProducer("buffer", 1024*1024*100);
producer.push(data);
// ... send more data ...
producer.close();  // Decrements active_producers
```

**Consumer Side:**
```python
# Python
consumer = SharedRingBufferConsumer("buffer")
while True:
    try:
        payload = consumer.pop()
        if payload is None:  # End-of-stream
            break
        # Process payload...
    except RingBufferException as e:
        # Handle errors (timeout, corruption, etc.)
        print(f"Error: {e.error_type}")
        break
```

```cpp
// C++
auto consumer = SharedRingBufferConsumer("buffer");
while (true) {
    try {
        auto payload = consumer.pop();
        if (!payload) {  // End-of-stream
            break;
        }
        // Process payload...
    } catch (const RingBufferException& e) {
        // Handle errors (timeout, corruption, etc.)
        std::cerr << "Error: " << e.what() << std::endl;
        break;
    }
}
```

**Error Handling:**
- `pop()` returns `None`/`nullopt` **only** for end-of-stream
- All error conditions (timeout, corruption, etc.) throw `RingBufferException`
- Exception contains `error_type` field for precise error identification
- Error types: NotInitialized, Timeout, BufferEmpty, CorruptPayload, 
  DeserializationFailed, InvalidMetadata, ShmNotFound, SizeMismatch

**Performance Impact:**
- EOS detection adds minimal overhead: a header read and a comparison.
- No additional slot markers or format changes are required.

## Object Serialization Flow

```
┌─────────────────────────────────────────────────────────┐
│                     Producer Side                        │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │   Detect object type   │
              │   (np.ndarray, str,    │
              │    dict, PIL.Image)    │
              └────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  Select serializer:    │
              │  - NumpySerializer     │
              │  - ImageSerializer     │
              │  - TextSerializer      │
              │  - JsonSerializer      │
              │  - BytesSerializer     │
              └────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  Serialize to:         │
              │  - metadata_dict       │
              │  - payload_bytes       │
              └────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  Encode metadata as    │
              │  JSON (validate ≤1024) │
              └────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  Check buffer space    │
              │  Wait if full (block)  │
              └────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  Write to shared mem:  │
              │  1. next_pos           │
              │  2. metadata_size      │
              │  3. payload_size       │
              │  4. metadata_json      │
              │  5. payload            │
              └────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  Update write_pos      │
              │  (atomic operation)    │
              └────────────────────────┘


┌─────────────────────────────────────────────────────────┐
│                     Consumer Side                        │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  Check data available  │
              │  Wait if empty (block) │
              └────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  Read from shared mem: │
              │  1. next_pos           │
              │  2. metadata_size      │
              │  3. payload_size       │
              │  4. metadata_json      │
              │  5. payload            │
              └────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  Parse JSON metadata   │
              │  Extract "type" field  │
              └────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  Select deserializer   │
              │  based on type         │
              └────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  Deserialize payload   │
              │  using metadata        │
              └────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  Update read_pos       │
              │  (atomic operation)    │
              └────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  Return object to user │
              └────────────────────────┘
```

## Circular Buffer Wraparound

```
Initial State:
┌────────────────────────────────────────┐
│ [Header] |                    |        │
│          ▲                              │
│    read_pos                             │
│    write_pos                            │
└────────────────────────────────────────┘

After Writing 3 Slots:
┌────────────────────────────────────────┐
│ [Header] | S0 | S1 | S2 |      |      │
│          ▲              ▲               │
│    read_pos       write_pos            │
└────────────────────────────────────────┘

After Consumer Reads 2:
┌────────────────────────────────────────┐
│ [Header] | S0 | S1 | S2 |      |      │
│                    ▲    ▲               │
│              read_pos   │               │
│                   write_pos            │
└────────────────────────────────────────┘

Buffer Nearly Full:
┌────────────────────────────────────────┐
│ [Header] | S2 | S3 | S4 | S5 | S6 | S7│
│                ▲                      ▲ │
│          read_pos           write_pos   │
└────────────────────────────────────────┘

After Wraparound:
┌────────────────────────────────────────┐
│ [Header] | S8 | S9 | S4 | S5 | S6 | S7│
│               ▲   ▲                     │
│         write_pos                       │
│              read_pos                   │
└────────────────────────────────────────┘
  (S8, S9 overwrote S2, S3 after they were consumed)
```

## Synchronization (MPMC)

```
This implementation supports multiple producers and multiple consumers across
processes.

Key points:
- A named POSIX semaphore provides cross-process mutual exclusion while updating
    shared header fields (`write_pos`, `read_pos`, counters).
- A second named semaphore is used as a minimal condition-like wakeup mechanism
    for blocking waits.
- Producers publish a slot by writing slot contents first and only then updating
    `write_pos`.
```

## Object Type Examples

### NumPy Array
```json
Metadata: {"type": "ndarray", "shape": [480, 640, 3], "dtype": "<u1"}
Payload:  [raw bytes of the array]
Size:     ~921,600 bytes for 640×480 RGB uint8
```

### PIL Image
```json
Metadata: {"type": "image", "mode": "RGB", "size": [640, 480]}
Payload:  [PNG encoded image bytes]
Size:     Variable (compressed)
```

### Text String
```json
Metadata: {"type": "text", "encoding": "utf-8", "length": 13}
Payload:  "Hello, World!" (UTF-8 bytes)
Size:     13 bytes
```

### JSON Object
```json
Metadata: {"type": "json", "encoding": "utf-8"}
Payload:  {"key": "value", "number": 42} (JSON string as bytes)
Size:     Variable
```

### Raw Bytes
```json
Metadata: {"type": "bytes", "size": 1024}
Payload:  [arbitrary binary data]
Size:     1024 bytes
```

### List Payload Format
```json
Metadata: {
    "type": "list",
    "version": "1.0",
    "count": 3,
    "items": [
        {"metadata": {"type": "bytes", "size": 4}, "payload_size": 4},
        {"metadata": {"type": "json", "encoding": "utf-8"}, "payload_size": 27},
        {"metadata": {"type": "text", "encoding": "utf-8"}, "payload_size": 9}
    ]
}
Payload: [bytes_of_child0][bytes_of_child1][bytes_of_child2]
```

- Payloads are concatenated; the per-item metadata only tracks the size so the reader can slice the shared buffer correctly.
- Lists **cannot be empty** and are limited to **10 items / 10 levels deep** to keep stack recursion and slot metadata predictable.
- Nested lists simply embed their own metadata in the parent `items` entry, so the deserializer walks depth-first and reconstructs a native Python list or a C++ `ListData` tree.

### Extensibility via `TypeRegistry`

Both Python and C++ expose a singleton `TypeRegistry` that maps `(type, version)` keys to deserializer callbacks.

- **Registration**: Callers register their custom deserializers before consuming data. In Python use `ipc0cp.type_registry.register_type("MyType", my_deserializer, version="1.0")`; in C++ call `ipc0cp::TypeRegistry::instance().register_type("MyType", my_deserializer, "1.0")`.
- **Builtin handlers**: The registry is seeded with `bytes`, `text`, `json`, `image`, `ndarray`, and `list`, so consumers never need to mutate core parsing logic.
- **Lookup flow**: When deserializing, the registry tries `(type, version)` first, then `(type, "")`, logging a warning and falling back to `BytesData` if nothing matches. This lets producers add new types safely without breaking older consumers.
