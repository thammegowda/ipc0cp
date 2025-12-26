# Architecture Overview

## High-Level Design

```
┌─────────────────────────────────────────────────────────────┐
│                     Shared Memory Region                     │
│                                                              │
│  ┌────────────────────────────────────────────────────┐    │
│  │              Header (24 bytes)                      │    │
│  │  ┌──────────────┬──────────────┬──────────────┐   │    │
│  │  │ write_offset │ read_offset  │ total_bytes  │   │    │
│  │  │   (uint64)   │   (uint64)   │   (uint64)   │   │    │
│  │  └──────────────┴──────────────┴──────────────┘   │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌────────────────────────────────────────────────────┐    │
│  │              Data Region (Variable)                 │    │
│  │                                                     │    │
│  │  ┌─────────┐      ┌─────────┐      ┌─────────┐   │    │
│  │  │ Slot 0  │ ───> │ Slot 1  │ ───> │ Slot 2  │   │    │
│  │  └─────────┘      └─────────┘      └─────────┘   │    │
│  │       │                │                │          │    │
│  │       │                │                │          │    │
│  │  ┌────▼────────────────▼────────────────▼──────┐  │    │
│  │  │   Circular linked list of variable slots   │  │    │
│  │  └────────────────────────────────────────────┘  │    │
│  └────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘

Producer (Process 1)          Consumer (Process 2)
    │                              │
    │  Writes at write_offset     │  Reads at read_offset
    │  Updates write_offset ──────│────> Observes write_offset
    │                              │  Updates read_offset
    │  Observes read_offset <─────│
    └──────────────────────────────┘
```

## Slot Structure

```
┌──────────────────────────────────────────────────────┐
│                    Single Slot                        │
├──────────────────────────────────────────────────────┤
│  next_offset     (8 bytes)  ─────> Next slot offset  │
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
              │  1. next_offset        │
              │  2. metadata_size      │
              │  3. payload_size       │
              │  4. metadata_json      │
              │  5. payload            │
              └────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  Update write_offset   │
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
              │  1. next_offset        │
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
              │  Update read_offset    │
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
│    read_offset                          │
│    write_offset                         │
└────────────────────────────────────────┘

After Writing 3 Slots:
┌────────────────────────────────────────┐
│ [Header] | S0 | S1 | S2 |      |      │
│          ▲              ▲               │
│    read_offset    write_offset         │
└────────────────────────────────────────┘

After Consumer Reads 2:
┌────────────────────────────────────────┐
│ [Header] | S0 | S1 | S2 |      |      │
│                    ▲    ▲               │
│              read_offset│               │
│                   write_offset         │
└────────────────────────────────────────┘

Buffer Nearly Full:
┌────────────────────────────────────────┐
│ [Header] | S2 | S3 | S4 | S5 | S6 | S7│
│                ▲                      ▲ │
│          read_offset        write_offset│
└────────────────────────────────────────┘

After Wraparound:
┌────────────────────────────────────────┐
│ [Header] | S8 | S9 | S4 | S5 | S6 | S7│
│               ▲   ▲                     │
│         write_offset                    │
│              read_offset                │
└────────────────────────────────────────┘
  (S8, S9 overwrote S2, S3 after they were consumed)
```

## Lock-Free Synchronization

```
Producer Operations:
1. Read read_offset (consumer's position)
2. Read write_offset (own position)
3. Calculate available space
4. If space available:
   - Write slot data
   - Update write_offset (atomic)
5. Else:
   - Wait (blocking) or return False (non-blocking)

Consumer Operations:
1. Read write_offset (producer's position)
2. Read read_offset (own position)
3. If data available (write_offset != read_offset):
   - Read slot data
   - Update read_offset (atomic)
4. Else:
   - Wait (blocking) or return None (non-blocking)

Key Properties:
✓ Each process only writes its own offset
✓ No locks needed (single producer, single consumer)
✓ Memory barriers implicit in Python (GIL)
✓ Wraparound handled transparently
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
