"""STDIO-based IPC using stdin/stdout for inter-process communication.

This module provides producer and consumer classes that use standard input/output
for IPC. While not zero-copy like shared memory, it's portable and works across
different process boundaries (e.g., network, containers).

Wire Format (per message):
    [metadata_size: 4 bytes little-endian uint32]
    [payload_size:  8 bytes little-endian uint64]
    [metadata_json: metadata_size bytes (UTF-8)]
    [start_sentinel: 1 byte]
    [payload:       payload_size bytes (binary)]
    [end_sentinel:   1 byte]

End-of-stream:
    metadata_size=0 and payload_size=0
"""

import sys
import struct
from typing import Any, Optional
from .serialize import MAX_METADATA_SIZE, serialize_object, deserialize_object
from .ipc import IPCException, IPCError


SENTINEL_BYTE = 0x00


class StdioProducer:
    """
    Producer that writes serialized objects to stdout.
    
    Uses the same serialization as SharedRingBufferProducer for consistency.
    Wire format: [metadata_size:4][payload_size:8][metadata_json][payload]
    
    Example:
        producer = StdioProducer()
        producer.push({"key": "value"})
        producer.push(np.array([1, 2, 3]))
        producer.close()  # Sends EOS marker
    """
    
    def __init__(self, output_stream=None):
        """
        Initialize STDIO producer.
        
        Args:
            output_stream: Stream to write to (default: sys.stdout.buffer)
        """
        self.output = output_stream or sys.stdout.buffer
        self.closed = False
    
    def push(self, obj: Any) -> bool:
        """
        Serialize and write an object to stdout.
        
        Args:
            obj: Any Python object supported by the serialization system
        
        Returns:
            True on success
            
        Raises:
            IPCException: If already closed or write fails
        """
        if self.closed:
            raise IPCException(
                IPCError.NOT_INITIALIZED,
                "Producer is closed"
            )
        
        try:
            # Serialize object
            metadata_json, payload_bytes = serialize_object(obj)

            metadata_bytes = metadata_json.encode('utf-8')
            metadata_size = len(metadata_bytes)
            if metadata_size > MAX_METADATA_SIZE:
                raise ValueError(
                    f"Metadata size {metadata_size} exceeds maximum {MAX_METADATA_SIZE}"
                )

            payload_size = len(payload_bytes)

            # Write header + fields
            self.output.write(struct.pack('<I', metadata_size))
            self.output.write(struct.pack('<Q', payload_size))
            self.output.write(metadata_bytes)
            self.output.write(bytes([SENTINEL_BYTE]))
            self.output.write(payload_bytes)
            self.output.write(bytes([SENTINEL_BYTE]))
            self.output.flush()
            
            return True
            
        except Exception as e:
            raise IPCException(
                IPCError.DESERIALIZATION_FAILED,
                f"Failed to serialize object: {e}"
            )
    
    def close(self):
        """
        Send end-of-stream marker (metadata_size=0, payload_size=0).
        """
        if not self.closed:
            try:
                # Send EOS marker
                self.output.write(struct.pack('<I', 0))
                self.output.write(struct.pack('<Q', 0))
                self.output.flush()
            except Exception:
                pass  # Ignore errors when sending EOS
            finally:
                self.closed = True


class StdioConsumer:
    """
    Consumer that reads serialized objects from stdin.
    
    Uses the same deserialization as SharedRingBufferConsumer for consistency.
    Wire format: [metadata_size:4][payload_size:8][metadata_json][payload]
    
    Example:
        consumer = StdioConsumer()
        while True:
            obj = consumer.pop()
            if obj is None:  # End-of-stream
                break
            print(obj)
    """
    
    def __init__(self, input_stream=None):
        """
        Initialize STDIO consumer.
        
        Args:
            input_stream: Stream to read from (default: sys.stdin.buffer)
        """
        self.input = input_stream or sys.stdin.buffer
        self.eos_received = False
    
    def pop(self, timeout: Optional[float] = None) -> Optional[Any]:
        """
        Read and deserialize an object from stdin.
        
        Args:
            timeout: Ignored for STDIO (blocking I/O only)
        
        Returns:
            Deserialized object, or None if end-of-stream reached
            
        Raises:
            IPCException: On read errors or corruption
        """
        if self.eos_received:
            return None
        
        try:
            # Read header
            metadata_size_bytes = self.input.read(4)
            if len(metadata_size_bytes) == 0:
                # EOF without EOS marker
                self.eos_received = True
                return None

            if len(metadata_size_bytes) != 4:
                raise IPCException(
                    IPCError.CORRUPT_PAYLOAD,
                    f"Incomplete metadata_size field: got {len(metadata_size_bytes)} bytes"
                )

            payload_size_bytes = self.input.read(8)
            if len(payload_size_bytes) != 8:
                raise IPCException(
                    IPCError.CORRUPT_PAYLOAD,
                    f"Incomplete payload_size field: got {len(payload_size_bytes)} bytes"
                )

            metadata_size = struct.unpack('<I', metadata_size_bytes)[0]
            payload_size = struct.unpack('<Q', payload_size_bytes)[0]

            # Check for EOS marker
            if metadata_size == 0 and payload_size == 0:
                self.eos_received = True
                return None

            if metadata_size > MAX_METADATA_SIZE:
                raise IPCException(
                    IPCError.CORRUPT_PAYLOAD,
                    f"Invalid metadata_size: {metadata_size}"
                )

            metadata_bytes = self.input.read(metadata_size)
            if len(metadata_bytes) != metadata_size:
                raise IPCException(
                    IPCError.CORRUPT_PAYLOAD,
                    f"Incomplete metadata: expected {metadata_size} bytes, got {len(metadata_bytes)}"
                )

            start_sentinel = self.input.read(1)
            if len(start_sentinel) != 1 or start_sentinel[0] != SENTINEL_BYTE:
                raise IPCException(
                    IPCError.CORRUPT_PAYLOAD,
                    f"Invalid start sentinel (expected {SENTINEL_BYTE}, got {start_sentinel[0] if start_sentinel else 'empty'})"
                )

            payload = self.input.read(payload_size)
            if len(payload) != payload_size:
                raise IPCException(
                    IPCError.CORRUPT_PAYLOAD,
                    f"Incomplete payload: expected {payload_size} bytes, got {len(payload)}"
                )

            end_sentinel = self.input.read(1)
            if len(end_sentinel) != 1 or end_sentinel[0] != SENTINEL_BYTE:
                raise IPCException(
                    IPCError.CORRUPT_PAYLOAD,
                    f"Invalid end sentinel (expected {SENTINEL_BYTE}, got {end_sentinel[0] if end_sentinel else 'empty'})"
                )

            metadata_json = metadata_bytes.decode('utf-8')
            obj = deserialize_object(metadata_json, payload)
            return obj
            
        except IPCException:
            raise
        except Exception as e:
            raise IPCException(
                IPCError.DESERIALIZATION_FAILED,
                f"Failed to deserialize object: {e}"
            )


