"""
STDIO-based IPC using stdin/stdout for inter-process communication.

This module provides producer and consumer classes that use standard input/output
for IPC. While not zero-copy like shared memory, it's portable and works across
different process boundaries (e.g., network, containers).

Wire Format:
    [length: 8 bytes little-endian][payload: length bytes]
    length=0 signals end-of-stream
"""

import sys
import struct
from typing import Any, Optional
from .serialize import serialize_object, deserialize_object
from .ipc import IPCException, IPCError


class StdioProducer:
    """
    Producer that writes serialized objects to stdout.
    
    Uses the same serialization as SharedRingBufferProducer for consistency.
    Wire format: [length:8 bytes][metadata+payload]
    
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
            
            # Combine metadata and payload
            metadata_encoded = metadata_json.encode('utf-8')
            combined = metadata_encoded + payload_bytes
            
            # Write length + combined data
            length = len(combined)
            self.output.write(struct.pack('<Q', length))
            self.output.write(combined)
            self.output.flush()
            
            return True
            
        except Exception as e:
            raise IPCException(
                IPCError.DESERIALIZATION_FAILED,
                f"Failed to serialize object: {e}"
            )
    
    def close(self):
        """
        Send end-of-stream marker (length=0) and close the stream.
        """
        if not self.closed:
            try:
                # Send EOS marker
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
    Wire format: [length:8 bytes][metadata+payload]
    
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
            # Read length
            length_bytes = self.input.read(8)
            if len(length_bytes) == 0:
                # EOF without EOS marker
                self.eos_received = True
                return None
            
            if len(length_bytes) != 8:
                raise IPCException(
                    IPCError.CORRUPT_PAYLOAD,
                    f"Incomplete length field: got {len(length_bytes)} bytes"
                )
            
            length = struct.unpack('<Q', length_bytes)[0]
            
            # Check for EOS marker
            if length == 0:
                self.eos_received = True
                return None
            
            # Read combined metadata + payload
            combined = self.input.read(length)
            if len(combined) != length:
                raise IPCException(
                    IPCError.CORRUPT_PAYLOAD,
                    f"Incomplete payload: expected {length} bytes, got {len(combined)}"
                )
            
            # Deserialize
            obj = deserialize_object(combined)
            return obj
            
        except IPCException:
            raise
        except Exception as e:
            raise IPCException(
                IPCError.DESERIALIZATION_FAILED,
                f"Failed to deserialize object: {e}"
            )
