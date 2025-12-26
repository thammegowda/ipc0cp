"""
Lock-free ring buffer using POSIX shared memory for single-producer/single-consumer IPC.

This module implements a variable-size slot ring buffer using a hybrid linked-list approach
with offset tracking in the header for efficient lock-free operations.

Supports generic objects with JSON metadata including:
- NumPy arrays
- PIL Images
- Text strings
- JSON objects
- Raw bytes
"""

import json
import logging
import struct
import time
from abc import ABC, abstractmethod
from multiprocessing import shared_memory
from typing import Any, Optional

from .serialize import SERIALIZERS, MAX_METADATA_SIZE, get_serializer

logger = logging.getLogger(__name__)

# Constants
HEADER_SIZE = 24  # 3 * uint64: write_offset, read_offset, total_data_bytes
SLOT_HEADER_SIZE = 20  # next_offset(8) + metadata_size(4) + payload_size(8)
SENTINEL_BYTE = 0x00  # Null byte for data integrity checking (before and after payload)
MAX_SLOT_SIZE = 10 * 1024 * 1024  # 10 MB
DEFAULT_TOTAL_DATA_BYTES = 1 * 1024 * 1024 * 1024  # 1 GB


class SharedRingBufferBase(ABC):
    """
    Lock-free ring buffer for variable-size generic objects using shared memory.
    
    Uses a hybrid linked-list approach where:
    - Header contains write_offset and read_offset for O(1) space checking
    - Each slot contains next_offset pointer for sequential traversal
    - Single producer writes at write_offset
    - Single consumer reads at read_offset
    
    Memory Layout:
        [Header: write_offset | read_offset | total_data_bytes]
        [Data Region: Slot0 → Slot1 → Slot2 → ...]
        
    Each Slot:
        next_offset (uint64, 8 bytes)
        metadata_size (uint32, 4 bytes)
        payload_size (uint64, 8 bytes)
        metadata_json (bytes, max 1024)
        start_sentinel (uint8, 1 byte, value=0x00)
        payload (bytes)
        end_sentinel (uint8, 1 byte, value=0x00)
        
    Sentinel bytes (0x00) before and after payload provide data integrity 
    checking to detect buffer overruns and corruption.
        
    Supported object types:
        - NumPy arrays (type='ndarray')
        - PIL Images (type='image')
        - Text strings (type='text')
        - JSON objects (type='json')
        - Raw bytes (type='bytes')
    """
    
    def __init__(
        self,
        shm_name: str,
        total_data_bytes: int = DEFAULT_TOTAL_DATA_BYTES,
        blocking: bool = True,
        max_slot_size: int = MAX_SLOT_SIZE,
        create: bool = True
    ):
        """
        Initialize the shared ring buffer.
        
        Args:
            shm_name: Name of the POSIX shared memory segment
            total_data_bytes: Total size of the data region in bytes
            blocking: Whether to block when buffer is full/empty
            max_slot_size: Maximum allowed size per slot (default 10MB)
            create: If True, create new shared memory; if False, attach to existing
        """
        self.shm_name = shm_name
        self.total_data_bytes = total_data_bytes
        self.blocking = blocking
        self.max_slot_size = max_slot_size
        self.shm: Optional[shared_memory.SharedMemory] = None
        self.last_error: Optional[str] = None  # Track last error
        
        # Total shared memory size
        self.shm_size = HEADER_SIZE + total_data_bytes
    
    def _get_write_offset(self) -> int:
        """Read write_offset from header."""
        return struct.unpack_from('Q', self.shm.buf, 0)[0]
    
    def _set_write_offset(self, offset: int):
        """Write write_offset to header."""
        struct.pack_into('Q', self.shm.buf, 0, offset)
    
    def _get_read_offset(self) -> int:
        """Read read_offset from header."""
        return struct.unpack_from('Q', self.shm.buf, 8)[0]
    
    def _set_read_offset(self, offset: int):
        """Write read_offset to header."""
        struct.pack_into('Q', self.shm.buf, 8, offset)
    
    def _available_space(self, write_offset: int, read_offset: int) -> int:
        """
        Calculate available space in the circular buffer.
        
        Args:
            write_offset: Current write position
            read_offset: Current read position
            
        Returns:
            Number of bytes available for writing
        """
        if write_offset >= read_offset:
            # Case 1: write is ahead of read
            # Available: from write to end, plus from start to read
            space_to_end = (HEADER_SIZE + self.total_data_bytes) - write_offset
            space_from_start = read_offset - HEADER_SIZE
            return space_to_end + space_from_start
        else:
            # Case 2: write has wrapped around
            # Available: from write to read
            return read_offset - write_offset
    
    def _normalize_offset(self, offset: int) -> int:
        """
        Normalize offset to wrap around the circular buffer.
        
        Args:
            offset: Raw offset value
            
        Returns:
            Normalized offset within [HEADER_SIZE, HEADER_SIZE + total_data_bytes)
        """
        if offset >= HEADER_SIZE + self.total_data_bytes:
            # Wrap to beginning of data region
            return HEADER_SIZE + (offset - HEADER_SIZE) % self.total_data_bytes
        return offset
    
    def close(self):
        """Close the shared memory segment."""
        if self.shm is not None:
            self.shm.close()
            logger.info(f"Closed shared memory '{self.shm_name}'")
    
    def unlink(self):
        """Unlink (delete) the shared memory segment."""
        if self.shm is not None:
            try:
                self.shm.unlink()
                logger.info(f"Unlinked shared memory '{self.shm_name}'")
            except Exception as e:
                logger.error(f"Failed to unlink shared memory: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
    
    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        return self._get_write_offset() == self._get_read_offset()
    
    def get_stats(self) -> dict:
        """Get buffer statistics."""
        write_offset = self._get_write_offset()
        read_offset = self._get_read_offset()
        available = self._available_space(write_offset, read_offset)
        
        return {
            'write_offset': write_offset,
            'read_offset': read_offset,
            'available_bytes': available,
            'used_bytes': self.total_data_bytes - available,
            'total_data_bytes': self.total_data_bytes,
            'is_empty': write_offset == read_offset,
        }


class SharedRingBufferProducer(SharedRingBufferBase):
    """
    Producer-only interface for the shared ring buffer.
    
    This class only exposes the push() method for writing objects to the buffer.
    Use this in producer processes to prevent accidental consumer operations.
    Always creates new shared memory.
    """
    
    def __init__(
        self,
        shm_name: str,
        total_data_bytes: int = DEFAULT_TOTAL_DATA_BYTES,
        blocking: bool = True,
        max_slot_size: int = MAX_SLOT_SIZE,
    ):
        """
        Initialize the producer and create shared memory.
        
        Args:
            shm_name: Name of the POSIX shared memory segment
            total_data_bytes: Total size of the data region in bytes
            blocking: Whether to block when buffer is full
            max_slot_size: Maximum allowed size per slot (default 10MB)
        """
        super().__init__(shm_name, total_data_bytes, blocking, max_slot_size)
        self._create()
    
    def _set_write_offset(self, offset: int):
        """Write write_offset to header."""
        struct.pack_into('Q', self.shm.buf, 0, offset)
    
    def _write_with_wrap(self, start_pos: int, data: bytes) -> int:
        """
        Write data to buffer handling wraparound.
        
        Args:
            start_pos: Starting position in buffer
            data: Bytes to write
            
        Returns:
            Position after write (normalized)
        """
        data_len = len(data)
        end_of_region = HEADER_SIZE + self.total_data_bytes
        
        if start_pos + data_len <= end_of_region:
            # No wraparound needed
            self.shm.buf[start_pos:start_pos + data_len] = data
            return start_pos + data_len
        else:
            # Wraparound needed
            first_part_len = end_of_region - start_pos
            self.shm.buf[start_pos:end_of_region] = data[:first_part_len]
            second_part_len = data_len - first_part_len
            self.shm.buf[HEADER_SIZE:HEADER_SIZE + second_part_len] = data[first_part_len:]
            return HEADER_SIZE + second_part_len
    
    def _write_uint64(self, pos: int, value: int) -> int:
        """Write a uint64 value handling wraparound."""
        data = struct.pack('Q', value)
        return self._write_with_wrap(pos, data)
    
    def _write_uint32(self, pos: int, value: int) -> int:
        """Write a uint32 value handling wraparound."""
        data = struct.pack('I', value)
        return self._write_with_wrap(pos, data)
    
    def _create(self):
        """Create new shared memory segment and initialize header."""
        try:
            # Try to unlink any existing segment with the same name
            try:
                existing = shared_memory.SharedMemory(name=self.shm_name)
                existing.close()
                existing.unlink()
            except FileNotFoundError:
                pass
            
            # Create new shared memory
            self.shm = shared_memory.SharedMemory(
                name=self.shm_name,
                create=True,
                size=self.shm_size
            )
            
            # Initialize header
            # write_offset = HEADER_SIZE (start of data region)
            # read_offset = HEADER_SIZE (empty buffer)
            # total_data_bytes = configured value
            struct.pack_into(
                'QQQ',
                self.shm.buf,
                0,
                HEADER_SIZE,  # write_offset
                HEADER_SIZE,  # read_offset
                self.total_data_bytes
            )
            
            logger.info(
                f"Created shared memory '{self.shm_name}' with {self.shm_size} bytes "
                f"({self.total_data_bytes} bytes data region)"
            )
            
        except Exception as e:
            logger.error(f"Failed to create shared memory: {e}")
            raise
    
    def push_raw(self, metadata_json: str, payload: bytes, timeout: Optional[float] = None) -> bool:
        """
        Push pre-serialized data into the ring buffer.
        
        Args:
            metadata_json: JSON metadata string
            payload: Binary payload data
            timeout: Maximum time to wait in seconds (None = infinite if blocking)
            
        Returns:
            True if successful, False if buffer is full and non-blocking
        """
        if self.shm is None:
            raise RuntimeError("Shared memory not initialized")
        
        metadata_bytes = metadata_json.encode('utf-8')
        metadata_size = len(metadata_bytes)
        
        if metadata_size > MAX_METADATA_SIZE:
            raise ValueError(f"Metadata size {metadata_size} exceeds maximum {MAX_METADATA_SIZE}")
        
        payload_size = len(payload)
        slot_size = SLOT_HEADER_SIZE + metadata_size + 1 + payload_size + 1
        
        # Wait for space if blocking
        start_time = time.time()
        while True:
            write_offset = self._get_write_offset()
            read_offset = self._get_read_offset()
            available = self._available_space(write_offset, read_offset)
            
            if available >= slot_size:
                break
            
            if not self.blocking:
                return False
            
            if timeout is not None and (time.time() - start_time) >= timeout:
                return False
            
            time.sleep(0.0005)
        
        # Write slot
        next_offset = write_offset + slot_size
        next_offset = self._normalize_offset(next_offset)
        
        current_pos = write_offset
        current_pos = self._write_uint64(current_pos, next_offset)
        current_pos = self._write_uint32(current_pos, metadata_size)
        current_pos = self._write_uint64(current_pos, payload_size)
        current_pos = self._write_with_wrap(current_pos, metadata_bytes)
        current_pos = self._write_with_wrap(current_pos, bytes([SENTINEL_BYTE]))
        current_pos = self._write_with_wrap(current_pos, payload)
        current_pos = self._write_with_wrap(current_pos, bytes([SENTINEL_BYTE]))
        
        self._set_write_offset(next_offset)
        return True
    
    def available_space(self) -> int:
        """
        Get available space in buffer.
        
        Returns:
            Available space in bytes
        """
        write_offset = self._get_write_offset()
        read_offset = self._get_read_offset()
        return self._available_space(write_offset, read_offset)
    
    def is_initialized(self) -> bool:
        """
        Check if shared memory is initialized.
        
        Returns:
            True if initialized, False otherwise
        """
        return self.shm is not None
    
    def shm_name_value(self) -> str:
        """
        Get the shared memory name.
        
        Returns:
            Shared memory segment name
        """
        return self.shm_name
    
    def push(self, obj: Any, timeout: Optional[float] = None) -> bool:
        """
        Push an object into the ring buffer (producer operation).
        
        Args:
            obj: Object to push (NumPy array, PIL Image, str, dict, list, bytes, etc.)
            timeout: Maximum time to wait in seconds (None = infinite if blocking)
            
        Returns:
            True if successful, False if buffer is full and non-blocking
            
        Raises:
            ValueError: If object is too large or cannot be serialized
            RuntimeError: If shared memory is not initialized
        """
        # Serialize object
        try:
            serializer = get_serializer(obj)
            metadata_dict, payload = serializer.serialize(obj)
        except Exception as e:
            raise ValueError(f"Failed to serialize object: {e}")
        
        # Validate payload size
        payload_size = len(payload)
        if payload_size > self.max_slot_size:
            logger.warning(
                f"Payload size {payload_size} bytes exceeds max_slot_size "
                f"{self.max_slot_size} bytes, skipping"
            )
            return False
        
        # Convert metadata to JSON
        try:
            metadata_json = json.dumps(metadata_dict)
        except Exception as e:
            raise ValueError(f"Failed to encode metadata as JSON: {e}")
        
        # Delegate to push_raw
        return self.push_raw(metadata_json, payload, timeout)


class SharedRingBufferConsumer(SharedRingBufferBase):
    """
    Consumer-only interface for the shared ring buffer.
    
    This class only exposes the pop() method for reading objects from the buffer.
    Use this in consumer processes to prevent accidental producer operations.
    Always attaches to existing shared memory.
    """
    
    def __init__(
        self,
        shm_name: str,
        total_data_bytes: int = DEFAULT_TOTAL_DATA_BYTES,
        blocking: bool = True,
        max_slot_size: int = MAX_SLOT_SIZE,
        auto_attach: bool = True,
    ):
        """
        Initialize the consumer and optionally attach to existing shared memory.
        
        Args:
            shm_name: Name of the POSIX shared memory segment
            total_data_bytes: Total size of the data region in bytes (should match producer)
            blocking: Whether to block when buffer is empty
            max_slot_size: Maximum allowed size per slot (default 10MB)
            auto_attach: If True, automatically attach to shared memory in constructor
        """
        super().__init__(shm_name, total_data_bytes, blocking, max_slot_size)
        if auto_attach:
            self._attach()
    
    def _set_read_offset(self, offset: int):
        """Write read_offset to header."""
        struct.pack_into('Q', self.shm.buf, 8, offset)
    
    def _read_uint64(self, pos: int) -> int:
        """Read a uint64 value handling wraparound."""
        data = self._read_bytes(pos, 8)
        return struct.unpack('Q', data)[0]
    
    def _read_uint32(self, pos: int) -> int:
        """Read a uint32 value handling wraparound."""
        data = self._read_bytes(pos, 4)
        return struct.unpack('I', data)[0]
    
    def _read_bytes(self, pos: int, length: int) -> bytes:
        """Read bytes from buffer handling wraparound."""
        end_of_region = HEADER_SIZE + self.total_data_bytes
        
        if pos + length <= end_of_region:
            return bytes(self.shm.buf[pos:pos + length])
        else:
            # Wraparound
            first_part_len = end_of_region - pos
            first_part = bytes(self.shm.buf[pos:end_of_region])
            second_part_len = length - first_part_len
            second_part = bytes(self.shm.buf[HEADER_SIZE:HEADER_SIZE + second_part_len])
            return first_part + second_part
    
    def _advance_pos(self, pos: int, offset: int) -> int:
        """Advance position by offset, handling wraparound."""
        new_pos = pos + offset
        return self._normalize_offset(new_pos)
    
    def _attach(self):
        """Attach to existing shared memory segment."""
        try:
            self.shm = shared_memory.SharedMemory(name=self.shm_name)
            
            # Verify size matches
            if self.shm.size != self.shm_size:
                raise ValueError(
                    f"Shared memory size mismatch: expected {self.shm_size}, "
                    f"got {self.shm.size}"
                )
            
            # Read total_data_bytes from header
            _, _, stored_total = struct.unpack_from('QQQ', self.shm.buf, 0)
            if stored_total != self.total_data_bytes:
                logger.warning(
                    f"total_data_bytes mismatch: using stored value {stored_total}"
                )
                self.total_data_bytes = stored_total
                self.shm_size = HEADER_SIZE + stored_total
            
            logger.info(f"Attached to shared memory '{self.shm_name}'")
            
        except FileNotFoundError:
            logger.error(f"Shared memory '{self.shm_name}' does not exist")
            raise
        except Exception as e:
            logger.error(f"Failed to attach to shared memory: {e}")
            raise
    
    def pop(self, timeout: Optional[float] = None) -> Optional[Any]:
        """
        Pop an object from the ring buffer (consumer operation).
        
        Args:
            timeout: Maximum time to wait in seconds (None = infinite if blocking)
            
        Returns:
            Deserialized object if successful, None if buffer is empty and non-blocking
            
        Raises:
            RuntimeError: If shared memory is not initialized
            ValueError: If slot data is corrupted or unsupported type
        """
        if self.shm is None:
            raise RuntimeError("Shared memory not initialized")
        
        # Wait for data if blocking
        start_time = time.time()
        while True:
            write_offset = self._get_write_offset()
            read_offset = self._get_read_offset()
            
            if write_offset != read_offset:
                break
            
            if not self.blocking:
                return None
            
            if timeout is not None and (time.time() - start_time) >= timeout:
                return None
            
            time.sleep(0.0005)  # 500 microseconds
        
        # Read slot at read_offset
        current_pos = read_offset
        
        # Read next_offset (8 bytes)
        next_offset = self._read_uint64(current_pos)
        current_pos = self._advance_pos(current_pos, 8)
        
        # Read metadata_size (4 bytes)
        metadata_size = self._read_uint32(current_pos)
        current_pos = self._advance_pos(current_pos, 4)
        
        # Validate metadata size
        if metadata_size > MAX_METADATA_SIZE:
            raise ValueError(f"Invalid metadata_size: {metadata_size}")
        
        # Read payload_size (8 bytes)
        payload_size = self._read_uint64(current_pos)
        current_pos = self._advance_pos(current_pos, 8)
        
        # Read metadata JSON
        metadata_bytes = self._read_bytes(current_pos, metadata_size)
        current_pos = self._advance_pos(current_pos, metadata_size)
        
        # Parse metadata
        try:
            metadata_str = metadata_bytes.decode('utf-8')
            metadata_dict = json.loads(metadata_str)
        except Exception as e:
            raise ValueError(f"Failed to parse metadata JSON: {e}")
        
        # Read and verify start sentinel
        start_sentinel = self._read_bytes(current_pos, 1)
        if len(start_sentinel) != 1 or start_sentinel[0] != SENTINEL_BYTE:
            error_msg = f"Data corruption: invalid start sentinel (expected {SENTINEL_BYTE}, got {start_sentinel[0] if start_sentinel else 'empty'})"
            self.last_error = error_msg
            raise ValueError(error_msg)
        current_pos = self._advance_pos(current_pos, 1)
        
        # Read payload
        payload = self._read_bytes(current_pos, payload_size)
        current_pos = self._advance_pos(current_pos, payload_size)
        
        # Read and verify end sentinel
        end_sentinel = self._read_bytes(current_pos, 1)
        if len(end_sentinel) != 1 or end_sentinel[0] != SENTINEL_BYTE:
            error_msg = f"Data corruption: invalid end sentinel (expected {SENTINEL_BYTE}, got {end_sentinel[0] if end_sentinel else 'empty'})"
            self.last_error = error_msg
            raise ValueError(error_msg)
        
        # Get object type and deserialize
        obj_type = metadata_dict.get('type')
        if obj_type not in SERIALIZERS:
            raise ValueError(f"Unsupported object type: {obj_type}")
        
        serializer = SERIALIZERS[obj_type]
        
        try:
            obj = serializer.deserialize(metadata_dict, payload)
        except Exception as e:
            error_msg = f"Failed to deserialize object: {e}"
            self.last_error = error_msg
            raise ValueError(error_msg)
        
        # Update read_offset to next_offset
        self._set_read_offset(next_offset)
        
        return obj

