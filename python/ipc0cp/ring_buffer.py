"""ipc0cp shared-memory ring buffer.

Multi-producer multi-consumer (MPMC) ring buffer using POSIX shared memory.

This module implements a variable-size slot ring buffer with POSIX semaphore-based
synchronization for true multi-process support with multiple concurrent producers
and consumers.

**POSIX REQUIREMENT**: This implementation uses POSIX named semaphores for
cross-process synchronization. It requires a POSIX-compliant platform (Linux, macOS)
and the `posix_ipc` library. Windows is not supported.

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
from typing import Any, Optional

from .ipc import IPCError, IPCException
from .posix_sync import (
    PosixCondition,
    PosixSharedMemory,
    cleanup_buffer_semaphores,
    get_buffer_semaphores,
    normalize_ipc_base_name,
)
from .serialize import MAX_METADATA_SIZE, serialize_object, deserialize_object

logger = logging.getLogger(__name__)

PRODUCER_WAIT_FOR_CONSUMER_TIMEOUT_S = 60.0

# Backwards-compat aliases within this module.
_normalize_ipc_base_name = normalize_ipc_base_name
_PosixSharedMemory = PosixSharedMemory
_PosixCondition = PosixCondition
_get_buffer_semaphores = get_buffer_semaphores
_cleanup_buffer_semaphores = cleanup_buffer_semaphores


# Constants
HEADER_SIZE = 64  # Extended header for MPMC: positions, counters, lock/condition
SLOT_HEADER_SIZE = 20  # next_pos(8) + metadata_size(4) + payload_size(8)
SENTINEL_BYTE = 0x00  # Null byte for data integrity checking (before and after payload)
MAX_SLOT_SIZE = 10 * 1024 * 1024  # 10 MB
DEFAULT_TOTAL_DATA_BYTES = 1 * 1024 * 1024 * 1024  # 1 GB

# Header layout (64 bytes):
# Offset 0-7:   write_pos (uint64)
# Offset 8-15:  read_pos (uint64)
# Offset 16-23: total_data_bytes (uint64)
# Offset 24-27: active_producers (uint32)
# Offset 28-31: active_consumers (uint32)
# Offset 32-35: total_producers_joined (uint32)
# Offset 36-39: total_consumers_joined (uint32)
# Offset 40-63: reserved (24 bytes)


class HeaderOffset:
    """Byte offsets for header fields."""
    WRITE_POS = 0
    READ_POS = 8
    TOTAL_DATA_BYTES = 16
    ACTIVE_PRODUCERS = 24
    ACTIVE_CONSUMERS = 28
    TOTAL_PRODUCERS_JOINED = 32
    TOTAL_CONSUMERS_JOINED = 36
    RESERVED = 40


class SharedRingBufferBase(ABC):
    """
    Multi-producer multi-consumer (MPMC) ring buffer for variable-size generic objects.
    
    Uses lock-based synchronization where:
    - Header contains write_pos, read_pos, and active producer/consumer counts
    - Named POSIX semaphores provide cross-process locking
    - Condition variables enable efficient blocking (no busy-wait)
    - Multiple producers can push concurrently
    - Multiple consumers can pop concurrently
    
    Memory Layout:
        [Header (64 bytes): write_pos | read_pos | total_data_bytes | 
                            active_producers | active_consumers | counters | reserved]
        [Data Region: Slot0 → Slot1 → Slot2 → ...]
        
    Each Slot:
        next_pos (uint64, 8 bytes)
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
        # Normalize once so all IPC objects (shm + semaphores) share a base name.
        self.shm_name = _normalize_ipc_base_name(shm_name)
        self.total_data_bytes = total_data_bytes
        self.blocking = blocking
        self.max_slot_size = max_slot_size
        self.shm: Optional[_PosixSharedMemory] = None
        self.last_error: Optional[str] = None  # Track last error
        self._closed = False  # Track if already closed
        
        # Synchronization primitives (shared via Manager for cross-process use)
        self.lock = None
        self.condition = None
        self._sync_name = f"{shm_name}_sync"  # Unique name for this buffer's sync primitives
        
        # Total shared memory size
        self.shm_size = HEADER_SIZE + total_data_bytes
    
    def _get_write_pos(self) -> int:
        """Read write_pos from header."""
        return struct.unpack_from('<Q', self.shm.buf, HeaderOffset.WRITE_POS)[0]
    
    def _set_write_pos(self, pos: int):
        """Write write_pos to header."""
        struct.pack_into('<Q', self.shm.buf, HeaderOffset.WRITE_POS, pos)
    
    def _get_read_pos(self) -> int:
        """Read read_pos from header."""
        return struct.unpack_from('<Q', self.shm.buf, HeaderOffset.READ_POS)[0]
    
    def _set_read_pos(self, pos: int):
        """Write read_pos to header."""
        struct.pack_into('<Q', self.shm.buf, HeaderOffset.READ_POS, pos)
    
    def _get_active_producers(self) -> int:
        """Read active_producers count from header."""
        return struct.unpack_from('<I', self.shm.buf, HeaderOffset.ACTIVE_PRODUCERS)[0]
    
    def _set_active_producers(self, count: int):
        """Write active_producers count to header."""
        struct.pack_into('<I', self.shm.buf, HeaderOffset.ACTIVE_PRODUCERS, count)
    
    def _get_active_consumers(self) -> int:
        """Read active_consumers count from header."""
        return struct.unpack_from('<I', self.shm.buf, HeaderOffset.ACTIVE_CONSUMERS)[0]
    
    def _set_active_consumers(self, count: int):
        """Write active_consumers count to header."""
        struct.pack_into('<I', self.shm.buf, HeaderOffset.ACTIVE_CONSUMERS, count)
    
    def _increment_active_producers(self) -> int:
        """Atomically increment active_producers. Returns new count."""
        with self.lock:
            count = self._get_active_producers() + 1
            self._set_active_producers(count)
            return count
    
    def _decrement_active_producers(self) -> int:
        """Atomically decrement active_producers. Returns new count."""
        with self.lock:
            count = max(0, self._get_active_producers() - 1)
            self._set_active_producers(count)
            self.condition.notify_all()  # Wake consumers waiting for data
            return count
    
    def _increment_active_consumers(self) -> int:
        """Atomically increment active_consumers. Returns new count."""
        with self.lock:
            count = self._get_active_consumers() + 1
            self._set_active_consumers(count)
            # Wake any producers waiting for a consumer to attach.
            if self.condition is not None:
                self.condition.notify_all()
            return count
    
    def _decrement_active_consumers(self) -> int:
        """Atomically decrement active_consumers. Returns new count."""
        with self.lock:
            count = max(0, self._get_active_consumers() - 1)
            self._set_active_consumers(count)
            self.condition.notify_all()  # Wake producers waiting for space
            return count
    
    def _available_space(self, write_pos: int, read_pos: int) -> int:
        """
        Calculate available space in the circular buffer.
        
        Args:
            write_pos: Current write position
            read_pos: Current read position
            
        Returns:
            Number of bytes available for writing
        """
        if write_pos >= read_pos:
            # Case 1: write is ahead of read
            # Available: from write to end, plus from start to read
            space_to_end = (HEADER_SIZE + self.total_data_bytes) - write_pos
            space_from_start = read_pos - HEADER_SIZE
            return space_to_end + space_from_start
        else:
            # Case 2: write has wrapped around
            # Available: from write to read
            return read_pos - write_pos
    
    def _normalize_pos(self, pos: int) -> int:
        """
        Normalize an absolute position to wrap around the circular buffer.
        
        Args:
            pos: Raw position value
            
        Returns:
            Normalized position within [HEADER_SIZE, HEADER_SIZE + total_data_bytes)
        """
        if pos >= HEADER_SIZE + self.total_data_bytes:
            # Wrap to beginning of data region
            return HEADER_SIZE + (pos - HEADER_SIZE) % self.total_data_bytes
        return pos
    
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
        return self._get_write_pos() == self._get_read_pos()
    
    def get_stats(self) -> dict:
        """Get buffer statistics."""
        write_pos = self._get_write_pos()
        read_pos = self._get_read_pos()
        available = self._available_space(write_pos, read_pos)
        
        return {
            'write_pos': write_pos,
            'read_pos': read_pos,
            'available_bytes': available,
            'used_bytes': self.total_data_bytes - available,
            'total_data_bytes': self.total_data_bytes,
            'is_empty': write_pos == read_pos,
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
        create_if_not_exists: bool = True,
    ):
        """
        Initialize the producer and create or attach to shared memory.
        
        Args:
            shm_name: Name of the POSIX shared memory segment
            total_data_bytes: Total size of the data region in bytes
            blocking: Whether to block when buffer is full
            max_slot_size: Maximum allowed size per slot (default 10MB)
            create_if_not_exists: If True, create new shared memory or attach if exists.
                                  If False, always try to attach to existing.
        """
        super().__init__(shm_name, total_data_bytes, blocking, max_slot_size)
        
        if create_if_not_exists:
            # Try to create, fallback to attach if already exists
            try:
                self._create()
            except FileExistsError:
                # Already exists, attach instead
                self._attach_producer()
        else:
            # Always attach
            self._attach_producer()
    
    def _set_write_pos(self, pos: int):
        """Write write_pos to header."""
        struct.pack_into('<Q', self.shm.buf, HeaderOffset.WRITE_POS, pos)
    
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
        data = struct.pack('<Q', value)
        return self._write_with_wrap(pos, data)
    
    def _write_uint32(self, pos: int, value: int) -> int:
        """Write a uint32 value handling wraparound."""
        data = struct.pack('<I', value)
        return self._write_with_wrap(pos, data)
    
    def _create(self):
        """Create new shared memory segment and initialize header."""
        # Create new shared memory (will raise FileExistsError if already exists)
        self.shm = _PosixSharedMemory(self.shm_name, create=True, size=self.shm_size)
        
        # Create POSIX semaphores for cross-process synchronization
        self.lock, self.condition = _get_buffer_semaphores(self.shm_name, create=True)
        
        # Initialize header (64 bytes)
        # Positions (24 bytes)
        struct.pack_into('<QQQ', self.shm.buf, HeaderOffset.WRITE_POS,
            HEADER_SIZE,  # write_pos
            HEADER_SIZE,  # read_pos
            self.total_data_bytes  # total_data_bytes
        )
        
        # MPMC counters (16 bytes)
        struct.pack_into('<IIII', self.shm.buf, HeaderOffset.ACTIVE_PRODUCERS,
            0,  # active_producers
            0,  # active_consumers
            0,  # total_producers_joined
            0   # total_consumers_joined
        )
        
        # Reserved (24 bytes) - zero-initialize
        struct.pack_into('24x', self.shm.buf, HeaderOffset.RESERVED)
        
        # Register this producer
        self._increment_active_producers()
        
        logger.info(
            f"Created shared memory '{self.shm_name}' with {self.shm_size} bytes "
            f"({self.total_data_bytes} bytes data region, MPMC mode with POSIX semaphores)"
        )
    
    def _attach_producer(self):
        """Attach to existing shared memory as an additional producer."""
        try:
            self.shm = _PosixSharedMemory(self.shm_name, create=False)
            
            # Attach to existing POSIX semaphores for cross-process synchronization
            self.lock, self.condition = _get_buffer_semaphores(self.shm_name, create=False)
            
            # Verify size matches
            if self.shm.size != self.shm_size:
                raise ValueError(
                    f"Shared memory size mismatch: expected {self.shm_size}, "
                    f"got {self.shm.size}"
                )
            
            # Read total_data_bytes from header
            _, _, stored_total = struct.unpack_from('<QQQ', self.shm.buf, HeaderOffset.WRITE_POS)
            if stored_total != self.total_data_bytes:
                logger.warning(
                    f"total_data_bytes mismatch: using stored value {stored_total}"
                )
                self.total_data_bytes = stored_total
                self.shm_size = HEADER_SIZE + stored_total
            
            # Register this producer
            self._increment_active_producers()
            
            logger.info(f"Producer attached to shared memory '{self.shm_name}'")
            
        except FileNotFoundError:
            logger.error(f"Shared memory '{self.shm_name}' does not exist")
            raise
        except Exception as e:
            logger.error(f"Failed to attach producer to shared memory: {e}")
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

        # Wait until at least one consumer is attached.
        # This prevents producers from racing ahead and exiting before a consumer can attach.
        # also, if no consumer ever going to read content, do we even need to work hard and waste cycles?
        consumer_wait_timeout = PRODUCER_WAIT_FOR_CONSUMER_TIMEOUT_S if timeout is None else timeout
        start_time = time.time()
        while True:
            with self.lock:
                if self._get_active_consumers() >= 1:
                    break

                # Non-blocking producers should fail fast when no consumers are attached.
                if not self.blocking:
                    raise IPCException(IPCError.NO_CONSUMERS, "No active consumers")

                elapsed = time.time() - start_time
                remaining = consumer_wait_timeout - elapsed
                if remaining <= 0:
                    raise IPCException(
                        IPCError.NO_CONSUMERS,
                        f"Timed out after {consumer_wait_timeout:.1f}s waiting for a consumer to attach"
                    )

                # Wait briefly; consumer attach will notify.
                self.condition.wait(timeout=min(0.01, remaining))
        
        # Wait for space if blocking (use condition variable)
        start_time = time.time()
        while True:
            with self.lock:
                write_pos = self._get_write_pos()
                read_pos = self._get_read_pos()
                available = self._available_space(write_pos, read_pos)
                
                if available >= slot_size:
                    # Reserve slot by updating write_pos atomically
                    next_pos = write_pos + slot_size
                    next_pos = self._normalize_pos(next_pos)
                    
                    # Write slot data BEFORE updating write_pos (prevents consumers from reading partial data)
                    current_pos = write_pos
                    current_pos = self._write_uint64(current_pos, next_pos)
                    current_pos = self._write_uint32(current_pos, metadata_size)
                    current_pos = self._write_uint64(current_pos, payload_size)
                    current_pos = self._write_with_wrap(current_pos, metadata_bytes)
                    current_pos = self._write_with_wrap(current_pos, bytes([SENTINEL_BYTE]))
                    current_pos = self._write_with_wrap(current_pos, payload)
                    current_pos = self._write_with_wrap(current_pos, bytes([SENTINEL_BYTE]))
                    
                    # Now update write_pos to make data visible to consumers
                    self._set_write_pos(next_pos)
                    
                    # Notify consumers that data is available
                    self.condition.notify_all()
                    
                    return True
                
                # Check if no consumers (and buffer full)
                active_consumers = self._get_active_consumers()
                if active_consumers == 0:
                    # No consumers means the buffer cannot drain; respect timeout if provided.
                    if timeout is None:
                        raise IPCException(
                            IPCError.NO_CONSUMERS,
                            "Buffer full and no active consumers"
                        )
                
                if not self.blocking:
                    return False
                
                if timeout is not None and (time.time() - start_time) >= timeout:
                    return False
            
            # Wait for space (releases lock while waiting)
            with self.lock:
                wait_timeout = 0.001  # 1ms
                if timeout is not None:
                    remaining = timeout - (time.time() - start_time)
                    if remaining <= 0:
                        return False
                    wait_timeout = min(wait_timeout, remaining)
                self.condition.wait(timeout=wait_timeout)
    
    def available_space(self) -> int:
        """
        Get available space in buffer.
        
        Returns:
            Available space in bytes
        """
        write_pos = self._get_write_pos()
        read_pos = self._get_read_pos()
        return self._available_space(write_pos, read_pos)
    
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
            metadata_json, payload = serialize_object(obj)
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
        
        # Delegate to push_raw
        return self.push_raw(metadata_json, payload, timeout)
    
    def close(self):
        """
        Close the producer and unregister from active producers.
        
        Decrements active_producers counter and notifies waiting consumers.
        Last producer does NOT unlink shared memory (consumers handle cleanup).
        """
        if self.shm is not None and not self._closed:
            # Unregister this producer
            try:
                remaining = self._decrement_active_producers()
                logger.info(f"Producer closed, {remaining} active producers remaining")
            except Exception as e:
                logger.warning(f"Failed to decrement active_producers: {e}")
            
            # Close semaphores (but don't unlink - consumers will handle that)
            try:
                if hasattr(self, 'condition') and self.condition is not None:
                    if not self.condition._closed:
                        if hasattr(self.condition, 'mutex') and self.condition.mutex is not None:
                            self.condition.mutex.close()
                        if hasattr(self.condition, 'wait_sem') and self.condition.wait_sem is not None:
                            self.condition.wait_sem.close()
                        self.condition._closed = True
            except Exception as e:
                logger.debug(f"Error closing semaphores (may already be closed): {e}")
            
            # Close shared memory (but don't unlink - let consumers clean up)
            self.shm.close()
            self._closed = True
            logger.info(f"Closed shared memory '{self.shm_name}'")

    def unlink(self):
        """Producers must not unlink shared memory.

        Shared memory and semaphores are owned/cleaned by the last consumer.
        """
        logger.warning(
            "Producer.unlink() ignored for '%s'; last consumer owns cleanup",
            self.shm_name,
        )
    
    def __del__(self):
        """Ensure producer unregisters on destruction."""
        try:
            # Check if attributes exist (might not if __init__ failed)
            if hasattr(self, '_closed') and self._closed:
                return
            if hasattr(self, 'shm') and self.shm is not None:
                self._decrement_active_producers()
        except:
            pass  # Ignore errors during cleanup


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
        auto_unlink: bool = True,
    ):
        """
        Initialize the consumer and optionally attach to existing shared memory.
        
        Args:
            shm_name: Name of the POSIX shared memory segment
            total_data_bytes: Total size of the data region in bytes (should match producer)
            blocking: Whether to block when buffer is empty
            max_slot_size: Maximum allowed size per slot (default 10MB)
            auto_attach: If True, automatically attach to shared memory in constructor
            auto_unlink: If True, automatically unlink (delete) shared memory when EOS is received
        """
        super().__init__(shm_name, total_data_bytes, blocking, max_slot_size)
        self.eos_received = False  # Track if end-of-stream was received
        self.auto_unlink = auto_unlink
        if auto_attach:
            self._attach()
    
    def _set_read_pos(self, pos: int):
        """Write read_pos to header."""
        struct.pack_into('<Q', self.shm.buf, HeaderOffset.READ_POS, pos)
    
    def _read_uint64(self, pos: int) -> int:
        """Read a uint64 value handling wraparound."""
        data = self._read_bytes(pos, 8)
        return struct.unpack('<Q', data)[0]
    
    def _read_uint32(self, pos: int) -> int:
        """Read a uint32 value handling wraparound."""
        data = self._read_bytes(pos, 4)
        return struct.unpack('<I', data)[0]
    
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
    
    def _advance_pos(self, pos: int, delta: int) -> int:
        """Advance position by delta bytes, handling wraparound."""
        new_pos = pos + delta
        return self._normalize_pos(new_pos)
    
    def _attach(self):
        """Attach to existing shared memory segment and synchronization primitives."""
        try:
            self.shm = _PosixSharedMemory(self.shm_name, create=False)
            
            # Attach to existing POSIX semaphores for cross-process synchronization
            self.lock, self.condition = _get_buffer_semaphores(self.shm_name, create=False)
            
            # Verify size matches
            if self.shm.size != self.shm_size:
                raise ValueError(
                    f"Shared memory size mismatch: expected {self.shm_size}, "
                    f"got {self.shm.size}"
                )
            
            # Read total_data_bytes from header
            _, _, stored_total = struct.unpack_from('<QQQ', self.shm.buf, HeaderOffset.WRITE_POS)
            if stored_total != self.total_data_bytes:
                logger.warning(
                    f"total_data_bytes mismatch: using stored value {stored_total}"
                )
                self.total_data_bytes = stored_total
                self.shm_size = HEADER_SIZE + stored_total
            
            # Register this consumer
            self._increment_active_consumers()
            
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
            Deserialized object, or None if no more data (all producers finished)
            
        Raises:
            IPCException: With error_type indicating the specific error
            ValueError: If metadata is invalid
        """
        if self.shm is None:
            raise IPCException(IPCError.NOT_INITIALIZED)
        
        # Wait for data if blocking (use condition variable)
        start_time = time.time()
        read_pos = None
        next_pos = None
        
        while True:
            with self.lock:
                write_pos = self._get_write_pos()
                current_read_pos = self._get_read_pos()
                
                # Check if data available
                if write_pos != current_read_pos:
                    # Reserve slot by reading next_pos and updating read_pos atomically
                    read_pos = current_read_pos  # Save the position we'll read from
                    temp_pos = current_read_pos
                    next_pos = self._read_uint64(temp_pos)  # Read where this slot ends
                    self._set_read_pos(next_pos)  # Reserve by moving read_pos forward
                    break  # Exit with slot reserved
                
                # No data available - check if all producers finished
                active_producers = self._get_active_producers()
                if active_producers == 0:
                    # All producers finished and buffer empty - graceful exit
                    self.eos_received = True
                    logger.info("All producers finished, buffer empty")
                    return None
                
                if not self.blocking:
                    raise IPCException(IPCError.BUFFER_EMPTY)
                
                if timeout is not None and (time.time() - start_time) >= timeout:
                    raise IPCException(IPCError.TIMEOUT, f"Timeout after {timeout} seconds")
            
            # Wait for data (releases lock while waiting)
            with self.lock:
                wait_timeout = 0.001  # 1ms
                if timeout is not None:
                    remaining = timeout - (time.time() - start_time)
                    if remaining <= 0:
                        raise IPCException(IPCError.TIMEOUT)
                    wait_timeout = min(wait_timeout, remaining)
                self.condition.wait(timeout=wait_timeout)
        
        # Read slot data (slot reserved via read_pos update above)
        # read_pos contains the original position, next_pos contains where it ends
        current_pos = read_pos
        
        # Skip next_pos field (8 bytes) - we already read it
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
        except Exception as e:
            raise ValueError(f"Failed to parse metadata JSON: {e}")
        
        # Read and verify start sentinel
        start_sentinel = self._read_bytes(current_pos, 1)
        if len(start_sentinel) != 1 or start_sentinel[0] != SENTINEL_BYTE:
            error_msg = f"Invalid start sentinel (expected {SENTINEL_BYTE}, got {start_sentinel[0] if start_sentinel else 'empty'})"
            self.last_error = error_msg
            raise IPCException(IPCError.CORRUPT_PAYLOAD, error_msg)
        current_pos = self._advance_pos(current_pos, 1)
        
        # Read payload
        payload = self._read_bytes(current_pos, payload_size)
        current_pos = self._advance_pos(current_pos, payload_size)
        
        # Read and verify end sentinel
        end_sentinel = self._read_bytes(current_pos, 1)
        if len(end_sentinel) != 1 or end_sentinel[0] != SENTINEL_BYTE:
            error_msg = f"Invalid end sentinel (expected {SENTINEL_BYTE}, got {end_sentinel[0] if end_sentinel else 'empty'})"
            self.last_error = error_msg
            raise IPCException(IPCError.CORRUPT_PAYLOAD, error_msg)
        
        # Deserialize using metadata_str and payload
        try:
            obj = deserialize_object(metadata_str, payload)
        except Exception as e:
            error_msg = f"Failed to deserialize: {e}"
            self.last_error = error_msg
            raise IPCException(IPCError.DESERIALIZATION_FAILED, error_msg) from e
        
        # Notify producers that space is available
        with self.lock:
            self.condition.notify_all()
        
        return obj
    
    def close(self):
        """
        Close the consumer and unregister from active consumers.
        
        Decrements active_consumers counter and notifies waiting producers.
        Last consumer unlinks shared memory if auto_unlink=True and cleans up semaphores.
        """
        if self.shm is not None and not self._closed:
            # Unregister this consumer
            try:
                remaining = self._decrement_active_consumers()
                logger.info(f"Consumer closed, {remaining} active consumers remaining")
                
                # Last consumer cleans up if auto_unlink enabled
                if remaining == 0:
                    if self.auto_unlink:
                        self.unlink()
                        logger.info("Last consumer unlinked shared memory")
                    
                    # Last consumer always cleans up semaphores
                    _cleanup_buffer_semaphores(self.shm_name)
                    logger.info("Last consumer cleaned up POSIX semaphores")
            except Exception as e:
                logger.warning(f"Failed to decrement active_consumers: {e}")
            
            # Close semaphores
            try:
                if hasattr(self, 'condition') and self.condition is not None:
                    if not self.condition._closed:
                        if hasattr(self.condition, 'mutex') and self.condition.mutex is not None:
                            self.condition.mutex.close()
                        if hasattr(self.condition, 'wait_sem') and self.condition.wait_sem is not None:
                            self.condition.wait_sem.close()
                        self.condition._closed = True
            except Exception as e:
                logger.debug(f"Error closing semaphores (may already be closed): {e}")
            
            # Close shared memory
            self.shm.close()
            self._closed = True
            logger.info(f"Closed shared memory '{self.shm_name}'")
    
    def __del__(self):
        """Ensure consumer unregisters on destruction."""
        try:
            # Check if attributes exist (might not if __init__ failed)
            if hasattr(self, '_closed') and self._closed:
                return
            if hasattr(self, 'shm') and self.shm is not None:
                self._decrement_active_consumers()
        except:
            pass  # Ignore errors during cleanup

