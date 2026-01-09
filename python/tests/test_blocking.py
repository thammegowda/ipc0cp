"""
Blocking and non-blocking behavior tests for SharedRingBuffer.
Tests buffer full/empty conditions and timeout handling.
"""

import time

import numpy as np
import pytest

from ipc0cp.ring_buffer import SharedRingBufferProducer, SharedRingBufferConsumer
from ipc0cp.ipc import IPCException, IPCError


class TestSharedRingBufferBlocking:
    """Test blocking and non-blocking behavior."""
    
    def test_buffer_full_blocking(self):
        """Test that producer blocks when buffer is full."""
        shm_name = "test_full_blocking"
        
        producer = SharedRingBufferProducer(
            shm_name=shm_name,
            total_data_bytes=200 * 1024,  # 200 KB
            blocking=True,
        )

        # Attach at least one consumer so producers are allowed to proceed.
        consumer = SharedRingBufferConsumer(
            shm_name=shm_name,
            blocking=True,
        )
        
        try:
            # Fill buffer
            images_pushed = 0
            while True:
                image = np.random.randint(0, 256, (64, 64, 3), dtype=np.uint8)
                # Use timeout to avoid infinite blocking in test
                if not producer.push(image, timeout=0.1):
                    break
                images_pushed += 1
            
            # Should have pushed multiple images
            assert images_pushed > 0
            
            # Buffer should be nearly full
            stats = producer.get_stats()
            assert stats['available_bytes'] < 50 * 1024
        finally:
            consumer.close()
            producer.close()
    
    def test_buffer_empty_nonblocking(self):
        """Test that consumer raises exception when buffer is empty and non-blocking."""
        shm_name = "test_empty_nonblocking"
        
        # Create the shared memory first with a producer
        producer = SharedRingBufferProducer(
            shm_name=shm_name,
            total_data_bytes=200 * 1024,  # 200 KB
            blocking=False,
        )
        
        consumer = SharedRingBufferConsumer(
            shm_name=shm_name,
            blocking=False,
        )
        
        try:
            # Try to pop from empty buffer - should raise exception
            with pytest.raises(IPCException) as exc_info:
                consumer.pop()
            assert exc_info.value.error_type == IPCError.BUFFER_EMPTY
        finally:
            consumer.close()
            producer.close()
    
    def test_buffer_empty_blocking_timeout(self):
        """Test that consumer respects timeout when buffer is empty."""
        shm_name = "test_empty_timeout"
        
        # Create the shared memory first with a producer
        producer = SharedRingBufferProducer(
            shm_name=shm_name,
            total_data_bytes=200 * 1024,  # 200 KB
            blocking=True,
        )
        
        consumer = SharedRingBufferConsumer(
            shm_name=shm_name,
            blocking=True,
        )
        
        try:
            start = time.time()
            # Should raise timeout exception
            with pytest.raises(IPCException) as exc_info:
                consumer.pop(timeout=0.5)
            elapsed = time.time() - start
            
            assert exc_info.value.error_type == IPCError.TIMEOUT
            assert 0.4 < elapsed < 0.7  # Should wait approximately 0.5 seconds
        finally:
            consumer.close()
            producer.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
