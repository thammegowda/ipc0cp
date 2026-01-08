"""
Basic functionality tests for SharedRingBuffer.
Tests creation, attachment, and simple push/pop operations.
"""

import numpy as np
import pytest

from ipc0cp.ring_buffer import SharedRingBufferProducer, SharedRingBufferConsumer


class TestSharedRingBufferBasic:
    """Basic tests for SharedRingBuffer functionality."""
    
    def test_create_and_attach(self):
        """Test creating and attaching to shared memory."""
        shm_name = "test_create_attach"
        
        # Create buffer
        producer = SharedRingBufferProducer(
            shm_name=shm_name,
            total_data_bytes=1024 * 1024,  # 1 MB
        )
        
        try:
            # Attach to same buffer
            consumer = SharedRingBufferConsumer(
                shm_name=shm_name,
                total_data_bytes=1024 * 1024,
            )
            
            try:
                # Verify both see same empty buffer
                assert producer.is_empty()
                assert consumer.is_empty()
            finally:
                consumer.close()
        finally:
            producer.close()
    
    def test_push_pop_single_image(self):
        """Test pushing and popping a single image."""
        shm_name = "test_single_image"
        
        producer = SharedRingBufferProducer(shm_name=shm_name)
        consumer = SharedRingBufferConsumer(shm_name=shm_name)
        
        try:
            # Create a simple test image
            original = np.random.randint(0, 256, (100, 100, 3), dtype=np.uint8)
            
            # Push image
            assert producer.push(original)
            
            # Pop image
            retrieved = consumer.pop()
            
            # Verify
            assert retrieved is not None
            assert retrieved.shape == original.shape
            assert retrieved.dtype == original.dtype
            np.testing.assert_array_equal(retrieved, original)
            
            # Buffer should be empty now
            assert consumer.is_empty()
        finally:
            consumer.close()
            producer.close()
    
    def test_grayscale_image(self):
        """Test with grayscale (2D) images."""
        shm_name = "test_grayscale"
        
        producer = SharedRingBufferProducer(shm_name=shm_name)
        consumer = SharedRingBufferConsumer(shm_name=shm_name)
        
        try:
            # Create grayscale image
            original = np.random.randint(0, 256, (64, 64), dtype=np.uint8)
            
            producer.push(original)
            retrieved = consumer.pop()
            
            assert retrieved.shape == (64, 64)
            np.testing.assert_array_equal(retrieved, original)
        finally:
            consumer.close()
            producer.close()
    
    def test_multiple_dtypes(self):
        """Test with different NumPy dtypes."""
        shm_name = "test_dtypes"
        
        producer = SharedRingBufferProducer(shm_name=shm_name)
        consumer = SharedRingBufferConsumer(shm_name=shm_name)
        
        try:
            dtypes = [np.uint8, np.int8, np.uint16, np.int16, 
                     np.uint32, np.int32, np.float32, np.float64]
            
            for dtype in dtypes:
                if np.issubdtype(dtype, np.floating):
                    original = np.random.rand(32, 32, 3).astype(dtype)
                else:
                    info = np.iinfo(dtype)
                    original = np.random.randint(
                        max(0, info.min), min(256, info.max),
                        (32, 32, 3), dtype=dtype
                    )
                
                producer.push(original)
                retrieved = consumer.pop()
                
                assert retrieved.dtype == dtype
                np.testing.assert_array_equal(retrieved, original)
        finally:
            consumer.close()
            producer.close()


class TestSharedRingBufferStats:
    """Test buffer statistics and monitoring."""
    
    def test_get_stats(self):
        """Test get_stats method."""
        shm_name = "test_stats"
        producer = SharedRingBufferProducer(shm_name=shm_name)
        consumer = SharedRingBufferConsumer(shm_name=shm_name)
        
        try:
            # Initial stats
            stats = producer.get_stats()
            assert stats['is_empty'] is True
            assert stats['used_bytes'] == 0
            assert stats['available_bytes'] == stats['total_data_bytes']
            
            # Push an image
            image = np.random.randint(0, 256, (100, 100, 3), dtype=np.uint8)
            producer.push(image)
            
            # Stats after push
            stats = producer.get_stats()
            assert stats['is_empty'] is False
            assert stats['used_bytes'] > 0
            assert stats['available_bytes'] < stats['total_data_bytes']
        finally:
            consumer.close()
            producer.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
