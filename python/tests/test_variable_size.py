"""
Variable-size slot tests for SharedRingBuffer.
Tests handling of different object sizes and oversized objects.
"""

import numpy as np
import pytest

from ipc0cp.ring_buffer import SharedRingBufferProducer, SharedRingBufferConsumer


class TestSharedRingBufferVariableSize:
    """Test variable-size image handling."""
    
    def test_mixed_size_images(self):
        """Test pushing images of different sizes."""
        shm_name = "test_mixed_sizes"
        
        producer = SharedRingBufferProducer(
            shm_name=shm_name,
            total_data_bytes=50 * 1024 * 1024,  # 50 MB
        )
        consumer = SharedRingBufferConsumer(
            shm_name=shm_name,
            total_data_bytes=50 * 1024 * 1024,
        )
        
        try:
            # Create images of various sizes
            sizes = [
                (64, 64, 3),      # ~12 KB
                (128, 128, 3),    # ~49 KB
                (256, 256, 3),    # ~196 KB
                (512, 512, 3),    # ~786 KB
                (1024, 1024, 3),  # ~3 MB
            ]
            
            images = [np.random.randint(0, 256, size, dtype=np.uint8) 
                     for size in sizes]
            
            # Push all images
            for img in images:
                assert producer.push(img)
            
            # Pop and verify all images
            for original in images:
                retrieved = consumer.pop()
                assert retrieved is not None
                assert retrieved.shape == original.shape
                np.testing.assert_array_equal(retrieved, original)
            
            assert consumer.is_empty()
        finally:
            consumer.close()
            producer.close()
    
    def test_large_image(self):
        """Test with large images close to max_slot_size."""
        shm_name = "test_large_image"
        
        producer = SharedRingBufferProducer(
            shm_name=shm_name,
            total_data_bytes=100 * 1024 * 1024,  # 100 MB
        )
        consumer = SharedRingBufferConsumer(
            shm_name=shm_name,
            total_data_bytes=100 * 1024 * 1024,
        )
        
        try:
            # Create a large image (~8 MB)
            original = np.random.randint(0, 256, (2048, 1536, 3), dtype=np.uint8)
            
            assert producer.push(original)
            retrieved = consumer.pop()
            
            assert retrieved is not None
            np.testing.assert_array_equal(retrieved, original)
        finally:
            consumer.close()
            producer.close()
    
    def test_oversized_image_rejected(self):
        """Test that images larger than max_slot_size are rejected."""
        shm_name = "test_oversized"
        
        producer = SharedRingBufferProducer(
            shm_name=shm_name,
            max_slot_size=1024 * 1024,  # 1 MB limit
        )
        
        try:
            # Create image larger than 1 MB
            large_image = np.random.randint(0, 256, (1024, 1024, 3), dtype=np.uint8)
            # This is ~3 MB
            
            # Should return False and log warning
            result = producer.push(large_image)
            assert result is False
        finally:
            producer.close()
            # Cleanup is consumer-owned; attach a consumer solely to unlink.
            consumer = SharedRingBufferConsumer(shm_name=shm_name)
            consumer.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
