"""
Circular buffer wraparound tests for SharedRingBuffer.
Tests that the circular buffer correctly wraps around.
"""

import numpy as np
import pytest

from ipc0cp.ring_buffer import SharedRingBufferProducer, SharedRingBufferConsumer


class TestSharedRingBufferCircular:
    """Test circular buffer wraparound behavior."""
    
    def test_circular_wraparound(self):
        """Test that buffer wraps around correctly."""
        shm_name = "test_wraparound"
        
        # Use small buffer to force wraparound
        producer = SharedRingBufferProducer(
            shm_name=shm_name,
            total_data_bytes=500 * 1024,  # 500 KB
        )
        consumer = SharedRingBufferConsumer(
            shm_name=shm_name,
        )
        
        try:
            # Push and pop many small images to cause wraparound
            for i in range(50):
                image = np.full((32, 32, 3), i, dtype=np.uint8)
                assert producer.push(image)
                
                retrieved = consumer.pop()
                assert retrieved is not None
                np.testing.assert_array_equal(retrieved, image)
            
            # Verify we've wrapped around
            stats = producer.get_stats()
            # After many operations, positions should have wrapped
            assert stats['write_pos'] != 24  # Not at initial position
        finally:
            consumer.close()
            producer.close()
    
    def test_wraparound_during_write(self):
        """Test wraparound in the middle of writing a slot."""
        shm_name = "test_write_wraparound"
        
        # Very small buffer to force mid-slot wraparound
        producer = SharedRingBufferProducer(
            shm_name=shm_name,
            total_data_bytes=100 * 1024,  # 100 KB
        )
        consumer = SharedRingBufferConsumer(
            shm_name=shm_name,
        )
        
        try:
            # Push and consume to move write_pos near the end
            images_pushed = []
            while True:
                small_img = np.random.randint(0, 256, (16, 16, 3), dtype=np.uint8)
                stats = producer.get_stats()
                
                # Stop when we're near the end
                if stats['write_pos'] > 90 * 1024:
                    break
                
                if producer.push(small_img, timeout=0.1):
                    images_pushed.append(small_img)
                else:
                    # Buffer full, consume some to make space
                    for _ in range(5):
                        if not consumer.is_empty():
                            consumer.pop()
            
            # Consume most images to make space for wrap
            while len(images_pushed) > 3 and not consumer.is_empty():
                consumer.pop()
                images_pushed.pop(0)
            
            # Now push a larger image that will wrap around
            wrap_image = np.random.randint(0, 256, (64, 64, 3), dtype=np.uint8)
            assert producer.push(wrap_image, timeout=1.0)
            
            # Consume remaining images
            count = 0
            last_image = None
            while not consumer.is_empty():
                img = consumer.pop(timeout=0.5)
                if img is not None:
                    last_image = img
                    count += 1
            
            # Verify the wrapped image was written and read correctly
            assert last_image is not None
            assert last_image.shape == wrap_image.shape
            np.testing.assert_array_equal(last_image, wrap_image)
        finally:
            consumer.close()
            producer.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
