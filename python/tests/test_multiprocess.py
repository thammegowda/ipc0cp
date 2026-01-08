"""
Multiprocess and subprocess IPC tests for SharedRingBuffer.
Tests with separate producer and consumer processes.
"""

import multiprocessing
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pytest

from ipc0cp.ring_buffer import SharedRingBufferProducer, SharedRingBufferConsumer


def _cleanup_ipc(name: str) -> None:
    """Best-effort cleanup for shared memory + semaphores."""
    import posix_ipc

    base = name.strip().lstrip("/")
    if not base:
        return

    for sem_suffix in ("_mutex", "_wait"):
        try:
            posix_ipc.unlink_semaphore(f"/{base}{sem_suffix}")
        except posix_ipc.ExistentialError:
            pass
        except Exception:
            pass

    try:
        posix_ipc.unlink_shared_memory(f"/{base}")
    except posix_ipc.ExistentialError:
        pass
    except Exception:
        pass


class TestSharedRingBufferMultiprocess:
    """Test with separate producer and consumer processes."""
    
    @staticmethod
    def producer_process(shm_name: str, num_images: int):
        """Producer process function."""
        producer = SharedRingBufferProducer(shm_name=shm_name, blocking=True)
        
        try:
            for i in range(num_images):
                # Create deterministic image for verification
                image = np.full((100, 100, 3), i % 256, dtype=np.uint8)
                producer.push(image)
                time.sleep(0.01)  # Small delay
        finally:
            producer.close()
    
    @staticmethod
    def consumer_process(shm_name: str, num_images: int, results_queue):
        """Consumer process function."""
        time.sleep(0.1)  # Let producer start first
        
        consumer = SharedRingBufferConsumer(shm_name=shm_name, blocking=True)
        
        try:
            received = []
            for _ in range(num_images):
                image = consumer.pop(timeout=5.0)
                if image is not None:
                    # Store the fill value for verification
                    received.append(int(image[0, 0, 0]))
        finally:
            consumer.close()
            results_queue.put(received)
    
    def test_separate_processes(self):
        """Test producer and consumer in separate processes."""
        shm_name = "test_multiprocess"
        num_images = 20
        
        _cleanup_ipc(shm_name)
        
        results_queue = multiprocessing.Queue()
        
        # Start producer and consumer processes
        producer = multiprocessing.Process(
            target=self.producer_process,
            args=(shm_name, num_images)
        )
        consumer = multiprocessing.Process(
            target=self.consumer_process,
            args=(shm_name, num_images, results_queue)
        )
        
        producer.start()
        consumer.start()
        
        # Wait for completion
        producer.join(timeout=10)
        consumer.join(timeout=10)
        
        # Verify results
        received = results_queue.get(timeout=1)
        expected = [i % 256 for i in range(num_images)]
        
        assert len(received) == num_images
        assert received == expected
        _cleanup_ipc(shm_name)
    
    def test_high_throughput(self):
        """Test high throughput with many small images."""
        shm_name = "test_throughput"
        num_images = 100
        
        _cleanup_ipc(shm_name)
        
        results_queue = multiprocessing.Queue()
        
        producer = multiprocessing.Process(
            target=self.producer_process,
            args=(shm_name, num_images)
        )
        consumer = multiprocessing.Process(
            target=self.consumer_process,
            args=(shm_name, num_images, results_queue)
        )
        
        start_time = time.time()
        
        producer.start()
        consumer.start()
        
        producer.join(timeout=30)
        consumer.join(timeout=30)
        
        elapsed = time.time() - start_time
        
        # Verify all images received
        received = results_queue.get(timeout=1)
        assert len(received) == num_images
        
        # Calculate throughput
        throughput = num_images / elapsed
        print(f"\nThroughput: {throughput:.1f} images/second")
        
        _cleanup_ipc(shm_name)


class TestSharedRingBufferSubprocessIPC:
    """Test with actual subprocess scripts for real IPC testing."""
    
    @pytest.mark.timeout(30)  # This test needs more time for subprocess spawning
    def test_subprocess_producer_consumer(self):
        """Test producer and consumer as separate subprocess scripts."""
        shm_name = "test_subprocess_ipc"
        num_objects = 50
        
        # Get the directory where the test scripts are
        test_dir = Path(__file__).parent
        producer_script = test_dir / "producer_example.py"
        consumer_script = test_dir / "consumer_example.py"
        
        assert producer_script.exists(), f"Producer script not found: {producer_script}"
        assert consumer_script.exists(), f"Consumer script not found: {consumer_script}"
        
        _cleanup_ipc(shm_name)
        
        # Start consumer first (it will wait for producer)
        consumer_process = subprocess.Popen(
            [sys.executable, str(consumer_script), 
             "-s", shm_name, 
             "--buffer-size", "50",
             "--wait-time", "0.5"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Give consumer time to start waiting
        time.sleep(0.2)
        
        # Start producer
        producer_process = subprocess.Popen(
            [sys.executable, str(producer_script),
             "-s", shm_name,
             "-n", str(num_objects),
             "--buffer-size", "50"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for both to complete
        try:
            producer_stdout, producer_stderr = producer_process.communicate(timeout=15)
            consumer_stdout, consumer_stderr = consumer_process.communicate(timeout=15)
            
            # Check exit codes
            assert producer_process.returncode == 0, \
                f"Producer failed with stderr: {producer_stderr}"
            assert consumer_process.returncode == 0, \
                f"Consumer failed with stderr: {consumer_stderr}"
            
            # Verify consumer received all objects
            assert f"Processed {num_objects} objects" in consumer_stdout, \
                f"Consumer output: {consumer_stdout}"
            
            print("\n--- Producer Output ---")
            print(producer_stdout)
            print("\n--- Consumer Output ---")
            print(consumer_stdout)
            
        except subprocess.TimeoutExpired:
            producer_process.kill()
            consumer_process.kill()
            pytest.fail("Subprocess test timed out")
        
        _cleanup_ipc(shm_name)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
