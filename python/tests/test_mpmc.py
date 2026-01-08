"""
Multi-producer multi-consumer (MPMC) tests for SharedRingBuffer using multiprocessing.

Tests various producer/consumer configurations and edge cases with true cross-process
communication using POSIX semaphores.

**POSIX REQUIREMENT**: These tests require POSIX-compliant platform (Linux, macOS)
and the posix_ipc library. Windows is not supported.
"""

import multiprocessing
import time

import pytest

from ipc0cp.ring_buffer import SharedRingBufferProducer, SharedRingBufferConsumer
from ipc0cp.ipc import IPCError, IPCException


# Top-level functions (required for multiprocessing pickling)

def producer_process(shm_name: str, producer_id: int, num_items: int, results_queue, start_barrier=None):
    """Producer process that sends numbered items."""
    print(f"Producer {producer_id} starting...")
    try:
        producer = SharedRingBufferProducer(
            shm_name=shm_name,
            total_data_bytes=10 * 1024 * 1024,  # 10 MB
            blocking=True
        )
        print(f"Producer {producer_id} initialized successfully")
    except Exception as e:
        print(f"Producer {producer_id} failed to initialize: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Wait for all producers to initialize if barrier provided
    if start_barrier is not None:
        print(f"Producer {producer_id} waiting at barrier...")
        try:
            start_barrier.wait(timeout=60)  # 60-second timeout
            print(f"Producer {producer_id} released from barrier, starting to send data")
        except Exception as e:
            print(f"Producer {producer_id} barrier timeout after 60s: {e}")
            producer.close()
            raise RuntimeError(f"Producer {producer_id} failed to synchronize with other producers")
    
    try:
        items_sent = 0
        for i in range(num_items):
            # Send item with producer_id embedded
            data = {"producer_id": producer_id, "item_num": i, "value": i * 100 + producer_id}
            success = producer.push(data)
            if not success:
                print(f"Producer {producer_id} failed to push item {i}")
                break
            items_sent += 1
            time.sleep(0.001)  # Small delay to simulate work
        
        print(f"Producer {producer_id} sent {items_sent} items")
        results_queue.put({"producer_id": producer_id, "items_sent": items_sent})
    except Exception as e:
        print(f"Producer {producer_id} error during push: {e}")
        import traceback
        traceback.print_exc()
    finally:
        producer.close()
        print(f"Producer {producer_id} closed")


def consumer_process(shm_name: str, consumer_id: int, results_queue):
    """Consumer process that receives items until no more producers."""
    # Retry attaching to shared memory (producer may not have created it yet)
    consumer = None
    for attempt in range(20):
        try:
            consumer = SharedRingBufferConsumer(
                shm_name=shm_name,
                total_data_bytes=10 * 1024 * 1024,
                blocking=True
            )
            break  # Success
        except FileNotFoundError:
            if attempt < 19:
                time.sleep(0.1)  # Wait for producer to create shared memory
            else:
                # Test setup failure - shared memory never appeared
                raise RuntimeError(
                    f"Consumer {consumer_id} failed to attach to shared memory "
                    f"'{shm_name}' after 20 attempts"
                )
    
    try:
        items_received = []
        while True:
            item = consumer.pop(timeout=2.0)
            if item is None:
                break  # All producers done
            items_received.append(item)
        
        results_queue.put({
            "consumer_id": consumer_id,
            "items_received": len(items_received),
            "items": items_received
        })
    finally:
        consumer.close()


@pytest.fixture
def cleanup_shm():
    """Fixture to cleanup shared memory and semaphores before and after tests."""
    shm_names = []
    
    def _register(name):
        """Register a shared memory name for cleanup."""
        import posix_ipc

        base = str(name).strip().lstrip("/")
        shm_names.append(base)

        # Cleanup before test
        try:
            posix_ipc.unlink_shared_memory(f"/{base}")
        except posix_ipc.ExistentialError:
            pass
        except Exception:
            pass

        # Also cleanup semaphores
        for sem_suffix in ("_mutex", "_wait"):
            try:
                posix_ipc.unlink_semaphore(f"/{base}{sem_suffix}")
            except posix_ipc.ExistentialError:
                pass
            except Exception:
                pass

        return base
    
    yield _register
    
    # Cleanup after test (even if test fails)
    for name in shm_names:
        import posix_ipc

        base = str(name).strip().lstrip("/")
        try:
            posix_ipc.unlink_shared_memory(f"/{base}")
        except posix_ipc.ExistentialError:
            pass
        except Exception:
            pass

        for sem_suffix in ("_mutex", "_wait"):
            try:
                posix_ipc.unlink_semaphore(f"/{base}{sem_suffix}")
            except posix_ipc.ExistentialError:
                pass
            except Exception:
                pass


class TestMPMCMultiprocessing:
    """Test multi-producer multi-consumer scenarios using separate processes."""
    
    def test_1_producer_1_consumer(self, cleanup_shm):
        """Test basic 1 producer, 1 consumer scenario."""
        shm_name = cleanup_shm("test_mpmc_1p1c")
        num_items = 100
        
        results_queue = multiprocessing.Queue()
        
        producer = multiprocessing.Process(
            target=producer_process,
            args=(shm_name, 0, num_items, results_queue)
        )
        consumer = multiprocessing.Process(
            target=consumer_process,
            args=(shm_name, 0, results_queue)
        )
        
        producer.start()
        consumer.start()
        
        producer.join(timeout=10)
        consumer.join(timeout=10)
        
        # Check results
        producer_result = results_queue.get(timeout=1)
        consumer_result = results_queue.get(timeout=1)
        
        assert producer_result["items_sent"] == num_items
        assert consumer_result["items_received"] == num_items
    
    def test_4_producers_4_consumers(self, cleanup_shm):
        """Test 4 producers, 4 consumers scenario."""
        shm_name = cleanup_shm("test_mpmc_4p4c")
        num_items_per_producer = 25
        num_producers = 4
        num_consumers = 4
        total_expected = num_items_per_producer * num_producers
        
        results_queue = multiprocessing.Queue()
        
        # Create barrier to synchronize producer starts
        start_barrier = multiprocessing.Barrier(num_producers)
        
        # Start all producers with barrier synchronization
        producers = []
        for i in range(num_producers):
            p = multiprocessing.Process(
                target=producer_process,
                args=(shm_name, i, num_items_per_producer, results_queue, start_barrier)
            )
            producers.append(p)
            p.start()
            # Small delay after first producer to ensure shared memory is created
            if i == 0:
                time.sleep(0.3)
        
        # Wait for all producers to initialize and hit barrier
        time.sleep(0.5)
        
        # Start consumers
        consumers = []
        for i in range(num_consumers):
            c = multiprocessing.Process(
                target=consumer_process,
                args=(shm_name, i, results_queue)
            )
            consumers.append(c)
            c.start()
        
        # Wait for all to finish
        for p in producers:
            p.join(timeout=15)
        for c in consumers:
            c.join(timeout=15)
        
        # Collect results
        all_items_sent = 0
        all_items_received = []
        
        while not results_queue.empty():
            result = results_queue.get(timeout=1)
            if "items_sent" in result:
                all_items_sent += result["items_sent"]
            elif "items" in result:
                all_items_received.extend(result["items"])
        
        # Verify all items sent == all items received
        assert all_items_sent == total_expected
        assert len(all_items_received) == total_expected
        
        # Verify no duplicates - each item should have unique (producer_id, item_num)
        item_keys = [(item["producer_id"], item["item_num"]) for item in all_items_received]
        assert len(item_keys) == len(set(item_keys)), "Duplicate items detected"
    
    def test_1_producer_4_consumers(self, cleanup_shm):
        """Test 1 producer, 4 consumers scenario."""
        shm_name = cleanup_shm("test_mpmc_1p4c")
        num_items = 100
        num_consumers = 4
        
        results_queue = multiprocessing.Queue()
        
        # Start producer
        producer = multiprocessing.Process(
            target=producer_process,
            args=(shm_name, 0, num_items, results_queue)
        )
        producer.start()
        
        # Start consumers
        consumers = []
        for i in range(num_consumers):
            c = multiprocessing.Process(
                target=consumer_process,
                args=(shm_name, i, results_queue)
            )
            consumers.append(c)
            c.start()
        
        # Wait for all
        producer.join(timeout=10)
        for c in consumers:
            c.join(timeout=10)
        
        # Collect results
        all_items_received = []
        while not results_queue.empty():
            result = results_queue.get(timeout=1)
            if "items" in result:
                all_items_received.extend(result["items"])
        
        # Should receive exactly num_items total
        assert len(all_items_received) == num_items
        
        # Verify no duplicates
        item_keys = [(item["producer_id"], item["item_num"]) for item in all_items_received]
        assert len(item_keys) == len(set(item_keys)), "Duplicate items detected"
    
    def test_4_producers_1_consumer(self, cleanup_shm):
        """Test 4 producers, 1 consumer scenario."""
        shm_name = cleanup_shm("test_mpmc_4p1c")
        num_items_per_producer = 25
        num_producers = 4
        total_expected = num_items_per_producer * num_producers
        
        results_queue = multiprocessing.Queue()
        
        # Create barrier to synchronize producer starts
        start_barrier = multiprocessing.Barrier(num_producers)
        
        # Start all producers with barrier synchronization
        producers = []
        for i in range(num_producers):
            p = multiprocessing.Process(
                target=producer_process,
                args=(shm_name, i, num_items_per_producer, results_queue, start_barrier)
            )
            producers.append(p)
            p.start()
            # Small delay after first producer to ensure shared memory is created
            if i == 0:
                time.sleep(0.3)
        
        # Wait for all producers to initialize and hit barrier
        time.sleep(0.5)
        
        # Start consumer
        consumer = multiprocessing.Process(
            target=consumer_process,
            args=(shm_name, 0, results_queue)
        )
        consumer.start()
        
        # Wait for all
        for p in producers:
            p.join(timeout=15)
        consumer.join(timeout=15)
        
        # Collect results
        all_items_received = []
        while not results_queue.empty():
            result = results_queue.get(timeout=1)
            if "items" in result:
                all_items_received.extend(result["items"])
        
        # Should receive exactly total_expected items
        assert len(all_items_received) == total_expected
        
        # Verify no duplicates
        item_keys = [(item["producer_id"], item["item_num"]) for item in all_items_received]
        assert len(item_keys) == len(set(item_keys)), "Duplicate items detected"
    
    def test_2_producers_1_consumer(self, cleanup_shm):
        """Test 2 producers, 1 consumer scenario."""
        shm_name = cleanup_shm("test_mpmc_2p1c")
        num_items_per_producer = 50
        num_producers = 2
        total_expected = num_items_per_producer * num_producers
        
        results_queue = multiprocessing.Queue()
        
        # Create barrier to synchronize producer starts
        start_barrier = multiprocessing.Barrier(num_producers)
        
        # Start all producers with barrier synchronization
        producers = []
        for i in range(num_producers):
            p = multiprocessing.Process(
                target=producer_process,
                args=(shm_name, i, num_items_per_producer, results_queue, start_barrier)
            )
            producers.append(p)
            p.start()
            # Small delay after first producer to ensure shared memory is created
            if i == 0:
                time.sleep(0.3)
        
        # Wait for all producers to initialize and hit barrier
        time.sleep(0.5)
        
        # Start consumer
        consumer = multiprocessing.Process(
            target=consumer_process,
            args=(shm_name, 0, results_queue)
        )
        consumer.start()
        
        # Wait for all
        for p in producers:
            p.join(timeout=15)
        consumer.join(timeout=15)
        
        # Collect results
        all_items_received = []
        all_items_sent = 0
        while not results_queue.empty():
            result = results_queue.get(timeout=1)
            if "items_sent" in result:
                all_items_sent += result["items_sent"]
            elif "items" in result:
                all_items_received.extend(result["items"])
        
        # Verify counts
        print(f"\n2P1C: Sent={all_items_sent}, Received={len(all_items_received)}")
        # Count items per producer
        producer_counts = {}
        for item in all_items_received:
            pid = item["producer_id"]
            producer_counts[pid] = producer_counts.get(pid, 0) + 1
        print(f"Items per producer: {producer_counts}")
        
        # Should receive exactly total_expected items
        assert all_items_sent == total_expected
        assert len(all_items_received) == total_expected
        
        # Verify no duplicates
        item_keys = [(item["producer_id"], item["item_num"]) for item in all_items_received]
        assert len(item_keys) == len(set(item_keys)), "Duplicate items detected"
    
    def test_6_producers_1_consumer(self, cleanup_shm):
        """Test 6 producers, 1 consumer scenario."""
        shm_name = cleanup_shm("test_mpmc_6p1c")
        num_items_per_producer = 20
        num_producers = 6
        total_expected = num_items_per_producer * num_producers
        
        results_queue = multiprocessing.Queue()
        
        # Create barrier to synchronize producer starts
        start_barrier = multiprocessing.Barrier(num_producers)
        
        # Start all producers with barrier synchronization
        producers = []
        for i in range(num_producers):
            p = multiprocessing.Process(
                target=producer_process,
                args=(shm_name, i, num_items_per_producer, results_queue, start_barrier)
            )
            producers.append(p)
            p.start()
            # Small delay after first producer to ensure shared memory is created
            if i == 0:
                time.sleep(0.3)
        
        # Wait for all producers to initialize and hit barrier
        time.sleep(0.5)
        
        # Start consumer
        consumer = multiprocessing.Process(
            target=consumer_process,
            args=(shm_name, 0, results_queue)
        )
        consumer.start()
        
        # Wait for all
        for p in producers:
            p.join(timeout=15)
        consumer.join(timeout=15)
        
        # Collect results
        all_items_received = []
        all_items_sent = 0
        while not results_queue.empty():
            result = results_queue.get(timeout=1)
            if "items_sent" in result:
                all_items_sent += result["items_sent"]
            elif "items" in result:
                all_items_received.extend(result["items"])
        
        # Verify counts
        print(f"\n6P1C: Sent={all_items_sent}, Received={len(all_items_received)}")
        # Count items per producer
        producer_counts = {}
        for item in all_items_received:
            pid = item["producer_id"]
            producer_counts[pid] = producer_counts.get(pid, 0) + 1
        print(f"Items per producer: {producer_counts}")
        
        # Should receive exactly total_expected items
        assert all_items_sent == total_expected
        assert len(all_items_received) == total_expected
        
        # Verify no duplicates
        item_keys = [(item["producer_id"], item["item_num"]) for item in all_items_received]
        assert len(item_keys) == len(set(item_keys)), "Duplicate items detected"
    
    def test_10_producers_1_consumer(self, cleanup_shm):
        """Test 10 producers, 1 consumer scenario."""
        shm_name = cleanup_shm("test_mpmc_10p1c")
        num_items_per_producer = 15
        num_producers = 10
        total_expected = num_items_per_producer * num_producers
        
        results_queue = multiprocessing.Queue()
        
        # Create barrier to synchronize producer starts
        start_barrier = multiprocessing.Barrier(num_producers)
        
        # Start all producers with barrier synchronization
        producers = []
        for i in range(num_producers):
            p = multiprocessing.Process(
                target=producer_process,
                args=(shm_name, i, num_items_per_producer, results_queue, start_barrier)
            )
            producers.append(p)
            p.start()
            # Small delay after first producer to ensure shared memory is created
            if i == 0:
                time.sleep(0.3)
        
        # Wait for all producers to initialize and hit barrier
        time.sleep(0.5)
        
        # Start consumer
        consumer = multiprocessing.Process(
            target=consumer_process,
            args=(shm_name, 0, results_queue)
        )
        consumer.start()
        
        # Wait for all
        for p in producers:
            p.join(timeout=20)
        consumer.join(timeout=20)
        
        # Collect results
        all_items_received = []
        all_items_sent = 0
        while not results_queue.empty():
            result = results_queue.get(timeout=1)
            if "items_sent" in result:
                all_items_sent += result["items_sent"]
            elif "items" in result:
                all_items_received.extend(result["items"])
        
        # Verify counts
        print(f"\n10P1C: Sent={all_items_sent}, Received={len(all_items_received)}")
        # Count items per producer
        producer_counts = {}
        for item in all_items_received:
            pid = item["producer_id"]
            producer_counts[pid] = producer_counts.get(pid, 0) + 1
        print(f"Items per producer: {producer_counts}")
        
        # Should receive exactly total_expected items
        assert all_items_sent == total_expected
        assert len(all_items_received) == total_expected
        
        # Verify no duplicates
        item_keys = [(item["producer_id"], item["item_num"]) for item in all_items_received]
        assert len(item_keys) == len(set(item_keys)), "Duplicate items detected"
    
    def test_producer_no_consumers(self, cleanup_shm):
        """Test producer with no consumers - should raise error on buffer full."""
        shm_name = cleanup_shm("test_mpmc_no_consumers")
        
        producer = SharedRingBufferProducer(
            shm_name=shm_name,
            total_data_bytes=1024,  # Small buffer
            blocking=False  # Non-blocking
        )
        
        try:
            # Fill buffer until we get an error
            count = 0
            while count < 100:
                try:
                    producer.push(b"x" * 100)
                    count += 1
                except IPCException as e:
                    if e.error_type == IPCError.BUFFER_FULL or e.error_type == IPCError.NO_CONSUMERS:
                        # Expected - no consumers to drain buffer
                        break
                    raise
            
            # Should have gotten buffer full error before 100 items
            assert count < 100, "Expected BUFFER_FULL error"
            
        finally:
            producer.close()
    
    def test_consumer_no_producers(self, cleanup_shm):
        """Test consumer with no producers - should timeout."""
        shm_name = cleanup_shm("test_mpmc_no_producers")
        
        # Create producer just to initialize buffer, then close immediately
        producer = SharedRingBufferProducer(shm_name=shm_name)
        producer.close()
        
        consumer = SharedRingBufferConsumer(
            shm_name=shm_name,
            blocking=True
        )
        
        try:
            # Try to pop with short timeout - should get None
            item = consumer.pop(timeout=0.5)
            assert item is None, "Expected None when no producers active"
            
        finally:
            consumer.close()
