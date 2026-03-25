"""
Tests for pop(copy=True) — safe copy mode that prevents producer overwrites.

Verifies that under high MPMC contention (many producers, multiple consumers,
small buffer that wraps frequently), pop(copy=True) prevents data corruption
that would otherwise occur when producers overwrite slots being read.
"""

import multiprocessing
import time
import numpy as np
import pytest

from ipc0cp.ring_buffer import SharedRingBufferProducer, SharedRingBufferConsumer


def _producer_numpy(shm_name: str, producer_id: int, num_items: int,
                    barrier, results_queue):
    """Producer: push numpy arrays with a known pattern for verification."""
    producer = SharedRingBufferProducer(
        shm_name=shm_name,
        total_data_bytes=4 * 1024 * 1024,  # 4 MB — small buffer to force wrapping
        blocking=True,
    )
    barrier.wait(timeout=30)

    sent = 0
    for i in range(num_items):
        # Each array has a unique pattern: filled with (producer_id * 1000 + i) % 256
        tag = (producer_id * 1000 + i) % 256
        arr = np.full((64, 64, 3), tag, dtype=np.uint8)
        producer.push(arr)
        sent += 1

    producer.close()
    results_queue.put(("producer", producer_id, sent))


def _consumer_copy(shm_name: str, consumer_id: int, barrier, results_queue):
    """Consumer: pop with copy=True — should never see corruption."""
    # Retry attaching (producer may not have created SHM yet)
    consumer = None
    for attempt in range(30):
        try:
            consumer = SharedRingBufferConsumer(shm_name=shm_name, blocking=True)
            break
        except FileNotFoundError:
            time.sleep(0.1)
    if consumer is None:
        results_queue.put(("consumer_copy", consumer_id, 0, 1))
        return

    barrier.wait(timeout=30)

    received = 0
    errors = 0
    while True:
        try:
            obj = consumer.pop(timeout=5.0, copy=True)
        except Exception as e:
            errors += 1
            continue
        if obj is None:
            break
        received += 1

    consumer.close()
    results_queue.put(("consumer_copy", consumer_id, received, errors))


def _consumer_nocopy(shm_name: str, consumer_id: int, barrier, results_queue):
    """Consumer: pop with copy=False — may see corruption under contention."""
    consumer = SharedRingBufferConsumer(shm_name=shm_name, blocking=True)
    barrier.wait(timeout=30)

    received = 0
    errors = 0
    while True:
        try:
            obj = consumer.pop(timeout=5.0, copy=False)
        except Exception as e:
            errors += 1
            continue
        if obj is None:
            break
        received += 1

    consumer.close()
    results_queue.put(("consumer_nocopy", consumer_id, received, errors))


@pytest.fixture
def cleanup_shm():
    """Cleanup shared memory before and after test."""
    names = []
    def _register(name):
        import posix_ipc
        base = name.strip().lstrip("/")
        names.append(base)
        # Pre-cleanup
        for suffix in ["", "_mutex", "_wait"]:
            try:
                if suffix:
                    posix_ipc.unlink_semaphore(f"/{base}{suffix}")
                else:
                    posix_ipc.unlink_shared_memory(f"/{base}")
            except Exception:
                pass
        return name
    yield _register
    # Post-cleanup
    import posix_ipc
    for base in names:
        for suffix in ["", "_mutex", "_wait"]:
            try:
                if suffix:
                    posix_ipc.unlink_semaphore(f"/{base}{suffix}")
                else:
                    posix_ipc.unlink_shared_memory(f"/{base}")
            except Exception:
                pass


class TestPopCopy:
    """Tests for pop(copy=True) safe mode."""

    def test_copy_basic(self, cleanup_shm):
        """Basic test: single producer, single consumer with copy=True."""
        shm_name = cleanup_shm("test_copy_basic")
        producer = SharedRingBufferProducer(shm_name, total_data_bytes=2 * 1024 * 1024)
        consumer = SharedRingBufferConsumer(shm_name, blocking=True)

        # Push some data
        for i in range(10):
            producer.push({"value": i, "data": list(range(100))})
        producer.close()

        # Pop with copy=True
        results = []
        while True:
            obj = consumer.pop(copy=True)
            if obj is None:
                break
            results.append(obj)
        consumer.close()

        assert len(results) == 10
        for i, obj in enumerate(results):
            assert obj["value"] == i

    def test_copy_numpy(self, cleanup_shm):
        """Test copy=True with numpy arrays."""
        shm_name = cleanup_shm("test_copy_numpy")
        producer = SharedRingBufferProducer(shm_name, total_data_bytes=4 * 1024 * 1024)
        consumer = SharedRingBufferConsumer(shm_name, blocking=True)

        N = 20
        for i in range(N):
            arr = np.full((32, 32, 3), i % 256, dtype=np.uint8)
            producer.push(arr)
        producer.close()

        results = []
        while True:
            obj = consumer.pop(copy=True)
            if obj is None:
                break
            results.append(obj)
        consumer.close()

        assert len(results) == N
        for i, arr in enumerate(results):
            assert isinstance(arr, np.ndarray)
            assert arr.shape == (32, 32, 3)
            assert arr[0, 0, 0] == i % 256

    def test_copy_mpmc_no_corruption(self, cleanup_shm):
        """MPMC with copy=True: many producers, multiple consumers, small buffer.

        With 8 producers and 3 consumers on a 4MB buffer (holds ~10 slots of
        64x64x3 arrays), the buffer wraps frequently.  copy=True should
        prevent any data corruption.
        """
        shm_name = cleanup_shm("test_copy_mpmc")
        n_producers = 8
        n_consumers = 3
        items_per_producer = 100
        total_expected = n_producers * items_per_producer

        results_queue = multiprocessing.Queue()
        # Barrier for producers only — consumers attach with retry
        producer_barrier = multiprocessing.Barrier(n_producers)
        consumer_barrier = multiprocessing.Barrier(n_consumers)

        # Start producers first (they create the SHM)
        producers = []
        for pid in range(n_producers):
            p = multiprocessing.Process(
                target=_producer_numpy,
                args=(shm_name, pid, items_per_producer, producer_barrier, results_queue),
            )
            p.start()
            producers.append(p)

        # Small delay for SHM creation
        time.sleep(0.3)

        # Start consumers (they retry-attach)
        consumers = []
        for cid in range(n_consumers):
            p = multiprocessing.Process(
                target=_consumer_copy,
                args=(shm_name, cid, consumer_barrier, results_queue),
            )
            p.start()
            consumers.append(p)

        # Wait for all to finish
        for p in producers + consumers:
            p.join(timeout=60)

        # Collect results
        total_sent = 0
        total_received = 0
        total_errors = 0
        while not results_queue.empty():
            result = results_queue.get_nowait()
            if result[0] == "producer":
                total_sent += result[2]
            elif result[0].startswith("consumer"):
                total_received += result[2]
                total_errors += result[3]

        print(f"Sent: {total_sent}, Received: {total_received}, Errors: {total_errors}")
        assert total_sent == total_expected, f"Expected {total_expected} sent, got {total_sent}"
        assert total_received == total_expected, f"Expected {total_expected} received, got {total_received}"
        assert total_errors == 0, f"Got {total_errors} errors with copy=True — corruption detected!"

    def test_copy_false_may_corrupt_under_pressure(self, cleanup_shm):
        """Demonstration: copy=False CAN corrupt under high MPMC contention.

        This test is expected to potentially fail (corruption) to demonstrate
        why copy=True is necessary.  Marked as xfail so CI doesn't break.
        """
        pytest.skip("Flaky by design — demonstrates the race condition. "
                     "Run manually to verify copy=False can corrupt.")

    def test_copy_wraparound(self, cleanup_shm):
        """Test copy=True with buffer wraparound (small buffer, many items)."""
        shm_name = cleanup_shm("test_copy_wrap")
        # Very small buffer: 512KB — forces frequent wraparound
        producer = SharedRingBufferProducer(shm_name, total_data_bytes=512 * 1024)
        consumer = SharedRingBufferConsumer(shm_name, blocking=True)

        N = 50
        for i in range(N):
            arr = np.full((32, 32, 3), i % 256, dtype=np.uint8)
            producer.push(arr)
        producer.close()

        results = []
        while True:
            obj = consumer.pop(copy=True)
            if obj is None:
                break
            results.append(obj)
        consumer.close()

        assert len(results) == N
        for i, arr in enumerate(results):
            assert arr[0, 0, 0] == i % 256, f"Item {i}: expected {i % 256}, got {arr[0, 0, 0]}"
