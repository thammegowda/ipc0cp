"""Smoke test for push_raw_batch / pop_batch under multi-producer/consumer load.

Verifies that batched push + batched pop deliver every item exactly once with
intact payloads, matching the single-item path's semantics.
"""

import hashlib
import multiprocessing as mp
import os
import time

import numpy as np

from ipc0cp import SharedRingBufferProducer, SharedRingBufferConsumer
from ipc0cp.ipc import IPCError, IPCException
from ipc0cp.serialize import serialize_object, NumpyArray, JsonData, ListData


TOTAL_BYTES = 2 * 1024 * 1024  # small enough to force wraparound
MAX_SLOT = 8 * 1024 * 1024
N_PRODUCERS = 6
N_CONSUMERS = 3
ITEMS_PER_PRODUCER = 2000
PUSH_BATCH = 32
POP_BATCH = 16


def _payload(pid, idx):
    # Small variable-size numpy array + metadata so checksums are meaningful.
    n = 16 + (idx % 64)
    arr = np.full((n,), (pid * 100003 + idx) % 251, dtype=np.uint8)
    meta = {"pid": pid, "idx": idx, "csum": hashlib.md5(arr.tobytes()).hexdigest()}
    return serialize_object(ListData([NumpyArray(arr), JsonData(meta)]))


def _assert_ipc_error(fn, error_type):
    try:
        fn()
    except IPCException as exc:
        assert exc.error_type == error_type, exc
    else:
        raise AssertionError(f"expected IPCException({error_type})")


def _check_obj(obj):
    arr, meta = obj
    csum = hashlib.md5(np.asarray(arr).tobytes()).hexdigest()
    assert csum == meta["csum"]


def _test_empty_timeout_and_oversize():
    shm = f"batch_edges_{os.getpid()}"
    owner = SharedRingBufferProducer(
        shm, total_data_bytes=4096, max_slot_size=1024,
        create_if_not_exists=True,
    )
    nonblocking_cons = SharedRingBufferConsumer(
        shm, blocking=False, auto_unlink=False,
    )
    timeout_cons = SharedRingBufferConsumer(
        shm, blocking=True, auto_unlink=True,
    )

    _assert_ipc_error(lambda: nonblocking_cons.pop_batch(1), IPCError.BUFFER_EMPTY)
    nonblocking_cons.close()
    _assert_ipc_error(lambda: timeout_cons.pop_batch(1, timeout=0.01), IPCError.TIMEOUT)

    payload_too_large = b"x" * 2048
    try:
        owner.push_raw("{}", payload_too_large, timeout=0.01)
    except ValueError as exc:
        assert "max_slot_size" in str(exc)
    else:
        raise AssertionError("push_raw accepted payload above max_slot_size")

    try:
        owner.push_raw_batch([("{}", payload_too_large)], timeout=0.01)
    except ValueError as exc:
        assert "max_slot_size" in str(exc)
    else:
        raise AssertionError("push_raw_batch accepted payload above max_slot_size")

    owner.close()
    timeout_cons.close()


def _test_impossible_slot_rejected():
    shm = f"batch_impossible_{os.getpid()}"
    owner = SharedRingBufferProducer(
        shm, total_data_bytes=128, max_slot_size=256,
        create_if_not_exists=True,
    )
    cons = SharedRingBufferConsumer(shm, auto_unlink=True)
    payload = b"x" * 120

    try:
        owner.push_raw_batch([("{}", payload)], timeout=0.01)
    except ValueError as exc:
        assert "data region" in str(exc)
    else:
        raise AssertionError("push_raw_batch accepted an impossible slot")

    try:
        owner.push_raw("{}", payload, timeout=0.01)
    except ValueError as exc:
        assert "data region" in str(exc)
    else:
        raise AssertionError("push_raw accepted an impossible slot")

    owner.close()
    cons.close()


def _test_single_batch_interop_and_wraparound():
    shm = f"batch_interop_{os.getpid()}"
    prod = SharedRingBufferProducer(
        shm, total_data_bytes=4096, max_slot_size=2048,
        create_if_not_exists=True,
    )
    cons = SharedRingBufferConsumer(shm, auto_unlink=True)

    for idx in range(200):
        item = _payload(99, idx)
        if idx % 2 == 0:
            assert prod.push_raw(*item, timeout=1.0)
            objs = cons.pop_batch(10, max_bytes=1)
            assert len(objs) == 1
            _check_obj(objs[0])
        else:
            assert prod.push_raw_batch([item], timeout=1.0) == 1
            obj = cons.pop(copy=True)
            _check_obj(obj)

    prod.close()
    assert cons.pop_batch(1) == []
    cons.close()


def _test_partial_batch_write():
    shm = f"batch_partial_{os.getpid()}"
    prod = SharedRingBufferProducer(
        shm, total_data_bytes=4096, blocking=False, max_slot_size=2048,
        create_if_not_exists=True,
    )
    cons = SharedRingBufferConsumer(shm, auto_unlink=True)
    items = [_payload(7, idx) for idx in range(30)]

    pushed = prod.push_raw_batch(items, timeout=0.01)
    assert 0 < pushed < len(items), pushed
    objs = cons.pop_batch(len(items))
    assert len(objs) == pushed
    for obj in objs:
        _check_obj(obj)

    prod.close()
    assert cons.pop_batch(1) == []
    cons.close()


def producer_proc(shm, barrier):
    prod = SharedRingBufferProducer(
        shm, total_data_bytes=TOTAL_BYTES, max_slot_size=MAX_SLOT,
        create_if_not_exists=False,
    )
    pid = mp.current_process()._identity[0]
    barrier.wait()
    items = []
    for idx in range(ITEMS_PER_PRODUCER):
        items.append(_payload(pid, idx))
        if len(items) >= PUSH_BATCH:
            n = 0
            while n < len(items):
                n += prod.push_raw_batch(items[n:], timeout=30.0)
            items = []
    if items:
        n = 0
        while n < len(items):
            n += prod.push_raw_batch(items[n:], timeout=30.0)
    prod.close()


def consumer_proc(shm, barrier, result_q):
    cons = SharedRingBufferConsumer(shm, auto_unlink=False)
    barrier.wait()
    got = 0
    bad = 0
    while True:
        objs = cons.pop_batch(POP_BATCH)
        if not objs:
            break
        for obj in objs:
            try:
                _check_obj(obj)
            except AssertionError:
                bad += 1
            got += 1
    cons.close()
    result_q.put((got, bad))


def main():
    _test_empty_timeout_and_oversize()
    _test_impossible_slot_rejected()
    _test_single_batch_interop_and_wraparound()
    _test_partial_batch_write()

    shm = f"batch_smoke_{os.getpid()}"
    # Create the buffer up front (producer with create) and keep it alive.
    owner = SharedRingBufferProducer(
        shm, total_data_bytes=TOTAL_BYTES, max_slot_size=MAX_SLOT,
        create_if_not_exists=True,
    )

    barrier = mp.Barrier(N_PRODUCERS + N_CONSUMERS)
    result_q = mp.Queue()

    consumers = [mp.Process(target=consumer_proc, args=(shm, barrier, result_q))
                 for _ in range(N_CONSUMERS)]
    producers = [mp.Process(target=producer_proc, args=(shm, barrier))
                 for _ in range(N_PRODUCERS)]

    for p in consumers + producers:
        p.start()

    t0 = time.time()
    for p in producers:
        p.join()
    # Release the creator's producer slot so consumers observe EOS once the
    # real producers are done.
    owner.close()
    for p in consumers:
        p.join()
    dt = time.time() - t0

    total = 0
    bad = 0
    for _ in range(N_CONSUMERS):
        g, b = result_q.get()
        total += g
        bad += b

    expected = N_PRODUCERS * ITEMS_PER_PRODUCER
    print(f"expected={expected} received={total} corrupt={bad} "
          f"in {dt:.2f}s ({total/dt:.0f} items/s)")
    assert total == expected, f"item count mismatch: {total} != {expected}"
    assert bad == 0, f"{bad} corrupt payloads"
    print("PASS")


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()
