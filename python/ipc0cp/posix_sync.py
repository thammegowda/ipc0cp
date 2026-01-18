"""POSIX IPC helpers.

This module centralizes:
- POSIX shared memory (mmap-backed wrapper)
- POSIX named semaphore + condition emulation

It exists primarily to keep `ring_buffer.py` focused on buffer logic.
"""

from __future__ import annotations

import mmap
from typing import Optional

import posix_ipc

from .ipc import IPCError, IPCException


def normalize_ipc_base_name(name: str) -> str:
    """Normalize an IPC object base name.

    Public API uses names like "my_buffer". POSIX IPC names are typically
    "/my_buffer".

    We store the *base* name without a leading '/', and enforce it contains
    no additional '/'.
    """

    if not isinstance(name, str):
        raise TypeError("shm_name must be a string")
    base = name.strip()
    if base.startswith("/"):
        base = base.lstrip("/")
    if not base:
        raise ValueError("shm_name must be non-empty")
    if "/" in base:
        raise ValueError("shm_name must not contain '/' characters")
    return base


def posix_name(base_name: str) -> str:
    """Convert a normalized base name to a POSIX IPC name (leading '/')."""

    base = normalize_ipc_base_name(base_name)
    return f"/{base}"


class PosixSharedMemory:
    """POSIX shared memory segment backed by posix_ipc + mmap.

    This avoids multiprocessing.shared_memory's resource_tracker behavior.
    Exposes a SharedMemory-like surface: .buf, .size, .close(), .unlink().
    """

    def __init__(
        self,
        name: str,
        *,
        create: bool,
        size: Optional[int] = None,
        mode: int = 0o600,
    ):
        base = normalize_ipc_base_name(name)
        self.name = base
        self._posix_name = posix_name(base)

        flags = posix_ipc.O_CREAT | posix_ipc.O_EXCL if create else 0

        try:
            if create:
                if size is None or size <= 0:
                    raise ValueError("size must be a positive int when create=True")
                self._shm = posix_ipc.SharedMemory(self._posix_name, flags=flags, mode=mode, size=size)
                self.size = int(size)
            else:
                self._shm = posix_ipc.SharedMemory(self._posix_name, flags=flags)
                self.size = int(self._shm.size)
        except posix_ipc.ExistentialError as e:
            # Match multiprocessing.shared_memory semantics for create=True.
            if create:
                raise FileExistsError(str(e))
            raise FileNotFoundError(str(e))

        self._mmap = mmap.mmap(self._shm.fd, self.size)
        # Close the FD; the mmap keeps the mapping alive.
        try:
            self._shm.close_fd()
        except Exception:
            pass
        self.buf = memoryview(self._mmap)

    def close(self) -> None:
        try:
            if hasattr(self, "buf") and self.buf is not None:
                try:
                    self.buf.release()
                except Exception:
                    pass
        finally:
            try:
                if hasattr(self, "_mmap") and self._mmap is not None:
                    self._mmap.close()
            except Exception:
                pass

    def unlink(self) -> None:
        try:
            posix_ipc.unlink_shared_memory(self._posix_name)
        except posix_ipc.ExistentialError:
            # Already unlinked (or never existed); treat as idempotent.
            pass


class PosixCondition:
    """Condition variable emulation using POSIX semaphores.

    Uses two semaphores:
    - mutex: For mutual exclusion (lock/unlock)
    - wait_sem: For wait/notify signaling

    Note: This is a minimal condition-like primitive suitable for this project’s
    synchronization patterns (not a full POSIX condvar implementation).
    """

    def __init__(self, mutex_sem: posix_ipc.Semaphore, wait_sem: posix_ipc.Semaphore):
        self.mutex = mutex_sem
        self.wait_sem = wait_sem
        self._waiters = 0  # Track number of threads waiting (process-local)
        self._closed = False  # Track if semaphores have been closed

    def acquire(self, blocking: bool = True, timeout=None) -> bool:
        """Acquire the mutex lock."""
        if timeout is not None:
            try:
                self.mutex.acquire(timeout)
                return True
            except posix_ipc.BusyError:
                return False
        if blocking:
            self.mutex.acquire()
            return True
        try:
            self.mutex.acquire(0)
            return True
        except posix_ipc.BusyError:
            return False

    def release(self) -> None:
        """Release the mutex lock."""
        self.mutex.release()

    def wait(self, timeout=None) -> bool:
        """Wait for notification. Must be called while holding the lock.

        Returns True if notified, False if timeout occurred.
        """
        # Release mutex while waiting
        self.mutex.release()

        try:
            if timeout is not None:
                try:
                    self.wait_sem.acquire(timeout)
                    return True
                except posix_ipc.BusyError:
                    return False
                except posix_ipc.SignalError:
                    # Interrupted by a signal (e.g., test timeout alarm). Treat like a timeout.
                    return False

            try:
                self.wait_sem.acquire()
                return True
            except posix_ipc.SignalError:
                return False
        finally:
            # Reacquire mutex before returning
            self.mutex.acquire()

    def notify(self, n: int = 1) -> None:
        """Wake up one or more waiting threads."""
        for _ in range(n):
            try:
                self.wait_sem.release()
            except Exception:
                # Semaphore value might be at max, ignore
                pass

    def notify_all(self) -> None:
        """Wake up all waiting threads."""
        # Release multiple times (bounded by reasonable max waiters)
        for _ in range(100):
            try:
                self.wait_sem.release()
            except Exception:
                break

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False


def get_buffer_semaphores(buffer_name: str, create: bool = False):
    """Get or create POSIX semaphores for a ring buffer.

    Args:
        buffer_name: Name of the shared memory buffer (base name, no leading '/')
        create: If True, create new semaphores; if False, attach to existing

    Returns:
        Tuple of (lock, condition)
    """

    base = normalize_ipc_base_name(buffer_name)
    mutex_name = f"/{base}_mutex"
    wait_name = f"/{base}_wait"

    flags = posix_ipc.O_CREAT if create else 0
    mode = 0o600  # User read/write only

    try:
        if create:
            # Clean up any existing semaphores first (from previous crashed runs)
            try:
                posix_ipc.unlink_semaphore(mutex_name)
            except posix_ipc.ExistentialError:
                pass
            try:
                posix_ipc.unlink_semaphore(wait_name)
            except posix_ipc.ExistentialError:
                pass

            # Create new semaphores (mutex starts at 1, wait_sem starts at 0)
            mutex_sem = posix_ipc.Semaphore(mutex_name, flags=flags, mode=mode, initial_value=1)
            wait_sem = posix_ipc.Semaphore(wait_name, flags=flags, mode=mode, initial_value=0)
        else:
            # Try to attach to existing semaphores
            try:
                mutex_sem = posix_ipc.Semaphore(mutex_name)
                wait_sem = posix_ipc.Semaphore(wait_name)
            except posix_ipc.ExistentialError as e:
                raise FileNotFoundError(
                    f"Semaphores not found for buffer '{buffer_name}': {e}"
                )

        condition = PosixCondition(mutex_sem, wait_sem)
        return mutex_sem, condition

    except FileNotFoundError:
        raise
    except posix_ipc.ExistentialError as e:
        raise IPCException(
            IPCError.SYSTEM_ERROR,
            f"Failed to {'create' if create else 'attach to'} POSIX semaphores for buffer '{buffer_name}': {e}",
        )
    except Exception as e:
        raise IPCException(
            IPCError.SYSTEM_ERROR,
            f"Unexpected error managing POSIX semaphores for buffer '{buffer_name}': {e}",
        )


def cleanup_buffer_semaphores(buffer_name: str) -> None:
    """Unlink POSIX semaphores for a buffer.

    Should be called when the last consumer exits.
    """

    base = normalize_ipc_base_name(buffer_name)
    mutex_name = f"/{base}_mutex"
    wait_name = f"/{base}_wait"

    try:
        posix_ipc.unlink_semaphore(mutex_name)
    except posix_ipc.ExistentialError:
        pass

    try:
        posix_ipc.unlink_semaphore(wait_name)
    except posix_ipc.ExistentialError:
        pass
