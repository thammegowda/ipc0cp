"""Compatibility re-exports for POSIX shared memory helpers.

The POSIX shared-memory implementation was consolidated into `ipc0cp.posix_sync`.
This module remains to avoid breaking existing imports.
"""

from .posix_sync import PosixSharedMemory, normalize_ipc_base_name, posix_name

__all__ = ["PosixSharedMemory", "normalize_ipc_base_name", "posix_name"]
