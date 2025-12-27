"""
Common IPC types and exceptions for inter-process communication.
"""

from typing import Any


class IPCError:
    """Error types for IPC operations"""
    NONE = "None"
    NOT_INITIALIZED = "NotInitialized"
    SHM_NOT_FOUND = "ShmNotFound"
    SIZE_MISMATCH = "SizeMismatch"
    INVALID_METADATA = "InvalidMetadata"
    INVALID_SLOT = "InvalidSlot"
    TIMEOUT = "Timeout"
    BUFFER_EMPTY = "BufferEmpty"
    DESERIALIZATION_FAILED = "DeserializationFailed"
    CORRUPT_PAYLOAD = "CorruptPayload"


class IPCException(Exception):
    """Exception class for IPC errors"""
    
    def __init__(self, error_type: str, message: str = None):
        self.error_type = error_type
        if message is None:
            message = f"IPC error: {error_type}"
        super().__init__(message)
    
    def __str__(self):
        return f"{super().__str__()} (error_type={self.error_type})"

