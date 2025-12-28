"""
IPC0CP - Zero-copy Inter-Process Communication between Python and C++

This library provides functionality for exchanging items between Python and C++
using shared memory IPC (Inter-Process Communication).
"""

__version__ = "0.1.0"

from .ipc import (
    IPCError,
    IPCException,
)
from .ring_buffer import (
    SharedRingBufferProducer,
    SharedRingBufferConsumer,
    SharedRingBufferBase,
)
from .stdio import (
    StdioProducer,
    StdioConsumer,
)
from .serialize import (
    SerializableObject,
    BytesData,
    TextData,
    JsonData,
    ImageData,
    NumpyArray,
    ObjectType,
    MAX_METADATA_SIZE,
    serialize_object,
    deserialize_object,
    deserialize,
)
from .logger import (
    set_log_level,
    enable_logging,
    disable_logging,
    logger,
)

__all__ = [
    "IPCError",
    "IPCException",
    "SharedRingBufferProducer",
    "SharedRingBufferConsumer",
    "SharedRingBufferBase",
    "StdioProducer",
    "StdioConsumer",
    "SerializableObject",
    "BytesData",
    "TextData",
    "JsonData",
    "ImageData",
    "NumpyArray",
    "ObjectType",
    "MAX_METADATA_SIZE",
    "serialize_object",
    "deserialize_object",
    "deserialize",
    "set_log_level",
    "enable_logging",
    "disable_logging",
    "logger",
]
