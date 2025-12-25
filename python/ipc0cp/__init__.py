"""
IPC0CP - Zero-copy Inter-Process Communication between Python and C++

This library provides functionality for exchanging items between Python and C++
using shared memory IPC (Inter-Process Communication).
"""

__version__ = "0.1.0"

from .ring_buffer import (
    SharedRingBufferProducer,
    SharedRingBufferConsumer,
    SharedRingBufferBase,
)
from .serialize import (
    SERIALIZERS,
    MAX_METADATA_SIZE,
    ObjectSerializer,
    NumpySerializer,
    ImageSerializer,
    TextSerializer,
    JsonSerializer,
    BytesSerializer,
    get_serializer,
)

__all__ = [
    "SharedRingBufferProducer",
    "SharedRingBufferConsumer",
    "SharedRingBufferBase",
    "SERIALIZERS",
    "MAX_METADATA_SIZE",
    "ObjectSerializer",
    "NumpySerializer",
    "ImageSerializer",
    "TextSerializer",
    "JsonSerializer",
    "BytesSerializer",
    "get_serializer",
]
