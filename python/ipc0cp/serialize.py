"""
Serialization module for inter-process communication.

Provides serializable object classes matching C++ hierarchy:
- SerializableObject (base class)
  - BytesData (raw bytes)
    - TextData (text string)
      - JsonData (JSON object)
    - ImageData (PIL Image)
    - NumpyArray (NumPy array)
"""

import json
import logging
from abc import ABC, abstractmethod
from enum import Enum
from io import BytesIO
from typing import Any, Dict, List, Tuple, Optional

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False
    np = None

try:
    from PIL import Image
    HAS_PIL = True
except ImportError:
    HAS_PIL = False
    Image = None

from .type_registry import get_deserializer


# Constants
MAX_METADATA_SIZE = 1024
logger = logging.getLogger(__name__)


class ObjectType(Enum):
    """Object types supported by serialization"""
    NUMPY_ARRAY = "ndarray"
    IMAGE = "image"
    TEXT = "text"
    JSON = "json"
    BYTES = "bytes"
    LIST = "list"
    UNKNOWN = "unknown"


class SerializableObject(ABC):
    """
    Base class for serializable objects with polymorphic serialization.
    """
    
    @abstractmethod
    def get_type(self) -> ObjectType:
        """Get the object type"""
        pass
    
    @abstractmethod
    def serialize(self) -> Tuple[str, bytes]:
        """
        Serialize this object to metadata JSON and payload.
        
        Returns:
            Tuple of (metadata_json, payload_bytes)
        """
        pass


class BytesData(SerializableObject):
    """
    Raw bytes - base class for all data types.
    All serializable objects are fundamentally bytes with interpretation.
    """
    
    def __init__(self, data: bytes = b''):
        self.bytes = data if isinstance(data, bytes) else bytes(data)
    
    def get_type(self) -> ObjectType:
        return ObjectType.BYTES
    
    def serialize(self) -> Tuple[str, bytes]:
        """Serialize bytes object"""
        metadata = {
            "type": self.get_type().value,
            "size": len(self.bytes)
        }
        return json.dumps(metadata), self.bytes
    
    @staticmethod
    def deserialize(metadata: Dict[str, str], payload: bytes) -> 'BytesData':
        """Deserialize from metadata and payload"""
        return BytesData(payload)


class TextData(BytesData):
    """
    Text string - bytes with text encoding.
    """
    
    def __init__(self, text: str = '', encoding: str = 'utf-8'):
        self.text = text
        self.encoding = encoding
        super().__init__(text.encode(encoding))
    
    def get_type(self) -> ObjectType:
        return ObjectType.TEXT
    
    def serialize(self) -> Tuple[str, bytes]:
        """Serialize text object"""
        metadata = {
            "type": self.get_type().value,
            "encoding": self.encoding,
            "length": len(self.text)
        }
        return json.dumps(metadata), self.bytes
    
    @staticmethod
    def deserialize(metadata: Dict[str, str], payload: bytes) -> 'TextData':
        """Deserialize from metadata and payload"""
        encoding = metadata.get('encoding', 'utf-8')
        text = payload.decode(encoding)
        return TextData(text, encoding)


class JsonData(TextData):
    """
    JSON data - text with JSON structure.
    Extends TextData, provides JSON parsing capability.
    """
    
    def __init__(self, obj: Any):
        """
        Initialize from a Python object (dict, list, etc.)
        
        Args:
            obj: Python object to serialize as JSON
        """
        self.obj = obj
        json_str = json.dumps(obj)
        super().__init__(json_str, encoding='utf-8')
    
    def get_type(self) -> ObjectType:
        return ObjectType.JSON
    
    def serialize(self) -> Tuple[str, bytes]:
        """Serialize JSON object"""
        metadata = {
            "type": self.get_type().value,
            "encoding": self.encoding
        }
        return json.dumps(metadata), self.bytes
    
    def json(self) -> Any:
        """Parse and return JSON object"""
        return json.loads(self.text)
    
    @staticmethod
    def deserialize(metadata: Dict[str, str], payload: bytes) -> 'JsonData':
        """Deserialize from metadata and payload"""
        encoding = metadata.get('encoding', 'utf-8')
        json_str = payload.decode(encoding)
        obj = json.loads(json_str)
        return JsonData(obj)


class ImageData(BytesData):
    """
    Image representation (PNG-encoded).
    bytes contains PNG-encoded image data.
    """
    
    def __init__(self, image: Optional['Image.Image'] = None):
        """
        Initialize from PIL Image.
        
        Args:
            image: PIL Image object
        """
        if not HAS_PIL:
            raise ImportError("PIL is required for ImageData")
        
        self.mode = ''
        self.width = 0
        self.height = 0
        
        if image is not None:
            self.mode = image.mode
            self.width, self.height = image.size
            
            # Encode as PNG
            buffer = BytesIO()
            image.save(buffer, format='PNG')
            super().__init__(buffer.getvalue())
        else:
            super().__init__(b'')
    
    def get_type(self) -> ObjectType:
        return ObjectType.IMAGE
    
    def serialize(self) -> Tuple[str, bytes]:
        """Serialize image object"""
        metadata = {
            "type": self.get_type().value,
            "mode": self.mode,
            "size": json.dumps([self.width, self.height])
        }
        return json.dumps(metadata), self.bytes
    
    def to_image(self) -> 'Image.Image':
        """Convert back to PIL Image"""
        if not HAS_PIL:
            raise ImportError("PIL is required for ImageData")
        return Image.open(BytesIO(self.bytes))
    
    @staticmethod
    def deserialize(metadata: Dict[str, str], payload: bytes) -> 'ImageData':
        """Deserialize from metadata and payload"""
        if not HAS_PIL:
            raise ImportError("PIL is required for ImageData")
        
        img_obj = ImageData()
        img_obj.bytes = payload
        img_obj.mode = metadata.get('mode', 'RGB')
        
        # Parse size
        size_str = metadata.get('size', '[0,0]')
        size_list = json.loads(size_str)
        img_obj.width, img_obj.height = size_list[0], size_list[1]
        
        return img_obj


class NumpyArray(BytesData):
    """
    NumPy array representation.
    bytes contains raw array data.
    """
    
    def __init__(self, array: Optional['np.ndarray'] = None):
        """
        Initialize from NumPy array.
        
        Args:
            array: NumPy array object
        """
        if not HAS_NUMPY:
            raise ImportError("NumPy is required for NumpyArray")
        
        self.shape: List[int] = []
        self.dtype = ''
        self.array = None
        
        if array is not None:
            self.array = array
            self.shape = list(array.shape)
            self.dtype = array.dtype.str
            super().__init__(array.tobytes())
        else:
            super().__init__(b'')
    
    def get_type(self) -> ObjectType:
        return ObjectType.NUMPY_ARRAY
    
    def serialize(self) -> Tuple[str, bytes]:
        """Serialize NumPy array"""
        metadata = {
            "type": self.get_type().value,
            "shape": json.dumps(self.shape),
            "dtype": self.dtype
        }
        return json.dumps(metadata), self.bytes
    
    def to_array(self) -> 'np.ndarray':
        """Convert back to NumPy array"""
        if not HAS_NUMPY:
            raise ImportError("NumPy is required for NumpyArray")
        if self.array is not None:
            return self.array
        return np.frombuffer(self.bytes, dtype=self.dtype).reshape(self.shape)
    
    def element_count(self) -> int:
        """Get total number of elements"""
        count = 1
        for dim in self.shape:
            count *= dim
        return count
    
    def element_size(self) -> int:
        """Get element size in bytes"""
        if self.element_count() == 0:
            return 0
        return len(self.bytes) // self.element_count()
    
    @staticmethod
    def deserialize(metadata: Dict[str, str], payload: bytes) -> 'NumpyArray':
        """Deserialize from metadata and payload"""
        if not HAS_NUMPY:
            raise ImportError("NumPy is required for NumpyArray")
        
        np_obj = NumpyArray()
        np_obj.bytes = payload
        np_obj.dtype = metadata.get('dtype', '<f8')
        
        # Parse shape
        shape_str = metadata.get('shape', '[]')
        np_obj.shape = json.loads(shape_str)
        
        return np_obj


class ListData(SerializableObject):
    """Collection of heterogeneous serializable objects."""

    MAX_ITEMS = 10
    MAX_DEPTH = 10

    def __init__(self, items: List[SerializableObject]):
        if not items:
            raise ValueError("ListData cannot be empty")
        if len(items) > self.MAX_ITEMS:
            raise ValueError("ListData exceeds maximum items")
        self._items = items
        if self._depth() > self.MAX_DEPTH:
            raise ValueError("ListData exceeds maximum depth")

    @property
    def items(self) -> List[SerializableObject]:
        return self._items

    def _depth(self) -> int:
        max_child = 0
        for item in self._items:
            if isinstance(item, ListData):
                max_child = max(max_child, item._depth())
        return 1 + max_child

    def get_type(self) -> ObjectType:
        return ObjectType.LIST

    def serialize(self) -> Tuple[str, bytes]:
        metadata = {
            "type": self.get_type().value,
            "version": "1.0",
            "count": len(self._items)
        }

        item_entries: List[Dict[str, Any]] = []
        payload_bytes = bytearray()

        for item in self._items:
            item_meta_json, item_payload = item.serialize()
            item_entries.append({
                "metadata": json.loads(item_meta_json),
                "payload_size": len(item_payload)
            })
            payload_bytes.extend(item_payload)

        metadata["items"] = item_entries
        return json.dumps(metadata), bytes(payload_bytes)

    @staticmethod
    def deserialize(metadata: Dict[str, Any], payload: bytes) -> 'ListData':
        count = metadata.get("count")
        if not isinstance(count, int) or count <= 0 or count > ListData.MAX_ITEMS:
            raise ValueError("Invalid list count")

        items_meta = metadata.get("items")
        if not isinstance(items_meta, list) or len(items_meta) != count:
            raise ValueError("Invalid list metadata")

        items: List[SerializableObject] = []
        offset = 0

        for idx, entry in enumerate(items_meta):
            if not isinstance(entry, dict):
                raise ValueError(f"ListData entry {idx} must be an object")

            payload_size = entry.get("payload_size")
            if not isinstance(payload_size, int):
                raise ValueError(f"ListData entry {idx} payload_size invalid")

            if offset + payload_size > len(payload):
                raise ValueError(f"ListData entry {idx} payload truncated")

            payload_slice = payload[offset:offset + payload_size]
            offset += payload_size

            metadata_entry = entry.get("metadata")
            if not isinstance(metadata_entry, dict):
                raise ValueError(f"ListData entry {idx} metadata invalid")

            nested_meta_json = json.dumps(metadata_entry)
            item = deserialize(nested_meta_json, payload_slice)
            items.append(item)

        if offset != len(payload):
            raise ValueError("ListData payload length mismatch")

        return ListData(items)


# Global deserializer function
def deserialize(metadata_json: str, payload: bytes) -> SerializableObject:
    """
    Deserialize from metadata JSON and payload.
    
    Args:
        metadata_json: JSON metadata string
        payload: Binary payload
        
    Returns:
        Deserialized SerializableObject
        
    Raises:
        ValueError: If object type is unsupported
    """
    # Parse metadata
    if isinstance(metadata_json, bytes):
        metadata_json = metadata_json.decode('utf-8')
    
    metadata = json.loads(metadata_json)
    obj_type = metadata.get('type', 'unknown')
    version = metadata.get('version', '')

    custom_deserializer = get_deserializer(obj_type, version)
    if custom_deserializer:
        return custom_deserializer(metadata, payload)

    if obj_type == ObjectType.LIST.value:
        return ListData.deserialize(metadata, payload)
    if obj_type in {ObjectType.NUMPY_ARRAY.value, 'ndarray'}:
        return NumpyArray.deserialize(metadata, payload)
    if obj_type in {ObjectType.IMAGE.value, 'image'}:
        return ImageData.deserialize(metadata, payload)
    if obj_type in {ObjectType.TEXT.value, 'text'}:
        return TextData.deserialize(metadata, payload)
    if obj_type in {ObjectType.JSON.value, 'json'}:
        return JsonData.deserialize(metadata, payload)
    if obj_type in {ObjectType.BYTES.value, 'bytes'}:
        return BytesData.deserialize(metadata, payload)

    logger.warning("Unknown object type '%s' version '%s'; falling back to BytesData", obj_type, version)
    return BytesData(payload)


# Helper functions for high-level API
def _to_serializable(obj: Any, depth: int = 0) -> SerializableObject:
    if isinstance(obj, SerializableObject):
        return obj
    if HAS_NUMPY and isinstance(obj, np.ndarray):
        return NumpyArray(obj)
    if HAS_PIL and isinstance(obj, Image.Image):
        return ImageData(obj)
    if isinstance(obj, str):
        return TextData(obj)
    if isinstance(obj, bytes):
        return BytesData(obj)
    if isinstance(obj, (dict, int, float, bool, type(None))):
        return JsonData(obj)
    if isinstance(obj, (list, tuple)):
        if depth >= ListData.MAX_DEPTH:
            raise ValueError("ListData exceeds maximum depth")
        return ListData([_to_serializable(item, depth + 1) for item in obj])
    raise ValueError(f"No serializer found for type: {type(obj)}")

def serialize_object(obj: Any) -> Tuple[str, bytes]:
    """
    Serialize a Python object to metadata JSON and payload.
    
    Args:
        obj: Python object (numpy array, PIL Image, str, dict, list, bytes, etc.)
        
    Returns:
        Tuple of (metadata_json, payload_bytes)
    """
    serializable = _to_serializable(obj)
    return serializable.serialize()


def deserialize_object(metadata_json: str, payload: bytes) -> Any:
    """Deserialize from metadata JSON and payload."""
    obj = deserialize(metadata_json, payload)

    return _to_native(obj)


def _to_native(obj: SerializableObject) -> Any:
    """Convert SerializableObject back to a native Python value."""
    if isinstance(obj, ListData):
        return [_to_native(item) for item in obj.items]
    if isinstance(obj, NumpyArray):
        return obj.to_array()
    if isinstance(obj, ImageData):
        return obj.to_image()
    if isinstance(obj, JsonData):
        return obj.json()
    if isinstance(obj, TextData):
        return obj.text
    if isinstance(obj, BytesData):
        return obj.bytes
    return obj


