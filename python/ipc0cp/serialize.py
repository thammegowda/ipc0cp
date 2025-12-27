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
from abc import ABC, abstractmethod
from enum import Enum
from io import BytesIO
from typing import Any, Dict, List, Tuple, Optional, Union

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


# Constants
MAX_METADATA_SIZE = 1024


class ObjectType(Enum):
    """Object types supported by serialization"""
    NUMPY_ARRAY = "ndarray"
    IMAGE = "image"
    TEXT = "text"
    JSON = "json"
    BYTES = "bytes"
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
    
    # Dispatch to appropriate deserializer
    if obj_type == ObjectType.NUMPY_ARRAY.value or obj_type == 'ndarray':
        return NumpyArray.deserialize(metadata, payload)
    elif obj_type == ObjectType.IMAGE.value or obj_type == 'image':
        return ImageData.deserialize(metadata, payload)
    elif obj_type == ObjectType.TEXT.value or obj_type == 'text':
        return TextData.deserialize(metadata, payload)
    elif obj_type == ObjectType.JSON.value or obj_type == 'json':
        return JsonData.deserialize(metadata, payload)
    elif obj_type == ObjectType.BYTES.value or obj_type == 'bytes':
        return BytesData.deserialize(metadata, payload)
    else:
        raise ValueError(f"Unsupported object type: {obj_type}")


# Helper functions for high-level API
def serialize_object(obj: Any) -> Tuple[str, bytes]:
    """
    Serialize a Python object to metadata JSON and payload.
    
    Args:
        obj: Python object (numpy array, PIL Image, str, dict, list, bytes, etc.)
        
    Returns:
        Tuple of (metadata_json, payload_bytes)
    """
    if HAS_NUMPY and isinstance(obj, np.ndarray):
        np_obj = NumpyArray(obj)
        return np_obj.serialize()
    elif HAS_PIL and isinstance(obj, Image.Image):
        img_obj = ImageData(obj)
        return img_obj.serialize()
    elif isinstance(obj, str):
        text_obj = TextData(obj)
        return text_obj.serialize()
    elif isinstance(obj, bytes):
        bytes_obj = BytesData(obj)
        return bytes_obj.serialize()
    elif isinstance(obj, (dict, list, int, float, bool, type(None))):
        json_obj = JsonData(obj)
        return json_obj.serialize()
    else:
        raise ValueError(f"No serializer found for type: {type(obj)}")


def deserialize_object(metadata_json: str, payload: bytes) -> Any:
    """Deserialize from metadata JSON and payload."""
    obj = deserialize(metadata_json, payload)

    # Convert to native Python types
    if isinstance(obj, NumpyArray):
        return obj.to_array()
    elif isinstance(obj, ImageData):
        return obj.to_image()
    elif isinstance(obj, JsonData):
        return obj.json()
    elif isinstance(obj, TextData):
        return obj.text
    elif isinstance(obj, BytesData):
        return obj.bytes
    else:
        return obj


# Backward compatibility - old serializer classes
SERIALIZERS = {
    'ndarray': NumpyArray,
    'image': ImageData,
    'text': TextData,
    'json': JsonData,
    'bytes': BytesData,
}


def get_serializer(obj: Any):
    """Get appropriate serializer class for an object (backward compatibility)"""
    if HAS_NUMPY and isinstance(obj, np.ndarray):
        return NumpyArray
    elif HAS_PIL and isinstance(obj, Image.Image):
        return ImageData
    elif isinstance(obj, str):
        return TextData
    elif isinstance(obj, bytes):
        return BytesData
    elif isinstance(obj, (dict, list, int, float, bool, type(None))):
        return JsonData
    else:
        raise ValueError(f"No serializer found for type: {type(obj)}")
