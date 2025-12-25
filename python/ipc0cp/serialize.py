"""
Serialization classes for SharedRingBuffer.

Provides serializers for different object types:
- NumPy arrays
- PIL Images
- Text strings
- JSON objects
- Raw bytes
"""

import json
from io import BytesIO
from typing import Any, Dict, Tuple

import numpy as np
from PIL import Image


# Constants
MAX_METADATA_SIZE = 1024  # Maximum JSON metadata size in bytes


class ObjectSerializer:
    """Base class for object serializers."""
    
    @staticmethod
    def serialize(obj: Any) -> Tuple[Dict[str, Any], bytes]:
        """
        Serialize an object to metadata and payload bytes.
        
        Args:
            obj: Object to serialize
            
        Returns:
            Tuple of (metadata_dict, payload_bytes)
        """
        raise NotImplementedError
    
    @staticmethod
    def deserialize(metadata: Dict[str, Any], payload: bytes) -> Any:
        """
        Deserialize an object from metadata and payload bytes.
        
        Args:
            metadata: Metadata dictionary
            payload: Payload bytes
            
        Returns:
            Deserialized object
        """
        raise NotImplementedError


class NumpySerializer(ObjectSerializer):
    """Serializer for NumPy arrays."""
    
    @staticmethod
    def serialize(obj: np.ndarray) -> Tuple[Dict[str, Any], bytes]:
        """Serialize NumPy array."""
        if not isinstance(obj, np.ndarray):
            raise ValueError("Object must be a NumPy array")
        
        # Ensure contiguous array
        if not obj.flags['C_CONTIGUOUS']:
            obj = np.ascontiguousarray(obj)
        
        metadata = {
            'type': 'ndarray',
            'shape': list(obj.shape),
            'dtype': obj.dtype.str,
        }
        
        payload = obj.tobytes()
        
        return metadata, payload
    
    @staticmethod
    def deserialize(metadata: Dict[str, Any], payload: bytes) -> np.ndarray:
        """Deserialize NumPy array."""
        shape = tuple(metadata['shape'])
        dtype = np.dtype(metadata['dtype'])
        
        array = np.frombuffer(payload, dtype=dtype).reshape(shape)
        # Make a copy since the buffer will be overwritten
        return array.copy()


class ImageSerializer(ObjectSerializer):
    """Serializer for PIL Images."""
    
    @staticmethod
    def serialize(obj: Image.Image) -> Tuple[Dict[str, Any], bytes]:
        """Serialize PIL Image."""
        if not isinstance(obj, Image.Image):
            raise ValueError("Object must be a PIL Image")
        
        # Convert to bytes using PNG format (lossless)
        buffer = BytesIO()
        obj.save(buffer, format='PNG')
        payload = buffer.getvalue()
        
        metadata = {
            'type': 'image',
            'mode': obj.mode,
            'size': obj.size,  # (width, height)
        }
        
        return metadata, payload
    
    @staticmethod
    def deserialize(metadata: Dict[str, Any], payload: bytes) -> Image.Image:
        """Deserialize PIL Image."""
        buffer = BytesIO(payload)
        return Image.open(buffer)


class TextSerializer(ObjectSerializer):
    """Serializer for text strings."""
    
    @staticmethod
    def serialize(obj: str) -> Tuple[Dict[str, Any], bytes]:
        """Serialize text string."""
        if not isinstance(obj, str):
            raise ValueError("Object must be a string")
        
        payload = obj.encode('utf-8')
        
        metadata = {
            'type': 'text',
            'encoding': 'utf-8',
            'length': len(obj),
        }
        
        return metadata, payload
    
    @staticmethod
    def deserialize(metadata: Dict[str, Any], payload: bytes) -> str:
        """Deserialize text string."""
        encoding = metadata.get('encoding', 'utf-8')
        return payload.decode(encoding)


class JsonSerializer(ObjectSerializer):
    """Serializer for JSON objects."""
    
    @staticmethod
    def serialize(obj: Any) -> Tuple[Dict[str, Any], bytes]:
        """Serialize JSON-serializable object."""
        try:
            json_str = json.dumps(obj)
            payload = json_str.encode('utf-8')
        except (TypeError, ValueError) as e:
            raise ValueError(f"Object is not JSON-serializable: {e}")
        
        metadata = {
            'type': 'json',
            'encoding': 'utf-8',
        }
        
        return metadata, payload
    
    @staticmethod
    def deserialize(metadata: Dict[str, Any], payload: bytes) -> Any:
        """Deserialize JSON object."""
        encoding = metadata.get('encoding', 'utf-8')
        json_str = payload.decode(encoding)
        return json.loads(json_str)


class BytesSerializer(ObjectSerializer):
    """Serializer for raw bytes."""
    
    @staticmethod
    def serialize(obj: bytes) -> Tuple[Dict[str, Any], bytes]:
        """Serialize raw bytes."""
        if not isinstance(obj, bytes):
            raise ValueError("Object must be bytes")
        
        metadata = {
            'type': 'bytes',
            'size': len(obj),
        }
        
        return metadata, obj
    
    @staticmethod
    def deserialize(metadata: Dict[str, Any], payload: bytes) -> bytes:
        """Deserialize raw bytes."""
        return payload


# Registry of serializers
SERIALIZERS = {
    'ndarray': NumpySerializer,
    'image': ImageSerializer,
    'text': TextSerializer,
    'json': JsonSerializer,
    'bytes': BytesSerializer,
}


def get_serializer(obj: Any) -> ObjectSerializer:
    """
    Get appropriate serializer for an object.
    
    Args:
        obj: Object to serialize
        
    Returns:
        Serializer class
        
    Raises:
        ValueError: If no suitable serializer found
    """
    if isinstance(obj, np.ndarray):
        return NumpySerializer
    elif isinstance(obj, Image.Image):
        return ImageSerializer
    elif isinstance(obj, str):
        return TextSerializer
    elif isinstance(obj, bytes):
        return BytesSerializer
    elif isinstance(obj, (dict, list, int, float, bool, type(None))):
        return JsonSerializer
    else:
        raise ValueError(f"No serializer found for type: {type(obj)}")
