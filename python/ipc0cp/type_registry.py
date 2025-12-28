"""Type registry for custom serializable types.

Provides a singleton registry for registering and retrieving custom object types.
Enables extensibility beyond built-in types (bytes, text, json, ndarray, image).

Usage:
    # Register a custom type
    def my_deserializer(metadata, payload):
        return MyCustomType(metadata, payload)
    
    from ipc0cp.type_registry import register_type
    register_type("MyType", my_deserializer, version="1.0")
    
    # Get deserializer for a type
    from ipc0cp.type_registry import get_deserializer
    deser = get_deserializer("MyType", version="1.0")
    if deser:
        obj = deser(metadata, payload)
"""

import logging
from typing import Optional, Callable, Dict, List, TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .serialize import SerializableObject
else:
    SerializableObject = Any

logger = logging.getLogger(__name__)


class TypeRegistry:
    """Singleton registry for custom serializable types."""
    
    _instance: Optional['TypeRegistry'] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._registry: Dict[str, Callable[[Dict[str, str], bytes], SerializableObject]] = {}
        
        self._initialized = True
    
    def register_type(
        self,
        type_name: str,
        deserializer: Callable[[Dict[str, str], bytes], SerializableObject],
        version: Optional[str] = None
    ) -> None:
        """
        Register a custom type.
        
        If type+version already exists, logs warning and overwrites.
        
        Args:
            type_name: Name of the type (e.g., "Point2D", "BoundingBox")
            deserializer: Function(metadata_dict, payload_bytes) -> SerializableObject
            version: Optional version string (e.g., "1.0", "2.1")
        
        Raises:
            ValueError: If type_name is empty or invalid
        """
        if not type_name or type_name.startswith("/"):
            raise ValueError("Type name cannot be empty or start with '/'")
        
        if not callable(deserializer):
            raise ValueError("Deserializer must be callable")
        
        key = self._make_key(type_name, version)
        
        # Warn if overwriting existing custom type
        if key in self._registry:
            logger.warning(f"Type '{key}' already registered - overwriting")
        
        self._registry[key] = deserializer
    
    def get_deserializer(
        self,
        type_name: str,
        version: Optional[str] = None
    ) -> Optional[Callable[[Dict[str, str], bytes], SerializableObject]]:
        """
        Get deserializer for a type.
        
        Args:
            type_name: Type name
            version: Optional version
        
        Returns:
            Deserializer function or None if not found
        """
        key = self._make_key(type_name, version)
        return self._registry.get(key)
    
    def get_registered_types(self) -> List[str]:
        """Get all registered type names."""
        return sorted(list(self._registry.keys()))
    
    @staticmethod
    def _make_key(type_name: str, version: Optional[str]) -> str:
        """Create registry key from type name and optional version."""
        if not version:
            return type_name
        return f"{type_name}@{version}"


# Singleton instance
_registry = TypeRegistry()


def register_type(
    type_name: str,
    deserializer: Callable[[Dict[str, str], bytes], SerializableObject],
    version: Optional[str] = None
) -> None:
    """
    Register a custom serializable type.
    
    Example:
        def my_deserializer(metadata, payload):
            # metadata is dict with type, version, and custom fields
            # payload is bytes
            return MyCustomType(metadata, payload)
        
        register_type("Point2D", my_deserializer, version="1.0")
    
    Args:
        type_name: Type name (will be stored in metadata["type"])
        deserializer: Function to deserialize from (metadata_dict, payload_bytes)
        version: Optional version string for schema versioning
    
    Raises:
        ValueError: If type_name or deserializer invalid
    """
    _registry.register_type(type_name, deserializer, version)


def get_deserializer(
    type_name: str,
    version: Optional[str] = None
) -> Optional[Callable]:
    """
    Get deserializer for a type.
    
    Args:
        type_name: Type name
        version: Optional version
    
    Returns:
        Deserializer function or None
    """
    return _registry.get_deserializer(type_name, version)


def get_registered_types() -> List[str]:
    """Get list of all registered type names."""
    return _registry.get_registered_types()
