"""Pytest configuration for ipc0cp tests."""

import pytest
import logging

# Configure logging for tests
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

@pytest.fixture(scope="session", autouse=True)
def cleanup_shared_memory():
    """Cleanup any leftover shared memory segments after test session."""
    yield
    
    # Cleanup code would go here if needed
    # For now, tests handle their own cleanup
    pass
