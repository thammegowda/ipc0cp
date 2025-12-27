"""Simple logger for IPC operations."""
import sys
import logging

# Create logger for ipc0cp
logger = logging.getLogger('ipc0cp')
logger.setLevel(logging.WARNING)  # Only show warnings and errors by default

# Create console handler that writes to stderr
_handler = logging.StreamHandler(sys.stderr)
_handler.setLevel(logging.DEBUG)

# Create formatter
_formatter = logging.Formatter('[%(levelname)s] %(message)s')
_handler.setFormatter(_formatter)

# Add handler to logger
logger.addHandler(_handler)

def set_log_level(level: str):
    """
    Set the logging level for ipc0cp.
    
    Args:
        level: One of 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
    """
    logger.setLevel(getattr(logging, level.upper()))

def enable_logging():
    """Enable INFO level logging."""
    set_log_level('INFO')

def disable_logging():
    """Disable all logging except CRITICAL."""
    set_log_level('CRITICAL')
