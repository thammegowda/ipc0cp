#!/usr/bin/env python3
"""
Consumer for benchmarking: receives random bytes via STDIO or shared memory.
"""

import argparse
import os
import struct
import sys
import time
from typing import Optional

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

from ipc0cp.ring_buffer import SharedRingBufferConsumer
from ipc0cp import IPCException


def consume_stdio() -> dict:
    """
    Consume random bytes via STDIN (binary mode) until EOF.
    
    Protocol: [length:8 bytes][payload:length bytes]
        
    Returns:
        Statistics dict with bytes_received, messages_received, elapsed_time
    """
    # Use binary stdin
    stdin = sys.stdin.buffer
    
    bytes_received = 0
    messages_received = 0
    start_time = time.time()
    
    while True:
        # Read length prefix (8 bytes)
        length_bytes = stdin.read(8)
        if len(length_bytes) < 8:
            # EOF or incomplete read
            break
        
        size = struct.unpack('<Q', length_bytes)[0]
        
        # Check for end-of-stream marker
        if size == 0:
            # Producer signaled end of stream
            break
        
        # Read payload
        payload = stdin.read(size)
        if len(payload) < size:
            # Incomplete read
            break
        
        bytes_received += size
        messages_received += 1
    
    elapsed_time = time.time() - start_time
    
    return {
        'bytes_received': bytes_received,
        'messages_received': messages_received,
        'elapsed_time': elapsed_time,
        'throughput_mbps': (bytes_received / elapsed_time) / (1024 * 1024),
    }


def consume_shm(shm_name: str) -> dict:
    """
    Consume random bytes via shared memory ring buffer until empty.
    
    Args:
        shm_name: Shared memory segment name
        
    Returns:
        Statistics dict with bytes_received, messages_received, elapsed_time
    """
    # Wait a bit for producer to create shared memory
    time.sleep(0.5)
    
    # Create consumer with 2GB buffer
    consumer = SharedRingBufferConsumer(
        shm_name=shm_name,
        total_data_bytes=2 * 1024 * 1024 * 1024,  # 2 GB
        blocking=True,
        auto_attach=True,
    )
    
    bytes_received = 0
    messages_received = 0
    start_time = time.time()
    
    try:
        while True:
            # Pop from ring buffer (blocking with no timeout)
            # Returns None only for end-of-stream, throws exception for errors
            try:
                payload = consumer.pop(timeout=None)
            except IPCException as e:
                # Handle exceptions - these are real errors, not EOS
                print(f"Error during pop: {e} (error_type={e.error_type})", file=sys.stderr)
                raise
            
            if payload is None:
                # End-of-stream received - clean exit
                break
            
            # Normal data
            size = len(payload)
            bytes_received += size
            messages_received += 1
        
        elapsed_time = time.time() - start_time
        
        return {
            'bytes_received': bytes_received,
            'messages_received': messages_received,
            'elapsed_time': elapsed_time,
            'throughput_mbps': (bytes_received / elapsed_time) / (1024 * 1024),
        }
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser(description='Benchmark consumer')
    
    # Transport mode
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--stdio', action='store_true', help='Use STDIN/STDOUT')
    group.add_argument('--shm', type=str, help='Shared memory segment name')
    
    # Benchmark parameters
    parser.add_argument('--quiet', action='store_true',
                        help='Suppress progress output')
    
    args = parser.parse_args()
    
    if not args.quiet:
        print(f"Consumer starting...", file=sys.stderr)
    
    if args.stdio:
        stats = consume_stdio()
    else:
        stats = consume_shm(args.shm)
    
    # Print stats to stderr
    print(f"\nConsumer Stats:", file=sys.stderr)
    print(f"  Bytes received: {stats['bytes_received']:,} ({stats['bytes_received'] / (1024**3):.2f} GB)", file=sys.stderr)
    print(f"  Messages received: {stats['messages_received']:,}", file=sys.stderr)
    print(f"  Elapsed time: {stats['elapsed_time']:.2f} seconds", file=sys.stderr)
    print(f"  Throughput: {stats['throughput_mbps']:.2f} MB/s", file=sys.stderr)


if __name__ == '__main__':
    main()
