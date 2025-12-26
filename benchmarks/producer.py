#!/usr/bin/env python3
"""
Producer for benchmarking: sends random bytes via STDIO or shared memory.
"""

import argparse
import os
import random
import struct
import sys
import time
from typing import Optional

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

from ipc0cp.ring_buffer import SharedRingBufferProducer


def produce_stdio(min_size: int, max_size: int, duration: float) -> dict:
    """
    Produce random bytes via STDOUT (binary mode).
    
    Protocol: [length:8 bytes][payload:length bytes]
    
    Args:
        min_size: Minimum payload size in bytes
        max_size: Maximum payload size in bytes
        duration: How long to run in seconds
        
    Returns:
        Statistics dict with bytes_sent, messages_sent, elapsed_time
    """
    # Use binary stdout
    stdout = sys.stdout.buffer
    
    bytes_sent = 0
    messages_sent = 0
    start_time = time.time()
    
    while True:
        elapsed = time.time() - start_time
        if elapsed >= duration:
            break
        
        # Generate random payload
        size = random.randint(min_size, max_size)
        payload = os.urandom(size)
        
        # Send length prefix (8 bytes, little-endian uint64)
        length_bytes = struct.pack('<Q', size)
        stdout.write(length_bytes)
        
        # Send payload
        stdout.write(payload)
        stdout.flush()
        
        bytes_sent += size
        messages_sent += 1
    
    # Send end-of-stream marker (length=0)
    end_marker = struct.pack('<Q', 0)
    stdout.write(end_marker)
    stdout.flush()
    
    elapsed_time = time.time() - start_time
    
    return {
        'bytes_sent': bytes_sent,
        'messages_sent': messages_sent,
        'elapsed_time': elapsed_time,
        'throughput_mbps': (bytes_sent / elapsed_time) / (1024 * 1024),
    }


def produce_shm(shm_name: str, min_size: int, max_size: int, duration: float) -> dict:
    """
    Produce random bytes via shared memory ring buffer.
    
    Args:
        shm_name: Shared memory segment name
        min_size: Minimum payload size in bytes
        max_size: Maximum payload size in bytes
        duration: How long to run in seconds
        
    Returns:
        Statistics dict with bytes_sent, messages_sent, elapsed_time
    """
    # Create producer with 2GB buffer
    producer = SharedRingBufferProducer(
        shm_name=shm_name,
        total_data_bytes=2 * 1024 * 1024 * 1024,  # 2 GB
        blocking=True,
        max_slot_size=10 * 1024 * 1024,  # 10 MB
    )
    
    bytes_sent = 0
    messages_sent = 0
    start_time = time.time()
    
    try:
        while True:
            elapsed = time.time() - start_time
            if elapsed >= duration:
                break
            
            # Generate random payload
            size = random.randint(min_size, max_size)
            payload = os.urandom(size)
            
            # Push to ring buffer
            success = producer.push(payload, timeout=5.0)
            
            if success:
                bytes_sent += size
                messages_sent += 1
            else:
                # Timeout - consumer might be slow
                pass
        
        elapsed_time = time.time() - start_time
        
        return {
            'bytes_sent': bytes_sent,
            'messages_sent': messages_sent,
            'elapsed_time': elapsed_time,
            'throughput_mbps': (bytes_sent / elapsed_time) / (1024 * 1024),
        }
    finally:
        # Close sends EOS marker and cleans up
        producer.close()
        producer.unlink()


def main():
    parser = argparse.ArgumentParser(description='Benchmark producer')
    
    # Transport mode
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--stdio', action='store_true', help='Use STDIN/STDOUT')
    group.add_argument('--shm', type=str, help='Shared memory segment name')
    
    # Benchmark parameters
    parser.add_argument('--min-size', type=int, default=512 * 1024, 
                        help='Minimum payload size in bytes (default: 512KB)')
    parser.add_argument('--max-size', type=int, default=5 * 1024 * 1024,
                        help='Maximum payload size in bytes (default: 5MB)')
    parser.add_argument('--duration', type=float, default=60.0,
                        help='Duration to run in seconds (default: 60)')
    parser.add_argument('--quiet', action='store_true',
                        help='Suppress progress output')
    
    args = parser.parse_args()
    
    if not args.quiet:
        print(f"Producer starting: min_size={args.min_size}, max_size={args.max_size}, duration={args.duration}",
              file=sys.stderr)
    
    if args.stdio:
        stats = produce_stdio(args.min_size, args.max_size, args.duration)
    else:
        stats = produce_shm(args.shm, args.min_size, args.max_size, args.duration)
    
    # Print stats to stderr so it doesn't interfere with STDIO mode
    print(f"\nProducer Stats:", file=sys.stderr)
    print(f"  Bytes sent: {stats['bytes_sent']:,} ({stats['bytes_sent'] / (1024**3):.2f} GB)", file=sys.stderr)
    print(f"  Messages sent: {stats['messages_sent']:,}", file=sys.stderr)
    print(f"  Elapsed time: {stats['elapsed_time']:.2f} seconds", file=sys.stderr)
    print(f"  Throughput: {stats['throughput_mbps']:.2f} MB/s", file=sys.stderr)


if __name__ == '__main__':
    main()
