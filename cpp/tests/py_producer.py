#!/usr/bin/env python3
"""
Test Python producer -> C++ consumer IPC.

This script creates a SharedRingBufferProducer and pushes various object types
that will be consumed by the C++ consumer test program.
"""

import sys
import time
import argparse
from pathlib import Path


# <root>/cpp/tests/<me>.py
root = Path(__file__).resolve().parent.parent.parent / 'python'

# Add parent directory to path
sys.path.insert(0, str(root))

from ipc0cp import SharedRingBufferProducer


def main():
    parser = argparse.ArgumentParser(description='Python producer for cross-language IPC test')
    parser.add_argument('-s', '--shm', default='test_py_cpp_ipc', help='Shared memory name')
    parser.add_argument('-n', '--num-objects', type=int, default=20, help='Number of objects to send')
    parser.add_argument('--buffer-size', type=int, default=50, help='Buffer size in MB')
    
    args = parser.parse_args()
    
    buffer_size_bytes = args.buffer_size * 1024 * 1024
    
    print(f"Python Producer starting...")
    print(f"Shared memory name: {args.shm}")
    print(f"Buffer size: {args.buffer_size} MB")
    print(f"Number of objects: {args.num_objects}")
    
    # Create producer
    producer = SharedRingBufferProducer(
        shm_name=args.shm,
        total_data_bytes=buffer_size_bytes,
        blocking=True
    )
    
    print(f"Created shared memory")
    print(f"Pushing {args.num_objects} objects...\n")
    
    try:
        start_time = time.time()
        
        for i in range(args.num_objects):
            # Keep this script dependency-free so it runs in minimal CI images.
            # Only use object types that do not require optional packages.
            obj_type = i % 3

            if obj_type == 0:
                obj = f"Hello from Python! Message #{i}"
                obj_desc = f"Text: {obj}"
            elif obj_type == 1:
                obj = {
                    "message": f"Object #{i}",
                    "timestamp": time.time(),
                    "data": [1, 2, 3, 4, 5],
                }
                obj_desc = "JSON object"
            else:
                obj = bytes([i % 256] * 1024)
                obj_desc = f"Bytes: {len(obj)} bytes"
            
            success = producer.push(obj)
            if success:
                print(f"[{i+1}/{args.num_objects}] Pushed: {obj_desc}")
            else:
                print(f"[{i+1}/{args.num_objects}] Failed to push: {obj_desc}")
            
            # Small delay to allow consumer to process
            time.sleep(0.005)
        
        elapsed = time.time() - start_time
        print(f"\nPushed {args.num_objects} objects in {elapsed:.2f} seconds")
        if args.num_objects > 0:
            print(f"Average: {elapsed/args.num_objects*1000:.2f} ms/object")
        
        # Get final stats
        stats = producer.get_stats()
        print(f"\nFinal buffer stats:")
        print(f"  Write pos: {stats['write_pos']}")
        print(f"  Read pos: {stats['read_pos']}")
        print(f"  Available: {stats['available_bytes']} bytes")
        print(f"  Used: {stats['used_bytes']} bytes")
        print(f"  Empty: {stats['is_empty']}")
        
    finally:
        # Keep buffer open for a bit to allow consumer to finish
        print("\nWaiting 2 seconds for consumer to finish...")
        time.sleep(2)
        
        producer.close()
        # Producers do not own shared-memory cleanup; the last consumer unlinks.
        print("Producer closed (consumer will clean up shared memory)")


if __name__ == "__main__":
    main()
