"""
Example consumer script demonstrating the SharedRingBuffer with various object types.

This script creates a consumer that receives different types of objects from the shared
memory ring buffer and processes them accordingly.
"""

import argparse
import sys
import time
import numpy as np
from PIL import Image
from ipc0cp import SharedRingBufferConsumer

def main():
    parser = argparse.ArgumentParser(description='Consumer for SharedRingBuffer IPC')
    parser.add_argument('-s', '--shm', default='example_buffer',
                        help='Shared memory name (default: example_buffer)')
    parser.add_argument('--buffer-size', type=int, default=100,
                        help='Buffer size in MB (default: 100)')
    parser.add_argument('--wait-time', type=float, default=0.5,
                        help='Time to wait for producer to start (default: 0.5)')
    args = parser.parse_args()
    
    # Attach to existing shared ring buffer
    shm_name = args.shm
    
    # Wait a bit for producer to create the buffer
    time.sleep(args.wait_time)
    
        buffer = SharedRingBufferConsumer(
            shm_name=shm_name,
            blocking=True,
        )
    
    print(f"Consumer attached to shared memory: {shm_name}")
    
    try:
        count = 0
        while True:
            # Pop object with timeout
            obj = buffer.pop(timeout=10.0)
            
            if obj is None:
                print("No more data, exiting...")
                break
            
            count += 1
            
            # Handle different object types
            if isinstance(obj, np.ndarray):
                print(f"[{count}] Received NumPy array: shape={obj.shape}, "
                      f"dtype={obj.dtype}, mean={obj.mean():.2f}")
            
            elif isinstance(obj, Image.Image):
                print(f"[{count}] Received PIL Image: size={obj.size}, mode={obj.mode}")
                # Could save to file: obj.save(f"frame_{count}.png")
            
            elif isinstance(obj, str):
                print(f"[{count}] Received text: {obj[:50]}...")
            
            elif isinstance(obj, dict):
                print(f"[{count}] Received JSON object: keys={list(obj.keys())}")
                if 'frame_id' in obj:
                    print(f"  Frame ID: {obj['frame_id']}, "
                          f"Timestamp: {obj.get('timestamp', 'N/A')}")
            
            elif isinstance(obj, bytes):
                print(f"[{count}] Received bytes: size={len(obj)} bytes, "
                      f"first byte={obj[0] if obj else 'empty'}")
            
            elif isinstance(obj, list):
                print(f"[{count}] Received list: length={len(obj)}")
            
            else:
                print(f"[{count}] Received unknown type: {type(obj)}")
            
            # Show buffer stats periodically
            if count % 10 == 0:
                stats = buffer.get_stats()
                used_mb = stats['used_bytes'] / (1024 * 1024)
                total_mb = stats['total_data_bytes'] / (1024 * 1024)
                print(f"  Buffer: {used_mb:.2f} / {total_mb:.2f} MB used")
        
        print(f"\nConsumer finished. Processed {count} objects.")
        
    finally:
        buffer.close()
        # Unlink to clean up shared memory
        buffer.unlink()
        print("Consumer closed and shared memory cleaned up.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
