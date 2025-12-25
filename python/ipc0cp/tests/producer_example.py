"""
Example producer script demonstrating the SharedRingBuffer with various object types.

This script creates a producer that pushes different types of objects into the shared
memory ring buffer: NumPy arrays (images), PIL Images, text strings, JSON objects,
and raw bytes.
"""

import argparse
import sys
import time
import numpy as np
from PIL import Image
from ipc0cp import SharedRingBufferProducer

def main():
    parser = argparse.ArgumentParser(description='Producer for SharedRingBuffer IPC')
    parser.add_argument('-s', '--shm', default='example_buffer',
                        help='Shared memory name (default: example_buffer)')
    parser.add_argument('-n', '--num-objects', type=int, default=100,
                        help='Number of objects to send (default: 100)')
    parser.add_argument('--buffer-size', type=int, default=100,
                        help='Buffer size in MB (default: 100)')
    args = parser.parse_args()
    
    # Create shared ring buffer
    shm_name = args.shm
    buffer = SharedRingBufferProducer(
        shm_name=shm_name,
        total_data_bytes=args.buffer_size * 1024 * 1024,
        blocking=True,
    )
    
    print(f"Producer started with shared memory: {shm_name}")
    
    try:
        for i in range(args.num_objects):
            # Vary the type of object we send
            obj_type = i % 5
            
            if obj_type == 0:
                # Send NumPy array (image)
                image = np.random.randint(0, 256, (480, 640, 3), dtype=np.uint8)
                buffer.push(image)
                print(f"[{i}] Pushed NumPy array: shape={image.shape}, dtype={image.dtype}")
            
            elif obj_type == 1:
                # Send PIL Image
                pil_img = Image.new('RGB', (320, 240), 
                                   color=(i % 256, (i * 2) % 256, (i * 3) % 256))
                buffer.push(pil_img)
                print(f"[{i}] Pushed PIL Image: size={pil_img.size}, mode={pil_img.mode}")
            
            elif obj_type == 2:
                # Send text string
                text = f"Message number {i}: Hello from producer!"
                buffer.push(text)
                print(f"[{i}] Pushed text: {text[:50]}...")
            
            elif obj_type == 3:
                # Send JSON object
                json_obj = {
                    "frame_id": i,
                    "timestamp": time.time(),
                    "data": [1, 2, 3, 4, 5],
                    "metadata": {"source": "camera_1", "fps": 30}
                }
                buffer.push(json_obj)
                print(f"[{i}] Pushed JSON object: frame_id={json_obj['frame_id']}")
            
            else:
                # Send raw bytes
                data = bytes([i % 256] * 1000)
                buffer.push(data)
                print(f"[{i}] Pushed bytes: size={len(data)} bytes")
            
            # Show buffer stats periodically
            if i % 10 == 0:
                stats = buffer.get_stats()
                used_mb = stats['used_bytes'] / (1024 * 1024)
                total_mb = stats['total_data_bytes'] / (1024 * 1024)
                print(f"  Buffer: {used_mb:.2f} / {total_mb:.2f} MB used")
            
            time.sleep(0.05)  # 50ms delay between sends
        
        print("\nProducer finished. Waiting for consumer to drain buffer...")
        time.sleep(2)
        
    finally:
        buffer.close()
        print("Producer closed.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
