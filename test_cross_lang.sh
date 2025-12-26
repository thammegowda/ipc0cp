#!/bin/bash
# Test cross-language IPC: Python Producer → C++ Consumer

cd "$(dirname "$0")"

echo "Starting Python producer in background..."
python3 - << 'EOF' &
import time
import numpy as np
from PIL import Image
from ipc0cp import SharedRingBufferProducer

# Create producer
producer = SharedRingBufferProducer("test_py_cpp_ipc", total_data_bytes=50*1024*1024)

# Send 10 objects of different types
print("Python: Sending 10 objects...")

# 1. NumPy array
arr = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.float32)
producer.push(arr)
print("Python: Sent NumPy array")

# 2. Text
producer.push("Hello from Python!")
print("Python: Sent text")

# 3. JSON
producer.push({"message": "JSON data", "count": 42})
print("Python: Sent JSON")

# 4. Bytes
producer.push(b"Raw bytes data")
print("Python: Sent bytes")

# 5. PIL Image
img = Image.new('RGB', (100, 100), color='red')
producer.push(img)
print("Python: Sent PIL image")

# Send 5 more for good measure
for i in range(5):
    producer.push(f"Message {i+6}")
    print(f"Python: Sent message {i+6}")

print("Python: All objects sent. Waiting for consumer...")
time.sleep(5)
print("Python: Cleaning up...")
producer.unlink()
EOF

PYTHON_PID=$!

# Wait a bit for Python to create the shared memory
sleep 1

echo "Starting C++ consumer..."
timeout 10 ./build/test_consumer

# Clean up
kill $PYTHON_PID 2>/dev/null || true
wait $PYTHON_PID 2>/dev/null || true

echo "Cross-language test complete!"
