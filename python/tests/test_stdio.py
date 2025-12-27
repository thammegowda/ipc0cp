#!/usr/bin/env python3
"""
Test script for STDIO IPC - demonstrates the same API as ring buffer
"""

import sys
import subprocess
import json
import numpy as np
from ipc0cp import StdioProducer, StdioConsumer


def producer_process():
    """Producer sends data to stdout (run as subprocess)"""
    producer = StdioProducer()
    
    # Send various types of data
    producer.push({"message": "Hello from Python!"})
    producer.push("This is a text string")
    producer.push(np.array([1, 2, 3, 4, 5]))
    producer.push({"numbers": [10, 20, 30], "flag": True})
    
    # Close sends EOS marker
    producer.close()
    print("Producer sent 4 objects + EOS", file=sys.stderr)


def consumer_process():
    """Consumer reads data from stdin (run as subprocess)"""
    consumer = StdioConsumer()
    
    received = []
    while True:
        obj = consumer.pop()
        if obj is None:  # End-of-stream
            break
        
        # Convert to JSON-serializable format for output verification
        if isinstance(obj, np.ndarray):
            obj_info = {"type": "ndarray", "shape": list(obj.shape), "dtype": str(obj.dtype), "data": obj.tolist()}
        elif isinstance(obj, dict):
            obj_info = {"type": "dict", "value": obj}
        elif isinstance(obj, str):
            obj_info = {"type": "str", "value": obj}
        else:
            obj_info = {"type": type(obj).__name__, "value": str(obj)}
        
        received.append(obj_info)
        print(f"Received object {len(received)}: {type(obj).__name__}", file=sys.stderr)
    
    # Output results as JSON to stdout for verification
    print(json.dumps({"count": len(received), "objects": received}))
    print(f"Consumer received {len(received)} objects", file=sys.stderr)


def test_stdio_pipe():
    """Test STDIO IPC using subprocess with pipe"""
    # Create producer subprocess
    producer_cmd = [sys.executable, __file__, "producer"]
    producer = subprocess.Popen(
        producer_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Create consumer subprocess connected to producer's stdout
    consumer_cmd = [sys.executable, __file__, "consumer"]
    consumer = subprocess.Popen(
        consumer_cmd,
        stdin=producer.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Close producer's stdout in parent to allow EOF propagation
    producer.stdout.close()
    
    # Wait for both processes
    consumer_out, consumer_err = consumer.communicate(timeout=5)
    producer_out, producer_err = producer.communicate(timeout=5)
    
    # Check that both processes exited successfully
    assert producer.returncode == 0, f"Producer failed: {producer_err.decode()}"
    assert consumer.returncode == 0, f"Consumer failed: {consumer_err.decode()}"
    
    # Parse consumer output
    result = json.loads(consumer_out.decode())
    
    # Verify correct number of objects received
    assert result["count"] == 4, f"Expected 4 objects, got {result['count']}"
    
    # Verify object types
    assert result["objects"][0]["type"] == "dict"
    assert result["objects"][1]["type"] == "str"
    assert result["objects"][2]["type"] == "ndarray"
    assert result["objects"][3]["type"] == "dict"
    
    # Verify data content
    assert result["objects"][0]["value"]["message"] == "Hello from Python!"
    assert result["objects"][1]["value"] == "This is a text string"
    assert result["objects"][2]["data"] == [1, 2, 3, 4, 5]
    assert result["objects"][3]["value"]["numbers"] == [10, 20, 30]
    assert result["objects"][3]["value"]["flag"] is True


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: test_stdio.py [producer|consumer]")
        sys.exit(1)
    
    mode = sys.argv[1]
    if mode == "producer":
        producer_process()
    elif mode == "consumer":
        consumer_process()
    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)
