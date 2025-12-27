#!/bin/bash
set -eu
# Test script for cross-language IPC: Python producer -> C++ consumer

MYDIR="$(cd "$(dirname "$0")" && pwd)"
SHM_NAME="test_py_cpp_ipc"
NUM_OBJECTS=10
BUFFER_SIZE=50

echo "========================================="
echo "Cross-Language IPC Test"
echo "Python Producer -> C++ Consumer"
echo "========================================="
echo ""

# Clean up any existing shared memory
rm -f /dev/shm/${SHM_NAME} 2>/dev/null

# Start Python producer in background (it creates the shared memory)
echo "Starting Python producer in background..."
python3 "${MYDIR}/py_producer.py" \
    --shm ${SHM_NAME} \
    -n ${NUM_OBJECTS} \
    --buffer-size ${BUFFER_SIZE} &
PRODUCER_PID=$!

# Give producer time to create shared memory AND write first object
sleep 1

# Start C++ consumer (it will attach and consume)
echo "Starting C++ consumer..."
"${CMAKE_BINARY_DIR}/test_consumer" --shm ${SHM_NAME} --buffer-size ${BUFFER_SIZE}

# Wait for producer to finish
wait ${PRODUCER_PID}

echo ""
echo "========================================="
echo "Test completed successfully!"
echo "========================================="
