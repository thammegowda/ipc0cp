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
python3 - <<PY
import posix_ipc

name = "/" + "${SHM_NAME}".lstrip("/")
try:
    posix_ipc.unlink_shared_memory(name)
except Exception:
    pass
for sem in (name + "_mutex", name + "_wait"):
    try:
        posix_ipc.unlink_semaphore(sem)
    except Exception:
        pass
PY

PRODUCER_PID=""
PRODUCER_LOG="${MYDIR}/py_producer_${SHM_NAME}.log"

cleanup() {
    if [[ -n "${PRODUCER_PID}" ]]; then
        kill "${PRODUCER_PID}" 2>/dev/null || true
        # Give it a moment to exit
        for _ in $(seq 1 50); do
            kill -0 "${PRODUCER_PID}" 2>/dev/null || break
            sleep 0.1
        done
    fi
}

trap cleanup EXIT

# Start Python producer in background (it creates the shared memory)
echo "Starting Python producer in background..."
PYTHONUNBUFFERED=1 python3 -u "${MYDIR}/py_producer.py" \
    --shm ${SHM_NAME} \
    -n ${NUM_OBJECTS} \
    --buffer-size ${BUFFER_SIZE} >"${PRODUCER_LOG}" 2>&1 &
PRODUCER_PID=$!

# Wait for producer to create shared memory (fail fast with diagnostics)
for _ in $(seq 1 50); do
    if [[ -e "/dev/shm/${SHM_NAME}" ]]; then
        break
    fi
    sleep 0.1
done

if [[ ! -e "/dev/shm/${SHM_NAME}" ]]; then
    echo "Shared memory /dev/shm/${SHM_NAME} was not created in time. Python producer log:" >&2
    tail -n 200 "${PRODUCER_LOG}" >&2 || true
    exit 1
fi

# Give producer a moment to start pushing (it will gate on consumer attach)
sleep 0.2

# Start C++ consumer (it will attach and consume)
echo "Starting C++ consumer..."
set +e
"${CMAKE_BINARY_DIR}/test_consumer" --shm ${SHM_NAME} --buffer-size ${BUFFER_SIZE}
CONSUMER_RC=$?
set -e

if [[ ${CONSUMER_RC} -ne 0 ]]; then
    echo "C++ consumer failed (exit ${CONSUMER_RC}). Python producer log:" >&2
    tail -n 200 "${PRODUCER_LOG}" >&2 || true
    exit ${CONSUMER_RC}
fi

# Wait for producer to finish (but don't hang forever)
for _ in $(seq 1 300); do
    if ! kill -0 "${PRODUCER_PID}" 2>/dev/null; then
        PRODUCER_PID=""
        break
    fi
    sleep 0.1
done

if [[ -n "${PRODUCER_PID}" ]]; then
    echo "Python producer did not exit in time. Log:" >&2
    tail -n 200 "${PRODUCER_LOG}" >&2 || true
    exit 1
fi

echo ""
echo "========================================="
echo "Test completed successfully!"
echo "========================================="
