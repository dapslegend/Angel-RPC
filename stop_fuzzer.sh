#!/bin/bash

PID_FILE="fuzzer.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "No fuzzer PID file found. Process might not be running."
    exit 0
fi

PID=$(cat "$PID_FILE")

# Check if the process exists
if ! ps -p $PID > /dev/null; then
    echo "Process $PID not found. Cleaning up PID file."
    rm "$PID_FILE"
    exit 0
fi

# Try graceful shutdown first
echo "Attempting graceful shutdown of fuzzer (PID: $PID)..."
kill $PID

# Wait up to 30 seconds for the process to stop
for i in {1..30}; do
    if ! ps -p $PID > /dev/null; then
        echo "Fuzzer stopped successfully"
        rm "$PID_FILE"
        exit 0
    fi
    sleep 1
done

# If process still running, force kill
if ps -p $PID > /dev/null; then
    echo "Force stopping fuzzer..."
    kill -9 $PID
    rm "$PID_FILE"
fi

echo "Fuzzer stopped"
