#!/bin/bash

# Directory for logs
LOG_DIR="./logs"
mkdir -p "$LOG_DIR"

# Kill any existing fuzzer process
if [ -f fuzzer.pid ]; then
    OLD_PID=$(cat fuzzer.pid)
    if ps -p $OLD_PID > /dev/null; then
        echo "Stopping existing fuzzer process ($OLD_PID)"
        kill $OLD_PID
    fi
    rm fuzzer.pid
fi

# Start the fuzzer
echo "Starting fuzzer..."
nohup cargo run > "$LOG_DIR/fuzzer.log" 2>&1 &
echo $! > fuzzer.pid

echo "Fuzzer started with PID $(cat fuzzer.pid)"
echo "Logs available at $LOG_DIR/fuzzer.log"
