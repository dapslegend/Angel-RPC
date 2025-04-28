#!/bin/bash

LOG_FILE="./logs/fuzzer.log"
PID_FILE="fuzzer.pid"

if [ -f "$PID_FILE" ]; then
    PID=$(cat $PID_FILE)
    if ps -p $PID > /dev/null; then
        echo "Fuzzer is running (PID: $PID)"
        echo "Last 20 lines of log:"
        tail -n 20 "$LOG_FILE"
    else
        echo "Fuzzer process not found but PID file exists"
        rm "$PID_FILE"
    fi
else
    echo "Fuzzer is not running"
fi

# Show disk usage
echo -e "\nDisk usage:"
du -sh ./projects ./logs
