#!/bin/bash

LOG_FILE="./logs/fuzzer.log"
CHECK_INTERVAL=30  # Check every 30 seconds
TIMEOUT=300       # 5 minutes in seconds
RESTART_SCRIPT="./restart_fuzzer.sh"

# Function to get file size in bytes
get_file_size() {
    stat -f %z "$1" 2>/dev/null || echo 0
}

# Function to log messages with timestamp
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "./logs/monitor.log"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Check if log file exists
if [ ! -f "$LOG_FILE" ]; then
    log_message "Error: Log file $LOG_FILE does not exist"
    exit 1
fi

# Check if restart script exists and is executable
if [ ! -x "$RESTART_SCRIPT" ]; then
    log_message "Error: Restart script $RESTART_SCRIPT does not exist or is not executable"
    exit 1
fi

log_message "Starting fuzzer log monitor..."
last_size=$(get_file_size "$LOG_FILE")
last_change_time=$(date +%s)

while true; do
    # Get current file size
    current_size=$(get_file_size "$LOG_FILE")
    current_time=$(date +%s)
    
    # If size has changed, update last change time
    if [ "$current_size" -ne "$last_size" ]; then
        last_change_time=$current_time
        last_size=$current_size
        log_message "Log file size changed: $current_size bytes"
    else
        # Calculate time since last change
        time_diff=$((current_time - last_change_time))
        
        # If no change for 5 minutes, restart fuzzer
        if [ $time_diff -ge $TIMEOUT ]; then
            log_message "WARNING: No log file size change for $time_diff seconds"
            log_message "Executing restart script..."
            
            # Execute restart script
            $RESTART_SCRIPT
            if [ $? -eq 0 ]; then
                log_message "Fuzzer restart successful"
            else
                log_message "Error: Fuzzer restart failed"
            fi
            
            # Reset timer
            last_change_time=$current_time
            sleep 60  # Wait a minute before resuming monitoring
        fi
    fi
    
    sleep $CHECK_INTERVAL
done
