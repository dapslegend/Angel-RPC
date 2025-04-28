#!/bin/bash

# Directory containing logs
LOG_DIR="./logs"

# Keep logs for last 7 days
DAYS_TO_KEEP=7

# Size threshold for rotation (5MB in bytes)
SIZE_THRESHOLD=$((5 * 1024 * 1024))

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Remove logs older than DAYS_TO_KEEP days
find "$LOG_DIR" -name "fuzzer_*.log*" -type f -mtime +$DAYS_TO_KEEP -delete

# Rotate the main log if it exists and is larger than 5MB
MAIN_LOG="$LOG_DIR/fuzzer.log"
if [ -f "$MAIN_LOG" ]; then
    # Use ls -l instead of stat for better compatibility
    FILE_SIZE=$(ls -l "$MAIN_LOG" | awk '{print $5}')
    if [ $FILE_SIZE -gt $SIZE_THRESHOLD ]; then
        # Create timestamp with date and time
        TIMESTAMP=$(date "+%Y%m%d_%H%M%S")
        
        # Create a temporary copy of the log
        cp "$MAIN_LOG" "$MAIN_LOG.tmp"
        
        # Compress the temporary copy
        gzip -c "$MAIN_LOG.tmp" > "$LOG_DIR/fuzzer_${TIMESTAMP}.log.gz"
        
        # Remove temporary file
        rm "$MAIN_LOG.tmp"
        
        # Force clear the original log file
        cat /dev/null > "$MAIN_LOG"
        
        # Add rotation message to new log
        echo "Log rotated at $(date)" >> "$MAIN_LOG"
        
        # Double check file was cleared
        if [ -s "$MAIN_LOG" ]; then
            # If file still has content (besides our rotation message), try alternative method
            rm "$MAIN_LOG"
            touch "$MAIN_LOG"
            echo "Log rotated at $(date)" > "$MAIN_LOG"
        fi
    fi
fi

# Clean up old project results
find "./projects" -type f -mtime +$DAYS_TO_KEEP -delete 2>/dev/null

# Remove empty directories
find "./projects" -type d -empty -delete 2>/dev/null