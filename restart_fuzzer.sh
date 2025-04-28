#!/bin/bash

echo "Stopping fuzzer..."
./stop_fuzzer.sh

# Wait a moment to ensure clean shutdown
sleep 2

echo "Starting fuzzer..."
./start_fuzzer.sh
