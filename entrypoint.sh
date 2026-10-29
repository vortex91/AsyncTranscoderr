#!/bin/bash

# 1. Start your Rust coordinator in the background
echo "Starting Rust coordinator..."
/app/media_manager all &

# 2. Start your secondary Python sync service in the background
echo "Starting Arr Sync Service..."
python3 arr_sync_service.py &

# 3. Start your primary Python node monitoring service in the foreground
echo "Starting Python node service..."
exec python3 node_sa.py
