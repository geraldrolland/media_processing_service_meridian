#!/bin/bash
echo "Starting media processing service..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8001
