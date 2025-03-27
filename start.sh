#!/bin/bash

# Start DataFlowX services and dashboard
set -e

echo "Starting DataFlowX..."

# Start Docker services
echo "Starting Docker services..."
docker-compose up -d

# Wait a bit for services to start
echo "Waiting for services to start..."
sleep 10

# Run the sample pipeline in the background
echo "Starting the sample pipeline..."
python scripts/sample_pipeline.py &
PIPELINE_PID=$!

# Start the dashboard
echo "Starting the dashboard..."
streamlit run dashboard.py

# Clean up pipeline process on exit
kill $PIPELINE_PID