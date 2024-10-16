#!/bin/bash

echo "Start Luigi ETL Pipeline Process"

# Virtual environment path
VENV_PATH="/Users/user/data-eng/venv/bin/activate"

# Activate virtual environment
source "$VENV_PATH"

# Set PYTHONPATH to include the directory of the Python script
export PYTHONPATH="/Users/user/data-eng"

# Run the ETL pipeline by executing the Python script
echo "Running ETL Pipeline"
python /Users/user/data-eng/etl_pipeline.py >> /Users/user/data-eng/log/load_data.log 2>&1

# Simple logging
dt=$(date '+%d/%m/%Y %H:%M:%S')
echo "Luigi Completed at ${dt}" >> /Users/user/data-eng/log/luigi-info.log

echo "End Luigi ETL Pipeline Process"
