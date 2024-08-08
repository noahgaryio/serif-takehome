#!/bin/bash

# Create logs directory if it does not exist
mkdir -p logs

SKIP_DOWNLOAD=false

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --skip-download) SKIP_DOWNLOAD=true ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

# Function to handle errors
handle_error() {
    echo "An error occurred. Exiting..."
    exit 1
}

# Run the Go script to download the file
if [ "$SKIP_DOWNLOAD" = false ]; then
    echo "Running extract.go..."
    go run scripts/extract.go || handle_error
else
    echo "Skipping download step."
fi

# Extract key information from the compressed file
echo "Running extract_key_info.py..."
spark-submit --conf spark.executor.logs.rolling.maxRetainedFiles=5 \
             --conf spark.executor.logs.rolling.strategy=time \
             --conf spark.executor.logs.rolling.time.interval=hourly \
             --conf spark.executor.logs.rolling.maxSize=128m \
             --driver-memory 8g \
             --executor-memory 8g \
             --num-executors 4 \
             --conf spark.memory.fraction=0.8 \
             --conf spark.memory.storageFraction=0.3 \
             --conf spark.sql.shuffle.partitions=400 \
             scripts/extract_key_info.py data/index_file.gz data/output_urls 2>logs/spark_logs.txt >logs/script_logs.txt || handle_error

echo "All steps completed successfully."
