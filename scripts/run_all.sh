#!/bin/bash

# Create logs directory if it does not exist
mkdir -p logs

SKIP_DOWNLOAD=false
SKIP_DECOMPRESSION=false

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --skip-download) SKIP_DOWNLOAD=true ;;
        --skip-decompression) SKIP_DECOMPRESSION=true ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

# Run the Go script to download the file
if [ "$SKIP_DOWNLOAD" = false ]; then
    echo "Running extract.go..."
    go run scripts/extract.go
    if [ $? -ne 0 ]; then
        echo "Failed to run extract.go"
        exit 1
    fi
else
    echo "Skipping download step."
fi

# Decompress the downloaded file
if [ "$SKIP_DECOMPRESSION" = false ]; then
    echo "Running decompress_gzip.py..."
    python3 scripts/decompress_gzip.py data/index_file.gz data/decompressed.json
    if [ $? -ne 0 ]; then
        echo "Failed to run decompress_gzip.py"
        exit 1
    fi
else
    echo "Skipping decompression step."
fi

# Extract key information from the decompressed file
echo "Running extract_key_info.py..."
spark-submit --conf spark.executor.logs.rolling.maxRetainedFiles=5 \
             --conf spark.executor.logs.rolling.strategy=time \
             --conf spark.executor.logs.rolling.time.interval=hourly \
             --conf spark.executor.logs.rolling.maxSize=128m \
             --conf spark.executor.memory=4g \
             --conf spark.driver.memory=4g \
             --conf spark.driver.extraJavaOptions=-Xmx4g \
             --conf spark.executor.extraJavaOptions=-Xmx4g \
             scripts/extract_key_info.py data/decompressed.json data/output_urls.txt 5 2>logs/spark_logs.txt >logs/script_logs.txt
if [ $? -ne 0 ]; then
    echo "Failed to run extract_key_info.py"
    exit 1
fi

echo "All steps completed successfully."
