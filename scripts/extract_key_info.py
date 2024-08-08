import sys
import logging
import os  # Import os module
import shutil  # Import shutil module
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, udf
from pyspark.sql.types import StringType
from urllib.parse import urlparse, urlunparse

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

# Set py4j and pyspark log levels to WARN to reduce noise
logging.getLogger('py4j').setLevel(logging.WARN)
logging.getLogger('pyspark').setLevel(logging.WARN)

def strip_query_params(url):
    parsed_url = urlparse(url)
    stripped_url = parsed_url._replace(query='')
    return urlunparse(stripped_url)

strip_query_params_udf = udf(strip_query_params, StringType())

def write_urls(df, output_path):
    # Clean up the output directory if it already exists
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    
    # Apply UDF to strip query parameters and deduplicate URLs
    df_dedup = df.withColumn("location", strip_query_params_udf(col("location"))).distinct()
    
    urls = df_dedup.select("location").rdd.map(lambda row: row[0])
    urls.coalesce(1).saveAsTextFile(output_path)
    logger.debug("DEBUG_URLS_WRITTEN: All URLs written to output file")

def main(input_path, output_path):
    try:
        spark = SparkSession.builder \
            .appName("Healthcare Data Processing") \
            .config("spark.executor.memory", "8g") \
            .config("spark.driver.memory", "8g") \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.memory.storageFraction", "0.3") \
            .config("spark.sql.shuffle.partitions", "400") \
            .getOrCreate()

        # Read the compressed JSON file directly
        df = spark.read.option("compression", "gzip").json(input_path)

        # Log the schema
        logger.debug("DEBUG_SCHEMA: Schema of the JSON file:")
        df.printSchema()

        # Explode arrays to get a flat structure
        df_exploded = df \
            .withColumn("in_network_files", explode(col("in_network_files"))) \
            .withColumn("reporting_plans", explode(col("reporting_plans")))

        # Log some exploded data
        logger.debug("DEBUG_EXPLODED_DATA: Sample of exploded data:")
        df_exploded.show(5, False)

        # Filter out rows where in_network_files and reporting_plans are not null
        df_filtered = df_exploded.filter(col("in_network_files").isNotNull() & col("reporting_plans").isNotNull())

        # Select columns
        df_selected = df_filtered.select(
            col("in_network_files.description").alias("description"),
            col("in_network_files.location").alias("location"),
            col("reporting_plans.plan_name").alias("plan_name")
        )

        # Log selected columns
        logger.debug("DEBUG_SELECTED_COLUMNS: Selected columns sample:")
        df_selected.show(5, False)

        # Write URLs to output file incrementally
        write_urls(df_selected, output_path)

    except Exception as e:
        logger.error("DEBUG_ERROR: An error occurred: %s", str(e))
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit script.py <input_path> <output_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    try:
        main(input_path, output_path)
    except Exception as e:
        logger.error("DEBUG_ERROR: An error occurred: %s", str(e))
        sys.exit(1)
