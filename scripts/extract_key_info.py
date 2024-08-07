import sys
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

# Set py4j and pyspark log levels to WARN to reduce noise
logging.getLogger('py4j').setLevel(logging.WARN)
logging.getLogger('pyspark').setLevel(logging.WARN)

def main(input_path, output_path, max_records):
    spark = SparkSession.builder \
        .appName("Healthcare Data Processing") \
        .getOrCreate()

    # Read the JSON file
    df = spark.read.json(input_path)

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

    # Cache exploded DataFrame
    df_exploded.cache()

    # Collect unique plan names and descriptions
    unique_plan_names = df_exploded.select("reporting_plans.plan_name").distinct().limit(5)
    unique_descriptions = df_exploded.select("in_network_files.description").distinct().limit(5)

    # Log unique plan names and descriptions
    logger.debug("DEBUG_UNIQUE_PLAN_NAMES: Unique plan names")
    unique_plan_names.show(truncate=False)
    logger.debug("DEBUG_UNIQUE_DESCRIPTIONS: Unique in-network file descriptions")
    unique_descriptions.show(truncate=False)

    # Filter out rows where in_network_files and reporting_plans are not null
    logger.debug("Total records before filtering: {}".format(df_exploded.count()))
    df_filtered = df_exploded.filter(col("in_network_files").isNotNull() & col("reporting_plans").isNotNull())
    logger.debug("Total records after filtering: {}".format(df_filtered.count()))

    # Log filtered data
    logger.debug("DEBUG_FILTERED_DATA: Sample of filtered data:")
    df_filtered.show(5, False)

    # Select columns and limit the number of records
    df_selected = df_filtered.select(
        col("in_network_files.description").alias("description"),
        col("in_network_files.location").alias("location"),
        col("reporting_plans.plan_name").alias("plan_name")
    ).limit(max_records)

    # Log selected columns
    logger.debug("DEBUG_SELECTED_COLUMNS: Selected columns sample:")
    df_selected.show(5, False)

    # Collect URLs and write to output file
    urls = df_selected.select("location").rdd.map(lambda row: row[0]).collect()
    logger.debug("DEBUG_COLLECTED_URLS: {}".format(urls))
    if not urls:
        logger.debug("DEBUG_NO_URLS_FOUND: No URLs were found in the data.")
    else:
        try:
            with open(output_path, 'w') as f:
                for url in urls:
                    f.write("%s\n" % url)
            logger.debug("DEBUG_URLS_WRITTEN: URLs written to output_urls.txt")
        except Exception as e:
            logger.error("DEBUG_FILE_WRITE_ERROR: An error occurred while writing URLs to file: %s", str(e))

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: spark-submit script.py <input_path> <output_path> <max_records>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    max_records = int(sys.argv[3])

    try:
        main(input_path, output_path, max_records)
    except Exception as e:
        logger.error("DEBUG_ERROR: An error occurred: %s", str(e))
        sys.exit(1)
