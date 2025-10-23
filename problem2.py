#!/usr/bin/env python3
"""
Problem 1: Log Level Distribution - CLUSTER

Analyze the distribution of log levels (INFO, WARN, ERROR, DEBUG) across all log files. This problem requires basic PySpark operations and simple aggregations.
"""

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import  regexp_extract, col, count, rand
import time

# Setup 
INPUT_DIR = 's3a://gjl53-assignment-spark-cluster-logs'

# Configure logging with basicConfig
logging.basicConfig(
    level=logging.INFO,  # Set the log level to INFO
    # Define log message format
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)

logger = logging.getLogger(__name__)


def create_spark_session(master_url):
    """Create a Spark session optimized for cluster execution."""
    spark = (
        SparkSession.builder
        .appName("Problem1_Cluster")

        # Cluster Configuration
        .master(master_url)  # Connect to Spark cluster

        # Memory Configuration
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")

        # Executor Configuration
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "6")  # Use all available cores across cluster

        # S3 Configuration - Use S3A for AWS S3 access
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")

        # Performance settings for cluster execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        # Serialization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # Arrow optimization for Pandas conversion
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")

        .getOrCreate()
    )

    logger.info("Spark session created successfully for cluster execution")
    return spark


def solve_problem(spark):
    """
    Solve Problem 1: Log Level Distribution 

    Requirements:
    1. Count occurances of each log level (INFO< WARN, ERROR, DEBUG)
    2. Create a random sample of 10 log entries
    3. Generate a summary report with total lines, valid log entries, unique levels, and level distribution
    """
    logger.info("Starting Problem 1: Log Level Distribution ")

    start_time = time.time()

    # Read all text files from the input directory (recursively) into a Spark DataFrame
    logger.info(f"Reading log files into single DataFrame")
    logs_df = spark.read.option("recursiveFileLookup", "true").text(INPUT_DIR)

    # Define a regex pattern to capture log levels (INFO, WARN, ERROR, DEBUG)
    pattern = r"(INFO|WARN|ERROR|DEBUG)"

    # Extract the log level from each line and store it in a new column
    logger.info("Extracting log levels from log entries")
    logs_parsed = logs_df.withColumn(
        'log_level', regexp_extract('value', pattern, 1)
        )

    # Keep only rows that contain a valid log level
    valid_df = logs_parsed.filter(col("log_level") != "")

    # Count how many times each log level appears
    logger.info("Step 1. - Counting occurrences of each log level")
    counts_df = valid_df.groupBy("log_level").agg(count("*").alias("count"))

    
    # Randomly select 10 log entries to create a sample of the data
    logger.info("Step 2. - Randomly selecting 10 log entries for sample")
    sample_df = valid_df.orderBy(rand()).limit(10)
    # Rename the text column to 'log_entry' for clarity in the sample file
    sample_df = sample_df.withColumnRenamed('value', 'log_entry')

    # Calculate totals
    logger.info("Step 3. - Generating summary report")
    total_lines = logs_df.count()
    total_valid = valid_df.count()
    unique_levels = counts_df.count()

    # Precompute log level distribution lines
    distribution_lines = []
    for _, row in counts_df.toPandas().iterrows():
        level = row["log_level"]
        cnt = int(row["count"])
        pct = (cnt / total_valid) * 100 if total_valid > 0 else 0.0
        distribution_lines.append(f"  {level:<5}: {cnt:10,} ({pct:6.2f}%)")

    # Combine summary and distribution
    summary_text = (
        f"Total log lines processed: {total_lines}\n"
        f"Total lines with log levels: {total_valid}\n"
        f"Unique log levels found: {unique_levels}\n\n"
        "Log level distribution:\n" +
        "\n".join(distribution_lines)
    )

    # Save results to outfiles
    logger.info("Saving output files to disk")
    counts_df.toPandas().to_csv(f'problem1_counts.csv', index=False)
    sample_df.toPandas().to_csv(f'problem1_sample.csv', index=False)
    with open(f'problem1_summary.txt', "w") as f:
        f.write(summary_text)

    # Time Summary
    execution_time = time.time() - start_time
    logger.info(f"Problem execution completed in {execution_time:.2f} seconds")
    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETED - Summary Statistics")
    print("=" * 70)
    print(f"Execution time: {execution_time:.2f} seconds")
    print("=" * 70)

    return logs_df


def main():
    """Main function for Problem 1 - Cluster Version."""

    logger.info("Starting Problem 1: Log Level Distribution")
    # Get master URL from command line or environment variable
    if len(sys.argv) > 1:
        master_url = sys.argv[1]
    else:
        # Try to get from environment variable
        master_private_ip = os.getenv("MASTER_PRIVATE_IP")
        if master_private_ip:
            master_url = f"spark://{master_private_ip}:7077"
        else:
            print("Error: Master URL not provided")
            return 1

    logger.info(f"Using Spark master URL: {master_url}")

    overall_start = time.time()

    # Create Spark session
    logger.info("Initializing Spark session for cluster execution")
    spark = create_spark_session(master_url)

    # Solve Problem
    try:
        logger.info("Starting Problem analysis with data files")
        result_df = solve_problem(spark)
        success = True
        logger.info("Problem analysis completed successfully!")
    except Exception as e:
        logger.exception(f"Error occurred while solving Problem: {str(e)}")
        print(f"Error solving Problem: {str(e)}")
        success = False

    # Clean up
    logger.info("Stopping Spark session")
    spark.stop()

    # Overall timing
    overall_end = time.time()
    total_time = overall_end - overall_start
    logger.info(f"Total execution time: {total_time:.2f} seconds")

    print("\n" + "=" * 70)
    if success:
        print("✅ Problem COMPLETED SUCCESSFULLY!")
        print(f"\nTotal execution time: {total_time:.2f} seconds")
        print("\nFiles created:")
        print("  - problem1_counts.csv (Log level counts)")
        print("  - problem1_sample.csv (10 random sample log entries with their levels)")
        print("  - problem1_summary.txt (Summary statistics)")
    else:
        print("Problem failed - check error messages above")
    print("=" * 70)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())