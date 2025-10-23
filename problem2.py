#!/usr/bin/env python3
"""
Problem 3: Real-World Data Analysis (Reddit Data Processing) - CLUSTER

Apply your Spark cluster skills to analyze real-world social media data.
"""

import os
import sys
import time
import logging
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, from_unixtime
import pandas as pd

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
        .appName("Reddit_Cluster")

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


def get_s3_paths():
    """Generate S3 paths for reddit data (cluster reads directly from S3)."""

    logger.info(f"Preparing S3 paths for Reddit data")
    print(f"\nPreparing to read Reddit data from S3...")
    print("=" * 60)

    bucket_name = 'gjl53-spark-reddit'

    # set up boto3 client to list objects in S3 bucket
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name)

    s3_paths = []
    # iterate through all objects in the bucket and create s3 paths
    for page in pages:
        if "Contents" in page:
            for obj in page['Contents']:
                s3_path = f"s3a://{bucket_name}/{obj['Key']}"
                s3_paths.append(s3_path)        

    logger.info(f"Prepared {len(s3_paths)} S3 paths for direct reading")
    print(f"\n✅ Prepared {len(s3_paths)} S3 paths - will read directly from S3")

    return s3_paths


def solve_problem3(spark, data_files):
    """
    Solve Problem 3: Reddit Data Processing

    Requirements:
    1. Count the total number of unique comments and unique users (authors) in the dataset
    2. Find the top 10 most active subreddits by comment count
    3. Identify peak commenting hours (UTC) across all subreddits
    """

    logger.info("Starting Problem 3: Reddit Data Processing")
    print("\nSolving Problem 3: Reddit Data Processing")
    print("=" * 60)

    start_time = time.time()

    # Read all parquet files into a single DataFrame
    logger.info(f"Reading {len(data_files)} parquet files into single DataFrame")
    print("Reading all data files into a single DataFrame...")
    reddit_df = spark.read.parquet(*data_files)

    total_rows = reddit_df.count()
    logger.info(f"Successfully loaded {total_rows:,} total rows from {len(data_files)} files")
    print(f"✅ Loaded {total_rows:,} total rows from {len(data_files)} files")

    # Step 1: Dataset Statistics
    logger.info("Step 1: Compute Dataset Statistics")
    print("\nStep 1: Compute Dataset Statistics...")
    # Count the total number of unique comments and unique users (authors) in the dataset
    unique_comments = reddit_df.select('body').distinct().count()
    unique_users = reddit_df.select('author').distinct().count()
    # save stats to csv
    stats_df = pd.DataFrame(
        [{'unique_comments': unique_comments,
        'unique_users': unique_users}]
    )
    stats_df.to_csv("dataset_stats_cluster.csv", index=False)

    # Step 2: Most Popular Subreddits
    logger.info("Step 2: Find Most Popular Subreddits")
    print("Step 2: Finding Most Popular Subreddits...")
    # Find the top 10 most active subreddits by comment count
    subreddit_counts_df = (
        reddit_df.groupBy('subreddit')
        .count()
        .orderBy('count', ascending=False)
        .limit(10)
    )
    subreddit_counts_df.toPandas().to_csv("top_subreddits_cluster.csv", index=False)

    # Step 3: Temporal Analysis
    logger.info("Step 3: Calculating Temporal Analysis")
    print("Step 3: Calculating Temporal Analysis...")
    # Identify peak commenting hours (UTC) across all subreddits
    df_hours = (
        reddit_df.withColumn('hour', hour(from_unixtime(col('created_utc'))))
    )
    peak_hours_df = (
        df_hours.groupBy('hour')
        .count()
        .orderBy('count', ascending=False)
    )
    peak_hours_df.toPandas().to_csv('peak_hours_cluster.csv', index=False)

    #  Save to CSV
    print(f"✅ Results saved to csv files")

    # Calculate execution time
    end_time = time.time()
    execution_time = end_time - start_time
    logger.info(f"Problem 3 execution completed in {execution_time:.2f} seconds")

    # Print summary statistics
    print("\n" + "=" * 60)
    print("REDDIT ANALYSIS COMPLETED - Summary Statistics")
    print("=" * 60)
    print(f"Total rows processed: {total_rows:,}")
    print(f"Total unique comments: {unique_comments:,}")
    print(f"Total unique authors: {unique_users:,}")
    print(f"Execution time: {execution_time:.2f} seconds")

    return reddit_df


def main():
    """Main function for Problem 3 - Cluster Version."""

    logger.info("Starting Problem 3: Reddit Data Processing")
    print("=" * 70)
    print("Problem 3: Reddit Data Processing")
    print("Real-World Data Analysis")
    print("=" * 70)

    # Get master URL from command line or environment variable
    if len(sys.argv) > 1:
        master_url = sys.argv[1]
    else:
        # Try to get from environment variable
        master_private_ip = os.getenv("MASTER_PRIVATE_IP")
        if master_private_ip:
            master_url = f"spark://{master_private_ip}:7077"
        else:
            print("❌ Error: Master URL not provided")
            print("Usage: python nyc_tlc_problem1_cluster.py spark://MASTER_IP:7077")
            print("   or: export MASTER_PRIVATE_IP=xxx.xxx.xxx.xxx")
            return 1

    print(f"Connecting to Spark Master at: {master_url}")
    logger.info(f"Using Spark master URL: {master_url}")

    overall_start = time.time()

    # Create Spark session
    logger.info("Initializing Spark session for cluster execution")
    spark = create_spark_session(master_url)

    # Get S3 paths for all reddit parquets
    logger.info(f"Preparing S3 paths for reddit data:")
    data_files = get_s3_paths()

    if len(data_files) == 0:
        logger.error("No S3 paths available. Cannot proceed with analysis")
        print("❌ No S3 paths available. Exiting...")
        spark.stop()
        return 1

    # Solve Problem 3
    try:
        logger.info("Starting Problem 3 analysis with downloaded data files")
        result_df = solve_problem3(spark, data_files)
        success = True
        logger.info("Problem 3 analysis completed successfully")
    except Exception as e:
        logger.exception(f"Error occurred while solving Problem 3: {str(e)}")
        print(f"❌ Error solving Problem 3: {str(e)}")
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
        print("✅ Problem 3 COMPLETED SUCCESSFULLY!")
        print(f"\nTotal execution time: {total_time:.2f} seconds")
        print("\nFiles created:")
        print("  - dataset_stats_cluster.csv (Unique comments and users)")
        print("  - top_subreddits_cluster.csv (Top 10 most popular subreddits)")
        print("  - peak_hours_cluster.csv (Hourly comment distribution)")
        print("\nNext steps:")
        print("  1. Check csv files for the complete results")
        print("  2. Verify the output matches the expected format")
        print("  3. Submit csv files as part of your solution")
    else:
        print("❌ Problem 3 failed - check error messages above")
    print("=" * 70)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())