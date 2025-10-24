#!/usr/bin/env python3
"""
Problem 2: Cluster Usage Analysis - LOCAL

Analyze cluster usage patterns to understand which clusters are most heavily used over time. Extract cluster IDs, application IDs, and application start/end times to create a time-series dataset suitable for visualization with Seaborn.
"""

import os
import sys
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    min as spark_min, max as spark_max, 
    regexp_extract, col, input_file_name, 
    to_timestamp, countDistinct
)

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
        .appName("Problem2_Cluster")

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

def generate_visualizations(timeline_df, cluster_summary_df):
    """Create bar chart and density plot based on precomputed DataFrames"""
    import matplotlib.pyplot as plt
    import seaborn as sns
    from pyspark.sql.functions import col

    # 4. Create bar chart showing number of applications per cluster
    logger.info("Step 4. - Creating bar chart of applications per cluster")
    # Convert to pandas for plotting and sort for cleaner display
    cluster_pd = cluster_summary_df.toPandas().sort_values('num_applications', ascending=False)

    # create bar chart
    plt.figure(figsize=(10,6))
    bars = plt.bar(
        cluster_pd['cluster_id'].astype(str),
        cluster_pd['num_applications'],
        color=plt.cm.tab10(range(len(cluster_pd)))
    )
    # Add value labels on top of each bar (ChatGPT used to help generate this code)
    for bar in bars:
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            height,
            f"{int(height)}",
            ha='center',
            va='bottom',
            fontsize=10
        )
    # Add title and labels
    plt.title("Number of Applications per Cluster", fontsize=14)
    plt.xlabel("Cluster ID", fontsize=12)
    plt.ylabel("Number of Applications", fontsize=12)
    plt.xticks(rotation=45, ha='right')
    # Save image
    plt.tight_layout()
    plt.savefig("problem2_bar_chart.png")
    plt.close()


    # 5. # Create density plot showing job duration distribution for the largest cluster
    logger.info("Step 5. - Creating density plot of job durations for largest cluster")
    # Find largest cluster
    largest_id = (
        cluster_summary_df
        .orderBy(col('num_applications').desc())
        .first()['cluster_id']
    )
    largest_df = timeline_df.filter(col('cluster_id') == largest_id)

    # Create duration column
    duration_df = largest_df.withColumn(
        'duration_sec',
        (col('end_time').cast('long') - col('start_time').cast('long'))
    )

    # Convert to pandas for plotting
    duration_pd = duration_df.select("duration_sec").toPandas()
    durations = duration_pd["duration_sec"].dropna()

    # create historgram and density plot
    plt.figure(figsize=(10, 6))
    sns.histplot(durations, kde=True, bins=30, color="skyblue")
    # log scale x to handle skewed duration
    plt.xscale('log')
    # add title and labels
    plt.title(f'Job Duration Distribution for Cluster {largest_id} (n={len(durations)})', fontsize=14)
    plt.xlabel('Job Duration (seconds, log scale)')
    plt.ylabel('Density')
    # save image
    plt.tight_layout()
    plt.savefig("problem2_density_plot.png")
    plt.close()

def solve_problem(spark):
    """
    Solve Problem 2: Cluster Usage Analysis

    Requirements:
    1. Create time series data for each application
    2. Create aggregated cluster usage summary
    3. Generate a summary report with total clusters, apps, average apps per cluster and most used clusters
    4. Create a bar chart of apps per cluster
    5. Create density plot showing job duration distribution for largest cluster
    """
    logger.info("Starting Problem 2: Cluster Usage Analysis")

    start_time = time.time()

    # Read all text files from the input directory (recursively) into a Spark DataFrame
    logger.info(f"Reading log files into single DataFrame")
    logs_df = spark.read.option("recursiveFileLookup", "true").text(INPUT_DIR)

    # Extract timestamp, log level, component, and raw message from each log line
    logger.info("Extracting information from log entries")
    parsed_df = logs_df.select(
        regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
        regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)', 1).alias('log_level'),
        regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)\s+([^:]+):', 2).alias('component'),
        col('value').alias('message')
    )

    # Filter out rows with empty timestamps to avoid parsing errors
    valid_df = parsed_df.filter(col("timestamp") != "")

    # Extract application and cluster identifiers from file paths
    logger.info("Extracting information from file paths")
    full_df = (
        valid_df
        .withColumn('file_path', input_file_name())
        .withColumn('application_id', regexp_extract('file_path', r'(application_\d+_\d+)', 1))
        .withColumn('cluster_id', regexp_extract('application_id', r'application_(\d+)_\d+', 1))
        .withColumn('app_number', regexp_extract('application_id', r'application_\d+_(\d+)', 1))
        .withColumn('timestamp', to_timestamp('timestamp', 'yy/MM/dd HH:mm:ss'))
    )

    # 1. Create time series for each application
    logger.info("Step 1. - Creating time series data for each application")
    timeline_df = (
        full_df
        .groupBy('cluster_id', 'application_id', 'app_number')
        # Get start and end times for each application
        .agg(
            spark_min('timestamp').alias('start_time'),
            spark_max('timestamp').alias('end_time')
        )
        .orderBy('cluster_id', 'application_id')
    )

    # 2. Create Aggregated cluster statistics
    logger.info("Step 2. - Creating aggregated cluster usage summary")
    cluster_summary_df = (
        timeline_df
        .groupBy('cluster_id')
        # Count unique apps and get first/last times
        .agg(
            countDistinct('application_id').alias('num_applications'),
            spark_min('start_time').alias('cluster_first_app'),
            spark_max('end_time').alias('cluster_last_app')
            )
        .orderBy('cluster_id')
    )

    # 3. Create overall summary statistics
    logger.info("Step 3. - Generating overall summary report")
    total_clusters = cluster_summary_df.count()
    total_apps = timeline_df.count()
    avg_apps_per_cluster = total_apps / total_clusters

    # Identify top clusters by number of applications
    top_clusters = (
        cluster_summary_df.orderBy(col('num_applications').desc())
        .select('cluster_id', 'num_applications')
        .limit(5)
        .collect()
    )

    # Prepare summary text
    cluster_lines = [f"   Cluster {row['cluster_id']}: {row['num_applications']} applications" for row in top_clusters]
    summary_text = (
            f"Total unique clusters: {total_clusters}\n"
            f"Total applications: {total_apps}\n"
            f"Average applications per cluster: {avg_apps_per_cluster}\n\n"
            "Most heavily used clusters:\n" +
            "\n".join(cluster_lines)
        )
    
    # VISUALIZATIONS
    generate_visualizations(timeline_df, cluster_summary_df)

    # Save results to outfiles
    logger.info("Saving output files to disk")
    timeline_df.toPandas().to_csv('problem2_timeline.csv', index=False)
    cluster_summary_df.toPandas().to_csv('problem2_cluster_summary.csv', index=False)
    with open('problem2_stats.txt', "w") as f:
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
    """Main function for Problem 2 - Cluster Version."""

    logger.info("Starting Problem 2: Cluster Usage Analysis")
        
    # check if we want to skip spark
    skip_spark = '--skip-spark' in sys.argv
    if skip_spark:
        logger.info('Skipping Spark processing. Using existing CSVs.')
        # import packages and create simple spark session
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("Problem2_VizOnly").getOrCreate()
        # read in existing CSVs
        try:
            timeline_df = spark.read.csv('problem2_timeline.csv', header=True, inferSchema=True)
            cluster_summary_df = spark.read.csv('problem2_cluster_summary.csv', header=True, inferSchema=True)
        except Exception as e:
            logger.exception(f'Error reading CSV files: {str(e)}')
            print(f"Error solving Problem: {str(e)}")
            return 1
        # generate vizualizations
        generate_visualizations(timeline_df, cluster_summary_df)
        logger.info("Visualizations created from CSV files successfully.")
        return 0

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
        print("  - problem2_timeline.csv (Time series data for each application)")
        print("  - problem2_summary.csv (Aggregated cluster statistics)")
        print("  - problem2_stats.txt (Overall summary statistics)")
        print("  - problem2_bar_chart.png (Bar chart visualization)")
        print("  - problem2_density_plot.png (Faceted Density plot visualization)")
    else:
        print("Problem failed - check error messages above")
    print("=" * 70)

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())