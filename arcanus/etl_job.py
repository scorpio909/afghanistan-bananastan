# arcanus/etl_job.py
"""
The Conductor of Arcanus Prime's ETL (Extract, Transform, Load) process.
This Spark application performs the following key operations:
1. Reads raw data from the landing zone (expected to be Parquet files).
2. Enriches the data using User Defined Functions (UDFs) from the Transformation Engine (arcanus.udfs).
   This includes compressing symbol information and calculating resonance scores.
3. Atomically appends the enriched data to the main Delta Lake table, which serves as
   the system's single source of truth. Schema merging is enabled to accommodate
   potential schema evolutions.

This job is designed to be runnable via spark-submit and accepts command-line
arguments to specify input and output paths, overriding defaults from arcanus.config.
"""
import argparse
from pyspark.sql.functions import col, length
from . import config  # For default paths
from . import udfs    # For transformation functions
from .spark_session import get_spark_session # For obtaining a SparkSession

def run_job():
    """Main execution function for the ETL job."""
    spark = get_spark_session()
    job_name = spark.conf.get("spark.app.name")
    print(f"--- Starting Arcanus Prime ETL Job: {job_name} ---")
    print(f"Using Landing Zone: {config.landing_zone_path}")
    print(f"Using Delta Lake Output: {config.delta_lake_path}")

    # 1. READ raw data from the landing zone
    print(f"Step 1: Reading raw claims from Parquet source: {config.landing_zone_path}...")
    try:
        raw_df = spark.read.format("parquet").load(config.landing_zone_path)
        print(f"Successfully read {raw_df.count()} records from landing zone.")
        # For debugging, you might want to show schema or sample data:
        # print("Schema of raw_df:")
        # raw_df.printSchema()
        # print("Sample raw_df data:")
        # raw_df.show(5, truncate=False)
    except Exception as e:
        print(f"CRITICAL ERROR: Could not read from landing zone {config.landing_zone_path}.")
        print(f"Ensure data ingestion (arcanus.data_ingestion) has been run successfully.")
        print(f"Spark Error: {e}")
        spark.stop()
        return # Exit job

    # 2. TRANSFORM the data using the Enrichment Engine (UDFs)
    print("Step 2: Applying transformations to enrich claims...")
    try:
        enriched_df = (
            raw_df
            .withColumn("blob_z", udfs.compress_symbol_udf(col("symbol_value"), col("symbol_relations")))
            .withColumn("resonance", udfs.calculate_resonance_udf(length(col("blob_z")), col("symbol_relations")))
        )
        # If compress_symbol_udf returns None (on error), blob_z will be null.
        # length(col("blob_z")) will then be null.
        # calculate_resonance_udf is designed to handle null blob_size input (evaluates to 0 p_factor).

        print(f"Transformations applied. Resulting count: {enriched_df.count()} records.")
        # For debugging:
        # print("Schema of enriched_df:")
        # enriched_df.printSchema()
        # print("Sample enriched_df data (with blob_z and resonance):")
        # enriched_df.show(5, truncate=False)
    except Exception as e:
        print(f"CRITICAL ERROR: Failed during data transformation step.")
        print(f"Spark Error: {e}")
        spark.stop()
        return # Exit job

    # 3. WRITE the enriched data to the Delta Lake
    print(f"Step 3: Writing enriched data to Delta Lake: {config.delta_lake_path}...")
    try:
        (
            enriched_df.write
            .format("delta")
            .mode("append")  # Append new data to the Delta table
            .option("mergeSchema", "true") # Allow schema evolution
            .save(config.delta_lake_path)
        )
        print(f"Successfully wrote data to Delta Lake: {config.delta_lake_path}")
        # For verification, you could read back a sample or count from the Delta table here.
        # delta_table = spark.read.format("delta").load(config.delta_lake_path)
        # print(f"Delta table count after append: {delta_table.count()}")
    except Exception as e:
        print(f"CRITICAL ERROR: Failed to write to Delta Lake {config.delta_lake_path}.")
        print(f"Spark Error: {e}")
        spark.stop()
        return # Exit job

    print(f"--- Arcanus Prime ETL Job: {job_name} Completed Successfully ---")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Arcanus Prime ETL Job: Reads raw data, enriches it, and saves to Delta Lake.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter # Shows default values in help
    )
    parser.add_argument(
        "--landing-zone-path",
        default=config.landing_zone_path, # Default from config.py
        required=False,
        help="Path to the landing zone (Parquet files) for raw data."
    )
    parser.add_argument(
        "--delta-lake-path",
        default=config.delta_lake_path, # Default from config.py
        required=False,
        help="Path to the output Delta Lake for enriched data."
    )
    args = parser.parse_args()

    # Override config paths if provided via command line
    # This allows flexibility for running the job in different environments or with different datasets
    # without modifying the core config.py file.
    config.landing_zone_path = args.landing_zone_path
    config.delta_lake_path = args.delta_lake_path

    run_job()
