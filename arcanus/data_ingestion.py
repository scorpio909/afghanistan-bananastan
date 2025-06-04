# arcanus/data_ingestion.py
"""
Simulates the ingestion of raw data into the landing zone.
In a real-world scenario, this module would interface with actual data sources
like message queues (e.g., Kafka), streaming platforms, or external APIs.
For Arcanus Prime, this script generates sample data to enable ETL and analysis.
"""
import datetime
import random
import uuid
from . import config  # For accessing landing_zone_path and CLAIM_SCHEMA
from .spark_session import get_spark_session # For obtaining a SparkSession

def generate_and_save_raw_data(num_records: int = 1000):
    """
    Generates a specified number of raw claim records using the canonical schema
    and saves them as Parquet files in the configured landing zone.
    The data includes some None values to test downstream handling.

    Args:
        num_records (int): The number of claim records to generate.
    """
    print(f"Starting data generation: Creating {num_records} raw claim records...")

    spark = get_spark_session()

    # Define pools for generating varied data
    relations_pool = [["up"], ["down"], ["across"], ["up", "across"], []] # Includes empty list
    source_pool = ["SensorGrid-A", "IntelFeed-X", "ManualEntry-C", "LogFile-Z", "AutomatedSystem-B"]

    data_to_ingest = []
    print(f"Generating data for {num_records} records...")
    for i in range(num_records):
        # Introduce None for symbol_value in ~10% of records
        symbol_val = round(random.uniform(10.0, 500.0), 2) if random.random() > 0.1 else None

        # Introduce None for symbol_relations in ~10% of records
        relations = random.choice(relations_pool) if random.random() > 0.1 else None

        data_to_ingest.append((
            str(uuid.uuid4()),  # claim_id: Unique ID for the claim
            random.choice(source_pool),  # source: Origin of the claim
            datetime.datetime.now() - datetime.timedelta(seconds=random.randint(0, 86400 * 7)),  # event_timestamp: Randomly in the last week
            symbol_val,  # symbol_value: Numerical value, possibly None
            relations  # symbol_relations: List of relations, possibly None or empty
        ))
        if (i + 1) % (num_records // 10 or 1) == 0: # Print progress every 10%
             print(f"Generated {i+1}/{num_records} records...")

    print("Data generation complete. Creating Spark DataFrame...")
    # Create a DataFrame using the predefined canonical schema from config.py
    # This ensures data consistency from the point of ingestion.
    try:
        df = spark.createDataFrame(data_to_ingest, schema=config.CLAIM_SCHEMA)
    except Exception as e:
        print(f"ERROR: Failed to create Spark DataFrame. Error: {e}")
        spark.stop()
        raise # Re-raise the exception after stopping Spark

    # Save the DataFrame to the landing zone in Parquet format.
    # "overwrite" mode is used to replace any existing data from previous runs,
    # suitable for a data simulation script.
    print(f"Saving raw data to Parquet format at: {config.landing_zone_path}")
    try:
        df.write.mode("overwrite").parquet(config.landing_zone_path)
    except Exception as e:
        print(f"ERROR: Failed to write data to {config.landing_zone_path}. Error: {e}")
        spark.stop()
        raise # Re-raise the exception

    print(f"Successfully saved {df.count()} raw claim records to: {config.landing_zone_path}")

    print("Stopping Spark session for data ingestion...")
    spark.stop()
    print("Data ingestion process completed.")

if __name__ == '__main__':
    print("Running Arcanus Prime Data Ingestion independently...")
    # This script uses default paths and configurations from arcanus.config when run directly.
    # For custom paths or larger data volumes in a real scenario, consider:
    # - Command-line arguments (using argparse) for num_records, output paths.
    # - Environment variables.
    # - Integration into a larger workflow management system (e.g., Airflow).

    # Example: generate_and_save_raw_data(num_records=5000) # To generate more records
    generate_and_save_raw_data()
