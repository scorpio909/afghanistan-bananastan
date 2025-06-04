# arcanus/config.py
"""
Centralized configuration for the Arcanus Prime system.
All paths, table names, and application settings are defined here to
ensure system-wide coherence. These settings are primarily defaults
and can often be overridden by command-line arguments in job scripts.
"""
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, ArrayType

# --- PATHS ---
# These are default paths, intended for local execution and ease of testing.
# They are typically overridden by command-line arguments for etl_job.py and analysis_job.py
# when running in different environments or with different datasets.
# data_ingestion.py uses these defaults directly.
landing_zone_path = "data/landing/claims"
delta_lake_path = "data/delta_lake/arcanus_claims"
analysis_output_path = "data/delta_lake/analysis_summary"

# --- SPARK CONFIG ---
# These are used by arcanus.spark_session.get_spark_session() if not overridden
# by spark-submit configurations or environment variables.
SPARK_APP_NAME = "ArcanusPrime"
SPARK_MASTER = "local[*]"  # Default to local mode using all available cores for testing.

# --- DELTA LAKE CONFIG ---
# This table name is relevant if you choose to register the Delta path
# as a table in a Hive metastore or similar catalog.
CLAIMS_TABLE_NAME = "arcanus_claims_production"

# --- DATA SCHEMA ---
# The Canonical Schema for a Claim. This schema is enforced by data_ingestion.py
# and is the expected schema for data read by etl_job.py.
# Delta Lake's schema merging capabilities can handle schema evolution if needed.
CLAIM_SCHEMA = StructType([
    StructField("claim_id", StringType(), False),          # Unique identifier for the claim
    StructField("source", StringType(), True),             # Source system or feed of the claim
    StructField("event_timestamp", TimestampType(), True), # Timestamp of the event related to the claim
    StructField("symbol_value", DoubleType(), True),       # A numerical value associated with the claim's symbol
    StructField("symbol_relations", ArrayType(StringType()), True) # List of relationships (e.g., "up", "down")
])
