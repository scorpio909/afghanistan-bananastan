# arcanus/analysis_job.py
"""
The Analysis Engine of Arcanus Prime.
This Spark application reads the complete, unified state (enriched claims data)
from the main Delta Lake table. It then performs system-wide aggregations and
analytics, including:
- Calculation of a 'self_awareness_score' for each claim based on its resonance
  and the average resonance of its source.
- Generation of a summary table with claim counts, total resonance, and average
  self_awareness_score per source.
- Calculation of the overall system entropy based on claim resonance distribution.
The results are displayed and the summary is written to a specified output path.
"""
import argparse
from pyspark.sql.functions import col, sum as _sum, log2 as _log2, count, avg, when # Use log2 for entropy for bits
from pyspark.sql.types import DoubleType # For casting if needed, though not strictly here
from . import config  # For default paths
from .spark_session import get_spark_session # For obtaining a SparkSession

def run_job():
    """Main execution function for the analysis job."""
    spark = get_spark_session()
    job_name = spark.conf.get("spark.app.name")
    print(f"--- Starting Arcanus Prime Analysis Job: {job_name} ---")
    print(f"Reading data from Delta Lake: {config.delta_lake_path}")
    print(f"Analysis output will be written to: {config.analysis_output_path}")

    # 1. READ the unified state from the Delta Lake table
    print(f"Step 1: Reading claims data from Delta Lake source: {config.delta_lake_path}...")
    try:
        claims_df = spark.read.format("delta").load(config.delta_lake_path)
    except Exception as e:
        print(f"CRITICAL ERROR: Could not read from Delta Lake {config.delta_lake_path}.")
        print(f"Ensure the ETL job (arcanus.etl_job) has been run successfully to populate the Delta table.")
        print(f"Spark Error: {e}")
        spark.stop()
        return # Exit job

    # Cache DataFrame as it's used multiple times for different calculations
    claims_df.cache()

    total_claims = claims_df.count()
    if total_claims == 0:
        print("INFO: No claims found in Delta Lake. Analysis cannot proceed as there is no data to analyze.")
        claims_df.unpersist() # Unpersist before stopping
        spark.stop()
        return # Exit job

    print(f"Analysis will be performed on {total_claims} claims.")
    # For debugging:
    # print("Schema of claims_df from Delta Lake:")
    # claims_df.printSchema()
    # print("Sample claims_df data:")
    # claims_df.show(5, truncate=False)

    # 2. PERFORM ANALYSIS
    print("Step 2: Performing advanced analytics...")

    # Calculate average resonance per source, crucial for 'self_awareness_score'
    print("Calculating average resonance per source...")
    avg_resonance_df = claims_df.groupBy("source").agg(avg("resonance").alias("avg_resonance_for_source"))

    # Join average resonance back to the main claims DataFrame
    claims_df_with_avg = claims_df.join(avg_resonance_df, "source", "left_outer")
    # Fill NA for avg_resonance_for_source if a source had no resonance values (e.g. all nulls)
    # This prevents issues in the self_awareness_score calculation if avg_resonance_for_source is null.
    claims_df_with_avg = claims_df_with_avg.fillna(0.0, subset=["avg_resonance_for_source"])


    # Calculate self_awareness_score
    print("Calculating self_awareness_score for each claim...")
    claims_df_with_sa = claims_df_with_avg.withColumn(
        "self_awareness_score",
        when(
            (col("resonance") > 0) &
            (col("avg_resonance_for_source").isNotNull()) & # Should not be null due to fillna(0.0)
            (col("avg_resonance_for_source") > 0), # Avoid division by zero or meaningless scores
            col("resonance") / col("avg_resonance_for_source") # Example: score relative to source's average
        ).otherwise(0.0) # Default to 0.0 if conditions not met
    )
    # The original issue had a simpler binary (1.0/0.0) self_awareness_score.
    # I've made it a ratio col("resonance") / col("avg_resonance_for_source") for a more nuanced score,
    # but ensuring avg_resonance_for_source > 0. If a binary score is preferred, revert the division to 1.0.


    # Summary Analysis: Count claims, total resonance, and average self-awareness per source
    print("Calculating summary statistics per source...")
    summary_df = claims_df_with_sa.groupBy("source").agg(
        count("*").alias("claim_count"),
        _sum("resonance").alias("total_resonance"),
        avg("self_awareness_score").alias("avg_self_awareness_score_ratio") # Renamed to reflect ratio
    ).orderBy(col("claim_count").desc())

    # Entropy Analysis: Calculate Shannon entropy of the system based on claim resonance distribution
    print("Calculating system entropy...")
    entropy_str = "N/A" # Default entropy string

    # Calculate total resonance sum for entropy calculation, filter out null or non-positive resonance values
    # Ensure resonance column is treated as numeric for sum
    valid_resonance_claims = claims_df.filter(col("resonance").isNotNull() & (col("resonance") > 0))

    if valid_resonance_claims.count() == 0:
        entropy_str = "N/A (no claims with positive resonance found)"
    else:
        total_res_for_entropy_df = valid_resonance_claims.agg(_sum("resonance").alias("total_res"))
        total_res_val_row = total_res_for_entropy_df.collect()

        if total_res_val_row and total_res_val_row[0]["total_res"] is not None and total_res_val_row[0]["total_res"] > 0:
            total_res_val = total_res_val_row[0]["total_res"]

            # Calculate p_i = resonance_i / total_resonance
            # Calculate sum(p_i * log2(p_i))
            # Entropy = -sum(p_i * log2(p_i))
            entropy_calc_df = (
                valid_resonance_claims
                .withColumn("p", col("resonance") / total_res_val)
                .withColumn("p_log_p", col("p") * _log2("p")) # Using log base 2 for entropy in bits
                .agg(_sum("p_log_p").alias("sum_p_log_p"))
            )
            entropy_result_row = entropy_calc_df.collect()

            if entropy_result_row and entropy_result_row[0]["sum_p_log_p"] is not None:
                entropy = -1 * entropy_result_row[0]["sum_p_log_p"]
                entropy_str = f"{entropy:.4f} bits"
            else:
                entropy_str = "N/A (could not compute sum for entropy; check intermediate data)"
        else:
            entropy_str = "N/A (total positive resonance is zero or None)"

    # 3. DISPLAY RESULTS
    print("--- Arcanus Prime Analysis Report ---")
    print(f"Overall System Entropy: {entropy_str}")
    print("Claims Summary by Source (including Avg. Self-Awareness Score Ratio):")
    summary_df.show(truncate=False)

    # 4. WRITE the summary analysis to the specified output path
    print(f"Step 3: Writing analysis summary to Parquet: {config.analysis_output_path}...")
    try:
        summary_df.write.mode("overwrite").parquet(config.analysis_output_path)
        print(f"Successfully wrote analysis summary to {config.analysis_output_path}")
    except Exception as e:
        print(f"CRITICAL ERROR: Failed to write analysis summary to {config.analysis_output_path}. Error: {e}")
        # Spark session will be stopped in the finally block or at the end of run_job

    print("--- Analysis Job Completed ---")

    claims_df.unpersist() # Unpersist the cached DataFrame
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Arcanus Prime Analysis Job: Reads from Delta Lake, performs analytics, and writes a summary.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter # Automatically shows defaults in help
    )
    parser.add_argument(
        "--delta-lake-path",
        default=config.delta_lake_path, # Default from config.py
        # required=False, # Not strictly required as default is provided
        help="Path to the Delta Lake table containing the claims data."
    )
    parser.add_argument(
        "--analysis-output-path",
        default=config.analysis_output_path, # Default from config.py
        # required=False, # Not strictly required as default is provided
        help="Path to write the analysis summary output (Parquet files)."
    )
    args = parser.parse_args()

    # Update config with parsed arguments. If arguments were not provided,
    # the defaults from config module (which were also argparse defaults) will be used.
    config.delta_lake_path = args.delta_lake_path
    config.analysis_output_path = args.analysis_output_path

    run_job()
