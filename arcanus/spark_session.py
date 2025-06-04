# arcanus/spark_session.py
"""
Module for creating and configuring the SparkSession.
This ensures a consistent session with Delta Lake and other
extensions is available to all Spark applications.
"""
from pyspark.sql import SparkSession
# Assuming arcanus.config will be created in the next step and will contain
# SPARK_APP_NAME and SPARK_MASTER attributes.
from . import config

def get_spark_session() -> SparkSession:
    """
    Creates and returns a SparkSession configured for Arcanus Prime,
    with Delta Lake integration enabled.
    For NVIDIA GPU acceleration, ensure your Spark environment is
    configured with the RAPIDS Accelerator JAR and relevant GPU settings
    passed via spark-submit or spark-defaults.conf.
    """
    spark_builder = (
        SparkSession.builder
        .appName(config.SPARK_APP_NAME) # From arcanus.config
        .master(config.SPARK_MASTER)   # From arcanus.config
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # --- NVIDIA RAPIDS Accelerator Configurations ---
        # The following RAPIDS configurations are often set in spark-defaults.conf or via spark-submit arguments
        # in a production environment. Including them here for clarity and to ensure they are set if not overridden.
        .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
        .config("spark.rapids.sql.enabled", "true") # Master switch for RAPIDS SQL plugin
        .config("spark.rapids.sql.explain", "ALL") # More verbose explain, good for debugging. Options: NONE, TRUE, FALSE, ALL.
        .config("spark.rapids.sql.csv.read.enabled", "true") # Example: enable GPU-accelerated CSV reads if used
        .config("spark.rapids.memory.gpu.pooling.enabled", "true") # Recommended for better GPU memory management
        .config("spark.rapids.sql.batchsize", "10000") # Default is 10k, adjust based on data and GPU memory

        # --- GPU Resource Allocation (Highly Environment-Specific) ---
        # The settings below are EXAMPLES and MUST be reviewed and adjusted for your specific cluster environment
        # (e.g., YARN, Kubernetes, Standalone mode) and available hardware.
        # For local mode (master="local[*]"), Spark runs on a single JVM (the driver).
        # RAPIDS will attempt to use available local GPUs if sql.enabled=true, but executor/worker specific counts are less relevant.
        # Uncomment and configure the following as per your cluster setup if not running in local mode or if defaults are not suitable.

        # .config("spark.executor.resource.gpu.amount", "1") # Num GPUs per executor. For YARN/K8s.
        # .config("spark.task.resource.gpu.amount", "0.25") # Num tasks per GPU (e.g., 1/0.25 = 4 tasks per GPU). Affects parallelism.
                                                         # Ensure (executor.resource.gpu.amount / task.resource.gpu.amount) is whole number.
        # .config("spark.worker.resource.gpu.amount", "1") # Num GPUs per worker (for Spark Standalone cluster mode).
        # .config("spark.executor.resource.gpu.discoveryScript", "/opt/spark/scripts/getGpusResources.sh") # Path to a script on worker nodes that Spark uses to discover GPUs.
                                                                                                       # This script's path and content are specific to your cluster setup.
                                                                                                       # Often NOT needed for local mode or if Spark is already GPU-aware.

        # Python environment for workers (ensure consistency if using Python UDFs that need specific libraries like CuPy)
        # It's generally recommended to pass spark.python.default.python.exe via spark-submit,
        # pointing to your virtual environment's python executable, to ensure workers use the correct Python and packages.
        # e.g., --conf spark.python.default.python.exe=/path/to/your/venv/bin/python
        # The spark.executorEnv.PYTHONPATH approach below is an alternative but can be more complex to manage.
        # .config("spark.executorEnv.PYTHONPATH", "/path/to/your/venv/lib/python3.x/site-packages")
    )

    # You can add conditional logic here if needed, for example, based on config.SPARK_MASTER
    # if config.SPARK_MASTER.startswith("local"):
    #     print("INFO: Spark master is local. RAPIDS will attempt to use local GPUs if available.")
    #     # Optionally, adjust or remove cluster-specific GPU resource configs for local mode,
    #     # or rely on RAPIDS defaults for local execution. For instance, discoveryScript is usually not needed for local.
    # else:
    #     # For cluster modes, ensure the GPU resource configurations above are uncommented and correctly set.
    #     print("INFO: Spark master is not local. Ensure GPU resource configurations are set for cluster mode.")


    return spark_builder.getOrCreate()
