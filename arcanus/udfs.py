# arcanus/udfs.py
"""
The Transformation Engine for Arcanus Prime.
This module contains User Defined Functions (UDFs) that encapsulate
the core business logic for enriching claims, now optimized for GPU acceleration.
"""
import pandas as pd
from pyspark.sql.functions import pandas_udf, udf
from pyspark.sql.types import IntegerType, BinaryType
from pyspark.sql.column import Column

# Attempt to import CuPy for GPU-accelerated Pandas UDFs
try:
    import cupy as cp
    _has_cupy = True
    print("INFO: udfs.py - CuPy library loaded for potential GPU acceleration.")
except ImportError:
    _has_cupy = False
    print("WARNING: udfs.py - CuPy not found. Pandas UDFs will run on CPU.")

# Attempt to import zstandard, provide a dummy if not found for local UDF logic.
# In a real Spark cluster, ensure zstandard is available on all worker nodes.
try:
    import zstandard as zstd
    ZSTD_COMPRESSOR = zstd.ZstdCompressor(level=3)
    print("INFO: udfs.py - zstandard library loaded for compression logic.")
except ImportError:
    class _Z_Fallback_UDF:
        def compress(self, data):
            # This is a placeholder. Production UDFs for compression
            # must have zstandard available on Spark executors.
            print("CRITICAL: udfs.py - zstandard not found. Compression UDF will return minimal data. Data loss likely.")
            return b'\x00' # Minimal binary
    ZSTD_COMPRESSOR = _Z_Fallback_UDF()
    print("CRITICAL: udfs.py - zstandard not found for compression logic. Using dummy fallback for local definition.")


# --- UDF 1: Calculate Resonance (GPU-Accelerated Pandas UDF) ---
@pandas_udf(IntegerType())
def calculate_resonance_udf(blob_size: pd.Series, relations: pd.Series) -> pd.Series:
    """
    Calculates resonance score based on blob size and relation flags.
    This is a vectorized Pandas UDF, designed to leverage CuPy for GPU acceleration
    if CuPy is available and Spark RAPIDS is configured.
    """
    if _has_cupy:
        # Convert Pandas Series to CuPy array for GPU computation
        blob_size_gpu = cp.asarray(blob_size.values)
        # Handle `relations` Series of lists. .apply with CuPy is often faster for complex types.
        # Ensure relations are handled gracefully if None or empty.
        # The following list comprehension runs on CPU, then the result is moved to GPU.
        relations_gpu = cp.asarray([1 if r and len(r) >= 2 else 0 for r in relations.values])

        p_factor_gpu = (blob_size_gpu <= 32).astype(cp.int32)
        e_factor_gpu = relations_gpu.astype(cp.int32)

        score_float_gpu = (p_factor_gpu * 0.6 + e_factor_gpu * 0.4) * 255
        score_clipped_gpu = cp.clip(score_float_gpu, 0, 255)

        return pd.Series(score_clipped_gpu.get().astype(int)) # Convert back to NumPy array, then Pandas Series
    else:
        # Fallback to NumPy (CPU) if CuPy is not available
        p_factor_cpu = (blob_size <= 32).astype(int)
        e_factor_cpu = relations.apply(lambda r: 1 if r and len(r) >= 2 else 0).astype(int)

        score_float_cpu = (p_factor_cpu * 0.6 + e_factor_cpu * 0.4) * 255
        score_clipped_cpu = score_float_cpu.clip(0, 255)
        return score_clipped_cpu.astype(int)


# --- UDF 2: Compress Symbol (Standard UDF - Compression generally CPU-bound) ---
# Note: Compression algorithms like zstandard are typically CPU-bound.
# While the UDF framework can be Pandas UDF, the actual compression work
# is unlikely to benefit from GPU directly unless a specific GPU-accelerated
# compression library is used (e.g., cuDF for certain operations, but not zstd).
# We keep this as a standard PySpark UDF as the core logic is CPU-intensive.
@udf(returnType=BinaryType())
def compress_symbol_udf(value: float, relations: list) -> bytes:
    """
    Core logic for compressing a symbol into a binary blob using zstandard.
    Returns None if an error occurs during compression.
    """
    try:
        relations_list = relations if relations is not None else []
        # Bit flags for relations: up=1, down=2, across=4
        flags = sum(1 << i for i, r_type in enumerate(["up", "down", "across"]) if r_type in relations_list)
        payload_value = value if value is not None else 0.0
        payload = f"{payload_value}:{flags}".encode('utf-8')
        return ZSTD_COMPRESSOR.compress(payload)
    except Exception as e:
        print(f"ERROR: compress_symbol_udf failed for value: {value}, relations: {relations}. Error: {e}")
        return None # Return None on error
