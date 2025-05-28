from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, BinaryType

@udf(returnType=IntegerType())
def calculate_resonance_udf(blob_size: int, relations: list) -> int:
    # Simple logic for demonstration
    p = 1 if blob_size <= 32 else 0
    e = 1 if relations and len(relations) >= 2 else 0
    score = int((p * 0.6 + e * 0.4) * 255)
    return max(0, min(255, score))

# UDF definition as per the current subtask description
def compress_symbol_logic(value: float, relations: list) -> bytes:
    try:
        import zstandard as zstd
        # This line will raise TypeError if relations is None
        flags = sum(1 << i for i, r in enumerate(["up", "down", "across"]) if r in relations)
        # value or 0.0 handles None for value by using 0.0 instead
        payload = f"{value or 0.0}:{flags}".encode('utf-8')
        return zstd.ZstdCompressor(level=3).compress(payload)
    except (ImportError, TypeError):
        # Fallback if zstd is not on an executor or input is null/TypeError
        return b'\x00'

compress_symbol_udf = udf(compress_symbol_logic, BinaryType())
