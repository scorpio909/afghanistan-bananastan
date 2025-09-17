# arcanus/arcanus_utils.py
import hashlib
import json
import time
import math
import lzma

# Try to import zstandard, otherwise use fallback _Z
try:
    import zstandard
    zstd_compressor_instance = zstandard.ZstdCompressor()
    zstd_decompressor_instance = zstandard.ZstdDecompressor()
    
    # Define wrapper functions for consistent interface if needed, or use instances directly
    def zstd_compress(data):
        return zstd_compressor_instance.compress(data)
    def zstd_decompress(data):
        return zstd_decompressor_instance.decompress(data)
    
    zstd = zstandard # Export the module itself for other uses (e.g. ZstdError)
                    # or export the wrapper functions/instances if preferred.
                    # For simplicity, keep previous pattern of exporting instances/mock.
    # Export effective compressor and decompressor instances
    zstd_comp_effective = zstd_compressor_instance
    zstd_decomp_effective = zstd_decompressor_instance
    # print("INFO: Using system zstandard library.")
except ImportError:
    # print("WARNING: zstandard library not found. Using fallback _Z class for compression.")
    class _Z_Fallback:
        def __init__(self):
            pass # No specific state needed for lzma
        
        def compress(self, data_bytes: bytes) -> bytes:
            # lzma.compress expects bytes
            return lzma.compress(data_bytes)

        def decompress(self, data_bytes: bytes) -> bytes:
            # lzma.decompress expects bytes
            return lzma.decompress(data_bytes)

    _z_instance = _Z_Fallback()
    zstd_comp_effective = _z_instance
    zstd_decomp_effective = _z_instance
    zstd = _Z_Fallback() # For isinstance checks or if module-like attributes are expected

# Try to import numpy, otherwise use fallback _NP
try:
    import numpy
    np = numpy
    # print("INFO: Using system numpy library.")
except ImportError:
    # print("WARNING: numpy library not found. Using fallback _NP class.")
    class _NP_Fallback:
        @staticmethod
        def array(data, dtype=None):
            return list(data) # Fallback returns a Python list

        @staticmethod
        def log(x): # Natural log
            try: return math.log(float(x))
            except (ValueError, TypeError): return float('nan')

        @staticmethod
        def log2(x): # Log base 2
            try:
                x_f = float(x)
                if x_f <= 0: return -float('inf') if x_f == 0 else float('nan')
                return math.log2(x_f)
            except (ValueError, TypeError): return float('nan')
        
        @staticmethod
        def sum(data, axis=None): # Fallback sum, axis ignored
            if isinstance(data, list):
                # Attempt to flatten if nested for axis!=None, otherwise sum direct elements
                if axis is not None:
                    flat_list = []
                    for item in data:
                        if isinstance(item, list): flat_list.extend(item)
                        else: flat_list.append(item)
                    return sum(v for v in flat_list if isinstance(v, (int, float)))
                return sum(v for v in data if isinstance(v, (int, float)))
            return sum(data) # For non-list iterables of numbers

        @staticmethod
        def clip(a, a_min, a_max, out=None):
            if isinstance(a, list):
                return [max(float(a_min), min(float(val), float(a_max))) for val in a]
            return max(float(a_min), min(float(a), float(a_max)))

        @staticmethod
        def mean(s: list):
            numeric_s = [float(x) for x in s if isinstance(x, (int, float, str)) and str(x).replace('.','',1).isdigit()]
            try: # Attempt to convert string numbers
                 numeric_s = [float(x) for x in s]
            except (ValueError, TypeError): # If conversion fails for any, filter
                 numeric_s = [float(x) for x in s if isinstance(x, (int, float))]

            return sum(numeric_s) / len(numeric_s) if numeric_s and len(numeric_s) > 0 else 0.0
        
        @staticmethod
        def random_uniform(low=0.0, high=1.0, size=None):
            import random
            if size is None: return random.uniform(low, high)
            if isinstance(size, int): return [random.uniform(low, high) for _ in range(size)]
            total_elements = 1
            if isinstance(size, tuple):
                for dim in size: total_elements *= dim
                return [random.uniform(low, high) for _ in range(total_elements)]
            return random.uniform(low,high) # Default fallback

    np = _NP_Fallback() # np will be an instance of _NP_Fallback

# CONFIG dictionary
CONFIG = {
    "version": "0.8.2", # Example version
    "kernel_hash": "initial_utils_kernel_hash", # Placeholder
    "param_d": 0.015, 
    "feature_flag_X": True
}

# Shared ledger
ledger = [] # Global ledger list, accessible for append/read by other modules

def lock_manifest(code_content: str) -> str:
    if not isinstance(code_content, str):
        code_content = str(code_content)
    return hashlib.sha512(code_content.encode('utf-8')).hexdigest()

def validate_claim(claim_object: dict) -> bool:
    if not isinstance(claim_object, dict): return False
    return 'id' in claim_object and 'source' in claim_object # Basic validation

def compress_data(input_data) -> bytes: # Renamed from 'compress' to avoid conflict if 'zstd.compress' is imported
    """Compresses data (bytes or string) using zstd_comp_effective."""
    data_bytes = input_data
    if not isinstance(input_data, bytes):
        data_bytes = str(input_data).encode('utf-8')
    try:
        return zstd_comp_effective.compress(data_bytes)
    except Exception as e:
        # print(f"COMPRESS_DATA_ERROR: {e}")
        return lzma.compress(data_bytes) # Fallback to lzma if zstd instance fails

def decompress_data(compressed_data: bytes) -> bytes:
    """Decompresses data using zstd_decomp_effective."""
    try:
        return zstd_decomp_effective.decompress(compressed_data)
    except Exception as e:
        # print(f"DECOMPRESS_DATA_ERROR: {e}")
        try:
            return lzma.decompress(compressed_data) # Fallback to lzma
        except lzma.LZMAError:
            return b'' # Return empty if all decompression fails

def resonance_LUT(claim_data: dict) -> float:
    if not validate_claim(claim_data): return 0.0
    
    claim_id = str(claim_data.get('id', ""))
    source = str(claim_data.get('source', ""))
    res_value_raw = claim_data.get('res', 0.0)
    
    try: res_value = float(res_value_raw)
    except (ValueError, TypeError): res_value = 0.0
        
    score = (len(claim_id) + len(source) + res_value) / 100.0 # Arbitrary calculation
    return np.clip(score, 0.0, 1.0) # Use the aliased np

def compute_entropy(ledger_data_list: list) -> float:
    if not isinstance(ledger_data_list, list) or not ledger_data_list: return 0.0

    counts = {} # Using 'counts' instead of 'probabilities' before normalization
    total_value = 0.0
    
    for entry in ledger_data_list:
        if not isinstance(entry, dict): continue

        res_val_raw = entry.get('res', 0.0) # Robust access
        try: res_val = float(res_val_raw)
        except (ValueError, TypeError): res_val = 0.0
        
        if res_val <= 0: continue

        category = entry.get('source', str(entry.get('id', 'unknown'))) # Use source or ID
        counts[category] = counts.get(category, 0.0) + res_val
        total_value += res_val

    if total_value == 0: return 0.0 # Sum of probabilities (values) is zero

    entropy_val = 0.0
    for category_key in counts:
        if counts[category_key] > 0:
            p_k = counts[category_key] / total_value
            log_p_k = np.log2(p_k) # np refers to numpy or _NP_Fallback
            entropy_val -= p_k * log_p_k
            
    return entropy_val if not (isinstance(entropy_val, float) and math.isnan(entropy_val)) else 0.0

# _Z class is effectively replaced by zstd_comp_effective/zstd_decomp_effective instances
# _NP class is effectively replaced by np instance of _NP_Fallback if numpy not found

# For direct import if needed, though zstd_comp_effective/np are preferred.
_Z = type(zstd) # Will be zstandard.ZstdCompressor or _Z_Fallback
_NP = type(np) # Will be numpy module or _NP_Fallback
```
