# arcanus/lbt_modules.py
import asyncio
import json
import math
import statistics # For mean, if available and no _NP.mean
# import zstd # Will be imported from arcanus_utils
import aiosqlite # For log_claim's asyncio.run part
import time # For default timestamps

# Imports from arcanus_utils (as per Step 4)
from .arcanus_utils import (
    # _Z, _NP, # Classes themselves, not typically used if instances are provided
    CONFIG, ledger,
    validate_claim, 
    compress_data, # Renamed from compress
    resonance_LUT, 
    compute_entropy as compute_entropy_util, # Aliased in case of signature diffs
    zstd_comp_effective, zstd_decomp_effective, # Direct compressor/decompressor
    np # numpy or _NP_Fallback instance
)
# Note: The prompt for lbt_modules asks for import of _Z, _NP, compress.
# In arcanus_utils:
# - _Z class was effectively replaced by zstd_comp_effective / zstd_decomp_effective instances.
# - _NP class was effectively replaced by np instance (which is _NP_Fallback if numpy not found).
# - 'compress' was renamed to 'compress_data'.
# - compute_entropy from utils will replace the local one.

# For database interactions (simplified)
DB_PATH = "arcanus_lbt.db"

# _NP class and np_impl definition removed, will use 'np' from arcanus_utils.
# Local 'zstd' import removed, will use 'zstd_comp_effective' and 'zstd_decomp_effective'.

# 8. log_claim uses asyncio.run for now.
# This function is specific to lbt_modules for DB interaction, so it remains.
# It does not directly use shared components that were removed, only aiosqlite.
async def _save_claim_to_db_async(claim_payload: dict):
    # This function remains async, as it performs async DB operations.
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("CREATE TABLE IF NOT EXISTS lbt_claims (id TEXT PRIMARY KEY, data TEXT, timestamp TEXT)")
            claim_id = claim_payload.get('id', f"default_id_{str(math.floor(time.time()))}") 
            timestamp = claim_payload.get('timestamp', str(math.floor(time.time())))
            
            await db.execute("INSERT OR REPLACE INTO lbt_claims (id, data, timestamp) VALUES (?, ?, ?)",
                             (claim_id, json.dumps(claim_payload), timestamp))
            await db.commit()
    except Exception as e:
        # Log error specific to DB operation, including claim ID if available
        print(f"DB_SAVE_ASYNC_ERROR: Failed to save claim {claim_payload.get('id', 'N/A')}: {e}")
        raise # Optionally re-raise to allow caller to handle DB errors


async def log_claim(claim_data: dict): # Changed to async def
    claim_id = claim_data.get('id', f"claim_{str(math.floor(time.time()))}")
    db_payload = {
        'id': claim_id,
        'original_claim_data': claim_data,
        'timestamp': claim_data.get('timestamp', str(math.floor(time.time())))
    }
    try:
        # Replaced asyncio.run with await; removed complex RuntimeError handling for nested loops.
        await _save_claim_to_db_async(db_payload)
    except Exception as e_gen: 
        # Catch general errors from _save_claim_to_db_async (e.g., DB connection, query errors)
        # The specific RuntimeError for "loop already running" is not an issue with 'await'.
        print(f"LBT_LOG_CLAIM_ERROR: Error saving claim {claim_id} to DB: {e_gen}")
        # Depending on policy, might re-raise or handle (e.g., queue claim for later).


# 5. get_claim_from_db_by_id: ensure blob_z is bytes
# This function remains synchronous as per its original design.
# If it were to become async, its callers would need to change.
def get_claim_from_db_by_id(claim_id: str) -> dict | None:
    # This is a synchronous stub. A real implementation would use asyncio.run or be async.
    # print(f"LBT_DB_GET_CLAIM: Attempting to fetch claim ID {claim_id}") # Noisy
    mock_db_data = {
        "claim_example_1": {"id": "claim_example_1", "data": "some data", "blob_z": "string_blob_needs_encoding"},
        "claim_example_2": {"id": "claim_example_2", "data": "other data", "blob_z": b"already_bytes_blob"},
        "claim_example_3": {"id": "claim_example_3", "data": "no blob", "blob_z": None},
        "claim_example_4": {"id": "claim_example_4", "data": "wrong type blob", "blob_z": 12345},
    }
    retrieved_item = mock_db_data.get(claim_id)
    if retrieved_item:
        # 1. Use .get for retrieved_item dictionary access
        blob_z_raw = retrieved_item.get('blob_z') # Default is None if not present
        if isinstance(blob_z_raw, str):
            # 5. Encode string blob to bytes
            retrieved_item['blob_z'] = blob_z_raw.encode('latin-1')
        elif not isinstance(blob_z_raw, bytes) and blob_z_raw is not None:
            print(f"LBT_DB_GET_CLAIM_WARN: blob_z for {claim_id} is of unexpected type {type(blob_z_raw)}. Setting to None.")
            retrieved_item['blob_z'] = None
        return retrieved_item
    return None


def summarize_ledger(ledger_file_path: str) -> dict:
    ledger_entries = []
    try:
        with open(ledger_file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    entry = json.loads(line)
                    ledger_entries.append(entry)
                except json.JSONDecodeError:
                    print(f"LEDGER_SUMMARY_WARN: Invalid JSON in {ledger_file_path} line {line_num}. Skipping.")
    except FileNotFoundError:
        print(f"LEDGER_SUMMARY_ERROR: File not found {ledger_file_path}")
        return {"error": "File not found", "mean_value": 0.0, "total_entries": 0}

    if not ledger_entries: # 3. Check for empty list
        return {"mean_value": 0.0, "total_entries": 0, "warning": "No entries found or all invalid."}

    values = []
    for entry in ledger_entries:
        value = entry.get('value', 0.0) # 1. Use .get, default to 0.0
        try:
            values.append(float(value)) # 2. Ensure numeric
        except (ValueError, TypeError):
            values.append(0.0) # Default for non-convertible
            print(f"LEDGER_SUMMARY_WARN: Invalid numeric value '{value}' in entry {entry.get('id', 'N/A')}. Using 0.0.")
            
    mean_value = np.mean(values) # Use imported np
    return {"mean_value": mean_value, "total_entries": len(ledger_entries)}


def monitor_state(current_state: dict) -> dict:
    # 1. Use .get for current_state access
    # This function uses np.mean, so it's affected by np import change.
    monitored_values_raw = current_state.get('values', []) 
    
    if not isinstance(monitored_values_raw, list): # Check if 'values' is actually a list
        print(f"MONITOR_STATE_ERROR: 'values' field is not a list (type: {type(monitored_values_raw)}).")
        return {"status": "INVALID_INPUT_TYPE", "mean": 0.0}

    if not monitored_values_raw: # 3. Check for empty list
        return {"status": "NO_DATA", "mean": 0.0}
        
    numeric_values = []
    for v_idx, v in enumerate(monitored_values_raw):
        try:
            numeric_values.append(float(v)) # 2. Ensure numeric
        except (ValueError, TypeError):
            print(f"MONITOR_STATE_WARN: Non-numeric value '{v}' at index {v_idx} skipped.")
            
    if not numeric_values: # 3. Check if all were non-numeric and skipped
        return {"status": "NO_VALID_DATA", "mean": 0.0}

    mean_val = np.mean(numeric_values) # Use imported np
    return {"status": "OK", "mean": mean_val, "points_processed": len(numeric_values)}


def alert_anomalies(data_points: list, threshold_factor=2.0) -> list:
    # This function uses np.mean, so it's affected by np import change.
    if not isinstance(data_points, list) or not data_points: # 3. Check for empty/invalid list
        return []

    values = []
    for i, dp_raw in enumerate(data_points):
        val_to_convert = 0.0 # Default value if dp is not dict or directly convertible
        if isinstance(dp_raw, dict):
            val_to_convert = dp_raw.get('value', 0.0) # 1. Use .get
        elif isinstance(dp_raw, (int, float, str)): # Allow direct numbers or convertible strings
            val_to_convert = dp_raw
        else:
            print(f"ALERT_ANOMALIES_WARN: Data point at index {i} is complex type {type(dp_raw)}, using 0.0.")

        try:
            values.append(float(val_to_convert)) # 2. Ensure numeric
        except (ValueError, TypeError):
            print(f"ALERT_ANOMALIES_WARN: Invalid numeric data point '{val_to_convert}' at index {i}. Using 0.0.")
            values.append(0.0) 
            
    if len(values) < 2 : # Need at least 2 points for meaningful std dev, or handle differently
        return [] # Not enough data to find anomalies based on std dev

    mean_val = np.mean(values) # Use imported np
    # Basic std dev; ensure elements are numeric for subtraction.
    std_dev = math.sqrt(np.mean([(float(x) - mean_val)**2 for x in values])) # Use imported np

    if std_dev == 0: # 3. Check for zero divisor
        return [] # No deviation, so no anomalies by this method

    anomalies = []
    for i, value in enumerate(values):
        z_score = (value - mean_val) / std_dev # std_dev is non-zero here
        if abs(z_score) > threshold_factor:
            anomalies.append({"index": i, "value": value, "z_score": z_score})
    return anomalies


def analyze_mutation_feedback(feedback_list: list) -> dict:
    if not isinstance(feedback_list, list) or not feedback_list: # 3. Check for empty/invalid list
        return {"total_feedback": 0, "valid_items": 0, "positive_rate": 0.0}

    positive_feedback_count = 0
    valid_feedback_items = 0
    for item_idx, item in enumerate(feedback_list):
        if not isinstance(item, dict): # Ensure item is a dictionary
            print(f"MUTATION_FEEDBACK_WARN: Item at index {item_idx} is not a dict. Skipping.")
            continue
        
        valid_feedback_items += 1 # Count item as processed for rate calculation base
        # 1. Use .get for item dictionary access
        feedback_type = item.get('type', 'neutral') 
        score = 0.0
        try:
            # 2. Ensure numeric score, default to 0.0
            score = float(item.get('score', 0.0)) 
        except (ValueError, TypeError):
            print(f"MUTATION_FEEDBACK_WARN: Invalid score '{item.get('score')}' in item {item_idx}. Using 0.0.")
            
        if feedback_type == 'positive' or score > 0.5: # Define "positive"
            positive_feedback_count += 1
            
    # 3. Check for zero divisor (valid_feedback_items)
    positive_rate = (positive_feedback_count / valid_feedback_items) if valid_feedback_items > 0 else 0.0
    return {"total_feedback": len(feedback_list), "valid_items": valid_feedback_items, "positive_rate": positive_rate}


# Local compute_entropy function is removed.
# If lbt_modules needs to compute entropy, it should use compute_entropy_util from arcanus_utils.
# The existing local compute_entropy was not called by other functions in this file.

def _decompress_blob(blob_z, item_id_for_log="N/A"):
    # This function now uses decompress_data from arcanus_utils,
    # which itself handles zstd/fallback and byte conversion.
    # The original _decompress_blob had detailed error handling for zstd.ZstdError etc.
    # decompress_data in arcanus_utils has its own error handling.
    # We simplify _decompress_blob to just call the utility.
    
    # The old logic for ensuring bytes before calling zstd.decompress:
    # if isinstance(blob_z, str):
    #     try:
    #         blob_z = blob_z.encode('latin-1') 
    #     except Exception as e_encode:
    #         print(f"DECOMPRESS_ERROR: Failed to encode string blob_z for item {item_id_for_log}: {e_encode}")
    #         return None
    # elif not isinstance(blob_z, bytes):
    #     print(f"DECOMPRESS_ERROR: blob_z for item {item_id_for_log} is not bytes or string (type: {type(blob_z)}). Cannot decompress.")
    #     return None
    # The new arcanus_utils.decompress_data expects bytes. This function should ensure it passes bytes.
    
    data_bytes_to_decompress = None
    if isinstance(blob_z, str):
        try:
            data_bytes_to_decompress = blob_z.encode('latin-1')
        except Exception as e_encode:
            print(f"DECOMPRESS_ENCODE_ERROR for item {item_id_for_log}: {e_encode}")
            return None
    elif isinstance(blob_z, bytes):
        data_bytes_to_decompress = blob_z
    else:
        print(f"DECOMPRESS_TYPE_ERROR: blob_z for item {item_id_for_log} is not bytes or string (type: {type(blob_z)}).")
        return None

    try:
        # decompress_data from arcanus_utils returns bytes
        decompressed_bytes = decompress_data(data_bytes_to_decompress)
        if decompressed_bytes:
            return json.loads(decompressed_bytes.decode('utf-8'))
        return None # Decompression failed in util
    except UnicodeDecodeError as e:
        print(f"DECOMPRESS_JSON_UNICODE_ERROR for item {item_id_for_log}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"DECOMPRESS_JSON_ERROR for item {item_id_for_log}: {e}")
        return None
    # Errors from decompress_data (like zstd.ZstdError via fallback) are handled in decompress_data,
    # which might print and return empty bytes or raise, depending on its implementation.
    # Assuming decompress_data returns None or empty bytes on failure.


def topological_map(compressed_claims: list) -> list:
    # This function calls _decompress_blob, which is now updated.
    results = []
    if not isinstance(compressed_claims, list):
        print("TOPOLOGICAL_MAP_ERROR: Input is not a list.")
        return []

    for i, c_item_raw in enumerate(compressed_claims):
        if not isinstance(c_item_raw, dict): # Ensure item is a dictionary
            print(f"TOPOLOGICAL_MAP_WARN: Item at index {i} is not a dict. Skipping.")
            results.append({"id": f"invalid_item_{i}", "map_data": None, "error": "item_not_dict"})
            continue

        # 1. Use .get for c_item_raw dictionary access
        item_id = c_item_raw.get('id', f"item_{i}") 
        blob_z = c_item_raw.get('blob_z')

        if blob_z is None:
            # print(f"TOPOLOGICAL_MAP_INFO: Missing 'blob_z' for item {item_id}. Skipping.") # Noisy
            results.append({"id": item_id, "map_data": None, "error": "missing_blob_z"})
            continue
            
        decompressed_content = _decompress_blob(blob_z, item_id)
        if decompressed_content is not None:
            results.append({"id": item_id, "map_data": decompressed_content})
        else:
            results.append({"id": item_id, "map_data": None, "error": "decompression_failed"})
    return results


def aae_cluster_source(items: list) -> dict:
    clusters = {}
    if not isinstance(items, list):
        print("AAE_CLUSTER_ERROR: Input is not a list.")
        return {}

    for i, item_raw in enumerate(items):
        if not isinstance(item_raw, dict): # Ensure item is a dictionary
            print(f"AAE_CLUSTER_WARN: Item at index {i} is not a dict. Skipping.")
            continue

        # 1. Use .get for item_raw dictionary access
        item_id = item_raw.get('id', f"source_item_{i}")
        blob_z = item_raw.get('blob_z')
        source_type = item_raw.get('type', 'unknown_type')

        if blob_z is None:
            # print(f"AAE_CLUSTER_INFO: Missing 'blob_z' for item {item_id}. Skipping.") # Noisy
            continue

        decompressed_data = _decompress_blob(blob_z, item_id)
        # Cluster based on source_type, even if decompression fails (put into a 'failed' sub-list)
        if source_type not in clusters: clusters[source_type] = []
        
        if decompressed_data is not None:
            clusters[source_type].append({"id": item_id, "content": decompressed_data})
        else:
            clusters[source_type].append({"id": item_id, "content": None, "error": "decompression_failed"})
    return clusters


def cos_dist(v1: list, v2: list) -> float:
    # This function does not directly use np or zstd, but relies on _decompress_blob.
    if not (isinstance(v1, list) and isinstance(v2, list)):
        print("COS_DIST_ERROR: Inputs must be lists.")
        return math.nan # Indicate error/undefined

    if len(v1) != len(v2):
        print("COS_DIST_ERROR: Vectors must have the same length.")
        return math.nan 
    if not v1: # len(v1) == 0, and thus len(v2) == 0
        return 1.0 # Conventionally, two empty vectors are perfectly similar or undefined. 1.0 implies similarity.

    dot_product = 0.0
    norm_v1_sq = 0.0 # Sum of squares for v1
    norm_v2_sq = 0.0 # Sum of squares for v2

    for i in range(len(v1)):
        try:
            x1 = float(v1[i]) # 2. Ensure numeric
            x2 = float(v2[i]) # 2. Ensure numeric
        except (ValueError, TypeError):
            print(f"COS_DIST_ERROR: Non-numeric element at index {i}. v1[i]='{v1[i]}', v2[i]='{v2[i]}'.")
            return math.nan 

        dot_product += x1 * x2
        norm_v1_sq += x1**2
        norm_v2_sq += x2**2

    if norm_v1_sq == 0 or norm_v2_sq == 0: # 3. Check for zero magnitude vectors
        # If one vector is all zeros, dot product is 0. If both are all zeros, norm_v1_sq and norm_v2_sq are 0.
        # Convention: 0.0 if one is zero vector, 1.0 if both are zero vectors (perfectly similar).
        return 1.0 if norm_v1_sq == 0 and norm_v2_sq == 0 else 0.0

    magnitude_v1 = math.sqrt(norm_v1_sq)
    magnitude_v2 = math.sqrt(norm_v2_sq)
    
    # Final division, magnitudes are non-zero here
    result = dot_product / (magnitude_v1 * magnitude_v2)
    # Clip result to [-1.0, 1.0] to handle potential floating point inaccuracies
    return max(-1.0, min(1.0, result))

# Minimal __main__ for library file, or remove if not needed for testing imports.
if __name__ == "__main__":
    # np will be the imported version from arcanus_utils
    is_numpy_real = False
    try:
        import numpy as actual_numpy
        is_numpy_real = (type(np) == type(actual_numpy))
    except ImportError:
        pass # actual_numpy not imported, np is _NP_Fallback
    print(f"Arcanus LBT Modules loaded. DB Path: {DB_PATH}, Numpy used: {is_numpy_real}")
    # Example: Test log_claim with a simple dict
    # log_claim({"id": "main_test_001", "data": "Some test data from __main__"})
    # print("Test claim logged (if no errors). Check arcanus_lbt.db")
