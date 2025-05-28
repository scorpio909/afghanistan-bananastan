# arcanus/genesis_loader_final_arcanus.py
import asyncio
import json
import time
# Not importing 'logging' as aiologger is primary (Modification 3)

# Primary logger is aiologger
import aiologger
# Modification 4: Use aiologger.basicConfig (or other sync setup) and get root logger.
# aiologger.basicConfig is often called once globally.
# Here, we ensure it's configured before logger retrieval if not already.
# For a library file, direct basicConfig call might be too broad.
# Assuming basicConfig is called by an application entry point or here for standalone.
if not aiologger.logger.handlers: # Crude check if already configured
    aiologger.basicConfig(level=aiologger.levels.INFO) 
logger = aiologger.logger # Get root logger

# Remove placeholder LBTModules class and instance (Step 2)
# Placeholder class LBTModules removed.
# lbt_modules = LBTModules() line removed.

# Add correct import for lbt_modules (Step 3)
from . import lbt_modules # This imports the actual lbt_modules.py
from .arcanus_utils import np # np will be actual numpy or the _NP_Fallback from utils

# Calls to lbt_modules.log_claim(...) will now use the imported module.
# These calls are already using 'await' correctly.


async def validate_inference_output(output_data, threshold=0.5):
    # Modification 5: Ensure compatibility if output_data is already a list
    data_for_cos_dist = output_data.tolist() if hasattr(output_data, 'tolist') else output_data
    
    if not data_for_cos_dist: return 0.0
    # np_impl.mean replaced by np.mean (imported from arcanus_utils)
    score = np.mean(data_for_cos_dist) 
    
    # Modification 2: Ensure await for log_claim
    await lbt_modules.log_claim({"validation_score": score, "threshold": threshold, "type": str(type(output_data))})
    return score

async def assign_inference_trust_score(inference_id, output_data):
    # Modification 5: Ensure compatibility if output_data is already a list
    processed_data = output_data.tolist() if hasattr(output_data, 'tolist') else output_data
    
    score = 0.0
    if processed_data and len(processed_data) > 0: # ensure processed_data is not empty for division
        # Using np.sum and len for mean calculation (imported from arcanus_utils)
        score = np.sum(processed_data) / len(processed_data) 
    
    # Modification 2: Ensure await for log_claim
    await lbt_modules.log_claim({"inference_id": inference_id, "trust_score": score, "data_sample": processed_data[:5]})
    return score

async def post_claim_batch(claims_batch):
    entropy_claims = []
    for c_idx, c in enumerate(claims_batch):
        # Modification 6: Use c.get('res', 0) to safely access the resonance score. Default to 0.0 float.
        resonance_score = c.get('res', 0.0) 
        
        # Ensure np.random_uniform exists (imported from arcanus_utils)
        random_factor = 1.0
        if hasattr(np, 'random_uniform') and callable(np.random_uniform):
            random_factor = np.random_uniform(0.5, 1.5)
        else: # Should not happen if using _NP_Fallback which defines random_uniform
            import random
            random_factor = random.uniform(0.5, 1.5)

        adjusted_score = resonance_score * random_factor
        
        processed_claim = {
            "id": c.get("id", f"claim_idx_{c_idx}"), # Default ID if not present
            "resonance": resonance_score,
            "adjusted_score": adjusted_score
        }
        entropy_claims.append(processed_claim)
        # Modification 2: Ensure await for log_claim
        await lbt_modules.log_claim({"posted_claim_id": processed_claim["id"], "initial_resonance": resonance_score})
    
    await logger.info(f"Processed batch of {len(entropy_claims)} claims for entropy calculation.")
    return entropy_claims

async def main_logic():
    await logger.info("Starting genesis loader process...")
    
    # Determine if np.array is callable (it is for numpy, and for our _NP_Fallback)
    # The _NP_Fallback from arcanus_utils provides 'array'.
    np_array_callable = hasattr(np, 'array') and callable(np.array)

    example_data1 = [0.1, 0.2, 0.7, 0.9]
    # Use imported np.array
    example_output_np_like = np.array(example_data1) if np_array_callable else list(example_data1)
    
    example_data2 = [0.3, 0.4, 0.6, 0.8]
    example_output_list = example_data2 # Already a list

    validation_score_1 = await validate_inference_output(example_output_np_like)
    await logger.info(f"Validation score (numpy-like): {validation_score_1}")
    
    validation_score_2 = await validate_inference_output(example_output_list)
    await logger.info(f"Validation score (list): {validation_score_2}")

    trust_score_1 = await assign_inference_trust_score("inf_001", example_output_np_like)
    await logger.info(f"Trust score (numpy-like): {trust_score_1}")

    trust_score_2 = await assign_inference_trust_score("inf_002", example_output_list)
    await logger.info(f"Trust score (list): {trust_score_2}")

    claims = [
        {"id": "claim_A", "source": "X", "res": 120.5},
        {"id": "claim_B", "source": "Y"}, # 'res' is missing
        {"id": "claim_C", "source": "Z", "res": 90.7},
    ]
    processed_claims = await post_claim_batch(claims)
    await logger.info(f"Posted claims: {json.dumps(processed_claims, indent=2)}")
    
    await logger.info("Genesis loader process finished.")

if __name__ == "__main__":
    # Standard asyncio loop management.
    # aiologger.basicConfig() was called at the top level of the script.
    try:
        asyncio.run(main_logic())
    except RuntimeError as e:
        # Handle cases where asyncio.run() might fail (e.g. Jupyter, nested loops)
        if "Cannot run an event loop while another loop is running" in str(e) or \
           "Event loop is closed" in str(e):
            await logger.warning(f"RuntimeError with event loop: {e}. Attempting to run with new/existing loop.")
            loop = asyncio.get_event_loop_policy().get_event_loop()
            if loop.is_closed():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            if not loop.is_running():
                loop.run_until_complete(main_logic())
            else:
                # If loop is already running, can't use run_until_complete.
                # This scenario is complex; typically requires nest_asyncio or different structuring.
                # For this script, we'll log and perhaps it completes if tasks were scheduled.
                async def _log_already_running(): # ensure log is awaited
                    await logger.error("Event loop is already running. main_logic might not complete as expected in this context.")
                asyncio.ensure_future(_log_already_running())
        else:
            # Re-raise other RuntimeErrors or any other exceptions
            async def _log_other_error(err_val):
                 await logger.error(f"Unhandled error: {err_val}")
            asyncio.run(_log_other_error(e)) # log it if possible
            raise
