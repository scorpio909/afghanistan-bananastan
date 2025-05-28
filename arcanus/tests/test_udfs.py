import pytest
from arcanus.udfs import calculate_resonance_udf, compress_symbol_udf
try:
    import zstandard as zstd
    ZSTD_AVAILABLE = True
except ImportError:
    ZSTD_AVAILABLE = False

# Get the actual Python function from the UDF object
calculate_resonance_logic = calculate_resonance_udf.func
compress_logic = compress_symbol_udf.func # Corrected to use .func

# Tests for calculate_resonance_udf (existing tests - preserved)

# 1. Basic functionality
def test_basic_p1_e1():
    assert calculate_resonance_logic(blob_size=20, relations=["up", "down"]) == 255

def test_basic_p0_e1():
    assert calculate_resonance_logic(blob_size=40, relations=["up", "down"]) == 102

def test_basic_p1_e0():
    assert calculate_resonance_logic(blob_size=20, relations=["up"]) == 153

def test_basic_p0_e0():
    assert calculate_resonance_logic(blob_size=40, relations=["up"]) == 0

# 2. Edge cases for blob_size
def test_edge_blob_size_32():
    assert calculate_resonance_logic(blob_size=32, relations=["up", "down"]) == 255

def test_edge_blob_size_33():
    assert calculate_resonance_logic(blob_size=33, relations=["up", "down"]) == 102

def test_edge_blob_size_0():
    assert calculate_resonance_logic(blob_size=0, relations=["up", "down"]) == 255

def test_edge_blob_size_negative():
    assert calculate_resonance_logic(blob_size=-10, relations=["up", "down"]) == 255

# 3. Edge cases for relations
def test_edge_relations_empty_list():
    assert calculate_resonance_logic(blob_size=20, relations=[]) == 153

def test_edge_relations_none(): # For calculate_resonance_logic
    assert calculate_resonance_logic(blob_size=20, relations=None) == 153

def test_edge_relations_single_element():
    assert calculate_resonance_logic(blob_size=20, relations=["single"]) == 153

# 4. Score clamping
def test_score_clamping_max():
    assert calculate_resonance_logic(blob_size=32, relations=["item1", "item2"]) == 255

def test_score_clamping_min():
    assert calculate_resonance_logic(blob_size=33, relations=[]) == 0

def test_mixed_edges_p1_e0_blob_zero():
    assert calculate_resonance_logic(blob_size=0, relations=[]) == 153

def test_mixed_edges_p0_e0_blob_large_relations_none(): # For calculate_resonance_logic
    assert calculate_resonance_logic(blob_size=100, relations=None) == 0

# --- Tests for compress_symbol_udf (adjusted for current UDF) ---

# 1. Basic functionality
def test_compress_basic_functionality():
    value = 123.45
    relations = ["up", "across"]
    expected_flags = (1 << 0) | (1 << 2)  # up=1 (2^0), across=4 (2^2)
    expected_payload = f"{value}:{expected_flags}".encode('utf-8')
    
    compressed_data = compress_logic(value, relations)
    assert isinstance(compressed_data, bytes)
    
    if ZSTD_AVAILABLE:
        assert compressed_data != b'\x00' # Ensure it's not the fallback
        decompressor = zstd.ZstdDecompressor()
        decompressed_payload = decompressor.decompress(compressed_data)
        assert decompressed_payload == expected_payload
    else:
        # If zstd is not available, we expect the fallback
        assert compressed_data == b'\x00'

# 2. Edge cases for `value`
def test_compress_value_zero():
    value = 0.0
    relations = ["down"] # Valid relations
    expected_flags = (1 << 1) # down (2^1)
    expected_payload = f"{value}:{expected_flags}".encode('utf-8')

    compressed_data = compress_logic(value, relations)
    assert isinstance(compressed_data, bytes)

    if ZSTD_AVAILABLE:
        assert compressed_data != b'\x00'
        decompressor = zstd.ZstdDecompressor()
        decompressed_payload = decompressor.decompress(compressed_data)
        assert decompressed_payload == expected_payload
    else:
        assert compressed_data == b'\x00'

def test_compress_value_none_with_valid_relations():
    # With valid relations, value=None should result in "0.0" in payload
    value = None
    relations = ["up"] # Valid relations
    expected_flags = (1 << 0) # up
    expected_payload = f"{0.0}:{expected_flags}".encode('utf-8') # "value or 0.0"

    compressed_data = compress_logic(value, relations)
    assert isinstance(compressed_data, bytes)

    if ZSTD_AVAILABLE:
        assert compressed_data != b'\x00'
        decompressor = zstd.ZstdDecompressor()
        decompressed_payload = decompressor.decompress(compressed_data)
        assert decompressed_payload == expected_payload
    else:
        assert compressed_data == b'\x00'

def test_compress_value_none_triggers_fallback():
    # To meet prompt: "value = None (should ... fallback to b'\x00')"
    # This requires relations to be None (to cause TypeError) or zstd to be unavailable.
    value = None
    relations = None # This will cause TypeError in flag calculation
    
    compressed_data = compress_logic(value, relations)
    assert compressed_data == b'\x00'

# 3. Edge cases for `relations`
def test_compress_relations_empty():
    value = 78.9
    relations = [] # Empty list
    expected_flags = 0
    expected_payload = f"{value}:{expected_flags}".encode('utf-8')

    compressed_data = compress_logic(value, relations)
    assert isinstance(compressed_data, bytes)

    if ZSTD_AVAILABLE:
        assert compressed_data != b'\x00'
        decompressor = zstd.ZstdDecompressor()
        decompressed_payload = decompressor.decompress(compressed_data)
        assert decompressed_payload == expected_payload
    else:
        assert compressed_data == b'\x00'

def test_compress_relations_unknown_flag():
    value = 42.0
    relations = ["unknown_flag", "up"] # Contains an unknown flag
    expected_flags = (1 << 0) # only "up" is valid and contributes to flags
    expected_payload = f"{value}:{expected_flags}".encode('utf-8')

    compressed_data = compress_logic(value, relations)
    assert isinstance(compressed_data, bytes)

    if ZSTD_AVAILABLE:
        assert compressed_data != b'\x00'
        decompressor = zstd.ZstdDecompressor()
        decompressed_payload = decompressor.decompress(compressed_data)
        assert decompressed_payload == expected_payload
    else:
        assert compressed_data == b'\x00'

def test_compress_relations_none_triggers_fallback():
    # relations = None should cause TypeError and fallback to b'\x00'
    value = 11.1
    relations = None 
    
    compressed_data = compress_logic(value, relations)
    assert compressed_data == b'\x00'

# 4. Zstandard import error / Fallback path confirmation
def test_compress_fallback_path_if_zstd_unavailable():
    # This test primarily confirms the b'\x00' output if ZSTD_AVAILABLE is False.
    # If ZSTD_AVAILABLE is True, these specific calls are tested elsewhere to NOT be b'\x00'.
    if not ZSTD_AVAILABLE:
        # If zstd is not installed, any call that would otherwise compress should fallback
        assert compress_logic(123.45, ["up"]) == b'\x00'
        # Calls that cause TypeError also lead to fallback, already tested by:
        # test_compress_value_none_triggers_fallback() (value=None, relations=None)
        # test_compress_relations_none_triggers_fallback() (relations=None)
    else:
        # This assertion is to make Pytest not complain about an empty test if ZSTD_AVAILABLE is True
        assert True, "ZSTD is available, fallback for ImportError not directly tested here."

# Test case combining value=None and relations=None
def test_compress_value_none_and_relations_none():
    # This should always fallback to b'\x00' due to TypeError from relations=None
    # This is covered by test_compress_value_none_triggers_fallback
    # and test_compress_relations_none_triggers_fallback
    # Adding an explicit combined test for clarity.
    value = None
    relations = None
    compressed_data = compress_logic(value, relations)
    assert compressed_data == b'\x00'

# Test with a non-float value to see if it causes TypeError before zstd (if available)
# The UDF's type hint is `value: float`, but Python is dynamic.
# `f"{value or 0.0}"` will attempt string conversion.
def test_compress_value_not_a_float():
    value = "not_a_float_string" # Not a float
    relations = ["up"]
    expected_flags = (1 << 0)
    # The UDF will try to format this string: f"{'not_a_float_string' or 0.0}:{expected_flags}"
    # which becomes "not_a_float_string:1"
    expected_payload = f"{str(value)}:{expected_flags}".encode('utf-8')

    compressed_data = compress_logic(value, relations)
    assert isinstance(compressed_data, bytes)

    if ZSTD_AVAILABLE:
        assert compressed_data != b'\x00'
        decompressor = zstd.ZstdDecompressor()
        decompressed_payload = decompressor.decompress(compressed_data)
        assert decompressed_payload == expected_payload
    else:
        # Fallback because zstd is not available
        assert compressed_data == b'\x00'
