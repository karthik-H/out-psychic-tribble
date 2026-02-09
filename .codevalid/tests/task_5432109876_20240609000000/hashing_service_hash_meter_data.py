import pytest
import json
import string
import random

from ekm_meter.service.hashing import hash_meter_data

from cryptography.hazmat.primitives.asymmetric import rsa, ec
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# Helper to generate RSA private key
def generate_rsa_private_key():
    return rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )

# Helper to generate EC private key (for negative test)
def generate_ec_private_key():
    return ec.generate_private_key(
        ec.SECP384R1(),
        backend=default_backend()
    )

# Helper to generate large meter data
def generate_large_meter_data(size=5000):
    return {f"key_{i}": f"value_{i}" for i in range(size)}

# Helper to check if string is hex
def is_hex(s):
    try:
        int(s, 16)
        return True
    except ValueError:
        return False

# Test Case 1: test_hash_with_valid_meter_data
def test_hash_with_valid_meter_data():
    meter_data = {"meter_id": "12345", "reading": 678.9}
    private_key = generate_rsa_private_key()
    signature = hash_meter_data(meter_data, private_key)
    assert isinstance(signature, str)
    assert len(signature) > 0
    assert is_hex(signature)

# Test Case 2: test_hash_with_empty_meter_data
def test_hash_with_empty_meter_data():
    meter_data = {}
    private_key = generate_rsa_private_key()
    signature = hash_meter_data(meter_data, private_key)
    assert isinstance(signature, str)
    assert len(signature) > 0
    assert is_hex(signature)

# Test Case 3: test_hash_with_unicode_characters
def test_hash_with_unicode_characters():
    meter_data = {"meter_id": "μ123", "reading": "值", "location": "東京"}
    private_key = generate_rsa_private_key()
    signature = hash_meter_data(meter_data, private_key)
    assert isinstance(signature, str)
    assert len(signature) > 0
    assert is_hex(signature)

# Test Case 4: test_hash_with_large_meter_data
def test_hash_with_large_meter_data():
    meter_data = generate_large_meter_data()
    private_key = generate_rsa_private_key()
    signature = hash_meter_data(meter_data, private_key)
    assert isinstance(signature, str)
    assert len(signature) > 0
    assert is_hex(signature)

# Test Case 5: test_invalid_private_key_type
@pytest.mark.parametrize("invalid_key", [
    "not_a_key",
    12345,
    b"fake_bytes",
    [],
    {}
])
def test_invalid_private_key_type(invalid_key):
    meter_data = {"meter_id": "12345", "reading": 678.9}
    with pytest.raises((TypeError, ValueError)):
        hash_meter_data(meter_data, invalid_key)

# Test Case 6: test_missing_private_key
def test_missing_private_key():
    meter_data = {"meter_id": "12345", "reading": 678.9}
    # None as private key
    with pytest.raises((TypeError, ValueError)):
        hash_meter_data(meter_data, None)
    # Omit private key argument (should raise TypeError)
    with pytest.raises(TypeError):
        hash_meter_data(meter_data)

# Test Case 7: test_unserializable_meter_data
def test_unserializable_meter_data():
    # Unserializable: function
    meter_data = {"meter_id": "12345", "callback": lambda x: x}
    private_key = generate_rsa_private_key()
    with pytest.raises((TypeError, json.JSONDecodeError, Exception)):
        hash_meter_data(meter_data, private_key)
    # Unserializable: open file handle
    meter_data = {"meter_id": "12345"}
    with open(__file__, "r") as f:
        meter_data["file"] = f
        with pytest.raises((TypeError, Exception)):
            hash_meter_data(meter_data, private_key)

# Test Case 8: test_hash_with_non_dict_meter_data
@pytest.mark.parametrize("bad_data", [
    ["meter_id", "reading"],
    "meter_id:12345",
    12345,
    67.89,
    None
])
def test_hash_with_non_dict_meter_data(bad_data):
    private_key = generate_rsa_private_key()
    with pytest.raises((TypeError, ValueError)):
        hash_meter_data(bad_data, private_key)

# Test Case 9: test_signature_hex_encoding
def test_signature_hex_encoding():
    meter_data = {"meter_id": "12345", "reading": 678.9}
    private_key = generate_rsa_private_key()
    signature = hash_meter_data(meter_data, private_key)
    assert isinstance(signature, str)
    assert is_hex(signature)
    # Check length: hex string should be twice the length of raw signature bytes
    # To get raw signature length, sign hash manually
    import hashlib
    from cryptography.hazmat.primitives.asymmetric import padding
    from cryptography.hazmat.primitives import hashes
    json_data = json.dumps(meter_data, sort_keys=True, ensure_ascii=False)
    hash_bytes = hashlib.sha256(json_data.encode("utf-8")).digest()
    raw_signature = private_key.sign(
        hash_bytes,
        padding.PKCS1v15(),
        hashes.SHA256()
    )
    assert len(signature) == len(raw_signature) * 2

# Test Case 10: test_private_key_wrong_algorithm
def test_private_key_wrong_algorithm():
    meter_data = {"meter_id": "12345", "reading": 678.9}
    ec_key = generate_ec_private_key()
    with pytest.raises((NotImplementedError, TypeError, ValueError, Exception)):
        hash_meter_data(meter_data, ec_key)