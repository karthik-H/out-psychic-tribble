import json
import hashlib
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from ekm_meter.config.settings import settings
from ekm_meter.domain.models import MeterData

class HashingService:
    def __init__(self):
        self.private_key_path = settings.PRIVATE_KEY_PATH
        self.private_key = self._load_private_key()

    def _load_private_key(self):
        with open(self.private_key_path, "rb") as key_file:
            return serialization.load_pem_private_key(
                key_file.read(),
                password=None,
            )

    def hash_meter_data(self, meter_data: MeterData) -> str:
        # Serialize meter data to JSON string
        data_str = json.dumps(meter_data.__dict__, sort_keys=True)
        # Hash using SHA-256
        sha256_hash = hashlib.sha256(data_str.encode("utf-8")).digest()
        # Sign hash with private key
        signature = self.private_key.sign(
            sha256_hash,
            padding.PKCS1v15(),
            hashes.SHA256()
        )
        # Return hex-encoded signature
        return signature.hex()