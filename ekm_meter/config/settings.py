import os
from dotenv import load_dotenv

class Settings:
    def __init__(self, env_path: str = ".env"):
        load_dotenv(env_path)
        self.EKM_API_URL = os.getenv("EKM_API_URL")
        self.EKM_METER_NUMBER = os.getenv("EKM_METER_NUMBER")
        self.EKM_API_KEY = os.getenv("EKM_API_KEY")
        self.CLOUD_INGEST_URL = os.getenv("CLOUD_INGEST_URL")
        self.PRIVATE_KEY_PATH = os.getenv("PRIVATE_KEY_PATH")
        self.EXTRACTION_INTERVAL_SECONDS = int(os.getenv("EXTRACTION_INTERVAL_SECONDS", "60"))

        # Validation
        required = [
            ("EKM_API_URL", self.EKM_API_URL),
            ("EKM_METER_NUMBER", self.EKM_METER_NUMBER),
            ("EKM_API_KEY", self.EKM_API_KEY),
            ("CLOUD_INGEST_URL", self.CLOUD_INGEST_URL),
            ("PRIVATE_KEY_PATH", self.PRIVATE_KEY_PATH),
        ]
        missing = [name for name, value in required if not value]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

settings = Settings()