import requests
from ekm_meter.config.settings import settings

class CloudIngestionService:
    def __init__(self):
        self.ingest_url = settings.CLOUD_INGEST_URL

    def ingest(self, hashed_data: str):
        try:
            response = requests.post(
                self.ingest_url,
                json={"hashed_data": hashed_data},
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            raise RuntimeError(f"Failed to ingest data to cloud: {e}")