import time
from ekm_meter.repository.ekm_api import EKMAPIRepository
from ekm_meter.service.hashing import HashingService
from ekm_meter.service.ingestion import CloudIngestionService
from ekm_meter.config.settings import settings
from ekm_meter.utils.logger import setup_logger

logger = setup_logger("EKMController")

def run_extraction_cycle():
    ekm_repo = EKMAPIRepository()
    hashing_service = HashingService()
    ingestion_service = CloudIngestionService()

    while True:
        try:
            logger.info("Starting extraction cycle")
            meter_data = ekm_repo.fetch_meter_data()
            logger.info(f"Fetched meter data for meter {settings.EKM_METER_NUMBER}")
            hashed_data = hashing_service.hash_meter_data(meter_data)
            logger.info("Hashed meter data successfully")
            ingestion_service.ingest(hashed_data)
            logger.info("Ingested hashed data to cloud successfully")
        except Exception as e:
            logger.error(f"Error during extraction cycle: {e}")
        time.sleep(settings.EXTRACTION_INTERVAL_SECONDS)

if __name__ == "__main__":
    run_extraction_cycle()