import pytest
from unittest.mock import patch, MagicMock, call
import time

# Import the function under test
from ekm_meter.controller.main import run_extraction_cycle

# Patch settings for interval and meter number
@pytest.fixture(autouse=True)
def patch_settings(monkeypatch):
    class DummySettings:
        EXTRACTION_INTERVAL_SECONDS = 60
        EKM_METER_NUMBER = "12345"
        PRIVATE_KEY_PATH = "/tmp/private.pem"
        EKM_API_URL = "https://api.ekm.com"
        EKM_API_KEY = "dummy"
        CLOUD_INGEST_URL = "https://cloud.ingest.com"
    monkeypatch.setattr("ekm_meter.config.settings.settings", DummySettings())
    yield

@pytest.fixture
def meter_data():
    return MagicMock(
        meter_name="Meter1",
        meter_data={"kwh": 100},
        meter_day_of_week="Monday",
        reading_date="2024-06-09",
        model="EKM",
        address="123 Main St",
        firmware="v1.0",
        total_watt_hour=1000.0,
        voltage=120.0,
        amps=10.0,
        total_power_watts=1200.0,
        ct_ratio=1.0,
        frequency_hz=60.0,
        __dict__={
            "meter_name": "Meter1",
            "meter_data": {"kwh": 100},
            "meter_day_of_week": "Monday",
            "reading_date": "2024-06-09",
            "model": "EKM",
            "address": "123 Main St",
            "firmware": "v1.0",
            "total_watt_hour": 1000.0,
            "voltage": 120.0,
            "amps": 10.0,
            "total_power_watts": 1200.0,
            "ct_ratio": 1.0,
            "frequency_hz": 60.0,
        }
    )

def run_single_cycle():
    # Helper to run only one cycle of the infinite loop
    with patch("ekm_meter.controller.main.EKMAPIRepository") as MockRepo, \
         patch("ekm_meter.controller.main.HashingService") as MockHash, \
         patch("ekm_meter.controller.main.CloudIngestionService") as MockIngest, \
         patch("ekm_meter.controller.main.logger") as mock_logger, \
         patch("ekm_meter.controller.main.settings") as mock_settings, \
         patch("ekm_meter.controller.main.time.sleep") as mock_sleep:
        # Set up mocks
        repo = MockRepo.return_value
        hash_service = MockHash.return_value
        ingest_service = MockIngest.return_value
        mock_settings.EXTRACTION_INTERVAL_SECONDS = 60
        mock_settings.EKM_METER_NUMBER = "12345"
        # Patch the while loop to break after one iteration
        original_while = run_extraction_cycle.__code__
        def fake_while(*args, **kwargs):
            # Only run one iteration
            for _ in range(1):
                yield
        # Run the function, but forcibly break after one cycle
        # Instead, we patch time.sleep to raise StopIteration to break the loop
        mock_sleep.side_effect = StopIteration
        try:
            run_extraction_cycle()
        except StopIteration:
            pass
        return repo, hash_service, ingest_service, mock_logger, mock_settings, mock_sleep

@pytest.mark.usefixtures("patch_settings")
def test_successful_extraction_cycle(meter_data):
    with patch("ekm_meter.controller.main.EKMAPIRepository") as MockRepo, \
         patch("ekm_meter.controller.main.HashingService") as MockHash, \
         patch("ekm_meter.controller.main.CloudIngestionService") as MockIngest, \
         patch("ekm_meter.controller.main.logger") as mock_logger, \
         patch("ekm_meter.controller.main.time.sleep") as mock_sleep, \
         patch("ekm_meter.controller.main.settings") as mock_settings:
        repo = MockRepo.return_value
        hash_service = MockHash.return_value
        ingest_service = MockIngest.return_value
        repo.fetch_meter_data.return_value = meter_data
        hash_service.hash_meter_data.return_value = "hashed"
        ingest_service.ingest.return_value = {"status": "ok"}
        mock_settings.EXTRACTION_INTERVAL_SECONDS = 60
        mock_settings.EKM_METER_NUMBER = "12345"
        mock_sleep.side_effect = StopIteration
        try:
            run_extraction_cycle()
        except StopIteration:
            pass
        repo.fetch_meter_data.assert_called_once()
        hash_service.hash_meter_data.assert_called_once_with(meter_data)
        ingest_service.ingest.assert_called_once_with("hashed")
        assert mock_logger.info.call_count >= 3
        mock_sleep.assert_called_once_with(60)

@pytest.mark.usefixtures("patch_settings")
def test_ekm_api_failure():
    with patch("ekm_meter.controller.main.EKMAPIRepository") as MockRepo, \
         patch("ekm_meter.controller.main.HashingService") as MockHash, \
         patch("ekm_meter.controller.main.CloudIngestionService") as MockIngest, \
         patch("ekm_meter.controller.main.logger") as mock_logger, \
         patch("ekm_meter.controller.main.time.sleep") as mock_sleep, \
         patch("ekm_meter.controller.main.settings") as mock_settings:
        repo = MockRepo.return_value
        hash_service = MockHash.return_value
        ingest_service = MockIngest.return_value
        repo.fetch_meter_data.side_effect = Exception("EKM API failure")
        mock_settings.EXTRACTION_INTERVAL_SECONDS = 60
        mock_sleep.side_effect = StopIteration
        try:
            run_extraction_cycle()
        except StopIteration:
            pass
        repo.fetch_meter_data.assert_called_once()
        hash_service.hash_meter_data.assert_not_called()
        ingest_service.ingest.assert_not_called()
        mock_logger.error.assert_called()
        mock_sleep.assert_called_once_with(60)

@pytest.mark.usefixtures("patch_settings")
def test_hashing_failure(meter_data):
    with patch("ekm_meter.controller.main.EKMAPIRepository") as MockRepo, \
         patch("ekm_meter.controller.main.HashingService") as MockHash, \
         patch("ekm_meter.controller.main.CloudIngestionService") as MockIngest, \
         patch("ekm_meter.controller.main.logger") as mock_logger, \
         patch("ekm_meter.controller.main.time.sleep") as mock_sleep, \
         patch("ekm_meter.controller.main.settings") as mock_settings:
        repo = MockRepo.return_value
        hash_service = MockHash.return_value
        ingest_service = MockIngest.return_value
        repo.fetch_meter_data.return_value = meter_data
        hash_service.hash_meter_data.side_effect = Exception("Hashing failure")
        mock_settings.EXTRACTION_INTERVAL_SECONDS = 60
        mock_sleep.side_effect = StopIteration
        try:
            run_extraction_cycle()
        except StopIteration:
            pass
        repo.fetch_meter_data.assert_called_once()
        hash_service.hash_meter_data.assert_called_once_with(meter_data)
        ingest_service.ingest.assert_not_called()
        mock_logger.error.assert_called()
        mock_sleep.assert_called_once_with(60)

@pytest.mark.usefixtures("patch_settings")
def test_ingestion_failure(meter_data):
    with patch("ekm_meter.controller.main.EKMAPIRepository") as MockRepo, \
         patch("ekm_meter.controller.main.HashingService") as MockHash, \
         patch("ekm_meter.controller.main.CloudIngestionService") as MockIngest, \
         patch("ekm_meter.controller.main.logger") as mock_logger, \
         patch("ekm_meter.controller.main.time.sleep") as mock_sleep, \
         patch("ekm_meter.controller.main.settings") as mock_settings:
        repo = MockRepo.return_value
        hash_service = MockHash.return_value
        ingest_service = MockIngest.return_value
        repo.fetch_meter_data.return_value = meter_data
        hash_service.hash_meter_data.return_value = "hashed"
        ingest_service.ingest.side_effect = Exception("Ingestion failure")
        mock_settings.EXTRACTION_INTERVAL_SECONDS = 60
        mock_sleep.side_effect = StopIteration
        try:
            run_extraction_cycle()
        except StopIteration:
            pass
        repo.fetch_meter_data.assert_called_once()
        hash_service.hash_meter_data.assert_called_once_with(meter_data)
        ingest_service.ingest.assert_called_once_with("hashed")
        mock_logger.error.assert_called()
        mock_sleep.assert_called_once_with(60)

@pytest.mark.usefixtures("patch_settings")
def test_empty_meter_data():
    with patch("ekm_meter.controller.main.EKMAPIRepository") as MockRepo, \
         patch("ekm_meter.controller.main.HashingService") as MockHash, \
         patch("ekm_meter.controller.main.CloudIngestionService") as MockIngest, \
         patch("ekm_meter.controller.main.logger") as mock_logger, \
         patch("ekm_meter.controller.main.time.sleep") as mock_sleep, \
         patch("ekm_meter.controller.main.settings") as mock_settings:
        repo = MockRepo.return_value
        hash_service = MockHash.return_value
        ingest_service = MockIngest.return_value
        empty_data = MagicMock(__dict__={})
        repo.fetch_meter_data.return_value = empty_data
        hash_service.hash_meter_data.return_value = "hashed_empty"
        ingest_service.ingest.return_value = {"status": "ok"}
        mock_settings.EXTRACTION_INTERVAL_SECONDS = 60
        mock_sleep.side_effect = StopIteration
        try:
            run_extraction_cycle()
        except StopIteration:
            pass
        repo.fetch_meter_data.assert_called_once()
        hash_service.hash_meter_data.assert_called_once_with(empty_data)
        ingest_service.ingest.assert_called_once_with("hashed_empty")
        mock_logger.info.assert_any_call("Starting extraction cycle")
        mock_sleep.assert_called_once_with(60)

@pytest.mark.usefixtures("patch_settings")
def test_large_meter_data():
    with patch("ekm_meter.controller.main.EKMAPIRepository") as MockRepo, \
         patch("ekm_meter.controller.main.HashingService") as MockHash, \
         patch("ekm_meter.controller.main.CloudIngestionService") as MockIngest, \
         patch("ekm_meter.controller.main.logger") as mock_logger, \
         patch("ekm_meter.controller.main.time.sleep") as mock_sleep, \
         patch("ekm_meter.controller.main.settings") as mock_settings:
        repo = MockRepo.return_value
        hash_service = MockHash.return_value
        ingest_service = MockIngest.return_value
        large_data = MagicMock(__dict__={"meter_data": ["x"] * 1000000})
        repo.fetch_meter_data.return_value = large_data
        hash_service.hash_meter_data.return_value = "hashed_large"
        ingest_service.ingest.return_value = {"status": "ok"}
        mock_settings.EXTRACTION_INTERVAL_SECONDS = 60
        mock_sleep.side_effect = StopIteration
        try:
            run_extraction_cycle()
        except StopIteration:
            pass
        repo.fetch_meter_data.assert_called_once()
        hash_service.hash_meter_data.assert_called_once_with(large_data)
        ingest_service.ingest.assert_called_once_with("hashed_large")
        mock_logger.info.assert_any_call("Starting extraction cycle")
        mock_sleep.assert_called_once_with(60)

@pytest.mark.usefixtures("patch_settings")
def test_logging_failure(meter_data):
    with patch("ekm_meter.controller.main.EKMAPIRepository") as MockRepo, \
         patch("ekm_meter.controller.main.HashingService") as MockHash, \
         patch("ekm_meter.controller.main.CloudIngestionService") as MockIngest, \
         patch("ekm_meter.controller.main.logger") as mock_logger, \
         patch("ekm_meter.controller.main.time.sleep") as mock_sleep, \
         patch("ekm_meter.controller.main.settings") as mock_settings:
        repo = MockRepo.return_value
        hash_service = MockHash.return_value
        ingest_service = MockIngest.return_value
        repo.fetch_meter_data.return_value = meter_data
        hash_service.hash_meter_data.return_value = "hashed"
        ingest_service.ingest.return_value = {"status": "ok"}
        mock_logger.info.side_effect = [Exception("Logging failure"), None, None, None]
        mock_settings.EXTRACTION_INTERVAL_SECONDS = 60
        mock_sleep.side_effect = StopIteration
        try:
            run_extraction_cycle()
        except StopIteration:
            pass
        repo.fetch_meter_data.assert_called_once()
        hash_service.hash_meter_data.assert_called_once_with(meter_data)
        ingest_service.ingest.assert_called_once_with("hashed")
        # Logging failures should not halt the cycle
        mock_sleep.assert_called_once_with(60)

@pytest.mark.usefixtures("patch_settings")
def test_short_sleep_interval(meter_data):
    with patch("ekm_meter.controller.main.EKMAPIRepository") as MockRepo, \
         patch("ekm_meter.controller.main.HashingService") as MockHash, \
         patch("ekm_meter.controller.main.CloudIngestionService") as MockIngest, \
         patch("ekm_meter.controller.main.logger") as mock_logger, \
         patch("ekm_meter.controller.main.time.sleep") as mock_sleep, \
         patch("ekm_meter.controller.main.settings") as mock_settings:
        repo = MockRepo.return_value
        hash_service = MockHash.return_value
        ingest_service = MockIngest.return_value
        repo.fetch_meter_data.return_value = meter_data
        hash_service.hash_meter_data.return_value = "hashed"
        ingest_service.ingest.return_value = {"status": "ok"}
        mock_settings.EXTRACTION_INTERVAL_SECONDS = 1
        mock_sleep.side_effect = StopIteration
        try:
            run_extraction_cycle()
        except StopIteration:
            pass
        mock_sleep.assert_called_once_with(1)

@pytest.mark.usefixtures("patch_settings")
def test_long_sleep_interval(meter_data):
    with patch("ekm_meter.controller.main.EKMAPIRepository") as MockRepo, \
         patch("ekm_meter.controller.main.HashingService") as MockHash, \
         patch("ekm_meter.controller.main.CloudIngestionService") as MockIngest, \
         patch("ekm_meter.controller.main.logger") as mock_logger, \
         patch("ekm_meter.controller.main.time.sleep") as mock_sleep, \
         patch("ekm_meter.controller.main.settings") as mock_settings:
        repo = MockRepo.return_value
        hash_service = MockHash.return_value
        ingest_service = MockIngest.return_value
        repo.fetch_meter_data.return_value = meter_data
        hash_service.hash_meter_data.return_value = "hashed"
        ingest_service.ingest.return_value = {"status": "ok"}
        mock_settings.EXTRACTION_INTERVAL_SECONDS = 600
        mock_sleep.side_effect = StopIteration
        try:
            run_extraction_cycle()
        except StopIteration:
            pass
        mock_sleep.assert_called_once_with(600)

@pytest.mark.usefixtures("patch_settings")
def test_service_initialization_failure():
    with patch("ekm_meter.controller.main.EKMAPIRepository", side_effect=Exception("Repo init fail")) as MockRepo, \
         patch("ekm_meter.controller.main.HashingService") as MockHash, \
         patch("ekm_meter.controller.main.CloudIngestionService") as MockIngest, \
         patch("ekm_meter.controller.main.logger") as mock_logger:
        try:
            run_extraction_cycle()
        except Exception as e:
            assert "Repo init fail" in str(e)
        mock_logger.error.assert_called()