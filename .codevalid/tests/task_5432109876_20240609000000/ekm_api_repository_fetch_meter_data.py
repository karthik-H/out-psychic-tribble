import pytest
from unittest.mock import patch, MagicMock
from ekm_meter.repository.ekm_api import EKMAPIRepository
from ekm_meter.domain.models import MeterData

# Helper: sample valid meter data response
def valid_meter_data_response():
    return {
        "meter_name": "Test Meter",
        "meter_data": {"kwh": 1234.5},
        "meter_day_of_week": "Monday",
        "reading_date": "2024-06-09",
        "model": "EKM-123",
        "address": "123 Main St",
        "firmware": "v1.2.3",
        "total_watt_hour": 12345.6,
        "voltage": 120.0,
        "amps": 10.5,
        "total_power_watts": 1260.0,
        "ct_ratio": 200.0,
        "frequency_hz": 60.0,
    }

@pytest.fixture
def repo():
    with patch("ekm_meter.repository.ekm_api.settings") as mock_settings:
        mock_settings.EKM_API_URL = "https://api.ekm.com"
        mock_settings.EKM_METER_NUMBER = "valid_meter_id"
        mock_settings.EKM_API_KEY = "valid_api_key"
        yield EKMAPIRepository()

# Test Case 1: Successful fetch with valid meter ID and API credentials
def test_successful_fetch_with_valid_meter_id_and_api_credentials(repo):
    response_data = valid_meter_data_response()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = response_data
    mock_response.raise_for_status.return_value = None

    with patch("requests.get", return_value=mock_response) as mock_get:
        result = repo.fetch_meter_data()
        assert isinstance(result, MeterData)
        assert result.meter_name == response_data["meter_name"]
        assert result.meter_data == response_data["meter_data"]
        assert result.voltage == response_data["voltage"]
        mock_get.assert_called_once()

# Test Case 2: Fetch with invalid meter ID (404)
def test_fetch_with_invalid_meter_id_raises_runtimeerror(repo):
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")

    with patch("requests.get", return_value=mock_response):
        with pytest.raises(RuntimeError) as exc:
            repo.fetch_meter_data()
        assert "Failed to fetch meter data" in str(exc.value)

# Test Case 3: Fetch with invalid API credentials (401)
def test_fetch_with_invalid_api_credentials_raises_runtimeerror(repo):
    mock_response = MagicMock()
    mock_response.status_code = 401
    mock_response.raise_for_status.side_effect = requests.HTTPError("401 Unauthorized")

    with patch("requests.get", return_value=mock_response):
        with pytest.raises(RuntimeError) as exc:
            repo.fetch_meter_data()
        assert "Failed to fetch meter data" in str(exc.value)

# Test Case 4: API request timeout
def test_api_request_timeout_raises_runtimeerror(repo):
    with patch("requests.get", side_effect=requests.Timeout("Timeout")):
        with pytest.raises(RuntimeError) as exc:
            repo.fetch_meter_data()
        assert "Failed to fetch meter data" in str(exc.value)

# Test Case 5: Malformed JSON response from API
def test_malformed_json_response_raises_runtimeerror(repo):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_response.json.side_effect = ValueError("Malformed JSON")

    with patch("requests.get", return_value=mock_response):
        with pytest.raises(RuntimeError) as exc:
            repo.fetch_meter_data()
        assert "Failed to fetch meter data" in str(exc.value)

# Test Case 6: Missing required meter fields in API response
def test_missing_required_meter_fields_raises_runtimeerror(repo):
    incomplete_data = {
        "meter_name": "Test Meter",
        # missing meter_data, meter_day_of_week, etc.
    }
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = incomplete_data

    with patch("requests.get", return_value=mock_response):
        with pytest.raises(RuntimeError) as exc:
            repo.fetch_meter_data()
        assert "Failed to fetch meter data" in str(exc.value)

# Test Case 7: API returns error HTTP status (500)
def test_api_returns_error_http_status_raises_runtimeerror(repo):
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.raise_for_status.side_effect = requests.HTTPError("500 Internal Server Error")

    with patch("requests.get", return_value=mock_response):
        with pytest.raises(RuntimeError) as exc:
            repo.fetch_meter_data()
        assert "Failed to fetch meter data" in str(exc.value)

# Test Case 8: Empty response body from API
def test_empty_response_body_raises_runtimeerror(repo):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_response.json.side_effect = ValueError("No JSON object could be decoded")

    with patch("requests.get", return_value=mock_response):
        with pytest.raises(RuntimeError) as exc:
            repo.fetch_meter_data()
        assert "Failed to fetch meter data" in str(exc.value)

# Test Case 9: Multiple successive fetch calls
def test_multiple_successive_fetch_calls(repo):
    response_data = valid_meter_data_response()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = response_data
    mock_response.raise_for_status.return_value = None

    with patch("requests.get", return_value=mock_response) as mock_get:
        for _ in range(5):
            result = repo.fetch_meter_data()
            assert isinstance(result, MeterData)
            assert result.meter_name == response_data["meter_name"]
        assert mock_get.call_count == 5

# Test Case 10: Meter ID with special characters
def test_meter_id_with_special_characters(repo):
    special_meter_id = "meter_äöü!@#"
    with patch("ekm_meter.repository.ekm_api.settings") as mock_settings:
        mock_settings.EKM_API_URL = "https://api.ekm.com"
        mock_settings.EKM_METER_NUMBER = special_meter_id
        mock_settings.EKM_API_KEY = "valid_api_key"
        repo_special = EKMAPIRepository()

        response_data = valid_meter_data_response()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = response_data
        mock_response.raise_for_status.return_value = None

        with patch("requests.get", return_value=mock_response) as mock_get:
            result = repo_special.fetch_meter_data()
            assert isinstance(result, MeterData)
            assert result.meter_name == response_data["meter_name"]
            mock_get.assert_called_once()