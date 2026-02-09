import pytest
from unittest.mock import patch, Mock
from ekm_meter.service.ingestion import CloudIngestionService
from ekm_meter.config.settings import settings
import requests

@pytest.fixture
def service():
    return CloudIngestionService()

@pytest.fixture
def valid_url(monkeypatch):
    monkeypatch.setattr(settings, "CLOUD_INGEST_URL", "https://cloud.example.com/ingest")
    return settings.CLOUD_INGEST_URL

@pytest.fixture
def malformed_url(monkeypatch):
    monkeypatch.setattr(settings, "CLOUD_INGEST_URL", "ht!tp://bad_url")
    return settings.CLOUD_INGEST_URL

@pytest.fixture
def unreachable_url(monkeypatch):
    monkeypatch.setattr(settings, "CLOUD_INGEST_URL", "http://unreachable.example.com")
    return settings.CLOUD_INGEST_URL

@pytest.fixture
def hashed_data():
    return "abc123hashedmeterdata"

@pytest.fixture
def empty_hashed_data():
    return ""

@pytest.fixture
def large_hashed_data():
    return "A" * 10_000_000  # 10MB string

@pytest.fixture
def special_hashed_data():
    return "特殊字符!@#$%^&*()_+🚀"

# Test Case 1: successful_ingest_with_valid_hashed_data
def test_successful_ingest_with_valid_hashed_data(service, valid_url, hashed_data):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"result": "success"}
    mock_response.raise_for_status.return_value = None
    with patch("requests.post", return_value=mock_response) as mock_post:
        result = service.ingest(hashed_data)
        mock_post.assert_called_once_with(
            valid_url,
            json={"hashed_data": hashed_data},
            timeout=10
        )
        assert result == {"result": "success"}

# Test Case 2: ingest_with_non_successful_http_status
def test_ingest_with_non_successful_http_status(service, valid_url, hashed_data):
    mock_response = Mock()
    mock_response.status_code = 400
    mock_response.raise_for_status.side_effect = requests.HTTPError("400 Bad Request")
    with patch("requests.post", return_value=mock_response):
        with pytest.raises(RuntimeError) as exc:
            service.ingest(hashed_data)
        assert "Failed to ingest data to cloud" in str(exc.value)
        assert "400 Bad Request" in str(exc.value)

# Test Case 3: ingest_with_connection_timeout
def test_ingest_with_connection_timeout(service, valid_url, hashed_data):
    with patch("requests.post", side_effect=requests.Timeout("Connection timed out")):
        with pytest.raises(RuntimeError) as exc:
            service.ingest(hashed_data)
        assert "Failed to ingest data to cloud" in str(exc.value)
        assert "Connection timed out" in str(exc.value)

# Test Case 4: ingest_with_invalid_json_response
def test_ingest_with_invalid_json_response(service, valid_url, hashed_data):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_response.json.side_effect = ValueError("No JSON object could be decoded")
    with patch("requests.post", return_value=mock_response):
        with pytest.raises(RuntimeError) as exc:
            service.ingest(hashed_data)
        assert "Failed to ingest data to cloud" in str(exc.value)
        assert "No JSON object could be decoded" in str(exc.value)

# Test Case 5: ingest_with_empty_hashed_data
def test_ingest_with_empty_hashed_data(service, valid_url, empty_hashed_data):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"result": "empty_data_accepted"}
    with patch("requests.post", return_value=mock_response):
        result = service.ingest(empty_hashed_data)
        assert result == {"result": "empty_data_accepted"}

# Test Case 6: ingest_with_malformed_url
def test_ingest_with_malformed_url(service, malformed_url, hashed_data):
    with patch("requests.post", side_effect=requests.RequestException("Invalid URL")):
        with pytest.raises(RuntimeError) as exc:
            service.ingest(hashed_data)
        assert "Failed to ingest data to cloud" in str(exc.value)
        assert "Invalid URL" in str(exc.value)

# Test Case 7: ingest_with_large_hashed_data
def test_ingest_with_large_hashed_data(service, valid_url, large_hashed_data):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"result": "large_data_accepted"}
    with patch("requests.post", return_value=mock_response):
        result = service.ingest(large_hashed_data)
        assert result == {"result": "large_data_accepted"}

# Test Case 8: ingest_with_special_characters_in_hashed_data
def test_ingest_with_special_characters_in_hashed_data(service, valid_url, special_hashed_data):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"result": "special_characters_accepted"}
    with patch("requests.post", return_value=mock_response):
        result = service.ingest(special_hashed_data)
        assert result == {"result": "special_characters_accepted"}

# Test Case 9: ingest_with_unreachable_cloud_url
def test_ingest_with_unreachable_cloud_url(service, unreachable_url, hashed_data):
    with patch("requests.post", side_effect=requests.ConnectionError("DNS failure")):
        with pytest.raises(RuntimeError) as exc:
            service.ingest(hashed_data)
        assert "Failed to ingest data to cloud" in str(exc.value)
        assert "DNS failure" in str(exc.value)

# Test Case 10: ingest_with_internal_server_error_response
def test_ingest_with_internal_server_error_response(service, valid_url, hashed_data):
    mock_response = Mock()
    mock_response.status_code = 500
    mock_response.raise_for_status.side_effect = requests.HTTPError("500 Internal Server Error")
    with patch("requests.post", return_value=mock_response):
        with pytest.raises(RuntimeError) as exc:
            service.ingest(hashed_data)
        assert "Failed to ingest data to cloud" in str(exc.value)
        assert "500 Internal Server Error" in str(exc.value)