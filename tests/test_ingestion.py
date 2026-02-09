import unittest
from unittest.mock import patch
from ekm_meter.service.ingestion import CloudIngestionService

class TestCloudIngestionService(unittest.TestCase):
    @patch("ekm_meter.service.ingestion.requests.post")
    def test_ingest_success(self, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"result": "success"}
        ingestion_service = CloudIngestionService()
        result = ingestion_service.ingest("test_hash")
        self.assertEqual(result, {"result": "success"})

    @patch("ekm_meter.service.ingestion.requests.post")
    def test_ingest_failure(self, mock_post):
        mock_post.side_effect = Exception("Network error")
        ingestion_service = CloudIngestionService()
        with self.assertRaises(RuntimeError):
            ingestion_service.ingest("test_hash")

if __name__ == "__main__":
    unittest.main()