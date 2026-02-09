import unittest
from ekm_meter.domain.models import MeterData
from ekm_meter.service.hashing import HashingService

class TestHashingService(unittest.TestCase):
    def setUp(self):
        self.hashing_service = HashingService()
        self.meter_data = MeterData(
            meter_name="TestMeter",
            meter_data={"test": 123},
            meter_day_of_week="Monday",
            reading_date="2026-02-09",
            model="Pulse v.4",
            address="123 Main St",
            firmware="1.0.0",
            total_watt_hour=1000.0,
            voltage=120.0,
            amps=10.0,
            total_power_watts=1200.0,
            ct_ratio=1.0,
            frequency_hz=60.0
        )

    def test_hash_meter_data(self):
        hashed = self.hashing_service.hash_meter_data(self.meter_data)
        self.assertIsInstance(hashed, str)
        self.assertTrue(len(hashed) > 0)

if __name__ == "__main__":
    unittest.main()