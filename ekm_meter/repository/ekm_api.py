import requests
from ekm_meter.config.settings import settings
from ekm_meter.domain.models import MeterData

class EKMAPIRepository:
    def __init__(self):
        self.api_url = settings.EKM_API_URL
        self.meter_number = settings.EKM_METER_NUMBER
        self.api_key = settings.EKM_API_KEY

    def fetch_meter_data(self) -> MeterData:
        url = f"{self.api_url}/meters/{self.meter_number}/"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            # Extract required fields
            return MeterData(
                meter_name=data.get("meter_name"),
                meter_data=data.get("meter_data"),
                meter_day_of_week=data.get("meter_day_of_week"),
                reading_date=data.get("reading_date"),
                model=data.get("model"),
                address=data.get("address"),
                firmware=data.get("firmware"),
                total_watt_hour=float(data.get("total_watt_hour", 0)),
                voltage=float(data.get("voltage", 0)),
                amps=float(data.get("amps", 0)),
                total_power_watts=float(data.get("total_power_watts", 0)),
                ct_ratio=float(data.get("ct_ratio", 0)),
                frequency_hz=float(data.get("frequency_hz", 0)),
            )
        except Exception as e:
            raise RuntimeError(f"Failed to fetch meter data: {e}")