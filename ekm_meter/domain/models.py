from dataclasses import dataclass
from typing import Any, Dict

@dataclass
class MeterData:
    meter_name: str
    meter_data: Dict[str, Any]
    meter_day_of_week: str
    reading_date: str
    model: str
    address: str
    firmware: str
    total_watt_hour: float
    voltage: float
    amps: float
    total_power_watts: float
    ct_ratio: float
    frequency_hz: float