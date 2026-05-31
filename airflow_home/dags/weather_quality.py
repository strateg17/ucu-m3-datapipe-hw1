from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def validate_required_fields(record: dict[str, Any], required_fields: list[str]) -> None:
    missing = [field for field in required_fields if field not in record]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")


def validate_humidity(record: dict[str, Any]) -> None:
    humidity = float(record["humidity"])
    if not (0 <= humidity <= 100):
        raise ValueError(f"Humidity out of range: {humidity}")


def validate_cloudiness(record: dict[str, Any]) -> None:
    cloudiness = float(record["cloudiness"])
    if not (0 <= cloudiness <= 100):
        raise ValueError(f"Cloudiness out of range: {cloudiness}")


def validate_wind_speed(record: dict[str, Any]) -> None:
    wind_speed = float(record["wind_speed"])
    if wind_speed < 0:
        raise ValueError(f"Wind speed cannot be negative: {wind_speed}")


def validate_temperature(record: dict[str, Any]) -> None:
    temp = float(record["temp"])
    if temp < -100 or temp > 100:
        raise ValueError(f"Temperature looks unrealistic: {temp}")


def validate_timestamp_not_future(record: dict[str, Any], logical_date) -> None:
    ts = int(record["timestamp"])
    ts_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    logical_dt = logical_date if logical_date.tzinfo else logical_date.replace(tzinfo=timezone.utc)

    # допускаємо невеликий люфт, але timestamp не має бути явно після logical date
    if ts_dt > logical_dt:
        raise ValueError(
            f"Timestamp {ts_dt.isoformat()} is later than logical_date {logical_dt.isoformat()}"
        )


def run_quality_checks(record: dict[str, Any], logical_date) -> None:
    validate_required_fields(
        record,
        ["city", "timestamp", "temp", "humidity", "cloudiness", "wind_speed", "is_alert"],
    )
    validate_humidity(record)
    validate_cloudiness(record)
    validate_wind_speed(record)
    validate_temperature(record)
    validate_timestamp_not_future(record, logical_date)
