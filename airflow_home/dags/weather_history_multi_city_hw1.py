from __future__ import annotations

import json
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
# from airflow.providers.http.sensors.http import HttpSensor
# from airflow.providers.sqlite.operators.sqlite import SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# -----------------------------
# Static city config for One Call 3.0
# API 3.0 works with coordinates, not city names.
# -----------------------------
CITIES = {
    "Lviv": {"lat": 49.8397, "lon": 24.0297},
    "Kyiv": {"lat": 50.4501, "lon": 30.5234},
    "Kharkiv": {"lat": 49.9935, "lon": 36.2304},
    "Odesa": {"lat": 46.4825, "lon": 30.7233},
    "Zhmerynka": {"lat": 49.0371, "lon": 28.1120},
}

DEFAULT_ARGS = {
    "owner": "airflow",
}

# -----------------------------
# Helpers
# -----------------------------
def _build_history_endpoint(city_name: str, logical_date: datetime) -> str:
    """
    Build OpenWeather One Call 3.0 historical endpoint.
    We use logical_date from Airflow so catchup/backfill returns weather
    for the DAG run date, not for 'now'.
    """
    city = CITIES[city_name]
    api_key = Variable.get("WEATHER_API_KEY")

    # One Call timemachine expects unix UTC timestamp
    dt_unix = int(logical_date.timestamp())

    return (
        f"data/3.0/onecall/timemachine"
        f"?lat={city['lat']}"
        f"&lon={city['lon']}"
        f"&dt={dt_unix}"
        f"&appid={api_key}"
        f"&units=metric"
    )


def _check_api_for_city(city_name: str, **context) -> bool:
    """
    Small HTTP availability check using requests.
    Kept as a Python task because endpoint is dynamic and depends on logical_date.
    """
    import requests

    logical_date = context["logical_date"]
    endpoint = _build_history_endpoint(city_name, logical_date)

    base_url = "https://api.openweathermap.org/"
    response = requests.get(base_url + endpoint, timeout=30)

    if response.status_code != 200:
        raise ValueError(
            f"API check failed for {city_name}. "
            f"Status={response.status_code}, body={response.text}"
        )

    return True


def _extract_weather(city_name: str, **context) -> dict:
    """
    Extract weather from OpenWeather One Call 3.0 historical endpoint.
    """
    import requests

    logical_date = context["logical_date"]
    endpoint = _build_history_endpoint(city_name, logical_date)

    base_url = "https://api.openweathermap.org/"
    response = requests.get(base_url + endpoint, timeout=30)
    response.raise_for_status()

    payload = response.json()

    # Store raw response in XCom return value
    return payload


def _process_weather(city_name: str, ti, **context) -> tuple:
    """
    Return a tuple for SQL insert:
    (city, timestamp, temp, humidity, cloudiness, wind_speed)
    """
    info = ti.xcom_pull(task_ids=f"extract_{city_name.lower()}")

    # One Call timemachine returns weather data for a specified timestamp.
    # Depending on plan/shape, it can expose data in 'data' list or current-like object.
    # We normalize to one record.
    if isinstance(info, dict) and "data" in info and info["data"]:
        record = info["data"][0]
    else:
        record = info

    timestamp = record["dt"]
    temp = record["temp"]
    humidity = record["humidity"]
    cloudiness = record["clouds"]
    wind_speed = record["wind_speed"]

    return city_name, timestamp, temp, humidity, cloudiness, wind_speed


# -----------------------------
# DAG
# -----------------------------
with DAG(
    dag_id="weather_history_multi_city_v3",
    description="Hourly weather pipeline with OpenWeather One Call 3.0 and historical backfill support",
    start_date=datetime(2026, 3, 1),
    schedule="@hourly",
    catchup=True,
    default_args=DEFAULT_ARGS,
    tags=["weather", "sqlite", "openweather", "api-v3"],
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_table_sqlite",
        conn_id="weather_sqlite_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS measures (
            city TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            temp REAL,
            humidity REAL,
            cloudiness REAL,
            wind_speed REAL
        );
        """,
    )

    for city_name in CITIES.keys():
        city_slug = city_name.lower()

        check_api = PythonOperator(
            task_id=f"check_api_{city_slug}",
            python_callable=_check_api_for_city,
            op_kwargs={"city_name": city_name},
        )

        extract_data = PythonOperator(
            task_id=f"extract_{city_slug}",
            python_callable=_extract_weather,
            op_kwargs={"city_name": city_name},
        )

        process_data = PythonOperator(
            task_id=f"process_{city_slug}",
            python_callable=_process_weather,
            op_kwargs={"city_name": city_name},
        )

        inject_data = SQLExecuteQueryOperator(
            task_id=f"inject_{city_slug}",
            conn_id="weather_sqlite_conn",
            sql=f"""
            INSERT INTO measures (
                city,
                timestamp,
                temp,
                humidity,
                cloudiness,
                wind_speed
            )
            VALUES (
                '{{{{ ti.xcom_pull(task_ids="process_{city_slug}")[0] }}}}',
                {{{{ ti.xcom_pull(task_ids="process_{city_slug}")[1] }}}},
                {{{{ ti.xcom_pull(task_ids="process_{city_slug}")[2] }}}},
                {{{{ ti.xcom_pull(task_ids="process_{city_slug}")[3] }}}},
                {{{{ ti.xcom_pull(task_ids="process_{city_slug}")[4] }}}},
                {{{{ ti.xcom_pull(task_ids="process_{city_slug}")[5] }}}}
            );
            """,
        )

        create_table >> check_api >> extract_data >> process_data >> inject_data