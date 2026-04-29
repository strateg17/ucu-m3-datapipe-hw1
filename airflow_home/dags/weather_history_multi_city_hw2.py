from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.sqlite.operators.sqlite import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

CITIES = {
    "Lviv": {"lat": 49.8397, "lon": 24.0297},
    "Kyiv": {"lat": 50.4501, "lon": 30.5234},
    "Kharkiv": {"lat": 49.9935, "lon": 36.2304},
    "Odesa": {"lat": 46.4825, "lon": 30.7233},
    "Zhmerynka": {"lat": 49.0371, "lon": 28.1120},
}

WIND_ALERT_THRESHOLD = 10.0  # m/s

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

def build_history_url(city_name: str, logical_date: datetime) -> str:
    city = CITIES[city_name]
    api_key = Variable.get("WEATHER_API_KEY")
    dt_unix = int(logical_date.timestamp())
    return (
        "https://api.openweathermap.org/data/3.0/onecall/timemachine"
        f"?lat={city['lat']}"
        f"&lon={city['lon']}"
        f"&dt={dt_unix}"
        f"&appid={api_key}"
        f"&units=metric"
    )

def fetch_weather(city_name: str, logical_date: datetime, **_: Any) -> dict:
    url = build_history_url(city_name, logical_date)
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as e:
        raise RuntimeError(f"Fetch failed for {city_name}: {e}") from e

    if not payload:
        raise ValueError(f"Empty API response for {city_name}")

    return payload  # -> XCom

def transform_weather(city_name: str, ti, **_: Any) -> dict:
    raw = ti.xcom_pull(task_ids=f"{city_name}.fetch")

    if not raw:
        raise ValueError(f"No XCom payload found for {city_name}")

    if isinstance(raw, dict) and "data" in raw and raw["data"]:
        record = raw["data"][0]
    else:
        record = raw

    required = ["dt", "temp", "humidity", "clouds", "wind_speed"]
    missing = [k for k in required if k not in record]
    if missing:
        raise ValueError(f"{city_name}: missing fields {missing}")

    transformed = {
        "city": city_name,
        "timestamp": int(record["dt"]),
        "temp": float(record["temp"]),
        "humidity": float(record["humidity"]),
        "cloudiness": float(record["clouds"]),
        "wind_speed": float(record["wind_speed"]),
        "is_alert": float(record["wind_speed"]) >= WIND_ALERT_THRESHOLD,
    }
    return transformed  # -> XCom

def choose_route(city_name: str, ti, **_: Any) -> str:
    data = ti.xcom_pull(task_ids=f"{city_name}.transform")
    if not data:
        raise ValueError(f"No transformed data for {city_name}")

    if data["is_alert"]:
        return f"{city_name}.alert"
    return f"{city_name}.normal_load"

def alert_weather(city_name: str, ti, **_: Any) -> None:
    data = ti.xcom_pull(task_ids=f"{city_name}.transform")
    if not data:
        raise ValueError(f"No transformed data for {city_name}")
    print(
        f"ALERT for {city_name}: "
        f"wind_speed={data['wind_speed']} >= threshold={WIND_ALERT_THRESHOLD}"
    )

with DAG(
    dag_id="weather_history_multi_city_hw2",
    description="Weather pipeline with TaskGroups, XComs, branching, retries, and alert path",
    start_date=datetime(2026, 3, 1),
    schedule="@hourly",
    catchup=True,
    default_args=DEFAULT_ARGS,
    tags=["weather", "hw2", "taskgroup", "xcom", "branch", "celery"],
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="weather_sqlite_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS measures (
            city TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            temp REAL,
            humidity REAL,
            cloudiness REAL,
            wind_speed REAL,
            alert_flag INTEGER
        );
        """,
    )

    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    start >> create_table

    for city_name in CITIES.keys():
        with TaskGroup(group_id=city_name) as city_group:
            fetch = PythonOperator(
                task_id="fetch",
                python_callable=fetch_weather,
                op_kwargs={"city_name": city_name},
            )

            transform = PythonOperator(
                task_id="transform",
                python_callable=transform_weather,
                op_kwargs={"city_name": city_name},
            )

            branch = BranchPythonOperator(
                task_id="branch",
                python_callable=choose_route,
                op_kwargs={"city_name": city_name},
            )

            alert = PythonOperator(
                task_id="alert",
                python_callable=alert_weather,
                op_kwargs={"city_name": city_name},
            )

            normal_load = SQLExecuteQueryOperator(
                task_id="normal_load",
                conn_id="weather_sqlite_conn",
                sql=f"""
                INSERT INTO measures (
                    city, timestamp, temp, humidity, cloudiness, wind_speed, alert_flag
                ) VALUES (
                    '{{{{ ti.xcom_pull(task_ids="{city_name}.transform")["city"] }}}}',
                    {{{{ ti.xcom_pull(task_ids="{city_name}.transform")["timestamp"] }}}},
                    {{{{ ti.xcom_pull(task_ids="{city_name}.transform")["temp"] }}}},
                    {{{{ ti.xcom_pull(task_ids="{city_name}.transform")["humidity"] }}}},
                    {{{{ ti.xcom_pull(task_ids="{city_name}.transform")["cloudiness"] }}}},
                    {{{{ ti.xcom_pull(task_ids="{city_name}.transform")["wind_speed"] }}}},
                    0
                );
                """,
            )

            alert_load = SQLExecuteQueryOperator(
                task_id="alert_load",
                conn_id="weather_sqlite_conn",
                sql=f"""
                INSERT INTO measures (
                    city, timestamp, temp, humidity, cloudiness, wind_speed, alert_flag
                ) VALUES (
                    '{{{{ ti.xcom_pull(task_ids="{city_name}.transform")["city"] }}}}',
                    {{{{ ti.xcom_pull(task_ids="{city_name}.transform")["timestamp"] }}}},
                    {{{{ ti.xcom_pull(task_ids="{city_name}.transform")["temp"] }}}},
                    {{{{ ti.xcom_pull(task_ids="{city_name}.transform")["humidity"] }}}},
                    {{{{ ti.xcom_pull(task_ids="{city_name}.transform")["cloudiness"] }}}},
                    {{{{ ti.xcom_pull(task_ids="{city_name}.transform")["wind_speed"] }}}},
                    1
                );
                """,
            )

            join = EmptyOperator(
                task_id="join",
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            )

            fetch >> transform >> branch
            branch >> normal_load >> join
            branch >> alert >> alert_load >> join

        create_table >> city_group >> finish