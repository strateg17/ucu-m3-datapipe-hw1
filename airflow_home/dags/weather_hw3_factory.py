from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import requests

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor

from weather_storage import read_json, write_json
from weather_quality import run_quality_checks


CITIES = {
    "Lviv": {"slug": "lviv", "lat": 49.8397, "lon": 24.0297},
    "Kyiv": {"slug": "kyiv", "lat": 50.4501, "lon": 30.5234},
    "Kharkiv": {"slug": "kharkiv", "lat": 49.9935, "lon": 36.2304},
    "Odesa": {"slug": "odesa", "lat": 46.4825, "lon": 30.7233},
    "Zhmerynka": {"slug": "zhmerynka", "lat": 49.0371, "lon": 28.1120},
}

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def build_history_url(city_conf: dict[str, Any], logical_date: datetime, api_key: str) -> str:
    dt_unix = int(logical_date.timestamp())
    return (
        "https://api.openweathermap.org/data/3.0/onecall/timemachine"
        f"?lat={city_conf['lat']}"
        f"&lon={city_conf['lon']}"
        f"&dt={dt_unix}"
        f"&appid={api_key}"
        f"&units=metric"
    )


def fetch_raw_weather(city_name: str, city_conf: dict[str, Any], logical_date, api_key: str, **_: Any) -> dict:
    url = build_history_url(city_conf, logical_date, api_key)
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as e:
        raise RuntimeError(f"Fetch failed for {city_name}: {e}") from e

    if not payload:
        raise ValueError(f"Empty API response for {city_name}")

    return payload


def store_raw_weather(raw_payload: dict, raw_path: str, city_name: str, **_: Any) -> str:
    if not raw_payload:
        raise ValueError(f"No raw payload to persist for {city_name}")
    return write_json(raw_path, raw_payload)


def read_raw_weather(raw_path: str, city_name: str, **_: Any) -> dict:
    payload = read_json(raw_path)
    if not payload:
        raise ValueError(f"Raw payload is empty for {city_name}")
    return payload


def transform_weather(raw_payload: dict, city_name: str, wind_alert_threshold: float, **_: Any) -> dict:
    if isinstance(raw_payload, dict) and "data" in raw_payload and raw_payload["data"]:
        record = raw_payload["data"][0]
    else:
        record = raw_payload

    required = ["dt", "temp", "humidity", "clouds", "wind_speed"]
    missing = [k for k in required if k not in record]
    if missing:
        raise ValueError(f"{city_name}: missing raw fields {missing}")

    transformed = {
        "city": city_name,
        "timestamp": int(record["dt"]),
        "temp": float(record["temp"]),
        "humidity": float(record["humidity"]),
        "cloudiness": float(record["clouds"]),
        "wind_speed": float(record["wind_speed"]),
        "is_alert": float(record["wind_speed"]) >= float(wind_alert_threshold),
    }
    return transformed


def store_transformed_weather(transformed_payload: dict, staged_path: str, city_name: str, **_: Any) -> str:
    if not transformed_payload:
        raise ValueError(f"No transformed payload to persist for {city_name}")
    return write_json(staged_path, transformed_payload)


def quality_check_transformed(staged_path: str, logical_date, city_name: str, **_: Any) -> str:
    payload = read_json(staged_path)
    run_quality_checks(payload, logical_date)
    return staged_path


def create_ingestion_dag(city_name: str, city_conf: dict[str, Any]) -> DAG:
    city_slug = city_conf["slug"]
    dag_id = f"weather_ingestion_{city_slug}"

    with DAG(
        dag_id=dag_id,
        description=f"Weather ingestion DAG for {city_name}",
        start_date=datetime(2026, 3, 1),
        schedule="@hourly",
        catchup=True,
        default_args=DEFAULT_ARGS,
        render_template_as_native_obj=True,
        params={
            "city_name": Param(city_name, type="string"),
            "city_slug": Param(city_slug, type="string"),
            "api_key": Param("", type="string"),
            "raw_base_path": Param("airflow_home/data/raw", type="string"),
            "wind_alert_threshold": Param(10.0, type="number"),
        },
        tags=["weather", "hw3", "ingestion", city_slug],
    ) as dag:
        start = EmptyOperator(task_id="start")

        fetch = PythonOperator(
            task_id="fetch_raw",
            python_callable=fetch_raw_weather,
            op_kwargs={
                "city_name": city_name,
                "city_conf": city_conf,
                "logical_date": "{{ logical_date }}",
                "api_key": "{{ params.api_key }}",
            },
        )

        store_raw = PythonOperator(
            task_id="write_raw",
            python_callable=store_raw_weather,
            op_kwargs={
                "raw_payload": "{{ ti.xcom_pull(task_ids='fetch_raw') }}",
                "raw_path": "{{ params.raw_base_path }}/{{ params.city_slug }}/{{ ds }}/weather_raw.json",
                "city_name": city_name,
            },
        )

        end = EmptyOperator(task_id="end")

        start >> fetch >> store_raw >> end

    return dag


def create_processing_dag(city_name: str, city_conf: dict[str, Any]) -> DAG:
    city_slug = city_conf["slug"]
    dag_id = f"weather_processing_{city_slug}"
    upstream_dag_id = f"weather_ingestion_{city_slug}"

    with DAG(
        dag_id=dag_id,
        description=f"Weather processing DAG for {city_name}",
        start_date=datetime(2026, 3, 1),
        schedule="@hourly",
        catchup=True,
        default_args=DEFAULT_ARGS,
        render_template_as_native_obj=True,
        params={
            "city_name": Param(city_name, type="string"),
            "city_slug": Param(city_slug, type="string"),
            "raw_base_path": Param("airflow_home/data/raw", type="string"),
            "staged_base_path": Param("airflow_home/data/staged", type="string"),
            "final_db_path": Param("airflow_home/data/final/weather_hw3.db", type="string"),
            "wind_alert_threshold": Param(10.0, type="number"),
        },
        tags=["weather", "hw3", "processing", city_slug],
    ) as dag:
        start = EmptyOperator(task_id="start")

        wait_for_ingestion = ExternalTaskSensor(
            task_id="wait_for_ingestion",
            external_dag_id=upstream_dag_id,
            external_task_id="write_raw",
            allowed_states=["success"],
            failed_states=["failed", "skipped"],
            timeout=60 * 30,
            poke_interval=30,
            mode="poke",
        )

        read_raw = PythonOperator(
            task_id="read_raw",
            python_callable=read_raw_weather,
            op_kwargs={
                "raw_path": "{{ params.raw_base_path }}/{{ params.city_slug }}/{{ ds }}/weather_raw.json",
                "city_name": city_name,
            },
        )

        transform = PythonOperator(
            task_id="transform",
            python_callable=transform_weather,
            op_kwargs={
                "raw_payload": "{{ ti.xcom_pull(task_ids='read_raw') }}",
                "city_name": city_name,
                "wind_alert_threshold": "{{ params.wind_alert_threshold }}",
            },
        )

        store_staged = PythonOperator(
            task_id="write_staged",
            python_callable=store_transformed_weather,
            op_kwargs={
                "transformed_payload": "{{ ti.xcom_pull(task_ids='transform') }}",
                "staged_path": "{{ params.staged_base_path }}/{{ params.city_slug }}/{{ ds }}/weather_transformed.json",
                "city_name": city_name,
            },
        )

        quality_check = PythonOperator(
            task_id="quality_check",
            python_callable=quality_check_transformed,
            op_kwargs={
                "staged_path": "{{ params.staged_base_path }}/{{ params.city_slug }}/{{ ds }}/weather_transformed.json",
                "logical_date": "{{ logical_date }}",
                "city_name": city_name,
            },
        )

        create_table = SQLExecuteQueryOperator(
            task_id="create_table",
            conn_id="weather_sqlite_conn",
            sql="""
            CREATE TABLE IF NOT EXISTS measures_hw3 (
                city TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                temp REAL,
                humidity REAL,
                cloudiness REAL,
                wind_speed REAL,
                alert_flag INTEGER,
                source_file TEXT
            );
            """,
        )

        load_final = SQLExecuteQueryOperator(
            task_id="load_final",
            conn_id="weather_sqlite_conn",
            sql="""
            INSERT INTO measures_hw3 (
                city, timestamp, temp, humidity, cloudiness, wind_speed, alert_flag, source_file
            ) VALUES (
                '{{ ti.xcom_pull(task_ids="transform")["city"] }}',
                {{ ti.xcom_pull(task_ids="transform")["timestamp"] }},
                {{ ti.xcom_pull(task_ids="transform")["temp"] }},
                {{ ti.xcom_pull(task_ids="transform")["humidity"] }},
                {{ ti.xcom_pull(task_ids="transform")["cloudiness"] }},
                {{ ti.xcom_pull(task_ids="transform")["wind_speed"] }},
                {% if ti.xcom_pull(task_ids="transform")["is_alert"] %}1{% else %}0{% endif %},
                '{{ params.staged_base_path }}/{{ params.city_slug }}/{{ ds }}/weather_transformed.json'
            );
            """,
        )

        end = EmptyOperator(task_id="end")

        start >> wait_for_ingestion >> read_raw >> transform >> store_staged >> quality_check
        quality_check >> create_table >> load_final >> end

    return dag
