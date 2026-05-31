from __future__ import annotations

from weather_hw3_factory import CITIES, create_ingestion_dag


for city_name, city_conf in CITIES.items():
    dag = create_ingestion_dag(city_name, city_conf)
    globals()[dag.dag_id] = dag
