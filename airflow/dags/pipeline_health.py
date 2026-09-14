"""Periodic health checks for the local real-time analytics pipeline."""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from kafka.admin import KafkaAdminClient


KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_events")
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", "/opt/airflow/project"))
GOLD_PATH = PROJECT_ROOT / "data" / "gold" / "event_metrics"


@dag(
    dag_id="realtime_pipeline_health",
    description="Checks Kafka topic availability and freshness of the gold layer.",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["realtime", "ecommerce", "health"],
)
def realtime_pipeline_health():
    @task
    def kafka_topic_available() -> str:
        client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)
        try:
            if KAFKA_TOPIC not in client.list_topics():
                raise AirflowException(f"Kafka topic '{KAFKA_TOPIC}' does not exist.")
        finally:
            client.close()
        return KAFKA_TOPIC

    @task
    def gold_layer_fresh() -> str:
        files = list(GOLD_PATH.rglob("*.parquet")) if GOLD_PATH.exists() else []
        if not files:
            raise AirflowException(f"No gold-layer files found at {GOLD_PATH}.")

        newest_file = max(files, key=lambda path: path.stat().st_mtime)
        age = datetime.now().timestamp() - newest_file.stat().st_mtime
        if age > timedelta(minutes=20).total_seconds():
            raise AirflowException(
                f"Gold layer is stale: {newest_file.name} was last updated {age / 60:.1f} minutes ago."
            )
        return str(newest_file)

    kafka_topic_available() >> gold_layer_fresh()


realtime_pipeline_health()
