# StreamCart real-time analytics

A local, event-driven e-commerce analytics stack. Historical CSV activity is replayed into Kafka, transformed by Spark Structured Streaming through bronze/silver/gold layers, checked by an Airflow health DAG, and viewed in a Streamlit operations dashboard.

## Architecture

```text
CSV replay → Kafka (ecommerce_events) → Spark Structured Streaming
                                           ├─ bronze: parsed source records
                                           ├─ silver: validated, de-duplicated events
                                           ├─ gold: 5-minute event and revenue KPIs → Streamlit
                                           └─ quarantine: rejected records
                                                               ↑
                                                        Airflow health checks
```

## Project layout

| Path | Purpose |
| --- | --- |
| `producer/` | Validates and replays source CSV rows to Kafka. |
| `spark/` | Kafka-to-Parquet Structured Streaming medallion pipeline. |
| `airflow/dags/` | Kafka topic and gold-layer freshness monitoring. |
| `streamlit/` | Live gold-metrics operations dashboard. |
| `data/` | Raw input and local bronze, silver, gold, and quarantine outputs. |
| `config/settings.yaml` | Shared defaults and data contract. |

## Run locally with Docker

Prerequisites: Docker Compose and a local Python environment with the dependencies in `requirements.txt` (the existing `venv` is fine for the producer).

Start Kafka and create the topic:

```bash
docker compose up -d kafka kafka-init
```

Start the streaming job in a separate terminal. The initial connector package download can take a moment.

```bash
docker compose --profile pipeline up spark
```

Replay a sample of the bundled dataset in another terminal:

```bash
venv/bin/python producer/event_replayer.py \
  --file data/raw/2019-Oct.csv \
  --rate 100 \
  --max-events 10000
```

Launch the dashboard at <http://localhost:8501>:

```bash
docker compose --profile dashboard up --build dashboard
```

The gold job writes finalized event-time windows. With the default 10-minute watermark, a window appears after Spark has observed later events; this prevents late data from silently changing a finalized KPI.

## Airflow health DAG

Point a local Airflow installation at `airflow/dags`, set `PROJECT_ROOT` to this repository, and start Airflow. The `realtime_pipeline_health` DAG runs every five minutes and fails when the Kafka topic is unavailable or the gold layer has not been updated for 20 minutes.

```bash
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/airflow/dags"
export PROJECT_ROOT="$PWD"
export KAFKA_BOOTSTRAP_SERVER="localhost:9092"
airflow standalone
```

## Tests

Run the producer data-contract tests without a Kafka broker:

```bash
venv/bin/python -m unittest discover -s tests -v
```

## Configuration

The defaults are collected in `config/settings.yaml`. Override the producer or Spark arguments when targeting a remote broker or a different source topic. Kafka automatic topic creation is deliberately disabled; `kafka-init` creates the required three-partition topic explicitly.
