# StreamCart real-time analytics

StreamCart is a local e-commerce streaming analytics project. It replays historical CSV events to Kafka, processes them with Spark Structured Streaming, stores curated analytics data as Parquet, and displays the results in a Streamlit dashboard.

## Architecture

```text
CSV dataset
    |
    v
Kafka topic: ecommerce_events
    |
    v
Spark Structured Streaming
    |-- bronze: parsed source events and Kafka metadata
    |-- silver: validated and de-duplicated events
    |-- gold: windowed event, user, and revenue metrics
    `-- quarantine: invalid source events
    |
    v
Streamlit dashboard

Airflow health checks monitor Kafka availability and gold-layer freshness.
```

## Prerequisites

- Docker Desktop with Docker Compose
- Python 3.11 or later
- The source dataset at `data/raw/2019-Oct.csv`

If you do not already have a virtual environment, create one and install the Python dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Quick start

Run the commands from the repository root.

### 1. Update the project and start Kafka

```bash
git pull origin main
docker compose up -d --force-recreate kafka
docker compose run --rm kafka-init
```

The last command must finish successfully. It creates the `ecommerce_events` topic if it does not already exist.

### 2. Start Spark

Open a new terminal and start the streaming job:

```bash
docker compose --profile pipeline up spark
```

Keep this terminal open. On the first run, Spark downloads the Kafka connector, which can take a few minutes. Continue only after the Spark job is running without an error.

### 3. Replay events

Open another terminal:

```bash
source venv/bin/activate

venv/bin/python producer/event_replayer.py \
  --file data/raw/2019-Oct.csv \
  --rate 100 \
  --max-events 10000
```

Remove `--max-events 10000` to replay the entire dataset. Spark must already be running before replay begins.

### 4. Start the dashboard

Open a third terminal:

```bash
docker compose --profile dashboard up --build dashboard
```

Open <http://localhost:8501> in a browser. Keep the dashboard process running while you use it.

After the replay starts, Spark normally writes gold metrics within 5 to 30 seconds. Refresh the browser if the dashboard initially reports that no metrics are available.

## Verify the pipeline

Check that Spark wrote gold-layer Parquet files:

```bash
find data/gold/event_metrics -name 'part-*.parquet' | head
```

Check the most recent Spark logs:

```bash
docker compose logs --tail=100 spark
```

Run the producer data-contract tests:

```bash
venv/bin/python -m unittest discover -s tests -v
```

## Common issues

| Problem | Resolution |
| --- | --- |
| `kafka-init` exits with code 1 | Run `git pull origin main`, then rerun `docker compose up -d --force-recreate kafka` and `docker compose run --rm kafka-init`. Inspect `docker compose logs kafka-init` if it still fails. |
| Producer reports `KafkaTimeoutError` | The Kafka topic is unavailable. Complete the Kafka initialization step before replaying events. |
| Spark reports an Ivy cache error | Pull the latest project changes and restart Spark with `docker compose --profile pipeline up --force-recreate spark`. |
| Dashboard is empty | Confirm Spark was running before the replay. Check for files in `data/gold/event_metrics`, then refresh or restart the dashboard. |
| Dashboard shows a temporary-file error | Pull the latest changes and rebuild the dashboard with `docker compose --profile dashboard up --build --force-recreate dashboard`. |

## Stop the stack

Stop containers without deleting Kafka data:

```bash
docker compose --profile pipeline --profile dashboard down
```

Do not add `-v` unless you intentionally want to delete the Kafka volume and start with a new broker state.

## Data layers

| Layer | Location | Contents |
| --- | --- | --- |
| Raw | `data/raw/` | Input CSV dataset. |
| Bronze | `data/bronze/events/` | Parsed Kafka events with source metadata. |
| Silver | `data/silver/events/` | Validated, normalized, and de-duplicated events. |
| Gold | `data/gold/event_metrics/` | Aggregated event, active-user, and revenue metrics. |
| Quarantine | `data/quarantine/events/` | Events rejected by validation rules. |

Generated data, Spark checkpoints, and Kafka volumes are local runtime artifacts. They are ignored by Git and should not be committed to GitHub.

## Project layout

| Path | Purpose |
| --- | --- |
| `producer/` | Kafka CSV replay producer. |
| `spark/` | Spark Structured Streaming pipeline. |
| `streamlit/` | Dashboard source and container definition. |
| `airflow/dags/` | Optional pipeline health DAG. |
| `config/settings.yaml` | Default topic, storage, and pipeline settings. |
| `tests/` | Producer data-contract tests. |

## Airflow health checks

The optional `realtime_pipeline_health` DAG checks that the Kafka topic exists and that gold-layer output is recent. Install and configure Airflow separately, then point it at the project DAG folder:

```bash
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/airflow/dags"
export PROJECT_ROOT="$PWD"
export KAFKA_BOOTSTRAP_SERVER="localhost:9092"
airflow standalone
```
