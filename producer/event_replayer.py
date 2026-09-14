import argparse
import csv
import json
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

REQUIRED_COLUMNS = {
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
}


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Replay historical e-commerce events to Kafka."
    )
    parser.add_argument(
        "--file",
        type=Path,
        required=True,
        help="Path to the CSV dataset.",
    )
    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9092",
        help="Kafka bootstrap server.",
    )
    parser.add_argument(
        "--topic",
        default="ecommerce_events",
        help="Kafka topic.",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=10.0,
        help="Events per second.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Maximum number of events to replay.",
    )

    return parser.parse_args()


def create_producer(bootstrap_server):
    return KafkaProducer(
        bootstrap_servers=bootstrap_server,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )


def validate_columns(fieldnames):
    if not fieldnames:
        raise ValueError("CSV file does not contain a header.")

    missing = REQUIRED_COLUMNS - set(fieldnames)
    if missing:
        raise ValueError(f"CSV is missing required columns: {sorted(missing)}")


def clean_value(value):
    if value is None:
        return None

    value = value.strip()
    return value if value else None


def transform_row(row):
    return {
        "event_time": clean_value(row.get("event_time")),
        "event_type": clean_value(row.get("event_type")),
        "product_id": clean_value(row.get("product_id")),
        "category_id": clean_value(row.get("category_id")),
        "category_code": clean_value(row.get("category_code")),
        "brand": clean_value(row.get("brand")),
        "price": clean_value(row.get("price")),
        "user_id": clean_value(row.get("user_id")),
        "user_session": clean_value(row.get("user_session")),
    }


def main():
    args = parse_arguments()

    if not args.file.exists():
        raise FileNotFoundError(f"Dataset not found: {args.file}")

    if args.rate <= 0:
        raise ValueError("--rate must be greater than 0.")

    if args.max_events is not None and args.max_events <= 0:
        raise ValueError("--max-events must be greater than 0.")

    print(f"Dataset: {args.file}")
    print(f"Kafka: {args.bootstrap_server}")
    print(f"Topic: {args.topic}")
    print(f"Replay rate: {args.rate} events/sec")

    producer = create_producer(args.bootstrap_server)
    delay = 1.0 / args.rate
    sent_count = 0

    try:
        with args.file.open(mode="r", encoding="utf-8", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            validate_columns(reader.fieldnames)

            for row in reader:
                event = transform_row(row)

                try:
                    future = producer.send(args.topic, value=event)
                    future.get(timeout=10)
                except KafkaError as exc:
                    print(f"Kafka send failed: {exc}")
                    continue

                sent_count += 1

                if sent_count % 100 == 0:
                    print(f"Sent {sent_count:,} events")

                if args.max_events is not None and sent_count >= args.max_events:
                    break

                time.sleep(delay)
    except KeyboardInterrupt:
        print("\nReplay interrupted by user.")
    finally:
        producer.flush()
        producer.close()

    print(f"Replay finished. Total events sent: {sent_count:,}")


if __name__ == "__main__":
    main()
