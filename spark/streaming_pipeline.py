"""Kafka-to-Parquet Structured Streaming pipeline for e-commerce events."""

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    approx_count_distinct,
    coalesce,
    col,
    count,
    current_timestamp,
    from_json,
    lit,
    lower,
    sum as spark_sum,
    to_timestamp,
    trim,
    when,
    window,
)
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


EVENT_SCHEMA = StructType(
    [
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_session", StringType(), True),
    ]
)

VALID_EVENT_TYPES = ("view", "cart", "purchase")


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Stream e-commerce Kafka events into bronze, silver, and gold Parquet layers."
    )
    parser.add_argument("--bootstrap-server", default="kafka:9092")
    parser.add_argument("--topic", default="ecommerce_events")
    parser.add_argument("--output-root", type=Path, default=Path("data"))
    parser.add_argument("--watermark", default="10 minutes")
    parser.add_argument("--window", default="5 minutes")
    return parser.parse_args()


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ecommerce-realtime-analytics")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )


def read_kafka_events(spark: SparkSession, bootstrap_server: str, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_server)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(kafka_events: DataFrame) -> DataFrame:
    """Parse producer JSON and retain Kafka metadata for traceability."""
    return (
        kafka_events.select(
            col("timestamp").alias("kafka_timestamp"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("value").cast("string").alias("raw_value"),
            from_json(col("value").cast("string"), EVENT_SCHEMA).alias("event"),
        )
        .select("kafka_timestamp", "kafka_partition", "kafka_offset", "raw_value", "event.*")
        .withColumn("ingested_at", current_timestamp())
        .withColumn("event_time", to_timestamp(col("event_time")))
        .withColumn("event_type", lower(trim(col("event_type"))))
        .withColumn("product_id", col("product_id").cast(LongType()))
        .withColumn("category_id", col("category_id").cast(LongType()))
        .withColumn("user_id", col("user_id").cast(LongType()))
        .withColumn("price", col("price").cast(DoubleType()))
        .withColumn("brand", when(trim(col("brand")) == "", None).otherwise(trim(col("brand"))))
    )


def valid_event_condition():
    return coalesce(
        col("event_time").isNotNull()
        & col("event_type").isin(*VALID_EVENT_TYPES)
        & col("product_id").isNotNull()
        & col("user_id").isNotNull()
        & col("price").isNotNull()
        & (col("price") >= 0),
        lit(False),
    )


def build_silver_events(bronze_events: DataFrame, watermark: str) -> DataFrame:
    """Keep valid, de-duplicated analytical events in the silver layer."""
    valid = bronze_events.filter(valid_event_condition())

    return (
        valid.withWatermark("event_time", watermark)
        .dropDuplicates(["user_session", "event_time", "event_type", "product_id"])
        .withColumn("brand", coalesce(col("brand"), col("category_code"), col("event_type")))
    )


def build_quarantine_events(bronze_events: DataFrame) -> DataFrame:
    """Route malformed source records aside without stopping the main stream."""
    return bronze_events.filter(~valid_event_condition()).withColumn(
        "quarantine_reason",
        when(col("event_time").isNull(), "invalid event_time")
        .when(
            col("event_type").isNull() | (~col("event_type").isin(*VALID_EVENT_TYPES)),
            "invalid event_type",
        )
        .when(col("product_id").isNull(), "invalid product_id")
        .when(col("user_id").isNull(), "invalid user_id")
        .otherwise("invalid price"),
    )


def build_gold_metrics(silver_events: DataFrame, aggregation_window: str) -> DataFrame:
    """Calculate finalized windowed KPIs once each event-time window closes."""
    return (
        silver_events.groupBy(
            window(col("event_time"), aggregation_window),
            col("event_type"),
            col("brand"),
        )
        .agg(
            count("*").alias("event_count"),
            approx_count_distinct("user_id").alias("active_users"),
            spark_sum(
                when(col("event_type") == "purchase", col("price")).otherwise(0.0)
            ).alias("purchase_revenue"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "event_type",
            "brand",
            "event_count",
            "active_users",
            "purchase_revenue",
        )
    )


def start_parquet_query(
    dataframe: DataFrame,
    output_path: Path,
    checkpoint_path: Path,
    query_name: str,
):
    return (
        dataframe.writeStream.format("parquet")
        .queryName(query_name)
        .outputMode("append")
        .option("path", str(output_path))
        .option("checkpointLocation", str(checkpoint_path))
        .start()
    )


def write_gold_batch(batch: DataFrame, _batch_id: int, output_path: Path):
    """Materialize the complete aggregate so finite replays are visible immediately."""
    batch.write.mode("overwrite").parquet(str(output_path))


def start_gold_query(
    dataframe: DataFrame,
    output_path: Path,
    checkpoint_path: Path,
):
    return (
        dataframe.writeStream.queryName("gold-event-metrics")
        .outputMode("complete")
        .option("checkpointLocation", str(checkpoint_path))
        .foreachBatch(
            lambda batch, batch_id: write_gold_batch(batch, batch_id, output_path)
        )
        .start()
    )


def main():
    args = parse_arguments()
    root = args.output_root
    checkpoint_root = root / "checkpoints"

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bronze_events = parse_events(read_kafka_events(spark, args.bootstrap_server, args.topic))
    silver_events = build_silver_events(bronze_events, args.watermark)
    quarantine_events = build_quarantine_events(bronze_events)
    gold_metrics = build_gold_metrics(silver_events, args.window)

    queries = [
        start_parquet_query(
            bronze_events,
            root / "bronze" / "events",
            checkpoint_root / "bronze",
            "bronze-events",
        ),
        start_parquet_query(
            silver_events,
            root / "silver" / "events",
            checkpoint_root / "silver",
            "silver-events",
        ),
        start_gold_query(
            gold_metrics,
            root / "gold" / "event_metrics",
            checkpoint_root / "gold",
        ),
        start_parquet_query(
            quarantine_events,
            root / "quarantine" / "events",
            checkpoint_root / "quarantine",
            "quarantine-events",
        ),
    ]

    try:
        spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
