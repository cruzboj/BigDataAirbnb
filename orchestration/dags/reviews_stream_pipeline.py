# pyright: reportMissingImports=false
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.decorators import dag, task  # type: ignore[attr-defined]
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import get_current_context  # type: ignore[attr-defined]
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import functions as F

from config.const import SPARK_CORES_MAX, SPARK_MAVEN_REPOSITORIES, SPARK_S3_PACKAGES
from processing.const import BRONZE_BUCKET
from processing.schema import bronze_reviews_schema
from processing.session import SparkSessionManager
from processing.storage.s3 import S3StorageHandler

logger = logging.getLogger("airbnb.orchestration.reviews_stream_pipeline")


def _kafka_bootstrap_servers_from_connection(conn_id: str) -> str:
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson or {}

    bootstrap_servers = extra.get("bootstrap.servers")
    if bootstrap_servers:
        return str(bootstrap_servers)

    if conn.host and conn.port:
        return f"{conn.host}:{conn.port}"
    if conn.host:
        return conn.host

    raise ValueError(
        f"Kafka connection '{conn_id}' is missing bootstrap servers in extras or host."
    )


@dag(
    dag_id="reviews_stream_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
)
def reviews_stream_pipeline() -> None:
    topic = Variable.get("KAFKA_TOPIC", default_var="airbnb_reviews")

    @task(task_id="kafka_subscriber")
    def kafka_subscriber() -> dict[str, Any]:
        conn_id = "KAFKA_DEFAULT"
        conn = BaseHook.get_connection(conn_id)
        extra = conn.extra_dejson or {}

        bootstrap_servers = _kafka_bootstrap_servers_from_connection(conn_id)
        group_id = str(extra.get("group.id") or "airflow-kafka-sensor")
        auto_offset_reset = str(extra.get("auto.offset.reset") or "earliest")

        max_wait_seconds = int(
            Variable.get("KAFKA_MAX_WAIT_SECONDS", default_var="300")
        )
        idle_timeout_seconds = float(
            Variable.get("KAFKA_IDLE_TIMEOUT_SECONDS", default_var="3")
        )
        poll_timeout_seconds = float(
            Variable.get("KAFKA_POLL_TIMEOUT_SECONDS", default_var="1")
        )
        max_batch_messages = int(
            Variable.get("KAFKA_MAX_BATCH_MESSAGES", default_var="500")
        )

        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": auto_offset_reset,
                "enable.auto.commit": True,
            }
        )

        logger.info(
            "Starting Kafka batch consume: topic=%s bootstrap=%s group.id=%s auto.offset.reset=%s max_batch_messages=%s",
            topic,
            bootstrap_servers,
            group_id,
            auto_offset_reset,
            max_batch_messages,
        )

        records: list[Any] = []
        started_at = time.monotonic()
        last_message_at: float | None = None

        try:
            consumer.subscribe([topic])

            while True:
                msg = consumer.poll(poll_timeout_seconds)

                if msg is None:
                    if not records:
                        if time.monotonic() - started_at >= max_wait_seconds:
                            logger.info(
                                "No Kafka messages received in %ds for topic '%s'.",
                                max_wait_seconds,
                                topic,
                            )
                            break
                        continue

                    assert last_message_at is not None
                    if time.monotonic() - last_message_at >= idle_timeout_seconds:
                        break
                    continue

                error = msg.error()
                if error is not None:
                    if error.code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(f"Kafka consume error: {error}")

                raw_value = msg.value()
                if raw_value is None:
                    logger.warning(
                        "Skipping Kafka message with null value at %s[%s]@%s",
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                    )
                    continue

                decoded = raw_value.decode("utf-8", errors="replace")
                parsed: Any = decoded
                try:
                    parsed = json.loads(decoded)
                except json.JSONDecodeError:
                    logger.warning(
                        "Kafka value is not JSON at %s[%s]@%s; forwarding as string",
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                    )

                records.append(parsed)
                last_message_at = time.monotonic()

                key_bytes = msg.key()
                decoded_key = (
                    key_bytes.decode("utf-8", errors="replace")
                    if key_bytes is not None
                    else None
                )

                logger.info(
                    "Consumed Kafka message #%d from %s[%s]@%s key=%s payload=%s",
                    len(records),
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                    decoded_key,
                    decoded,
                )

                if len(records) >= max_batch_messages:
                    logger.info(
                        "Reached KAFKA_MAX_BATCH_MESSAGES=%d; ending current consume batch.",
                        max_batch_messages,
                    )
                    break
        finally:
            consumer.close()

        consumed_count = len(records)
        batch_full = consumed_count >= max_batch_messages

        logger.info(
            "Consumed %d Kafka message(s) in this batch. batch_full=%s",
            consumed_count,
            batch_full,
        )

        return {
            "payload": records,
            "received_at": datetime.now(timezone.utc).isoformat(),
            "consumed_count": consumed_count,
            "max_batch_messages": max_batch_messages,
            "batch_full": batch_full,
        }

    @task.short_circuit(task_id="has_messages")
    def has_messages(kafka_event: dict[str, Any]) -> bool:
        consumed_count = int(kafka_event.get("consumed_count", 0))
        if consumed_count <= 0:
            logger.info("No Kafka messages consumed in this run; skipping upload.")
            return False
        return True

    @task(task_id="upload_reviews_to_bronze")
    def upload_reviews_to_bronze(kafka_event: dict[str, Any]) -> str:
        context = get_current_context()
        dag_run = context.get("dag_run")

        conf: dict[str, Any] = (dag_run.conf or {}) if dag_run else {}
        payload = kafka_event.get(
            "payload", conf.get("kafka_event", {}).get("payload", [])
        )
        received_at = (
            kafka_event.get("received_at") or datetime.now(timezone.utc).isoformat()
        )

        if isinstance(payload, list):
            payload_records = payload
        else:
            payload_records = [payload]

        bronze_upload_batch_size = int(
            Variable.get("BRONZE_UPLOAD_BATCH_SIZE", default_var="500")
        )

        def normalize_record(record: Any) -> dict[str, Any] | None:
            parsed: Any = record

            if isinstance(record, bytes):
                parsed = record.decode("utf-8", errors="replace")

            if isinstance(parsed, str):
                try:
                    parsed = json.loads(parsed)
                except json.JSONDecodeError:
                    logger.warning("Skipping non-JSON Kafka payload: %s", parsed)
                    return None

            if (
                isinstance(parsed, dict)
                and "payload" in parsed
                and isinstance(parsed["payload"], (dict, str, bytes, list))
            ):
                nested = parsed["payload"]
                if isinstance(nested, str):
                    try:
                        nested = json.loads(nested)
                    except json.JSONDecodeError:
                        pass
                parsed = nested

            if isinstance(parsed, dict):
                return parsed

            logger.warning(
                "Skipping unsupported Kafka payload type: %s",
                type(parsed).__name__,
            )
            return None

        timestamp_key = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        output_key = f"reviews/review_{timestamp_key}.parquet"
        output_path = f"s3a://{BRONZE_BUCKET}/{output_key}"

        total_uploaded = 0
        batch_index = 0
        current_batch: list[dict[str, Any]] = []

        with SparkSessionManager("airbnb-kafka-to-bronze") as spark:
            storage = S3StorageHandler(spark)
            storage.create_buckets([BRONZE_BUCKET])

            def flush_batch(batch: list[dict[str, Any]]) -> int:
                nonlocal total_uploaded, batch_index

                if not batch:
                    return 0

                batch_index += 1
                for item_idx, item in enumerate(batch, start=1):
                    logger.info(
                        "MinIO upload payload row %d/%d in batch %d for %s: %s",
                        item_idx,
                        len(batch),
                        batch_index,
                        output_path,
                        json.dumps(item, ensure_ascii=False),
                    )

                json_rows = [json.dumps(item, ensure_ascii=False) for item in batch]
                raw_df = spark.createDataFrame(
                    [(row,) for row in json_rows], ["payload_json"]
                )

                parsed_df = (
                    raw_df.select(
                        F.from_json(F.col("payload_json"), bronze_reviews_schema).alias(
                            "review"
                        )
                    )
                    .select("review.*")
                    .withColumn(
                        "event_ingestion_time",
                        F.coalesce(
                            F.col("event_ingestion_time"),
                            F.to_timestamp(F.lit(received_at)),
                        ),
                    )
                    .withColumn(
                        "ingestion_ts",
                        F.coalesce(F.col("ingestion_ts"), F.current_timestamp()),
                    )
                )

                casted_df = parsed_df.select(
                    *[
                        F.col(schema_field.name)
                        .cast(schema_field.dataType)
                        .alias(schema_field.name)
                        for schema_field in bronze_reviews_schema
                    ]
                )

                write_mode = "overwrite" if total_uploaded == 0 else "append"
                storage.bucket_upload(
                    BRONZE_BUCKET,
                    output_key,
                    casted_df,
                    mode=write_mode,
                )

                total_uploaded += len(batch)
                logger.info(
                    "Uploaded batch %d with %d row(s) to %s (running total=%d)",
                    batch_index,
                    len(batch),
                    output_path,
                    total_uploaded,
                )
                return len(batch)

            for record in payload_records:
                normalized = normalize_record(record)
                if normalized is None:
                    continue

                current_batch.append(normalized)
                if len(current_batch) < bronze_upload_batch_size:
                    continue

                flush_batch(current_batch)
                current_batch = []

            flush_batch(current_batch)

        if total_uploaded == 0:
            raise ValueError(
                "No valid Kafka review payload records found in task input"
            )

        logger.info(
            "Uploaded %d Kafka review record(s) to %s",
            total_uploaded,
            output_path,
        )
        return output_path

    clean_reviews_to_silver = SparkSubmitOperator(
        task_id="clean_reviews_to_silver",
        application="/opt/airflow/processing/tasks/clean_reviews_to_silver.py",
        conn_id="my_spark_conn",
        packages=SPARK_S3_PACKAGES,
        repositories=SPARK_MAVEN_REPOSITORIES,
        deploy_mode="client",
        env_vars={"PYTHONPATH": "/opt/airflow"},
        conf={
            "spark.executorEnv.PYTHONPATH": "/home/iceberg",
            "spark.cores.max": SPARK_CORES_MAX,
        },
        verbose=True,
        retries=3,
        retry_delay=timedelta(minutes=1),
    )

    @task.short_circuit(task_id="needs_next_batch")
    def needs_next_batch(kafka_event: dict[str, Any]) -> bool:
        consumed_count = int(kafka_event.get("consumed_count", 0))
        max_batch_messages = int(kafka_event.get("max_batch_messages", 0))
        batch_full = bool(kafka_event.get("batch_full", False))

        should_continue = consumed_count > 0 and batch_full
        logger.info(
            "Batch drain decision: consumed_count=%d max_batch_messages=%d batch_full=%s should_continue=%s",
            consumed_count,
            max_batch_messages,
            batch_full,
            should_continue,
        )
        return should_continue

    trigger_next_consume_batch = TriggerDagRunOperator(
        task_id="trigger_next_consume_batch",
        trigger_dag_id="reviews_stream_pipeline",
        conf={"triggered_by": "batch_drain"},
        wait_for_completion=False,
    )

    kafka_batch_event = kafka_subscriber()
    has_records = has_messages(kafka_batch_event)
    bronze_path = upload_reviews_to_bronze(kafka_batch_event)
    continue_drain = needs_next_batch(kafka_batch_event)

    (
        has_records
        >> bronze_path
        >> clean_reviews_to_silver
        >> continue_drain
        >> trigger_next_consume_batch
    )


reviews_stream_pipeline()
