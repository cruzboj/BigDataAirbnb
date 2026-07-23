# pyright: reportMissingImports=false
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from airflow.decorators import dag, task  # type: ignore[attr-defined]
from airflow.models import Variable
from airflow.operators.python import get_current_context  # type: ignore[attr-defined]
from airflow.operators.trigger_dagrun import (  # type: ignore[attr-defined]
    TriggerDagRunOperator,
)
from pyspark.sql import functions as F

from processing.const import BRONZE_BUCKET
from processing.schema import bronze_reviews_schema
from processing.session import SparkSessionManager
from processing.storage.s3 import S3StorageHandler

logger = logging.getLogger("airbnb.orchestration.upload_kafka_bronze")


@dag(
    dag_id="upload_kafka_to_bronze_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def upload_kafka_to_bronze() -> None:
    @task(task_id="upload_message_to_bronze")
    def upload_message_to_bronze() -> str:
        context = get_current_context()
        dag_run = context.get("dag_run")
        run_id = context.get("run_id")

        conf: dict[str, Any] = (dag_run.conf or {}) if dag_run else {}
        kafka_event: dict[str, Any] = conf.get("kafka_event", {})

        payload = kafka_event.get("payload", kafka_event)
        received_at = (
            kafka_event.get("received_at") or datetime.now(timezone.utc).isoformat()
        )

        # Normalize payload into list[dict[str, Any]] for Spark JSON parsing.
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

        output_key = f"kafka/reviews/{(run_id or 'manual').replace(':', '_')}.parquet"
        output_path = f"s3a://{BRONZE_BUCKET}/{output_key}"

        total_uploaded = 0
        batch_index = 0
        current_batch: list[dict[str, Any]] = []

        with SparkSessionManager("airbnb-kafka-to-bronze") as spark:
            storage = S3StorageHandler(spark)
            storage.create_buckets([BRONZE_BUCKET])

            for record in payload_records:
                normalized = normalize_record(record)
                if normalized is None:
                    continue

                current_batch.append(normalized)

                if len(current_batch) < bronze_upload_batch_size:
                    continue

                batch_index += 1
                for item_idx, item in enumerate(current_batch, start=1):
                    logger.info(
                        "MinIO upload payload row %d/%d in batch %d for %s: %s",
                        item_idx,
                        len(current_batch),
                        batch_index,
                        output_path,
                        json.dumps(item, ensure_ascii=False),
                    )

                json_rows = [
                    json.dumps(item, ensure_ascii=False) for item in current_batch
                ]
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

                total_uploaded += len(current_batch)
                logger.info(
                    "Uploaded batch %d with %d row(s) to %s (running total=%d)",
                    batch_index,
                    len(current_batch),
                    output_path,
                    total_uploaded,
                )
                current_batch = []

            if current_batch:
                batch_index += 1
                for item_idx, item in enumerate(current_batch, start=1):
                    logger.info(
                        "MinIO upload payload row %d/%d in batch %d for %s: %s",
                        item_idx,
                        len(current_batch),
                        batch_index,
                        output_path,
                        json.dumps(item, ensure_ascii=False),
                    )

                json_rows = [
                    json.dumps(item, ensure_ascii=False) for item in current_batch
                ]
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

                total_uploaded += len(current_batch)
                logger.info(
                    "Uploaded batch %d with %d row(s) to %s (running total=%d)",
                    batch_index,
                    len(current_batch),
                    output_path,
                    total_uploaded,
                )

        if total_uploaded == 0:
            raise ValueError(
                "No valid Kafka review payload records found in dag_run.conf"
            )

        logger.info(
            "Uploaded %d Kafka review record(s) to %s",
            total_uploaded,
            output_path,
        )
        return output_path

    bronze_path = upload_message_to_bronze()

    trigger_kafka_reviews_cleaning = TriggerDagRunOperator(
        task_id="trigger_clean_kafka_reviews_to_silver",
        trigger_dag_id="clean_reviews",
        wait_for_completion=True,
        poke_interval=5,
    )

    bronze_path >> trigger_kafka_reviews_cleaning


upload_kafka_to_bronze()
