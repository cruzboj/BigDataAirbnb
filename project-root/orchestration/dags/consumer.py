# pyright: reportMissingImports=false
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from airflow.decorators import dag, task  # type: ignore[attr-defined]
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from confluent_kafka import Consumer, KafkaError

logger = logging.getLogger("airbnb.orchestration.kafka_subscriber")


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
    dag_id="kafka_subscriber",
    start_date=datetime(2026, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
)
def subscriber() -> None:
    topic = Variable.get("KAFKA_TOPIC", default_var="airbnb_reviews")

    @task(task_id="consume_kafka_messages")
    def consume_kafka_messages() -> dict[str, Any]:
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

    kafka_batch_event = consume_kafka_messages()
    has_records = has_messages(kafka_batch_event)

    trigger_bronze_upload = TriggerDagRunOperator(
        task_id="trigger_bronze_upload_dag",
        trigger_dag_id="upload_kafka_to_bronze_dag",
        conf={"kafka_event": kafka_batch_event},
        wait_for_completion=True,
        poke_interval=5,
    )

    continue_drain = needs_next_batch(kafka_batch_event)

    trigger_next_consume_batch = TriggerDagRunOperator(
        task_id="trigger_next_consume_batch",
        trigger_dag_id="kafka_subscriber",
        conf={"triggered_by": "batch_drain"},
        wait_for_completion=False,
    )

    has_records >> trigger_bronze_upload
    trigger_bronze_upload >> continue_drain >> trigger_next_consume_batch


subscriber()
