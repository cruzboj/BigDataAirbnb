from __future__ import annotations

import csv
import json
import logging
import os
import time
from collections.abc import Iterator
from typing import Any

from confluent_kafka import Producer

from config.const import KAFKA_BROKER, REVIEWS_PATH, TOPIC_NAME
from streaming.stream_type import ReviewType

logger = logging.getLogger(__name__)


class AirflowKafkaReviewProducer:
    """Stream review rows as JSON messages for the Airflow Kafka sensor."""

    def __init__(
        self,
        csv_path: str,
        broker: str,
        topic: str,
        throttle_seconds: float = 0.0,
    ) -> None:
        self.csv_path = csv_path
        self.topic = topic
        self.throttle_seconds = throttle_seconds
        self.producer = Producer(
            {
                "bootstrap.servers": broker,
                "client.id": "airbnb-review-csv-producer",
            }
        )

    def _read_rows(self) -> Iterator[dict[str, Any]]:
        with open(self.csv_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                yield row

    def _delivery_report(self, err: Any, msg: Any) -> None:
        if err is not None:
            logger.error("Delivery failed for key=%s: %s", msg.key(), err)
            return

        logger.debug(
            "Produced to %s [%s] at offset %s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )

    def stream(self, max_records: int | None = None) -> int:
        produced_count = 0

        for row in self._read_rows():
            if max_records is not None and produced_count >= max_records:
                break

            try:
                review = ReviewType.from_row(row)
            except (ValueError, TypeError) as exc:
                logger.warning("Skipping invalid row id=%s (%s)", row.get("id"), exc)
                continue

            payload = review.to_dict()
            payload_json = json.dumps(payload, ensure_ascii=False)
            message_key = str(payload.get("id") or "")

            logger.info(
                "Producing Kafka message #%d to topic '%s' key=%s payload=%s",
                produced_count + 1,
                self.topic,
                message_key,
                payload_json,
            )

            self.producer.produce(
                topic=self.topic,
                key=message_key,
                value=payload_json.encode("utf-8"),
                on_delivery=self._delivery_report,
            )
            self.producer.poll(0)
            produced_count += 1

            if self.throttle_seconds > 0:
                time.sleep(self.throttle_seconds)

        self.producer.flush()
        logger.info(
            "Finished producing %d review message(s) to topic '%s'.",
            produced_count,
            self.topic,
        )
        return produced_count


def main() -> None:
    topic = os.getenv("KAFKA_TOPIC", TOPIC_NAME)
    broker = os.getenv("KAFKA_BROKER", KAFKA_BROKER)
    csv_path = os.getenv("REVIEWS_CSV_PATH", REVIEWS_PATH)
    throttle_seconds = float(os.getenv("STREAM_DELAY_SECONDS", "0"))

    logger.info(
        "Starting producer with broker=%s topic=%s csv=%s",
        broker,
        topic,
        csv_path,
    )

    producer = AirflowKafkaReviewProducer(
        csv_path=csv_path,
        broker=broker,
        topic=topic,
        throttle_seconds=throttle_seconds,
    )
    producer.stream()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
