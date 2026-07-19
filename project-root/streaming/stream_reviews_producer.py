import csv
import json
import logging
import os
import time
from collections.abc import Iterator
from typing import Any

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from config.const import KAFKA_BROKER, REVIEWS_PATH, SCHEMA_REGISTRY_URL, TOPIC_NAME
from streaming.avro_schemas import review_avro_schema
from streaming.stream_type import ReviewType

logger = logging.getLogger(__name__)


def delivery_report(err, msg) -> None:
    if err is not None:
        logger.error("Delivery failed for record %s: %s", msg.key(), err)
    else:
        logger.info(
            "Record produced to %s [%s] at offset %s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


class ReviewStreamReader:
    def __init__(self, data_file: str, broker: str, registry_url: str):
        """Initialize producer and serializer for supported source file types."""
        self.data_file = data_file
        self.schema_registry_client = SchemaRegistryClient({"url": registry_url})

        _, extension = os.path.splitext(self.data_file)
        file_extension = extension.lower()
        if file_extension not in {".json", ".csv"}:
            raise ValueError(f"Unsupported file format: '{file_extension}'")

        avro_serializer = AvroSerializer(
            schema_registry_client=self.schema_registry_client,
            schema_str=review_avro_schema,
        )
        self.producer = self._init_producer(broker, avro_serializer)

    @staticmethod
    def _init_producer(
        broker: str, avro_serializer: AvroSerializer
    ) -> SerializingProducer:
        producer_conf = {
            "bootstrap.servers": broker,
            "value.serializer": avro_serializer,
        }
        return SerializingProducer(producer_conf)

    def read_rows(self) -> Iterator[dict[str, Any]]:
        """Yield rows from the input file."""
        if self.data_file.endswith(".json"):
            with open(self.data_file, mode="r", encoding="utf-8") as file:
                data = json.load(file)
                for row in data:
                    yield row
            return

        with open(self.data_file, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                yield row

    def stream_reviews(self) -> None:
        """Parse rows and produce records to Kafka with Avro serialization."""
        for row in self.read_rows():
            try:
                review = ReviewType.from_row(row)
            except ValueError as exc:
                logger.warning(
                    "Error casting row ID %s: %s. Skipping row.", row.get("id"), exc
                )
                continue

            self.producer.produce(
                topic=TOPIC_NAME,
                value=review.to_dict(),
                on_delivery=delivery_report,
            )
            self.producer.poll(0)
            time.sleep(0.5)

        self.producer.flush()
        logger.info("Streaming completed.")


def main() -> None:
    logger.info("Initializing Kafka producer...")
    streamer = ReviewStreamReader(
        data_file=REVIEWS_PATH,
        broker=KAFKA_BROKER,
        registry_url=SCHEMA_REGISTRY_URL,
    )
    streamer.stream_reviews()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
