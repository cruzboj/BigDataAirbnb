import logging

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from config.const import KAFKA_BROKER, SCHEMA_REGISTRY_URL, TOPIC_NAME
from streaming.avro_schemas import review_avro_schema

GROUP_ID = "my_test_group_1"

logger = logging.getLogger(__name__)


def main() -> None:
    """Consume and print enriched review records from Kafka."""
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(
        schema_registry_client=sr_client,
        schema_str=review_avro_schema,
    )

    consumer_conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "key.deserializer": None,
        "value.deserializer": avro_deserializer,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    }

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([TOPIC_NAME])

    logger.info("Listening for messages on topic '%s'...", TOPIC_NAME)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            review_data = msg.value()
            if not isinstance(review_data, dict):
                logger.warning(
                    "Skipping unexpected payload type: %s", type(review_data).__name__
                )
                continue

            logger.info("Received Review ID: %s", review_data.get("id"))
            logger.info("Listing ID: %s", review_data.get("listing_id"))
            logger.info("Score: %s", review_data.get("sentiment_score"))
            logger.info("Raw Data: %s", review_data)

    except KeyboardInterrupt:
        logger.info("Stopped by user.")
    finally:
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
