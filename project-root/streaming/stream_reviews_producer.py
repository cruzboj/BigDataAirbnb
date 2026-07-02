import csv
import json
import time
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from avro_schemas import review_avro_schema 
from stream_type import ReviewType

import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
from config.const import KAFKA_BROKER, SCHEMA_REGISTRY_URL, KAFKA_PATH, TOPIC_NAME

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"✅ Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
class ReviewStreamReader:
    def __init__(self,data_file, broker, registry_url ):
        """
            - check file format (data_file) and convert into Avro format
            - call read_row() to read the file and yield one row at a time
            - initialize Kafka producer with Avro serialization
        """
        try:
            self.data_file = data_file

            self.schema_registry_conf = {'url': registry_url}
            self.schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)

            _, extension = os.path.splitext(self.data_file)
            file_extension = extension.lower()

            if self.data_file.endswith('.json') or self.data_file.endswith('.csv'):
                avro_serializer = AvroSerializer(
                schema_registry_client=self.schema_registry_client,
                schema_str=review_avro_schema
                )
            
                self.producer = self.producer_init(broker, avro_serializer)
            else:
                raise ValueError(f"❌ Unsupported file format: '{file_extension}'")

        except ValueError as e:
            print(f"❌ Error initializing ReviewStreamReader: {e}")
            raise

    def producer_init(self, broker, avro_serializer):
        """
            Initialize Kafka producer with Avro serialization
        """
        producer_conf = {
        'bootstrap.servers': broker,
        'value.serializer': avro_serializer
        }

        producer = SerializingProducer(producer_conf)

        return producer


    def read_row(self):
        """
            Read the file and yield one row at a time
        """
        if self.data_file.endswith('.json'):
            with open(self.data_file, mode='r', encoding='utf-8') as file:
                data = json.load(file)
                for row in data:
                    yield row

        elif self.data_file.endswith('.csv'):
            with open(self.data_file, mode='r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    yield row

    def stream_reviews(self):
        """
            - verification by avro format (data_file)
            - read the file and yield one row at a time
            - produce each row to Kafka topic with Avro serialization
        """
        for row in self.read_row():
            try:
                review = ReviewType(row)
                

            except ValueError as e:
                print(f"Error casting row ID {row.get('id')}: {e}. Skipping row.")
                continue
            
            self.producer.produce(
                topic=TOPIC_NAME,
                value=review.__dict__,
                on_delivery=delivery_report
            )
        
            self.producer.poll(0)
                
            time.sleep(0.5)

        self.producer.flush()
        print("🎉 Streaming completed.")

if __name__ == "__main__":
    print("Initializing Kafka Producer...")
    streamer = ReviewStreamReader(
        data_file=KAFKA_PATH,
        broker=KAFKA_BROKER,
        registry_url=SCHEMA_REGISTRY_URL
    )
    
    streamer.stream_reviews()