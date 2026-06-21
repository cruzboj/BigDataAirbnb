from datetime import datetime
import os
import csv
import json
import time
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from avro_schemas import review_avro_schema 

#.env
KAFKA_BROKER = 'localhost:29092'
SCHEMA_REGISTRY_URL = 'http://localhost:8082'
CSV_FILE_PATH = '../reviews_enriched.csv' 
TOPIC_NAME = 'airbnb_reviews'

# TODO : kafka paroducer class, modules constent config.py, devide function to subfunctions, methods,

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
                row['listing_id'] = int(row['listing_id']) if row.get('listing_id') else None
                row['id'] = int(row['id']) if row.get('id') else None
                row["date"] = row["date"] if row.get("date") else None
                row['reviewer_id'] = int(row['reviewer_id']) if row.get('reviewer_id') else None
                row["reviewer_name"] = row["reviewer_name"] if row.get("reviewer_name") else None
                row["comments"] = row["comments"] if row.get("comments") else None
                row["language"] = row["language"] if row.get("language") else None
                row['sentiment_score'] = float(row['sentiment_score']) if row.get('sentiment_score') else None
                if row.get('sentiment_label'):
                    row['sentiment_label'] = [s.strip() for s in row['sentiment_label'].split(',')]
                else:
                    row['sentiment_label'] = None
                row['likes_votes'] = int(row['likes_votes']) if row.get('likes_votes') else None
                row["event_ingestion_time"] = row["event_ingestion_time"] if row.get("event_ingestion_time") else None
                row["raw_user_agent"] = row["raw_user_agent"] if row.get("raw_user_agent") else None
                row['bot_suspicion_score'] = float(row['bot_suspicion_score']) if row.get('bot_suspicion_score') else None
                row["reviewer_hash_id"] = row["reviewer_hash_id"] if row.get("reviewer_hash_id") else None
                row["aspect_sentiment_json"] = row["aspect_sentiment_json"] if row.get("aspect_sentiment_json") else None
                row["extracted_keywords"] = row["extracted_keywords"] if row.get("extracted_keywords") else None
                row['comment_character_count'] = int(row['comment_character_count']) if row.get('comment_character_count') else None
                row['readability_index'] = float(row['readability_index']) if row.get('readability_index') else None
                row["session_id"] = row["session_id"] if row.get("session_id") else None
                row['time_spent_on_review_ms'] = int(row['time_spent_on_review_ms']) if row.get('time_spent_on_review_ms') else None
                if row.get('contains_media'):
                    row['contains_media'] = str(row['contains_media']).lower() in ['true', '1', 'yes']
                else:
                    row['contains_media'] = None
                row["ingestion_ts"] = datetime.now().isoformat() if row.get("ingestion_ts") is None else row["ingestion_ts"]

            except ValueError as e:
                print(f"Error casting row ID {row.get('id')}: {e}. Skipping row.")
                continue
            
            self.producer.produce(
                topic=TOPIC_NAME,
                value=row,
                on_delivery=delivery_report
            )
        
            self.producer.poll(0)
                
            time.sleep(0.5)

        self.producer.flush()
        print("🎉 Streaming completed.")

if __name__ == "__main__":
    print("Initializing Kafka Producer...")
    streamer = ReviewStreamReader(
        data_file=CSV_FILE_PATH,
        broker=KAFKA_BROKER,
        registry_url=SCHEMA_REGISTRY_URL
    )
    
    # הפעלת הזרם
    streamer.stream_reviews()