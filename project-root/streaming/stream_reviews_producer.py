import os
import csv
import time
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from avro_schemas import review_avro_schema 

KAFKA_BROKER = 'localhost:29092'
SCHEMA_REGISTRY_URL = 'http://localhost:8082'
TOPIC_NAME = 'airbnb_reviews'

CSV_FILE_PATH = 'reviews_enriched.csv' 

# TODO : kafka paroducer class, modules constent config.py, devide function to subfunctions, methods,

def delivery_report(err, msg):
    """
    """
    if err is not None:
        print(f"❌ Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"✅ Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def main():
    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=review_avro_schema
    )

    producer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'value.serializer': avro_serializer
    }
    producer = SerializingProducer(producer_conf)

    print(f"Starting to stream from {CSV_FILE_PATH}...")
    
    with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        for row in reader:
            try:
                row['listing_id'] = int(row['listing_id']) if row.get('listing_id') else None
                row['id'] = int(row['id']) if row.get('id') else None
                row['reviewer_id'] = int(row['reviewer_id']) if row.get('reviewer_id') else None
                row['likes_votes'] = int(row['likes_votes']) if row.get('likes_votes') else None
                row['comment_character_count'] = int(row['comment_character_count']) if row.get('comment_character_count') else None
                row['time_spent_on_review_ms'] = int(row['time_spent_on_review_ms']) if row.get('time_spent_on_review_ms') else None
                
                row['sentiment_score'] = float(row['sentiment_score']) if row.get('sentiment_score') else None
                row['bot_suspicion_score'] = float(row['bot_suspicion_score']) if row.get('bot_suspicion_score') else None
                row['readability_index'] = float(row['readability_index']) if row.get('readability_index') else None
                
                if row.get('contains_media'):
                    row['contains_media'] = str(row['contains_media']).lower() in ['true', '1', 'yes']
                else:
                    row['contains_media'] = None

                if row.get('sentiment_label'):
                    row['sentiment_label'] = [s.strip() for s in row['sentiment_label'].split(',')]
                else:
                    row['sentiment_label'] = None
                    
                for key in row.keys():
                    if row[key] == "":
                        row[key] = None

            except ValueError as e:
                print(f"Error casting row ID {row.get('id')}: {e}. Skipping row.")
                continue

            producer.produce(
                topic=TOPIC_NAME,
                value=row,
                on_delivery=delivery_report
            )
            
            producer.poll(0)
            
            time.sleep(0.5)

    producer.flush()
    print("🎉 Streaming completed.")

if __name__ == '__main__':
    main()