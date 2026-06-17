from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

# ייבוא הסכמה
from avro_schemas import review_avro_schema

# --- הגדרות ---
KAFKA_BROKER = 'localhost:29092'
SCHEMA_REGISTRY_URL = 'http://localhost:8082'
TOPIC_NAME = 'airbnb_reviews'
GROUP_ID = 'my_test_group_1' # קבוצת צרכנים לבדיקה

def main():
    # 1. חיבור ל-Schema Registry והגדרת ה-Deserializer (המפענח)
    sr_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(
        schema_registry_client=sr_client,
        schema_str=review_avro_schema
    )

    # 2. הגדרת ה-Consumer
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'key.deserializer': None, # לא הגדרנו מפתח ב-Producer
        'value.deserializer': avro_deserializer,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest' # קריטי: אומר לו להתחיל לקרוא מההודעה הראשונה אי פעם ב-Topic
    }
    
    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([TOPIC_NAME])

    print(f"🎧 Listening for messages on topic '{TOPIC_NAME}'...")

    # 3. לולאת האזנה
    try:
        while True:
            msg = consumer.poll(1.0) # מחכה שנייה להודעה חדשה
            
            if msg is None:
                continue
            
            if msg.error():
                print(f"❌ Error: {msg.error()}")
                continue
            
            # הדפסת הנתונים שפוענחו בהצלחה!
            review_data = msg.value()
            print(f"\n✅ Received Review ID: {review_data.get('id')}")
            print(f"   Listing ID: {review_data.get('listing_id')}")
            print(f"   Score: {review_data.get('sentiment_score')}")
            print(f"   Raw Data: {review_data}")

    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.")
    finally:
        # סגירה מסודרת של הצרכן
        consumer.close()

if __name__ == '__main__':
    main()