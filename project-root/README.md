setup instructions -
    working with .gz format insted of csv
    CSV | GZ files must be in csv_folder

uv:
    .venv\Scripts\activate

run migrate: 
docker exec -w /home/iceberg/processing spark-iceberg python migrate_schema.py

run consumer:
docker exec -w /home/iceberg/processing spark-iceberg python spark_reviews_consumer.py