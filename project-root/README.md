setup instructions -
    working with .gz format insted of csv
    CSV | GZ files must be in csv_folder


run python3 with modules:
uv run -m <module>.<filename_only_without_suffix>

uv:
    .venv\Scripts\activate

    source .venv/bin/activate

run migrate: 
docker exec -w /home/iceberg/processing spark-iceberg python migrate_schema.py

run consumer:
docker exec -w /home/iceberg/processing spark-iceberg python spark_reviews_consumer.py
