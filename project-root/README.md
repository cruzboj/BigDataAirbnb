setup instructions -
    working with .gz format insted of csv
    CSV | GZ files must be in csv_folder


run python3 with modules:
uv run -m <module>.<filename_only_without_suffix>

uv:
    .venv\Scripts\activate

    source .venv/bin/activate

listing csv must contain name formating listing_<city_name>.csv.gz

run consumer:
docker exec -w /home/iceberg/processing spark-iceberg python spark_reviews_consumer.py

