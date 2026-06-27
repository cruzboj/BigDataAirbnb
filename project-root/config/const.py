#kafka configuration
KAFKA_BROKER = 'localhost:29092'
SCHEMA_REGISTRY_URL = 'http://localhost:8082'
CSV_FILE_PATH = '../csv_folder/reviews.csv' 
TOPIC_NAME = 'airbnb_reviews'

#spark configuration
LISTINGS_PATH = [
    '../csv_folder/listings_denmark.csv.gz' ,
    '../csv_folder/listings_chicago.csv.gz',
    ]


CSV_FILE_PATH2 = '../csv_folder/listings_denmark.csv'
CSV_FILE_PATH3 = '../csv_folder/listings_chicago.csv.gz'