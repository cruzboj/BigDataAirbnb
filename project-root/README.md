
run migrate: 
docker exec -w /home/iceberg/processing spark-iceberg python migrate_schema.py