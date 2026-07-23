from __future__ import annotations

import logging

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.const import STREAMING_DATA_OUTPUT, STREAMING_DATA_PATH
from processing.const import SILVER_BUCKET
from processing.schema import silver_review_schema
from processing.session import SparkSessionManager
from processing.storage.s3 import S3StorageHandler

logger = logging.getLogger("airbnb.processing.tasks.reviews_to_silver")


def _clean_sentiment_label() -> F.Column:
    normalized_existing = F.when(
        F.col("sentiment_label").isNotNull() & (F.size(F.col("sentiment_label")) > 0),
        F.transform(F.col("sentiment_label"), lambda x: F.lower(F.trim(x))),
    )
    return F.coalesce(normalized_existing, F.array(F.lit("positive")))


def _clean_sentiment_score() -> F.Column:
    score = F.col("sentiment_score").cast("double")
    return F.when(score.isNull() | F.isnan(score), F.lit(1.0)).otherwise(
        score.cast("float")
    )


def _clean_reviews_df(bronze_reviews_df):
    # Keep only rows where review id and reviewer id exist.
    base = bronze_reviews_df.filter(
        F.col("id").isNotNull() & F.col("reviewer_id").isNotNull()
    )

    cleaned = (
        base.withColumn(
            "date",
            F.coalesce(
                F.to_timestamp(F.col("date")),
                F.col("event_ingestion_time"),
                F.current_timestamp(),
            ),
        )
        .withColumn("listing_id", F.coalesce(F.col("listing_id"), F.lit(-1)))
        .withColumn(
            "reviewer_name", F.coalesce(F.trim(F.col("reviewer_name")), F.lit(""))
        )
        .withColumn(
            "comment",
            F.coalesce(
                F.trim(F.regexp_replace(F.col("comments"), r"\\s+", " ")),
                F.lit(""),
            ),
        )
        .withColumn(
            "language",
            F.coalesce(F.lower(F.trim(F.col("language"))), F.lit("unknown")),
        )
        .withColumn("sentiment_label", _clean_sentiment_label())
        .withColumn("sentiment_score", _clean_sentiment_score())
        .withColumn("likes_count", F.coalesce(F.col("likes_votes"), F.lit(0)))
        .withColumn("ingestion_ts", F.current_timestamp())
    )

    # start_date = comment date
    # end_date = next comment date by same reviewer
    # is_current = true when end_date is null
    scd_window = Window.partitionBy("reviewer_id").orderBy(
        F.col("date").asc(),
        F.col("id").asc(),
    )

    cleaned = (
        cleaned.withColumn("start_date", F.col("date"))
        .withColumn("end_date", F.lead("date").over(scd_window))
        .withColumn("is_current", F.col("end_date").isNull())
    )

    casted = cleaned.select(
        *[
            F.col(field.name).cast(field.dataType).alias(field.name)
            for field in silver_review_schema
        ]
    )

    return casted


def main() -> None:
    with SparkSessionManager("airbnb-kafka-reviews-to-silver") as spark:
        storage = S3StorageHandler(spark)
        storage.create_buckets([SILVER_BUCKET])

        logger.info("Reading bronze reviews from %s", STREAMING_DATA_PATH)
        bronze_reviews_df = storage.read_files(STREAMING_DATA_PATH)

        cleaned_reviews_df = _clean_reviews_df(bronze_reviews_df)
        cleaned_count = cleaned_reviews_df.count()

        if cleaned_count == 0:
            raise ValueError(
                "No review rows left after filtering out rows with missing id/reviewer_id."
            )

        logger.info("Writing %d cleaned review row(s) to silver", cleaned_count)
        storage.bucket_upload(
            SILVER_BUCKET,
            STREAMING_DATA_OUTPUT,
            cleaned_reviews_df,
            mode="append",
        )
        logger.info(
            "Uploaded cleaned reviews to s3a://%s/%s",
            SILVER_BUCKET,
            STREAMING_DATA_OUTPUT,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
