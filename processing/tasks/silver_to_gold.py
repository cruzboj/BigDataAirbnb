from __future__ import annotations

import logging
import os
import time

from minio.error import S3Error
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from processing.const import GOLD_BUCKET, SILVER_BUCKET
from processing.schema import GOLD_SCHEMAS
from processing.session import SparkSessionManager
from processing.storage.s3 import S3StorageHandler

logger = logging.getLogger("airbnb.processing.tasks.silver_to_gold")

REQUIRED_LISTING_PREFIXES = [
    "HostDetails.parquet",
    "HostMetrics.parquet",
    "RentDetails.parquet",
    "RentMetrics.parquet",
    "Location.parquet",
]
REQUIRED_REVIEW_PREFIXES = ["Review.parquet"]
REQUIRED_ADS_PREFIXES = ["providerDetails.parquet", "providerMetrics.parquet"]


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default))
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid value for %s=%r; using default=%s", name, raw, default)
        return default


def _prefix_has_data(storage: S3StorageHandler, bucket: str, prefix: str) -> bool:
    try:
        for obj in storage.client.list_objects(bucket, prefix=prefix, recursive=True):
            if obj.object_name:
                return True
    except S3Error as exc:
        logger.warning(
            "Failed listing bucket=%s prefix=%s while waiting: %s", bucket, prefix, exc
        )

    return False


def wait_for_silver_inputs(storage: S3StorageHandler, bucket: str) -> None:
    timeout_seconds = _env_float("GOLD_WAIT_TIMEOUT_SECONDS", 1800.0)
    poll_interval_seconds = _env_float("GOLD_WAIT_POLL_INTERVAL_SECONDS", 10.0)

    started = time.monotonic()

    while True:
        missing_listing = [
            prefix
            for prefix in REQUIRED_LISTING_PREFIXES
            if not _prefix_has_data(storage, bucket, prefix)
        ]
        missing_review = [
            prefix
            for prefix in REQUIRED_REVIEW_PREFIXES
            if not _prefix_has_data(storage, bucket, prefix)
        ]

        if not missing_listing and not missing_review:
            logger.info(
                "Silver readiness met (listing + review) in bucket '%s'.", bucket
            )
            return

        elapsed = time.monotonic() - started
        if elapsed >= timeout_seconds:
            raise TimeoutError(
                "Timed out waiting for silver inputs after "
                f"{timeout_seconds:.1f}s. Missing listing: {missing_listing}; "
                f"missing review: {missing_review}"
            )

        logger.info(
            "Waiting for silver inputs. missing_listing=%s missing_review=%s elapsed=%.1fs timeout=%.1fs",
            missing_listing,
            missing_review,
            elapsed,
            timeout_seconds,
        )
        time.sleep(poll_interval_seconds)


def _cast_to_schema(df: DataFrame, schema: StructType) -> DataFrame:
    cast_exprs = []
    for field in schema:
        source_expr = F.col(field.name) if field.name in df.columns else F.lit(None)
        cast_exprs.append(source_expr.cast(field.dataType).alias(field.name))
    return df.select(*cast_exprs)


def build_gold_listing_review(storage: S3StorageHandler) -> None:
    host_details_df = storage.read_files(f"s3a://{SILVER_BUCKET}/HostDetails.parquet")
    host_metrics_df = storage.read_files(f"s3a://{SILVER_BUCKET}/HostMetrics.parquet")
    rent_details_df = storage.read_files(f"s3a://{SILVER_BUCKET}/RentDetails.parquet")
    rent_metrics_df = storage.read_files(f"s3a://{SILVER_BUCKET}/RentMetrics.parquet")
    location_df = storage.read_files(f"s3a://{SILVER_BUCKET}/Location.parquet")
    review_df = storage.read_files(f"s3a://{SILVER_BUCKET}/Review.parquet")

    host_details_base = host_details_df.select(
        F.col("id").cast("int").alias("listing_id"),
        F.col("host_name"),
        F.col("host_total_listings_count"),
        F.col("property_type"),
        F.col("room_type"),
        F.col("ingestion_ts").alias("host_details_ingestion_ts"),
    ).dropDuplicates(["listing_id"])

    host_metrics_base = host_metrics_df.select(
        F.col("id").cast("int").alias("listing_id"),
        F.col("host_response_time"),
        F.col("host_response_rate"),
        F.col("host_acceptance_rate"),
        F.col("ingestion_ts").alias("host_metrics_ingestion_ts"),
    ).dropDuplicates(["listing_id"])

    rent_details_base = rent_details_df.select(
        F.col("id").cast("int").alias("listing_id"),
        F.col("accommodates"),
        F.col("bedrooms"),
        F.col("beds"),
        F.col("amenities"),
        F.col("ingestion_ts").alias("rent_details_ingestion_ts"),
    ).dropDuplicates(["listing_id"])

    rent_metrics_base = rent_metrics_df.select(
        F.col("id").cast("int").alias("listing_id"),
        F.col("price_per_night"),
        F.col("estimated_occupancy_l365d"),
        F.col("estimated_revenue_l365d"),
        F.col("availability_365"),
        F.col("ingestion_ts").alias("rent_metrics_ingestion_ts"),
    ).dropDuplicates(["listing_id"])

    location_base = location_df.select(
        F.col("id").cast("int").alias("listing_id"),
        F.col("neighbourhood"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("host_neighbourhood"),
        F.col("country"),
        F.col("city"),
        F.col("ingestion_ts").alias("location_ingestion_ts"),
    ).dropDuplicates(["listing_id"])

    dim_host_raw = (
        host_details_base.join(host_metrics_base, on="listing_id", how="left")
        .withColumn("host_key", F.col("listing_id"))
        .withColumn("start_date", F.current_timestamp())
        .withColumn("end_date", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("name", F.col("host_name"))
        .withColumn("response_time_category", F.col("host_response_time"))
        .withColumn(
            "ingestion_ts",
            F.coalesce(
                F.col("host_metrics_ingestion_ts"),
                F.col("host_details_ingestion_ts"),
                F.current_timestamp(),
            ),
        )
    )

    dim_property_raw = (
        rent_details_base.join(host_details_base, on="listing_id", how="left")
        .withColumn("start_date", F.current_timestamp())
        .withColumn("end_date", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("property_key", F.col("listing_id"))
        .withColumn("ameneties", F.col("amenities"))
        .withColumn(
            "ingestion_ts",
            F.coalesce(
                F.col("rent_details_ingestion_ts"),
                F.col("host_details_ingestion_ts"),
                F.current_timestamp(),
            ),
        )
    )

    dim_location_raw = location_base.withColumn(
        "location_key", F.col("listing_id")
    ).withColumn(
        "ingestion_ts",
        F.coalesce(F.col("location_ingestion_ts"), F.current_timestamp()),
    )

    fact_listing_raw = (
        rent_metrics_base.join(host_metrics_base, on="listing_id", how="left")
        .join(host_details_base, on="listing_id", how="left")
        .withColumn("id", F.col("listing_id"))
        .withColumn("property_key", F.col("listing_id"))
        .withColumn("location_key", F.col("listing_id"))
        .withColumn("host_key", F.col("listing_id"))
        .withColumn("total_listing_count", F.col("host_total_listings_count"))
        .withColumn(
            "ingestion_ts",
            F.coalesce(
                F.col("rent_metrics_ingestion_ts"),
                F.col("host_metrics_ingestion_ts"),
                F.col("host_details_ingestion_ts"),
                F.current_timestamp(),
            ),
        )
    )

    review_base = review_df.select(
        F.col("listing_id").cast("int").alias("listing_id"),
        F.col("reviewer_id").cast("int").alias("reviewer_key"),
        F.col("comment"),
        F.col("language"),
        F.col("likes_count").alias("like_count"),
        F.col("sentiment_score"),
        F.col("ingestion_ts").alias("review_ingestion_ts"),
    )

    review_counts = review_df.groupBy(
        F.col("listing_id").cast("int").alias("listing_id")
    ).agg(F.count(F.lit(1)).cast("int").alias("number_of_reviews_ltm"))

    fact_review_raw = (
        review_base.join(review_counts, on="listing_id", how="left")
        .withColumn("property_key", F.col("listing_id"))
        .withColumn("location_key", F.col("listing_id"))
        .withColumn("host_key", F.col("listing_id"))
        .withColumn(
            "ingestion_ts",
            F.coalesce(F.col("review_ingestion_ts"), F.current_timestamp()),
        )
    )

    neighbourhood_base = (
        location_base.join(rent_metrics_base, on="listing_id", how="left")
        .join(host_metrics_base, on="listing_id", how="left")
        .join(review_counts, on="listing_id", how="left")
        .join(
            review_base.groupBy("listing_id").agg(
                F.avg("sentiment_score").alias("avg_sentiment_score")
            ),
            on="listing_id",
            how="left",
        )
    )

    agg_neighborhood_performance_raw = (
        neighbourhood_base.groupBy(F.col("neighbourhood"))
        .agg(
            F.countDistinct("listing_id").alias("total_active_listings"),
            F.avg("price_per_night").alias("avg_price_per_night"),
            F.avg("estimated_revenue_l365d").alias("avg_annual_revenue"),
            F.avg("estimated_occupancy_l365d").alias("avg_occupancy_rate"),
            F.sum(F.coalesce(F.col("number_of_reviews_ltm"), F.lit(0))).alias(
                "total_reviews_ltm"
            ),
        )
        .withColumn("ingestion_ts", F.current_timestamp())
    )

    agg_host_economics_raw = (
        host_details_base.join(rent_metrics_base, on="listing_id", how="left")
        .join(host_metrics_base, on="listing_id", how="left")
        .groupBy(F.col("room_type"))
        .agg(
            F.countDistinct("listing_id").alias("total_listings"),
            F.avg("price_per_night").alias("avg_price_per_night"),
            F.avg("estimated_revenue_l365d").alias("avg_annual_revenue"),
            F.avg("host_acceptance_rate").alias("avg_acceptance_rate"),
            F.avg("host_response_rate").alias("avg_response_rate"),
        )
        .withColumn("ingestion_ts", F.current_timestamp())
    )

    agg_neighbourhood_invest_raw = (
        neighbourhood_base.groupBy(
            F.col("city").alias("city_name"),
            F.col("neighbourhood").alias("neighbourhood_name"),
        )
        .agg(
            F.countDistinct("listing_id").alias("neighbourhood_most_views"),
            F.sum(
                F.when(
                    F.col("price_per_night") > 0,
                    F.col("estimated_revenue_l365d") / F.col("price_per_night"),
                ).otherwise(F.lit(0.0))
            )
            .cast("int")
            .alias("neighbourhood_most_bookings"),
            F.sum(F.coalesce(F.col("number_of_reviews_ltm"), F.lit(0)))
            .cast("int")
            .alias("neighbourhood_highest_reviews"),
            F.avg("price_per_night").alias("avg_order_price"),
            F.avg("estimated_revenue_l365d").alias("avg_revenue_l365d"),
            F.expr("percentile_approx(availability_365, 0.5)")
            .cast("float")
            .alias("median_availability"),
            F.avg("host_response_rate").alias("avg_response_rate"),
        )
        .withColumn(
            "neighbourhood_needed_boost",
            F.array(
                F.when(
                    F.col("avg_revenue_l365d") > F.lit(20000), F.lit("low")
                ).otherwise(F.lit("high"))
            ),
        )
        .withColumn("ingestion_ts", F.current_date())
    )

    outputs: dict[str, DataFrame] = {
        "dimHost": _cast_to_schema(dim_host_raw, GOLD_SCHEMAS["dimHost"]),
        "dimProperty": _cast_to_schema(dim_property_raw, GOLD_SCHEMAS["dimProperty"]),
        "dimLocation": _cast_to_schema(dim_location_raw, GOLD_SCHEMAS["dimLocation"]),
        "factListing": _cast_to_schema(fact_listing_raw, GOLD_SCHEMAS["factListing"]),
        "factReview": _cast_to_schema(fact_review_raw, GOLD_SCHEMAS["factReview"]),
        "AggNeighbourhoodInvest": _cast_to_schema(
            agg_neighbourhood_invest_raw, GOLD_SCHEMAS["AggNeighbourhoodInvest"]
        ),
        "AggNeighborhoodPerformance": _cast_to_schema(
            agg_neighborhood_performance_raw,
            GOLD_SCHEMAS["AggNeighborhoodPerformance"],
        ),
        "AggHostEconomics": _cast_to_schema(
            agg_host_economics_raw, GOLD_SCHEMAS["AggHostEconomics"]
        ),
    }

    ads_ready = all(
        _prefix_has_data(storage, SILVER_BUCKET, prefix)
        for prefix in REQUIRED_ADS_PREFIXES
    )

    if ads_ready:
        provider_details_df = storage.read_files(
            f"s3a://{SILVER_BUCKET}/providerDetails.parquet"
        )
        provider_metrics_df = storage.read_files(
            f"s3a://{SILVER_BUCKET}/providerMetrics.parquet"
        )

        provider_details_base = provider_details_df.select(
            F.col("id").cast("int").alias("provider_id"),
            F.col("name"),
            F.col("ingestion_ts").alias("provider_details_ingestion_ts"),
        ).dropDuplicates(["provider_id"])

        provider_metrics_base = provider_metrics_df.select(
            F.col("id").cast("int").alias("provider_id"),
            F.col("avg_conversion_rate"),
            F.col("post_amount"),
            F.col("total_impressions"),
            F.col("total_clicks"),
            F.col("ctr"),
            F.col("total_ad_spend"),
            F.col("return_on_investment"),
            F.col("region_coverage"),
            F.col("churn_rate"),
            F.col("avg_cpc"),
            F.col("avg_cpm"),
            F.col("campaigns_count"),
            F.col("data_compliance_level"),
            F.col("partition_date"),
            F.col("ingestion_ts").alias("provider_metrics_ingestion_ts"),
        ).dropDuplicates(["provider_id"])

        dim_provider_raw = (
            provider_details_base.withColumn("provider_key", F.col("provider_id"))
            .filter(F.col("provider_key").isNotNull())
            .withColumn("start_date", F.current_timestamp())
            .withColumn("end_date", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .withColumn(
                "ingestion_ts",
                F.coalesce(
                    F.col("provider_details_ingestion_ts"), F.current_timestamp()
                ),
            )
        )

        fact_ad_raw = (
            provider_metrics_base.join(
                provider_details_base, on="provider_id", how="left"
            )
            .withColumn("provider_key", F.col("provider_id"))
            .withColumn(
                "ingestion_ts",
                F.coalesce(
                    F.col("provider_metrics_ingestion_ts"),
                    F.col("provider_details_ingestion_ts"),
                    F.current_timestamp(),
                ),
            )
        )

        agg_ads_raw = fact_ad_raw.select(
            F.col("provider_key").alias("provider_id"),
            F.col("name").alias("provider_name"),
            F.col("avg_conversion_rate").alias("provider_conversion_rate"),
            F.col("ctr").alias("most_clicked_provider"),
            F.col("post_amount"),
            F.col("avg_conversion_rate").alias("avg_campaign_conversion"),
            F.when(
                F.col("campaigns_count") > 0,
                F.col("total_ad_spend") / F.col("campaigns_count"),
            )
            .otherwise(F.lit(0.0))
            .alias("avg_campaign_cost"),
            (F.col("total_clicks") * F.col("avg_conversion_rate"))
            .cast("int")
            .alias("customer_gained"),
            F.col("ctr").alias("conversion_rate"),
            (F.col("avg_conversion_rate") * F.lit(100))
            .cast("int")
            .alias("new_customers_l30d_pct"),
            F.current_timestamp().alias("ingestion_ts"),
        )

        outputs["dimProvider"] = _cast_to_schema(
            dim_provider_raw, GOLD_SCHEMAS["dimProvider"]
        )
        outputs["factAd"] = _cast_to_schema(fact_ad_raw, GOLD_SCHEMAS["factAd"])
        outputs["AggAds"] = _cast_to_schema(agg_ads_raw, GOLD_SCHEMAS["AggAds"])
    else:
        logger.info(
            "Skipping ads gold outputs: missing at least one required silver ads input %s",
            REQUIRED_ADS_PREFIXES,
        )

    for table_name, table_df in outputs.items():
        output_key = f"{table_name}.parquet"
        logger.info(
            "Writing gold table %s to s3a://%s/%s", table_name, GOLD_BUCKET, output_key
        )
        storage.bucket_upload(GOLD_BUCKET, output_key, table_df)


def main() -> None:
    with SparkSessionManager("airbnb-silver-to-gold") as spark:
        storage = S3StorageHandler(spark)
        storage.create_buckets([GOLD_BUCKET])

        wait_for_silver_inputs(storage, SILVER_BUCKET)
        build_gold_listing_review(storage)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
