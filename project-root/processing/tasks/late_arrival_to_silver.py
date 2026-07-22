import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from processing.const import BRONZE_BUCKET, SILVER_BUCKET
from processing.schema import (
    silver_provider_details_schema,
    silver_provider_metrics_schema,
)
from processing.session import SparkSessionManager
from processing.storage.s3 import S3StorageHandler

logger = logging.getLogger("airbnb.processing.tasks.late_arrival_to_silver")


def _cast_to_schema(df: DataFrame, schema: StructType) -> DataFrame:
    cast_exprs = []
    for field in schema:
        source_expr = F.col(field.name) if field.name in df.columns else F.lit(None)
        cast_exprs.append(source_expr.cast(field.dataType).alias(field.name))

    return df.select(*cast_exprs)


def _clean_adsprovider_bronze(bronze_df: DataFrame) -> DataFrame:
    cleaned = (
        bronze_df.filter(F.col("id").isNotNull())
        .withColumn("id", F.col("id"))
        .withColumn(
            "name",
            F.when(
                F.col("name").isNull() | (F.trim(F.col("name")) == ""),
                F.lit(""),
            ).otherwise(F.trim(F.col("name"))),
        )
        .withColumn(
            "avg_conversion_rate", F.coalesce(F.col("avg_conversion_rate"), F.lit(0.0))
        )
        .withColumn("post_amount", F.coalesce(F.col("post_amount"), F.lit(0)))
        .withColumn(
            "total_impressions", F.coalesce(F.col("total_impressions"), F.lit(0))
        )
        .withColumn("total_clicks", F.coalesce(F.col("total_clicks"), F.lit(0)))
        .withColumn("ctr", F.coalesce(F.col("ctr"), F.lit(0.0)))
        .withColumn("total_ad_spend", F.col("total_ad_spend"))
        .withColumn(
            "return_on_investment",
            F.coalesce(F.col("return_on_investment"), F.lit(0.0)),
        )
        .withColumn(
            "region_coverage",
            F.when(
                F.col("region_coverage").isNull()
                | (F.trim(F.col("region_coverage")) == ""),
                F.lit("unkown"),
            ).otherwise(F.trim(F.col("region_coverage"))),
        )
        .withColumn("churn_rate", F.col("churn_rate_provider"))
        .withColumn("avg_cpc", F.coalesce(F.col("avg_cpc"), F.lit(0.0)))
        .withColumn("avg_cpm", F.coalesce(F.col("avg_cpm"), F.lit(0.0)))
        .withColumn(
            "campaigns_count",
            F.coalesce(F.col("concurrent_campaigns_count"), F.lit(0)),
        )
        .withColumn(
            "data_compliance_level",
            F.when(
                F.col("data_compliance_level").isNull()
                | (F.trim(F.col("data_compliance_level")) == ""),
                F.lit("unkown"),
            ).otherwise(F.trim(F.col("data_compliance_level"))),
        )
        .withColumn(
            "partition_date",
            F.coalesce(
                F.to_timestamp(F.col("partition_date")),
                F.to_timestamp(F.col("last_updated")),
                F.current_timestamp(),
            ),
        )
        .withColumn(
            "ingestion_ts", F.coalesce(F.col("ingestion_ts"), F.current_timestamp())
        )
    )

    return cleaned


def main() -> None:
    with SparkSessionManager("airbnb-late-arrival-to-silver") as spark:
        storage = S3StorageHandler(spark)

        bronze_path = f"s3a://{BRONZE_BUCKET}/adsprovider.parquet"
        logger.info("Reading late-arrival bronze ads providers from %s", bronze_path)

        bronze_df = storage.read_files(bronze_path)

        cleaned_df = _clean_adsprovider_bronze(bronze_df)
        cleaned_count = cleaned_df.count()

        if cleaned_count == 0:
            logger.info(
                "No late-arrival rows to write after cleaning. Skipping uploads."
            )
            return

        logger.info(
            "Writing %d cleaned late-arrival row(s) to silver tables", cleaned_count
        )

        provider_details_df = _cast_to_schema(
            cleaned_df,
            silver_provider_details_schema,
        )

        provider_metrics_df = _cast_to_schema(
            cleaned_df,
            silver_provider_metrics_schema,
        )

        storage.bucket_upload(
            SILVER_BUCKET, "providerDetails.parquet", provider_details_df
        )
        storage.bucket_upload(
            SILVER_BUCKET, "providerMetrics.parquet", provider_metrics_df
        )

        logger.info(
            "Uploaded providerDetails and providerMetrics silver parquet outputs."
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
