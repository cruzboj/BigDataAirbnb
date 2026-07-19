from pyspark.sql import DataFrame
from pyspark.sql.column import Column
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    lit,
    lower,
    monotonically_increasing_id,
    regexp_replace,
    to_timestamp,
    trim,
    when,
)


class ListingProcessor:
    """Spark-based cleanup and normalization for Airbnb listing records."""

    CLEANED_COLUMNS = [
        "id",
        "description",
        "neighborhood_overview",
        "price",
        "host_response_rate",
        "host_acceptance_rate",
        "host_is_superhost",
        "host_has_profile_pic",
        "host_identity_verified",
        "has_availability",
        "instant_bookable",
        "last_scraped",
        "host_since",
        "calendar_last_scraped",
        "first_review",
        "last_review",
    ]

    BOOLEAN_COLUMNS = [
        "host_is_superhost",
        "host_has_profile_pic",
        "host_identity_verified",
        "has_availability",
        "instant_bookable",
    ]

    TIMESTAMP_COLUMNS = [
        "last_scraped",
        "host_since",
        "calendar_last_scraped",
        "first_review",
        "last_review",
        "ingestion_ts",
    ]

    def __init__(self, dataframe: DataFrame):
        base_df = dataframe.withColumn("__row_id", monotonically_increasing_id())
        self.raw_df = base_df
        self.df = base_df

    def process(self) -> DataFrame:
        """Run all cleanup steps and return a cleaned dataframe."""
        return (
            self._clean_text_columns()
            ._normalize_price()
            ._normalize_rates()
            ._normalize_booleans()
            ._normalize_timestamps()
            ._apply_sanity_checks()
            .df
        )

    def validate_cleaned_data(self) -> None:
        """Validate cleaned listing data and print a sample of cleaned columns."""
        total_rows = self.df.count()
        print(f"\n=== Validation: total rows = {total_rows} ===")

        print("\n=== Validation: null counts for cleaned columns ===")
        null_agg_exprs = [
            count(when(col(column_name).isNull(), 1)).alias(f"{column_name}_nulls")
            for column_name in self.CLEANED_COLUMNS
        ]
        self.df.select(*null_agg_exprs).show(truncate=False)

        print("\n=== Validation: transform null counts (raw present, cleaned null) ===")
        joined_df = self.df.alias("clean").join(
            self.raw_df.alias("raw"), on="__row_id", how="inner"
        )

        source_missing_tokens = ["n/a", "na", "null", "none"]
        transform_null_agg_exprs = []
        for column_name in self.CLEANED_COLUMNS:
            raw_value = trim(col(f"raw.{column_name}").cast("string"))
            clean_value = col(f"clean.{column_name}")

            source_missing = (
                col(f"raw.{column_name}").isNull()
                | (raw_value == "")
                | lower(raw_value).isin(*source_missing_tokens)
            )

            transform_null_agg_exprs.append(
                count(when((~source_missing) & clean_value.isNull(), 1)).alias(
                    f"{column_name}_transform_nulls"
                )
            )

        joined_df.select(*transform_null_agg_exprs).show(truncate=False)

        print("\n=== Validation: 20 cleaned rows ===")
        self.df.select(*self.CLEANED_COLUMNS).show(20, truncate=False)

    def _clean_text_columns(self) -> "ListingProcessor":
        html_break_regex = r"<br\\s*/?>"

        self.df = self.df.withColumn(
            "description",
            trim(regexp_replace(col("description"), html_break_regex, " ")),
        ).withColumn(
            "neighborhood_overview",
            trim(regexp_replace(col("neighborhood_overview"), html_break_regex, " ")),
        )

        self.df = self.df.withColumn(
            "neighborhood_overview",
            when(
                col("neighborhood_overview").isNull()
                | (trim(col("neighborhood_overview")) == ""),
                col("description"),
            ).otherwise(col("neighborhood_overview")),
        )
        return self

    def _normalize_price(self) -> "ListingProcessor":
        raw_price = trim(col("price").cast("string"))

        self.df = self.df.withColumn(
            "price",
            when(
                raw_price.isNull()
                | (raw_price == "")
                | lower(raw_price).isin("n/a", "na", "null", "none"),
                lit(None).cast("double"),
            ).otherwise(self._safe_to_double("price", remove_pattern=r"[$,]")),
        )
        return self

    def _normalize_rates(self) -> "ListingProcessor":
        self.df = self.df.withColumn(
            "host_response_rate",
            self._safe_to_double("host_response_rate", remove_pattern="%", default=0.0),
        ).withColumn(
            "host_acceptance_rate",
            self._safe_to_int("host_acceptance_rate", remove_pattern="%", default=0),
        )
        return self

    def _normalize_booleans(self) -> "ListingProcessor":
        for column_name in self.BOOLEAN_COLUMNS:
            self.df = self.df.withColumn(column_name, self._parse_tf_bool(column_name))
        return self

    def _normalize_timestamps(self) -> "ListingProcessor":
        for column_name in self.TIMESTAMP_COLUMNS:
            self.df = self.df.withColumn(column_name, to_timestamp(col(column_name)))
        return self

    def _apply_sanity_checks(self) -> "ListingProcessor":
        review_score = self._safe_to_double("review_scores_rating")
        self.df = self.df.withColumn(
            "review_scores_rating",
            when((review_score >= 0) & (review_score <= 5), review_score).otherwise(
                lit(None).cast("double")
            ),
        )
        return self

    @staticmethod
    def _safe_to_double(
        column_name: str,
        remove_pattern: str | None = None,
        default: float | None = None,
    ) -> Column:
        value = trim(col(column_name).cast("string"))
        if remove_pattern:
            value = trim(regexp_replace(value, remove_pattern, ""))

        parsed = when(
            value.rlike(r"^[+-]?\d+(\.\d+)?$"),
            value.cast("double"),
        ).otherwise(lit(None).cast("double"))

        if default is not None:
            return coalesce(parsed, lit(float(default)))
        return parsed

    @staticmethod
    def _safe_to_int(
        column_name: str,
        remove_pattern: str | None = None,
        default: int | None = None,
    ) -> Column:
        value = trim(col(column_name).cast("string"))
        if remove_pattern:
            value = trim(regexp_replace(value, remove_pattern, ""))

        parsed = when(
            value.rlike(r"^[+-]?\d+$"),
            value.cast("int"),
        ).otherwise(lit(None).cast("int"))

        if default is not None:
            return coalesce(parsed, lit(int(default)))
        return parsed

    @staticmethod
    def _parse_tf_bool(column_name: str) -> Column:
        value = lower(trim(col(column_name).cast("string")))
        return (
            when(value.isin("t", "true", "1", "yes", "y"), lit(True))
            .when(value.isin("f", "false", "0", "no", "n"), lit(False))
            .otherwise(lit(None).cast("boolean"))
        )
