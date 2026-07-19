from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    lit,
    regexp_replace,
    trim,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def field(name: str, data_type, nullable: bool = True) -> StructField:
    return StructField(name, data_type, nullable)


STRING_ARRAY = ArrayType(StringType(), containsNull=True)


bronze_listing_schema = StructType(
    [
        field("id", IntegerType()),
        field("listing_url", StringType()),
        field("scrape_id", StringType()),
        field("last_scraped", TimestampType()),
        field("source", StringType()),
        field("name", StringType()),
        field("description", StringType()),
        field("neighborhood_overview", StringType()),
        field("picture_url", StringType()),
        field("host_id", StringType()),
        field("host_url", StringType()),
        field("host_name", StringType()),
        field("host_since", TimestampType()),
        field("host_location", StringType()),
        field("host_about", StringType()),
        field("host_response_time", StringType()),
        field("host_response_rate", StringType()),
        field("host_acceptance_rate", StringType()),
        field("host_is_superhost", StringType()),
        field("host_thumbnail_url", StringType()),
        field("host_picture_url", StringType()),
        field("host_neighbourhood", StringType()),
        field("host_listings_count", IntegerType()),
        field("host_total_listings_count", IntegerType()),
        field("host_verifications", STRING_ARRAY),
        field("host_has_profile_pic", StringType()),
        field("host_identity_verified", StringType()),
        field("neighbourhood", StringType()),
        field("neighbourhood_cleansed", StringType()),
        field("neighbourhood_group_cleansed", StringType()),
        field("latitude", DoubleType()),
        field("longitude", DoubleType()),
        field("property_type", StringType()),
        field("room_type", StringType()),
        field("accommodates", IntegerType()),
        field("bathrooms", IntegerType()),
        field("bathrooms_text", StringType()),
        field("bedrooms", IntegerType()),
        field("beds", IntegerType()),
        field("amenities", STRING_ARRAY),
        field("price", StringType()),
        field("minimum_nights", IntegerType()),
        field("maximum_nights", IntegerType()),
        field("minimum_minimum_nights", IntegerType()),
        field("maximum_minimum_nights", IntegerType()),
        field("minimum_maximum_nights", IntegerType()),
        field("maximum_maximum_nights", IntegerType()),
        field("minimum_nights_avg_ntm", FloatType()),
        field("maximum_nights_avg_ntm", FloatType()),
        field("calendar_updated", TimestampType()),
        field("has_availability", StringType()),
        field("availability_30", IntegerType()),
        field("availability_60", IntegerType()),
        field("availability_90", IntegerType()),
        field("availability_365", IntegerType()),
        field("calendar_last_scraped", TimestampType()),
        field("number_of_reviews", IntegerType()),
        field("number_of_reviews_ltm", IntegerType()),
        field("number_of_reviews_l30d", IntegerType()),
        field("availability_eoy", IntegerType()),
        field("number_of_reviews_ly", IntegerType()),
        field("estimated_occupancy_l365d", IntegerType()),
        field("estimated_revenue_l365d", IntegerType()),
        field("first_review", TimestampType()),
        field("last_review", TimestampType()),
        field("review_scores_rating", FloatType()),
        field("review_scores_accuracy", FloatType()),
        field("review_scores_cleanliness", FloatType()),
        field("review_scores_checkin", FloatType()),
        field("review_scores_communication", FloatType()),
        field("review_scores_location", FloatType()),
        field("review_scores_value", FloatType()),
        field("license", StringType()),
        field("instant_bookable", StringType()),
        field("calculated_host_listings_count", IntegerType()),
        field("calculated_host_listings_count_entire_homes", IntegerType()),
        field("calculated_host_listings_count_private_rooms", IntegerType()),
        field("calculated_host_listings_count_shared_rooms", IntegerType()),
        field("reviews_per_month", FloatType()),
    ]
)


bronze_reviews_schema = StructType(
    [
        field("listing_id", LongType()),
        field("id", LongType()),
        field("date", TimestampType()),
        field("reviewer_id", LongType()),
        field("reviewer_name", StringType()),
        field("comments", StringType()),
        field("language", StringType()),
        field("sentiment_score", FloatType()),
        field("sentiment_label", STRING_ARRAY),
        field("likes_votes", IntegerType()),
        field("event_ingestion_time", TimestampType()),
        field("raw_user_agent", StringType()),
        field("bot_suspicion_score", FloatType()),
        field("reviewer_hash_id", StringType()),
        field("aspect_sentiment_json", StringType()),
        field("extracted_keywords", StringType()),
        field("comment_character_count", IntegerType()),
        field("readability_index", FloatType()),
        field("session_id", StringType()),
        field("time_spent_on_review_ms", IntegerType()),
        field("contains_media", BooleanType()),
        field("ingestion_ts", TimestampType()),
    ]
)


bronze_ads_schema = StructType(
    [
        field("id", IntegerType()),
        field("name", StringType()),
        field("price", IntegerType()),
        field("n_listings", IntegerType()),
        field("last_updated", TimestampType()),
        field("avg_conversion_rate", FloatType()),
        field("post_amount", IntegerType()),
        field("total_impressions", LongType()),
        field("total_clicks", LongType()),
        field("ctr", FloatType()),
        field("total_ad_spend", FloatType()),
        field("return_on_investment", FloatType()),
        field("target_demographics", StringType()),
        field("region_coverage", StringType()),
        field("reliability_score", FloatType()),
        field("api_endpoint_latency", StringType()),
        field("churn_rate_provider", FloatType()),
        field("avg_cpc", FloatType()),
        field("avg_cpm", FloatType()),
        field("audience_interest_tags", StringType()),
        field("fraud_traffic_percent", FloatType()),
        field("attribution_window", StringType()),
        field("concurrent_campaigns_count", IntegerType()),
        field("market_share_percentage", FloatType()),
        field("data_compliance_level", StringType()),
        field("partition_date", TimestampType()),
        field("ingestion_ts", TimestampType()),
    ]
)


silver_host_details_schema = StructType(
    [
        field("id", IntegerType()),
        field("host_id", StringType()),
        field("listing_id", IntegerType()),
        field("host_url", StringType()),
        field("host_profile_id", StringType()),
        field("host_profile_url", StringType()),
        field("host_name", StringType()),
        field("host_thumbnail_url", StringType()),
        field("host_neighbourhood", StringType()),
        field("host_total_listings_count", IntegerType()),
        field("host_location", StringType()),
        field("host_about", StringType()),
        field("host_is_superhost", BooleanType()),
        field("host_picture_url", StringType()),
        field("host_verifications", STRING_ARRAY),
        field("host_has_profile_pic", BooleanType()),
        field("host_identity_verified", BooleanType()),
        field("property_type", StringType()),
        field("room_type", StringType()),
        field("ingestion_ts", TimestampType()),
    ]
)


silver_host_metrics_schema = StructType(
    [
        field("id", IntegerType()),
        field("host_response_time", StringType()),
        field("host_response_rate", FloatType()),
        field("host_acceptance_rate", IntegerType()),
        field("ingestion_ts", TimestampType()),
    ]
)


silver_rent_details_schema = StructType(
    [
        field("id", IntegerType()),
        field("accommodates", IntegerType()),
        field("bathrooms", IntegerType()),
        field("bathrooms_text", StringType()),
        field("bedrooms", IntegerType()),
        field("beds", IntegerType()),
        field("amenities", STRING_ARRAY),
        field("minimum_nights", IntegerType()),
        field("maximum_nights", IntegerType()),
        field("minimum_minimum_nights", IntegerType()),
        field("maximum_minimum_nights", IntegerType()),
        field("minimum_maximum_nights", IntegerType()),
        field("maximum_maximum_nights", IntegerType()),
        field("minimum_nights_avg_ntm", FloatType()),
        field("maximum_nights_avg_ntm", FloatType()),
        field("license", StringType()),
        field("ingestion_ts", TimestampType()),
    ]
)


silver_rent_metrics_schema = StructType(
    [
        field("id", IntegerType()),
        field("price_per_night", FloatType()),
        field("has_availability", BooleanType()),
        field("estimated_occupancy_l365d", IntegerType()),
        field("estimated_revenue_l365d", IntegerType()),
        field("availability_30", IntegerType()),
        field("availability_60", IntegerType()),
        field("availability_90", IntegerType()),
        field("availability_365", IntegerType()),
        field("availability_eoy", IntegerType()),
        field("instant_bookable", BooleanType()),
        field("calculated_host_listings_count", IntegerType()),
        field("calculated_host_listings_count_entire_homes", IntegerType()),
        field("calculated_host_listings_count_private_rooms", IntegerType()),
        field("calculated_host_listings_count_shared_rooms", IntegerType()),
        field("ingestion_ts", TimestampType()),
    ]
)


silver_location_schema = StructType(
    [
        field("id", IntegerType()),
        field("country", StringType()),
        field("city", StringType()),
        field("listing_url", StringType()),
        field("name", StringType()),
        field("description", StringType()),
        field("neighbourhood", StringType()),
        field("neighborhood_overview", StringType()),
        field("picture_url", StringType()),
        field("latitude", DoubleType()),
        field("longitude", DoubleType()),
        field("host_neighbourhood", StringType()),
        field("ingestion_ts", TimestampType()),
    ]
)


silver_review_schema = StructType(
    [
        field("id", IntegerType()),
        field("date", TimestampType()),
        field("start_date", TimestampType()),
        field("end_date", TimestampType()),
        field("is_current", BooleanType()),
        field("first_review", TimestampType()),
        field("last_review", TimestampType()),
        field("number_of_reviews", IntegerType()),
        field("number_of_reviews_ltm", IntegerType()),
        field("number_of_reviews_l30d", IntegerType()),
        field("number_of_reviews_ly", IntegerType()),
        field("review_scores_rating", FloatType()),
        field("review_scores_accuracy", FloatType()),
        field("review_scores_cleanliness", FloatType()),
        field("review_scores_checkin", FloatType()),
        field("review_scores_communication", FloatType()),
        field("review_scores_location", FloatType()),
        field("review_scores_value", FloatType()),
        field("reviews_per_month", FloatType()),
        field("reviewer_id", IntegerType()),
        field("reviewer_name", StringType()),
        field("comment", StringType()),
        field("language", StringType()),
        field("sentiment_score", FloatType()),
        field("sentiment_label", STRING_ARRAY),
        field("likes_count", IntegerType()),
        field("ingestion_ts", TimestampType()),
    ]
)


silver_metadata_schema = StructType(
    [
        field("id", IntegerType()),
        field("source", StringType()),
        field("scrape_id", StringType()),
        field("last_scraped", TimestampType()),
        field("calendar_last_scraped", TimestampType()),
        field("ingestion_ts", TimestampType()),
    ]
)


silver_provider_details_schema = StructType(
    [
        field("id", IntegerType()),
        field("name", StringType()),
        field("desc", StringType()),
        field("price_per_ad", IntegerType()),
        field("n_listings", IntegerType()),
        field("last_updated", TimestampType()),
        field("ingestion_ts", TimestampType()),
    ]
)


silver_provider_metrics_schema = StructType(
    [
        field("id", IntegerType()),
        field("avg_conversion_rate", FloatType()),
        field("post_amount", IntegerType()),
        field("total_impressions", LongType()),
        field("total_clicks", LongType()),
        field("ctr", FloatType()),
        field("total_ad_spend", FloatType()),
        field("return_on_investment", FloatType()),
        field("region_coverage", StringType()),
        field("churn_rate", FloatType()),
        field("avg_cpc", FloatType()),
        field("avg_cpm", FloatType()),
        field("campaigns_count", IntegerType()),
        field("data_compliance_level", StringType()),
        field("partition_date", TimestampType()),
        field("ingestion_ts", TimestampType()),
    ]
)


gold_fact_listing_schema = StructType(
    [
        field("id", IntegerType()),
        field("property_key", IntegerType()),
        field("location_key", IntegerType()),
        field("host_key", IntegerType()),
        field("total_listing_count", IntegerType()),
        field("price_per_night", FloatType()),
        field("estimated_occupancy_l365d", IntegerType()),
        field("estimated_revenue_l365d", IntegerType()),
        field("host_response_rate", FloatType()),
        field("host_acceptance_rate", IntegerType()),
        field("ingestion_ts", TimestampType()),
    ]
)


gold_dim_property_schema = StructType(
    [
        field("start_date", TimestampType()),
        field("end_date", TimestampType()),
        field("is_current", BooleanType()),
        field("property_key", IntegerType()),
        field("room_type", StringType()),
        field("property_type", StringType()),
        field("bedrooms", IntegerType()),
        field("beds", IntegerType()),
        field("ameneties", STRING_ARRAY),
        field("accommodates", IntegerType()),
        field("ingestion_ts", TimestampType()),
    ]
)


gold_dim_location_schema = StructType(
    [
        field("location_key", IntegerType()),
        field("neighbourhood", StringType()),
        field("latitude", DoubleType()),
        field("longitude", DoubleType()),
        field("host_neighbourhood", StringType()),
        field("country", StringType()),
        field("city", StringType()),
        field("ingestion_ts", TimestampType()),
    ]
)


gold_dim_host_schema = StructType(
    [
        field("host_key", IntegerType()),
        field("start_date", TimestampType()),
        field("end_date", TimestampType()),
        field("is_current", BooleanType()),
        field("name", StringType()),
        field("response_time_category", StringType()),
        field("ingestion_ts", TimestampType()),
    ]
)


gold_fact_review_schema = StructType(
    [
        field("reviewer_key", IntegerType()),
        field("property_key", IntegerType()),
        field("location_key", IntegerType()),
        field("host_key", IntegerType()),
        field("comment", StringType()),
        field("language", StringType()),
        field("like_count", IntegerType()),
        field("sentiment_score", FloatType()),
        field("review_scores_rating", FloatType()),
        field("review_scores_accuracy", FloatType()),
        field("review_scores_cleanliness", FloatType()),
        field("number_of_reviews_ltm", IntegerType()),
        field("ingestion_ts", TimestampType()),
    ]
)


gold_dim_provider_schema = StructType(
    [
        field("provider_key", IntegerType()),
        field("start_date", TimestampType()),
        field("end_date", TimestampType()),
        field("is_current", BooleanType()),
        field("name", StringType()),
        field("desc", StringType()),
        field("ingestion_ts", TimestampType()),
    ]
)


gold_fact_ad_schema = StructType(
    [
        field("provider_key", IntegerType()),
        field("avg_conversion_rate", FloatType()),
        field("post_amount", IntegerType()),
        field("total_impressions", LongType()),
        field("total_clicks", LongType()),
        field("ctr", FloatType()),
        field("total_ad_spend", FloatType()),
        field("return_on_investment", FloatType()),
        field("region_coverage", StringType()),
        field("churn_rate", FloatType()),
        field("avg_cpc", FloatType()),
        field("avg_cpm", FloatType()),
        field("campaigns_count", IntegerType()),
        field("data_compliance_level", StringType()),
        field("partition_date", TimestampType()),
        field("ingestion_ts", TimestampType()),
    ]
)


gold_agg_neighbourhood_invest_schema = StructType(
    [
        field("city_name", StringType()),
        field("neighbourhood_name", StringType()),
        field("neighbourhood_most_views", IntegerType()),
        field("neighbourhood_most_bookings", IntegerType()),
        field("neighbourhood_highest_reviews", IntegerType()),
        field("neighbourhood_needed_boost", STRING_ARRAY),
        field("avg_order_price", FloatType()),
        field("avg_revenue_l365d", FloatType()),
        field("avg_review_score", FloatType()),
        field("median_availability", FloatType()),
        field("avg_response_rate", FloatType()),
        field("ingestion_ts", DateType()),
    ]
)


gold_agg_ads_schema = StructType(
    [
        field("provider_id", IntegerType()),
        field("provider_name", StringType()),
        field("provider_conversion_rate", FloatType()),
        field("most_clicked_provider", FloatType()),
        field("post_amount", IntegerType()),
        field("avg_campaign_conversion", FloatType()),
        field("avg_campaign_cost", FloatType()),
        field("customer_gained", IntegerType()),
        field("conversion_rate", FloatType()),
        field("new_customers_l30d_pct", IntegerType()),
        field("ingestion_ts", TimestampType()),
    ]
)


gold_agg_neighborhood_performance_schema = StructType(
    [
        field("neighbourhood", StringType()),
        field("total_active_listings", IntegerType()),
        field("avg_price_per_night", FloatType()),
        field("avg_annual_revenue", FloatType()),
        field("avg_occupancy_rate", FloatType()),
        field("avg_review_score", FloatType()),
        field("total_reviews_ltm", IntegerType()),
        field("ingestion_ts", TimestampType()),
    ]
)


gold_agg_host_economics_schema = StructType(
    [
        field("room_type", StringType()),
        field("total_listings", IntegerType()),
        field("avg_price_per_night", FloatType()),
        field("avg_annual_revenue", FloatType()),
        field("avg_acceptance_rate", FloatType()),
        field("avg_response_rate", FloatType()),
        field("ingestion_ts", TimestampType()),
    ]
)


gold_ml_feature_premium_pricing_schema = StructType(
    [
        field("listing_key", IntegerType()),
        field("property_key", IntegerType()),
        field("location_key", IntegerType()),
        field("host_key", IntegerType()),
        field("property_type", StringType()),
        field("accommodates", IntegerType()),
        field("bedrooms", IntegerType()),
        field("amenities_count", IntegerType()),
        field("availability_30", IntegerType()),
        field("host_acceptance_rate", FloatType()),
        field("response_time_category", StringType()),
        field("avg_review_scores_rating", FloatType()),
        field("is_premium_target", BooleanType()),
        field("ingestion_ts", TimestampType()),
    ]
)


BRONZE_SCHEMAS = {
    "listing": bronze_listing_schema,
    "reviews": bronze_reviews_schema,
    "adsProviders": bronze_ads_schema,
}


SILVER_SCHEMAS = {
    "HostDetails": silver_host_details_schema,
    "HostMetrics": silver_host_metrics_schema,
    "RentDetails": silver_rent_details_schema,
    "RentMetrics": silver_rent_metrics_schema,
    "Location": silver_location_schema,
    "Review": silver_review_schema,
    "Metadata": silver_metadata_schema,
    "providerDetails": silver_provider_details_schema,
    "providerMetrics": silver_provider_metrics_schema,
}


GOLD_SCHEMAS = {
    "factListing": gold_fact_listing_schema,
    "dimProperty": gold_dim_property_schema,
    "dimLocation": gold_dim_location_schema,
    "dimHost": gold_dim_host_schema,
    "factReview": gold_fact_review_schema,
    "dimProvider": gold_dim_provider_schema,
    "factAd": gold_fact_ad_schema,
    "AggNeighbourhoodInvest": gold_agg_neighbourhood_invest_schema,
    "AggAds": gold_agg_ads_schema,
    "AggNeighborhoodPerformance": gold_agg_neighborhood_performance_schema,
    "AggHostEconomics": gold_agg_host_economics_schema,
    "ml_feature_premium_pricing": gold_ml_feature_premium_pricing_schema,
}


ALL_SCHEMAS = {}
ALL_SCHEMAS.update(BRONZE_SCHEMAS)
ALL_SCHEMAS.update(SILVER_SCHEMAS)
ALL_SCHEMAS.update(GOLD_SCHEMAS)


def get_schema(name: str) -> StructType:
    return ALL_SCHEMAS[name]


def list_schemas() -> tuple[str, ...]:
    return tuple(ALL_SCHEMAS.keys())


def format_schema(schema: StructType) -> StructType:
    """
    CSV datasource does not support ArrayType directly.
    Read array columns as strings first, then parse them back.
    """
    return StructType(
        [
            StructField(
                field.name,
                StringType()
                if isinstance(field.dataType, ArrayType)
                else field.dataType,
                field.nullable,
            )
            for field in schema.fields
        ]
    )


def parse_array_columns(df: DataFrame, target_schema: StructType) -> DataFrame:
    """
    Parse stringified list values (e.g. "['email','phone']") into arrays.
    """
    for field in target_schema.fields:
        if isinstance(field.dataType, ArrayType):
            normalized = regexp_replace(col(field.name), "'", '"')
            df = df.withColumn(
                field.name,
                when(
                    col(field.name).isNull() | (trim(col(field.name)) == ""),
                    lit(None).cast(field.dataType),
                ).otherwise(from_json(normalized, field.dataType)),
            )
    return df
