```mermaid

erDiagram
    listing {
        int id PK
        string listing_url
        string scrape_id
        datetime last_scraped
        string source
        string name
        string description
        string neighborhood_overview
        string picture_url
        
        string host_id
        string host_url
        string host_profile_id
        string host_profile_url
        string host_name
        string host_since

        string host_location 
        string host_about
        string host_response_time "[within an hour, within a day, a few days or more , N/A]"
        float host_response_rate
        int host_acceptance_rate
        bool host_is_superhost
        string host_thumbnail_url
        string host_picture_url
        string host_neighbourhood
        int host_listings_count
        int host_total_listings_count
        list host_verifications "[email,phone,work_email]"
        bool host_has_profile_pic 
        bool host_identity_verified
        string host_neighbourhood_cleansed
        string host_neighbourhood_group_cleansed
        location latitude
        location longitude
        string host_property_type
        list host_room_type "[Entire home/apt, Private room ,Shared room]"

        int accommodates
        int bathrooms
        string bathrooms_text
        int bedrooms
        int bed
        list amenities
        float price
        int minimum_nights
        int maximum_nights
        int minimum_minimum_nights
        int maximum_minimum_nights
        int minimum_maximum_nights
        int maximum_maximum_nights
        int minimum_nights_avg_ntm
        int maximum_nights_avg_ntm
        datetime calendar_updated
        bool has_availability
        int availability_30
        int availability_60
        int availability_90
        int availability_365
        datetime calendar_last_scraped
        int number_of_reviews
        int number_of_reviews_ltm
        int number_of_reviews_l30d
        int availability_eoy
        int number_of_reviews_ly 
        int estimated_occupancy_l365d
        int estimated_revenue_l365d 
        datetime first_review
        datetime last_review
        float review_scores_rating
        float review_scores_accuracy
        float review_scores_cleanliness
        float review_scores_checkin
        float review_scores_communication
        float review_scores_location
        float review_scores_value
        string license
        bool instant_bookable
        int calculated_host_listings_count
        int calculated_host_listings_count_entire_homes
        int calculated_host_listings_count_private_rooms
        int calculated_host_listings_count_shared_rooms
        float reviews_per_month

        string country
        string city
        datetime ingestion_ts
    }

    reviews {
        int listing_id PK
        int id PK
        datetime date   
        int reviewer_id 
        string reviewer_name    
        string comments

        string language
        float sentiment_score
        list sentiment_label "[positive,negative,neutral]"
        int likes_votes 
        timestamp event_ingestion_time
        string raw_user_agent 
        float bot_suspicion_score 
        string reviewer_hash_id 
        string aspect_sentiment_json 
        string extracted_keywords
        int comment_character_count 
        float readability_index 
        string session_id 
        int time_spent_on_review_ms 
        bool contains_media

        datetime ingestion_ts
    }

    adsProviders{
        int id PK
        string name
        int price
        int n_listings
        datetime last_updated
        float avg_conversion_rate
        int post_amount
        
        long total_impressions
        long total_clicks
        float ctr
        float total_ad_spend
        float return_on_investment
        string target_demographics
        string region_coverage
        float reliability_score

        string api_endpoint_latency
        float churn_rate_provider 
        float avg_cpc "(Cost Per Click)"
        float avg_cpm "(Cost Per Mille)"
        string audience_interest_tags
        float fraud_traffic_percent 
        string attribution_window 
        int concurrent_campaigns_count
        float market_share_percentage 
        string data_compliance_level "(GDPR/CCPA)"
        timestamp partition_date 

        datetime ingestion_ts
    }
