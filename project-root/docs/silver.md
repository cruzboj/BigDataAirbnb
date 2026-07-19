```mermaid

erDiagram
    HostDetails{
        int id PK
        string host_id
        int listing_id FK

        string host_url
        string host_profile_id
        string host_profile_url
        string host_name
        string host_thumbnail_url
        string host_neighbourhood 
        int host_total_listings_count

        string host_location 
        string host_about
        bool host_is_superhost
        string host_picture_url
        list host_verifications "[email,phone,work_email]"
        bool host_has_profile_pic 
        bool host_identity_verified
        string property_type
        list room_type "[Entire home/apt, Private room ,Shared room]" 

        datetime ingestion_ts
    }
    
    HostMetrics{
        int id PK "(FK listing_id from hostDetails)"

        list host_response_time "[within an hour, within a day, a few days or more , N/A]"
        float host_response_rate 
        int host_acceptance_rate

        datetime ingestion_ts
    }

    RentDetails{
        int id PK "(FK listing_id from hostDetails)"

        int accommodates
        int bathrooms
        string bathrooms_text
        int bedrooms
        int beds
        list amenities
        int minimum_nights
        int maximum_nights
        int minimum_minimum_nights
        int maximum_minimum_nights
        int minimum_maximum_nights
        int maximum_maximum_nights
        int minimum_nights_avg_ntm
        int maximum_nights_avg_ntm
        string license

        datetime ingestion_ts
    }

    RentMetrics{
        int id PK "(FK listing_id from hostDetails)"
        
        float price_per_night

        bool has_availability
        int estimated_occupancy_l365d
        int estimated_revenue_l365d
        int availability_30
        int availability_60
        int availability_90
        int availability_365 
        int availability_eoy
        bool instant_bookable
        int calculated_host_listings_count
        int calculated_host_listings_count_entire_homes
        int calculated_host_listings_count_private_rooms
        int calculated_host_listings_count_shared_rooms
        
        datetime ingestion_ts
    }

    Location{
        int id PK "(FK listing_id from hostDetails)"
        
        string country
        string city
        
        string listing_url
        string name
        string description
        string neighbourhood
        string neighborhood_overview
        string picture_url
        location latitude
        location longitude
        string host_neighbourhood 

        datetime ingestion_ts
    }

    Review{
        int id PK "(FK listing_id from hostDetails)"
        
        datetime date   
        datetime start_date
        datetime end_date
        bool is_current
        int number_of_reviews_ltm
        float review_scores_rating
        float review_scores_accuracy
        float review_scores_cleanliness

        int reviewer_id 
        string reviewer_name    
        string comment
        string language
        float sentiment_score
        list sentiment_label "[positive,negative,neutral]"
        int likes_count 

        datetime ingestion_ts
    }

    Metadata {
        int id PK
        
        string source
        string scrape_id
        datetime last_scraped
        datetime calendar_last_scraped

        datetime ingestion_ts
    }

    providerDetails{
        int id PK
        
        string name
        string desc
        int price_per_ad
        int n_listings
        datetime last_updated

        datetime ingestion_ts
    }

    providerMetrics{
        int id PK "FK from providerDetails"
        
        float avg_conversion_rate
        int post_amount
        long total_impressions
        long total_clicks
        float ctr
        float total_ad_spend
        float return_on_investment
        string region_coverage

        float churn_rate
        float avg_cpc "(Cost Per Click)"
        float avg_cpm "(Cost Per Mille)"

        int campaigns_count
        string data_compliance_level "(GDPR/CCPA)"
        datetime partition_date 

        datetime ingestion_ts
    }
