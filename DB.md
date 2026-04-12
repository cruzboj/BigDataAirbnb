```mermaid

erDiagram
    DB {
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
        string host_since "missing"
        int hosts_time_as_user_years
        int hosts_time_as_user_months
        int hosts_time_as_host_months
        int hosts_time_as_host_years
        string host_location 
        string host_about
        string host_response_time "missing"
        string host_response_rate "missing"
        string host_acceptance_rate "missing"
        bool host_is_superhost
        string host_thumbnail_url "missing"
        string host_picture_url
        string host_neighbourhood "missing"
        int host_listings_count
        int host_total_listings_count "missing"
        string host_verifications "null"
        string host_has_profile_pic 
        string host_identity_verified
        string host_neighbourhood_cleansed
        string host_neighbourhood_group_cleansed
        string host_latitude
        string host_longitude
        string host_property_type
        list host_room_type "[Entire home/apt, Private room ,Shared room]"

        string accommodates
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
        int estimated_revenue_l365d "missing"
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
        bool instant_bookable missing
        int calculated_host_listings_count
        int calculated_host_listings_count_entire_homes
        int calculated_host_listings_count_private_rooms
        calculated_host_listings_count_shared_rooms
        float reviews_per_month
    }