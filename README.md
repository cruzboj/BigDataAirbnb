```mermaid


erDiagram
    fact_airbnb ||--|| dimRentDetails : "has details"
    fact_airbnb ||--|| dimLocation : "located in"
    fact_airbnb ||--|| dimReview : "has reviews"
    fact_airbnb ||--|| dimDate : "has dates"
    fact_airbnb }|--|| dimHost : "hosted by"

    fact_airbnb{
        int id PK
        int host_id FK
        bool has_availability
        float price
    }

    dimHost{
        string host_id PK

        string host_url
        string host_profile_id
        string host_profile_url
        string host_name

        list host_response_time "[within an hour, within a day, a few days or more , N/A]"
        float host_response_rate 
        string host_thumbnail_url
        string host_neighbourhood 
        int host_total_listings_count
        int hosts_time_as_user_years
        int hosts_time_as_user_months
        int hosts_time_as_host_months
        int hosts_time_as_host_years
        string host_location 
        string host_about
        int host_acceptance_rate
        bool host_is_superhost
        string host_picture_url
        list host_verifications "[email,phone,work_email]"
        string host_has_profile_pic 
        string host_identity_verified
        string host_property_type
        list host_room_type "[Entire home/apt, Private room ,Shared room]" 
    }

    dimRentDetails{
        int id PK

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
        int availability_30
        int availability_60
        int availability_90
        int availability_365 
        int availability_eoy
        int estimated_occupancy_l365d
        int estimated_revenue_l365d
        string license
        bool instant_bookable
        int calculated_host_listings_count
        int calculated_host_listings_count_entire_homes
        int calculated_host_listings_count_private_rooms
        int calculated_host_listings_count_shared_rooms
    }

    dimLocation{
        int id PK

        string listing_url
        string scrape_id
        string source
        string name
        string description
        string neighbourhood
        string neighborhood_overview
        string picture_url
        string host_latitude
        string host_longitude
        string host_neighbourhood 
    }

    dimReview {
        int id PK

        int number_of_reviews
        int number_of_reviews_ltm
        int number_of_reviews_l30d
        int number_of_reviews_ly 
        float review_scores_rating
        float review_scores_accuracy
        float review_scores_cleanliness
        float review_scores_checkin
        float review_scores_communication
        float review_scores_location
        float review_scores_value
        float reviews_per_month
    }

    dimDate {
        int id PK

        datetime last_scraped
        datetime first_review
        datetime last_review
        datetime calendar_last_scraped
    }