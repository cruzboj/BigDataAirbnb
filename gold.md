```mermaid

erDiagram
    %% Business Question: What cities are best invest promoting in?
    %% Business Question: Which promotion strategy is the best?
    
     factListing ||--|| dimLocation : "type 0"
     factListing }o--|| dimHost: "type 2"
     factListing ||--|| dimProperty: "type 2"
     factReview ||--|| dimReviewer: "type 0"
     factReview }o--|| dimHost: "type 2"
     factReview ||--|| dimLocation : "type 0"
     factReview ||--|| dimProperty: "type 2"
     factAd ||--|| dimProvider: "type 2"
     
    
    
    factListing{
      int id
      int property_key FK
      int location_key FK
      int host_key FK
      
      int total_listing_count
      float price_per_night
      int estimated_occupancy_l365d
      int estimated_revenue_l365d
      float host_response_rate 
      int host_acceptance_rate
    }
    
    dimProperty{
      datetime start_date
      datetime end_date
      bool is_current
      int property_key
      list room_type
      string property_type
      int bedrooms
      int beds
      list ameneties
      int accommodates
    }
    
    dimLocation{
      int location_key PK

      string neighbourhood
      location latitude
      location longitude
      string host_neighbourhood 
    }
    
    dimHost {
      int host_key PK
      
      datetime start_date
      datetime end_date
      bool is_current
      string name
      string response_time_category
    }
    
    factReview{
      int reviewer_key FK
      int property_key FK
      int location_key FK
      int host_key FK
     
     string comment
     string language
     int like_count 
     float sentiment_score
     float review_scores_rating
     float review_scores_accuracy
     float review_scores_cleanliness
     int number_of_reviews_ltm
 
    }
    
    dimReviewer{
      int reviewer_key
      int number_of_reviews
      string reviewer_name    
    }
    
    dimProvider{
      int provider_key
      
      datetime start_date
      datetime end_date
      bool is_current
      string name
      string desc
    }
    
    factAd{
      int provider_key FK
      
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
    }

    AggNeighbourhoodInvest{
        string city_name 
        string neighbourhood_name
        int neighbourhood_most_views
        int neighbourhood_most_bookings "est_revenue_365 / price_per_night"
        int neighbourhood_highest_reviews
        list neighbourhood_needed_boost
        float avg_order_price
        float avg_revenue_l365d
        float avg_review_score
        float median_availability
        float avg_response_rate
    }
    
    AggAds{
        int provider_id PK
        string provider_name

        float provider_conversion_rate
        float most_clicked_provider
        int post_amount
        float avg_campaign_conversion
        float avg_campaign_cost
        int customer_gained
        float conversion_rate
        int new_customers_l30d_pct 
    }


    AggNeighborhoodPerformance {
        string neighbourhood PK "Grouped by dimLocation.neighbourhood"
        
        int total_active_listings "COUNT(factListing.id)"
        float avg_price_per_night "AVG(fact_airbnb.price)"
        float avg_annual_revenue "AVG(factListing.estimated_revenue_l365d)"
        float avg_occupancy_rate "AVG(factListing.estimated_occupancy_l365d)"
        float avg_review_score "AVG(factReview.review_scores_rating)"
        int total_reviews_ltm "SUM(factReview.number_of_reviews_ltm) - indicates current hype"
    }

    AggHostEconomics {
        string room_type PK 
        
        int total_listings "COUNT(factListing.id)"
        float avg_price_per_night "AVG(factListing.price)"
        float avg_annual_revenue "AVG(factListing.estimated_revenue_l365d)"
        float avg_acceptance_rate "AVG(factListing.host_acceptance_rate)"
        float avg_response_rate "AVG(factListing.host_response_rate)"
    }

    %% MACHINE LEARNING REQUIREMENTS
    %% 1. Business Problem: Predict if a listing can command a "Premium Price" (Top 20% in its neighborhood) based on property features, availability, and host reputation.
    %% 2. Feature Table: ml_feature_premium_pricing

    ml_feature_premium_pricing {
        int listing_key FK 
        int property_key FK 
        int location_key FK 
        int host_key FK 
        
        %% Features (X)
        string property_type "From dimProperty"
        int accommodates "From dimProperty"
        int bedrooms "From dimProperty"
        int amenities_count "Derived: Count of items in dimProperty.amenities"
        int availability_30 "From factListing"
        float host_acceptance_rate "From factListing"
        string response_time_category "From dimHost"
        float avg_review_scores_rating "Aggregated from AggNeighborhoodPerformance"
        
        %% Target Variable / Label (Y)
        bool is_premium_target "Label: 1 if price > 80th percentile of location, else 0"
    }
