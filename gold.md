```mermaid

erDiagram
    %% Business Question: What cities are best invest promoting in?
    
    neighbourhoodInvestMetrics {
        int neighbourhood_id PK
        string city_name 
        string neighbourhood_name
        int neighbourhood_most_views
        int neighbourhood_most_bookings
        int neighbourhood_highest_reviews
        list neighbourhood_needed_boost
        float avg_order_price
        float avg_revenue_l30d
        float avg_review_score
        float median_availability
        float avg_response_rate
    }
    
    adsMetrics{
        int provider_id PK
        string provider_name

        float provider_conversion_rate
        float most_clicked_age_group
        int post_amount
        float avg_campaign_conversion
        float avg_campaign_cost
        int customer_gained
        float conversion_rate
        int new_customers_l30d_pct 
    }


    gold_agg_neighborhood_performance {
        string neighbourhood PK "Grouped by dimLocation.neighbourhood"
        
        int total_active_listings "COUNT(fact_airbnb.id)"
        float avg_price_per_night "AVG(fact_airbnb.price)"
        float avg_annual_revenue "AVG(dimRentMetrics.estimated_revenue_l365d)"
        float avg_occupancy_rate "AVG(dimRentMetrics.estimated_occupancy_l365d)"
        float avg_review_score "AVG(dimReview.review_scores_rating)"
        int total_reviews_ltm "SUM(dimReview.number_of_reviews_ltm) - indicates current hype"
    }

    gold_agg_host_economics {
        string room_type PK "Grouped by dimHost.host_room_type"
        
        int total_listings "COUNT(fact_airbnb.id)"
        float avg_price_per_night "AVG(fact_airbnb.price)"
        float avg_annual_revenue "AVG(dimRentMetrics.estimated_revenue_l365d)"
        float avg_acceptance_rate "AVG(dimHostMetrics.host_acceptance_rate)"
        float avg_response_rate "AVG(dimHostMetrics.host_response_rate)"
    }

    ml_premium_prediction {
        float price
        int availability_30
        int estimated_revenue_l365d
        list amenities
        string host_property_type
        int host_acceptance_rate
    }

    