```mermaid

erDiagram
    goldMetrics {
        int listing_id PK "Derived from fact_airbnb.id"
        
        float revpar_index "RevPAR: estimated_revenue / (365 - availability_365)"
        float scarcity_ratio "availability_90 / 90 (Lower means more exclusive)"
        float flawless_reputation_score "Avg of cleanliness, accuracy, communication"
        int luxury_amenities_weight "Weighted score: Pool=5, Chef=5, Espresso=2..."
        float host_exclusivity_index "Calculated from acceptance_rate and listings_count"
        
        float price "From fact_airbnb.price"
        int accommodates "From dimRentDetails.accommodates"
        int strict_minimum_nights "From dimRentDetails.minimum_nights"
        bool is_instant_bookable "Often FALSE for true luxury (they vet guests)"
    }
    
    %% Gold Layer: Pre-Aggregated Tables for Business Analytics

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

    