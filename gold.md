erDiagram

    agg_neighborhood_performance {
        string neighborhood
        bool is_superhost
        float avg_price
        float avg_revenue
        float total_reviews
        int active_listings
    }

    ml_feature_set_price_prediction {
        float price PK "Target"
        int accommodates
        int bedrooms
        int bathrooms
        float review_score
        bool instant_bookable
        float lat
        float long
    }