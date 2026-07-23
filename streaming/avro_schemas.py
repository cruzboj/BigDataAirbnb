review_avro_schema = """
{
  "namespace": "airbnb.streaming",
  "type": "record",
  "name": "EnrichedReview",
  "fields": [
    {"name": "listing_id", "type": ["null", "long"], "default": null},
    {"name": "id", "type": ["null", "long"], "default": null},
    {"name": "date", "type": ["null", "string"], "default": null},
    {"name": "reviewer_id", "type": ["null", "long"], "default": null},
    {"name": "reviewer_name", "type": ["null", "string"], "default": null},
    {"name": "comments", "type": ["null", "string"], "default": null},
    {"name": "language", "type": ["null", "string"], "default": null},
    
    {"name": "sentiment_score", "type": ["null", "float"], "default": null}, 
    {"name": "sentiment_label", "type": ["null", {"type": "array", "items": "string"}], "default": null},

    {"name": "likes_votes", "type": ["null", "int"], "default": null},
    {"name": "event_ingestion_time", "type": ["null", "string"], "default": null},
    {"name": "raw_user_agent", "type": ["null", "string"], "default": null},
    {"name": "bot_suspicion_score", "type": ["null", "float"], "default": null},

    {"name": "reviewer_hash_id", "type": ["null", "string"], "default": null},
    {"name": "aspect_sentiment_json", "type": ["null", "string"], "default": null},
    {"name": "extracted_keywords", "type": ["null", "string"], "default": null},
    {"name": "comment_character_count", "type": ["null", "int"], "default": null},
    {"name": "readability_index", "type": ["null", "float"], "default": null},
    {"name": "session_id", "type": ["null", "string"], "default": null},
    {"name": "time_spent_on_review_ms", "type": ["null", "int"], "default": null},
    {"name": "contains_media", "type": ["null", "boolean"], "default": null},
    {"name": "ingestion_ts", "type": ["null", "string"], "default": null}
  ]
}
"""