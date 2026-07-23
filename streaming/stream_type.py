from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(slots=True)
class ReviewType:
    listing_id: int | None
    id: int | None
    date: str | None
    reviewer_id: int | None
    reviewer_name: str | None
    comments: str | None
    language: str | None
    sentiment_score: float | None
    sentiment_label: list[str] | None
    likes_votes: int | None
    event_ingestion_time: str | None
    raw_user_agent: str | None
    bot_suspicion_score: float | None
    reviewer_hash_id: str | None
    aspect_sentiment_json: str | None
    extracted_keywords: str | None
    comment_character_count: int | None
    readability_index: float | None
    session_id: str | None
    time_spent_on_review_ms: int | None
    contains_media: bool | None
    ingestion_ts: str

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "ReviewType":
        return cls(
            listing_id=_to_int(row.get("listing_id")),
            id=_to_int(row.get("id")),
            date=row.get("date"),
            reviewer_id=_to_int(row.get("reviewer_id")),
            reviewer_name=row.get("reviewer_name"),
            comments=row.get("comments") or row.get("comment"),
            language=row.get("language"),
            sentiment_score=_to_float(row.get("sentiment_score")),
            sentiment_label=_to_str_list(row.get("sentiment_label")),
            likes_votes=_to_int(row.get("likes_votes") or row.get("likes_count")),
            event_ingestion_time=_to_iso_ts(row.get("event_ingestion_time")),
            raw_user_agent=row.get("raw_user_agent"),
            bot_suspicion_score=_to_float(row.get("bot_suspicion_score")),
            reviewer_hash_id=row.get("reviewer_hash_id"),
            aspect_sentiment_json=row.get("aspect_sentiment_json"),
            extracted_keywords=row.get("extracted_keywords"),
            comment_character_count=_to_int(row.get("comment_character_count")),
            readability_index=_to_float(row.get("readability_index")),
            session_id=row.get("session_id"),
            time_spent_on_review_ms=_to_int(row.get("time_spent_on_review_ms")),
            contains_media=_to_bool(row.get("contains_media")),
            ingestion_ts=_to_iso_ts(row.get("ingestion_ts"))
            or datetime.now(timezone.utc).isoformat(),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _to_int(value: Any) -> int | None:
    return int(value) if value not in (None, "") else None


def _to_float(value: Any) -> float | None:
    return float(value) if value not in (None, "") else None


def _to_str_list(value: Any) -> list[str] | None:
    if value in (None, ""):
        return None

    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]

    if isinstance(value, str):
        raw = value.strip()
        if raw.startswith("[") and raw.endswith("]"):
            try:
                parsed = json.loads(raw.replace("'", '"'))
                if isinstance(parsed, list):
                    return [str(item).strip() for item in parsed if str(item).strip()]
            except json.JSONDecodeError:
                pass

    return [item.strip() for item in str(value).split(",") if item.strip()]


def _to_bool(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _to_iso_ts(value: Any) -> str | None:
    if value in (None, ""):
        return None

    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except ValueError:
        return str(value)
