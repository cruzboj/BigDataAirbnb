from urllib.parse import urlparse


def normalize_minio_endpoint(endpoint: str) -> tuple[str, bool]:
    raw_endpoint = endpoint.strip()
    if not raw_endpoint:
        raise ValueError("MINIO_ENDPOINT cannot be empty")

    parsed = urlparse(raw_endpoint if "://" in raw_endpoint else f"//{raw_endpoint}")
    secure = parsed.scheme.lower() == "https" if parsed.scheme else False
    normalized_endpoint = parsed.netloc

    if not normalized_endpoint:
        raise ValueError(f"Invalid MINIO_ENDPOINT: '{endpoint}'")

    return normalized_endpoint.rstrip("/"), secure
