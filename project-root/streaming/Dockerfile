FROM python:3.13

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ENV KAFKA_BROKER=kafka:9092
ENV SCHEMA_REGISTRY_URL=http://schema-registry:8081

COPY pyproject.toml uv.lock* ./

RUN uv sync --frozen --no-cache

COPY streaming/ ./streaming/
COPY config/ ./config/
COPY data/ ./data/

CMD ["uv", "run", "python", "streaming/stream_reviews_producer.py"]
