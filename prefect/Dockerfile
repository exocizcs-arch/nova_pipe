FROM python:3.13-slim

RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir \
    prefect \
    prefect-dbt \
    duckdb \
    dbt-duckdb \
    httpx

COPY ..

ENV PREFECT_API_URL=http://prefect-server:4200/api


