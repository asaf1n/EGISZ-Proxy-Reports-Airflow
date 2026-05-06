FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app/src

RUN apt-get update \
    && apt-get install -y --no-install-recommends firebird3.0-common libfbclient2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e .

ENTRYPOINT ["python", "-m", "proxy_reports_etl.cli"]
