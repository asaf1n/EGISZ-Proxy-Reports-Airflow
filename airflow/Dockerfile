FROM apache/airflow:2.8.3-python3.11

USER root

# Firebird client library for firebird-driver
RUN apt-get update \
    && apt-get install -y --no-install-recommends firebird3.0-common libfbclient2 \
    && rm -rf /var/lib/apt/lists/*

# Copy the project to install
COPY pyproject.toml README.md /opt/egisz-etl/
COPY src /opt/egisz-etl/src

RUN chown -R airflow:root /opt/egisz-etl

USER airflow
RUN pip install --no-cache-dir -e /opt/egisz-etl
