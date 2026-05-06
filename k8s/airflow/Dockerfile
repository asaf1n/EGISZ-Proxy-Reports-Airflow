FROM apache/airflow:2.11.2-python3.11

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONUTF8=1

USER root

# Firebird client library for firebird-driver
RUN apt-get update \
    && apt-get install -y --no-install-recommends firebird3.0-common libfbclient2 \
    && rm -rf /var/lib/apt/lists/*

COPY . /opt/proxy-reports-etl
RUN chown -R airflow:0 /opt/proxy-reports-etl

USER airflow
RUN pip install --no-cache-dir -e /opt/proxy-reports-etl


COPY airflow/dags/*.py /opt/airflow/dags/
