FROM apache/airflow:2.11.2-python3.11

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONUTF8=1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends firebird3.0-common libfbclient2 curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md /opt/egisz-elt/
COPY src /opt/egisz-elt/src

RUN chown -R airflow:0 /opt/egisz-elt

USER airflow
RUN pip install --no-cache-dir -e /opt/egisz-elt

COPY --chown=airflow:0 airflow/dags /opt/airflow/dags
