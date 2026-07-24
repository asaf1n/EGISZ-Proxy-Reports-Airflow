FROM apache/airflow:3.2.2-python3.11

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONUTF8=1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends firebird3.0-common libfbclient2 curl \
    && rm -rf /var/lib/apt/lists/*

# DAG-файлы самодостаточны (пакет egisz_elt не устанавливается); pyproject.toml —
# единственный источник версий runtime-зависимостей.
COPY --chown=airflow:0 pyproject.toml /opt/egisz-elt/pyproject.toml

USER airflow
RUN python -c "import tomllib; print('\n'.join(tomllib.load(open('/opt/egisz-elt/pyproject.toml', 'rb'))['project']['dependencies']))" > /tmp/egisz-requirements.txt \
    && pip install --no-cache-dir -r /tmp/egisz-requirements.txt \
    && rm /tmp/egisz-requirements.txt

COPY --chown=airflow:0 airflow/dags /opt/airflow/dags
