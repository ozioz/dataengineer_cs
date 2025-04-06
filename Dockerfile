FROM apache/airflow:2.5.0

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install --no-cache-dir --user \
    pymongo \
    sqlalchemy \
    psycopg2-binary \
    faker \
    pytz

# Ortam değişkenlerini ayarla
ENV PYTHONPATH=/opt/airflow/scripts:${PYTHONPATH} \
    PATH="/home/airflow/.local/bin:${PATH}"