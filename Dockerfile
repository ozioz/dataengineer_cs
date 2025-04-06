FROM apache/airflow:2.5.1-python3.9

# Root olarak sistem bağımlılıklarını kur
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        python3-dev \
        libpq-dev \
        libssl-dev \
        libffi-dev \
        git \
    && rm -rf /var/lib/apt/lists/*

# Airflow kullanıcısına geç
USER airflow

# Python bağımlılıklarını versiyonlu kur (production için kritik)
RUN pip install --no-cache-dir --user \
    pymongo==4.3.3 \
    sqlalchemy==1.4.46 \
    psycopg2-binary==2.9.5 \
    faker==15.3.4 \
    pytz==2022.7 \
    cryptography==38.0.4 \
    apache-airflow-providers-mongo==3.3.0

# Ortam değişkenleri
ENV PYTHONPATH=/opt/airflow/scripts:${PYTHONPATH} \
    PATH="/home/airflow/.local/bin:${PATH}" \
    AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS="false" \
    AIRFLOW__CORE__ENABLE_XCOM_PICKLING="true" \
    PIP_NO_CACHE_DIR="true"

# İş dizinini ayarla
WORKDIR /opt/airflow

# Sağlık kontrolü için basit bir test
HEALTHCHECK --interval=30s --timeout=30s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1