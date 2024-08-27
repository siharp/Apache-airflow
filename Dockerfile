FROM apache/airflow:2.10.0

# Salin file requirements.txt ke dalam container
COPY requirements.txt /requirements.txt

# Install dependencies dari requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Update, install git dan membersihkan cache
USER root
RUN apt-get update && apt-get install -y --no-install-recommends sudo git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Kembali ke user airflow
USER airflow
