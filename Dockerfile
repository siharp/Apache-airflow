FROM apache/airflow:2.8.0

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

USER root
RUN apt-get update && apt-get install -y sudo git

USER airflow
