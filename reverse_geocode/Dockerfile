FROM apache/airflow:2.9.2

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

COPY reverse_geocode/requirements.txt /requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

COPY reverse_geocode/dags/ /opt/airflow/dags/reverse_geocode/
# COPY reverse_geocode/data/ /reverse_geocode/data/
# doCOPY reverse_geocode/dags/run_reverse_geocode.py /opt/airflow/dags/reverse_geocode/run_reverse_geocode.py
COPY reverse_geocode/dags/reverse_geocode_run.py /opt/airflow/dags/reverse_geocode/reverse_geocode_run.py

COPY common/ /opt/airflow/common/
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"


 