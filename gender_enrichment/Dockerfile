# FROM apache/airflow:2.9.2

# USER root
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     && rm -rf /var/lib/apt/lists/*

# COPY gender_enrichment/requirements.txt /requirements.txt
# USER airflow
# RUN pip install --no-cache-dir -r /requirements.txt

# COPY gender_enrichment/dags/ /opt/airflow/dags/gender_enrichment/
# COPY gender_enrichment/data/ /gender_enrichment/data/
# COPY common/libs/ /opt/airflow/common/libs/

# USER airflow
FROM apache/airflow:2.9.2

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

COPY gender_enrichment/requirements.txt /requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

COPY gender_enrichment/dags/ /opt/airflow/dags/gender_enrichment/
# COPY gender_enrichment/data/ /gender_enrichment/data/
# doCOPY gender_enrichment/dags/run_gender_enrichment.py /opt/airflow/dags/gender_enrichment/run_gender_enrichment.py
COPY gender_enrichment/dags/enrich_gender_run.py /opt/airflow/dags/gender_enrichment/enrich_gender_run.py

COPY common/ /opt/airflow/common/
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"


