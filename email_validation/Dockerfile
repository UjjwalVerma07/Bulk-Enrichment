FROM apache/airflow:2.9.2

# Switch to root to install compiler
USER root
RUN apt-get update && apt-get install -y g++ make && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY email_validation/requirements.txt /requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

# Copy DAG code, cpp validator, and scripts
COPY email_validation/dags/ /opt/airflow/dags/email_validation/
COPY email_validation/cpp/ /opt/airflow/dags/email_validation/cpp/
COPY email_validation/scripts/ /opt/airflow/dags/email_validation/scripts/
COPY common/ /opt/airflow/common/
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

USER airflow
