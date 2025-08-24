FROM apache/airflow:2.9.2

# # Switch to root to install any OS dependencies (minimal here)
# USER root
# RUN apt-get update && rm -rf /var/lib/apt/lists/*

# # Switch back to airflow
# USER airflow

# Copy orchestrator code and config into container
COPY orchestrator/dags/ /opt/airflow/dags/orchestrator/
COPY orchestrator/config/ /opt/airflow/dags/config/
COPY common/libs/ /opt/airflow/common/libs/

# Install Python dependencies
COPY requirements.txt .
USER airflow
RUN pip install --no-cache-dir -r requirements.txt
