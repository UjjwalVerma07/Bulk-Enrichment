FROM apache/airflow:2.9.2


USER root
RUN apt-get update && apt-get install -y g++ make && rm -rf /var/lib/apt/lists/*


COPY email_validation/requirements.txt /requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

COPY email_validation/dags/ /opt/airflow/dags/email_validation/
COPY email_validation/cpp/ /opt/airflow/dags/email_validation/cpp/
COPY email_validation/scripts/ /opt/airflow/dags/email_validation/scripts/
COPY common/ /opt/airflow/common/
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

USER airflow
