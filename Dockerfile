FROM apache/airflow:2.9.2

# Switch to root to install packages
USER root
RUN apt-get update && apt-get install -y g++ make && rm -rf /var/lib/apt/lists/*
# Install Python Kafka client
RUN python -m pip install kafka-python apache-airflow-providers-docker

#Install the openlineage in the airflow contianers 
# and the marquez services in the docker-compose.yml file

USER airflow
RUN pip install openlineage-airflow openlineage-python boto3 psutil apache-airflow-providers-slack 