#  Bulk Data Enrichment Pipeline

A scalable data enrichment pipeline built with **Apache Airflow, Kafka, Docker, and MinIO (S3)**.  
The pipeline supports **both batch and real-time stream processing** with isolated microservices for each enrichment step.  

---

## Features

- **Isolated Microservices**  
  Each enrichment step (Email Validation, Reverse Geocode, Gender Enrichment) runs in its own Docker container via the **DockerOperator**.  

- **Dynamic Pipeline Configuration**  
  Orchestrator listens to the Kafka topic `pipeline-config` for incoming JSON pipeline configurations (no static `pipeline.yml`).  

- **Batch & Stream Modes**  
  - **Batch Mode**: Reads CSV files from S3 → processes → writes back to S3.  
  - **Stream Mode**: Reads JSON messages from Kafka → processes in real time → writes enriched data back to Kafka or S3.  

- **No Local File Dependency**  
  Fully streaming architecture. Data flows **Kafka → DAG → Kafka/S3** without intermediate local storage.  

- **Progress Tracking**  
  Each stage sends progress events to Kafka (`pipeline-progress`) and updates `progress.json` in S3.  

---

##  Repository Structure

```
bulk-enrichment/
├─ docker-compose.yml
├─
.env.example
├─ orchestrator/
│ ├─ dags/orchestrator_dag.py
│ └─ config/pipeline.yml # DAG execution sequence & S3 paths
├─ email_validation/
│ ├─ dags/email_validation_dag.py
│ ├─ cpp/validator.cpp # C++ email validation CLI
│ └─ scripts/run_validator.sh
├─ reverse_geocode/
│ ├─ dags/reverse_geocode_dag.py
│ └─ data/geo_master.csv # lat,lon,city reference
├─ gender_enrichment/
│ ├─ dags/gender_enrichment_dag.py
│ └─ data/gender_master.csv # name,gender reference
├─ common/
│ ├─ libs/s3_utils.py
│ ├─ libs/kafka_utils.py
│ └─ libs/progress.py
└─ samples/
├─ input/customers_raw.csv
└─ output/
Input File Specification
```

---

## ⚙️ Environment Setup

1. **Start Kafka, Airflow, and MinIO with Docker Compose**  

```bash
docker-compose up -d
```

2. **Enter Kafka container**  

```bash
docker exec -it bulk-enrichment-kafka-1 bash
cd opt/bitnami/kafka/bin
```

3. **Run script to create topics**  

```bash
#!/bin/bash
BROKER="localhost:9092"
TOPICS=(
  "pipeline-progress"
  "pipeline-config"
  "email-validation-input"
  "email-validation-output"
  "reverse-geocode-input"
  "reverse-geocode-output"
  "gender-enrich-input"
  "gender-enrich-output"
  "enrich-input-topic"
  "enrich-output-topic"
)
for TOPIC in "${TOPICS[@]}"; do
  echo "Creating topic: $TOPIC"
  kafka-topics.sh --create     --topic "$TOPIC"     --bootstrap-server "$BROKER"     --partitions 3     --replication-factor 1     --if-not-exists
done
```

---

##  Pipeline Configurations

### Batch Mode Config
Send this JSON to `pipeline-config` topic:

```json
{"input_bucket":"raw","input_key":"customer_raw.csv","mode":"batch","sequence":[{"dag_id":"email_validation","out_bucket":"staging","out_key":"email_validated.csv"},{"dag_id":"reverse_geocode","out_bucket":"staging","out_key":"geo_enriched.csv"},{"dag_id":"gender_enrichment","out_bucket":"enriched","out_key":"final_enriched.csv"}]}
```

### Stream Mode Config
Send this JSON to `pipeline-config` topic:

```json
{"main_input_topic":"enrich-input-topic","main_out_topic":"enrich-output-topic","mode":"stream","sequence":[{"dag_id":"email_validation","mode":"stream","input_topic":"enrich-input-topic","out_topic":"email-validation-output"},{"dag_id":"reverse_geocode","mode":"stream","input_topic":"email-validation-output","out_topic":"reverse-geocode-output"},{"dag_id":"gender_enrichment","mode":"stream","input_topic":"reverse-geocode-output","out_topic":"enrich-output-topic"}]}
```

---

## Running Stream Processing

1. Open a Kafka producer on the input topic:

```bash
./kafka-console-producer.sh --topic enrich-input-topic --bootstrap-server localhost:9092
```

2. Paste the following dummy JSON message:

```json
{"id":1,"name":"Amit","email":"amit@example.com","latitude":28.6139,"longitude":77.2090}
```

3. The orchestrator will run the enrichment steps in sequence and publish the final enriched message to `enrich-output-topic`.

4. Similary you can run all the dags standalone by triggering the particular dag then selecting mode as stream in the airflow UI
   
5. then running the command for uploading the input ./kafka-console-producer.sh --topic <topic-name> --bootstrap-server localhost:9092
   
6. Then to see the particular dags output you can run the command ./kafka-console-consumer.sh --topic <topic-name> --bootstrap-server localhost:9092
   
7. example - ./kafka-console-consumer.sh --topic enrich-output-topic --bootstrap-server localhost:9092
---

## Expected Output

After processing the dummy input above, you should get:

```json
{"id":1,"name":"Amit","email":"amit@example.com","latitude":28.6139,"longitude":77.2090,"city":"New Delhi","gender":"M","email_valid":true}
```

---

## Progress Tracking

- Each DAG task publishes progress events like:  

```json
{"stage":"reverse_geocode","status":"Started","time":"2025-08-28T11:10:52.434923"}
{"stage":"reverse_geocode","status":"Succeeded","time":"2025-08-28T11:11:10.123456"}
```

- Progress also stored in `s3://raw/progress.json`.

---

## Notes
- Each enrichment DAG can be run independently or orchestrated together.  
- Airflow UI can be used to trigger the orchestrator DAG manually.  
