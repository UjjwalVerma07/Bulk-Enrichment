# Bulk Enrichment Pipeline  

## 📌 Overview  
**Bulk Enrichment Pipeline** is a distributed data processing project built with **Apache Airflow, Docker, Kafka, and MinIO**.  
It automates the enrichment of raw customer data using a modular pipeline consisting of:  

- **Email Validation** – validates and cleans email addresses using a C++ validator wrapped in a shell script.  
- **Reverse Geocoding** – enriches customer records with location data (city) based on latitude & longitude.  
- **Gender Enrichment** – enriches customer records with gender details based on reference data.  
- **Orchestrator DAG** – controls the execution sequence of all enrichment steps based on a YAML pipeline config.  

The system leverages **Airflow DAGs** for orchestration, **Kafka** for progress/status events, and **MinIO (S3-compatible)** for storage.  

---

## 🏗️ Architecture  
```text
                 ┌─────────────────┐
                 │   Orchestrator   │
                 │      (DAG)       │
                 └───────┬─────────┘
                         │
       ┌─────────────────┼─────────────────┐
       ▼                 ▼                 ▼
┌─────────────┐   ┌───────────────┐   ┌─────────────┐
│ Email       │   │ Reverse        │  | Gender      │
│ Validation  │   │ Geocode        │  │ Enrichment  │
└─────────────┘   └───────────────┘   └─────────────┘
       │                 │                 │
       ▼                 ▼                 ▼
    ┌───────────────────────────────────────────┐
    │              MinIO (S3) Storage           |
    │   (raw → staging → enriched buckets)      │
    └───────────────────────────────────────────┘

Kafka → Sends progress/status events at each stage

⚙️ Tech Stack

Apache Airflow – DAG scheduling & orchestration
Docker & Docker Compose – containerized environment
Postgres – Airflow metadata DB
Kafka + Zookeeper – messaging & progress tracking
MinIO – object storage (S3-compatible)
Python (pandas,kafka-python) – data processing & DAGs
C++ – custom email validation binary

📂 Project Structure
bulk-enrichment/
├── orchestrator/
│   └── dags/
│       └── orchestrator_dag.py
├── email_validation/
│   ├── dags/
│   │   └── email_validation_dag.py
│   ├── cpp/
│   │   └── validator (binary) + validator.cpp
│   └── scripts/
│       └── run_validator.sh
├── reverse_geocode/
│   └── dags/reverse_geocode_dag.py
├── gender_enrichment/
│   └── dags/gender_enrichment_dag.py
├── common/
│   └── libs/ (s3_utils.py, kafka_utils.py, progress.py)
├── samples/
│   ├── input/
│   │   └── customer_raw.csv
│   └── output/
├── orchestrator/config/pipeline.yml   # Defines pipeline sequence
├── docker-compose.yml
└── Dockerfile

🚀 Setup Instructions
1. Clone the repository
git clone https://github.com/UjjwalVerma07/Bulk-Enrichment.git
cd Bulk-Enrichment

2. Build & Start the environment
docker-compose up --build -d


This will start:
Airflow Webserver (port 8080)
Airflow Scheduler
Postgres DB
Kafka & Zookeeper
MinIO (ports 9000, 9001)

3. Access Services

Airflow UI → http://localhost:8080
MinIO Console → http://localhost:9001
(Default MinIO creds → minioadmin:minioadmin)

4. Run the Orchestrator DAG

Upload your raw data (customer_raw.csv) into raw bucket (MinIO).
Configure pipeline sequence in orchestrator/config/pipeline.yml.
Trigger the orchestrator_dag in Airflow.
The pipeline will automatically:
Download raw data from MinIO
Run email validation → reverse geocode → gender enrichment (as per config)
Upload enriched output to enriched bucket

📜 Example pipeline.yml
input_bucket: raw
input_key: customer_raw.csv
sequence:
  - dag_id: email_validation
    out_bucket: staging
    out_key: email_validated.csv
  - dag_id: reverse_geocode
    out_bucket: staging
    out_key: reverse_geocode_enriched.csv
  - dag_id: gender_enrichment
    out_bucket: enriched
    out_key: final_enriched.csv

📝 Key Features

Modular DAGs (can run independently or via orchestrator)
Config-driven pipeline (via pipeline.yml)
Kafka integration for pipeline status updates
MinIO for staging & final outputs
Custom C++ binary integration with Airflow DAGs

Note: The Working code is at the Production Branch
