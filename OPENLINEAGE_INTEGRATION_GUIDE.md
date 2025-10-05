# OpenLineage Integration Guide

This guide provides instructions for integrating OpenLineage lineage tracking into your ETL pipelines.

## Quick Start

### 1. Import OpenLineage Utilities

```python
from uuid import uuid4
from common.libs.openlineage_utils import (
    OpenLineageClient, 
    create_processing_facet, 
    add_output_statistics_to_dataset
)
```

### 2. Initialize Client

```python
# Initialize OpenLineage client (reads from environment variables)
ol_client = OpenLineageClient()
run_id = str(uuid4())
job_name = "my_enrichment_job"
```

### 3. Define Datasets

#### For S3 Datasets:

```python
input_dataset = ol_client.create_s3_dataset(
    bucket="raw",
    key="input_data.csv",
    schema_fields=[
        {"name": "email", "type": "STRING", "description": "Email address"},
        {"name": "name", "type": "STRING", "description": "Full name"}
    ],
    description="Raw customer data"
)

output_dataset = ol_client.create_s3_dataset(
    bucket="enriched",
    key="validated_data.csv",
    schema_fields=[
        {"name": "email", "type": "STRING", "description": "Email address"},
        {"name": "name", "type": "STRING", "description": "Full name"},
        {"name": "is_valid", "type": "BOOLEAN", "description": "Email validation status"}
    ],
    description="Validated customer data"
)
```

#### For Kafka Topics:

```python
input_dataset = ol_client.create_kafka_dataset(
    topic="customer-input",
    schema_fields=[
        {"name": "email", "type": "STRING"},
        {"name": "timestamp", "type": "TIMESTAMP"}
    ],
    description="Customer input stream"
)

output_dataset = ol_client.create_kafka_dataset(
    topic="customer-validated",
    schema_fields=[
        {"name": "email", "type": "STRING"},
        {"name": "is_valid", "type": "BOOLEAN"},
        {"name": "timestamp", "type": "TIMESTAMP"}
    ],
    description="Validated customer stream"
)
```

### 4. Emit Events

#### START Event:

```python
# Define job facets
job_facets = {
    "jobType": {
        "_producer": ol_client.producer,
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/JobTypeJobFacet.json",
        "processingType": "BATCH",  # or "STREAMING"
        "integration": "S3",  # or "KAFKA"
        "jobType": "VALIDATION"  # or "ENRICHMENT", "TRANSFORMATION"
    }
}

# Emit START event
ol_client.emit_start_event(
    job_name=job_name,
    run_id=run_id,
    inputs=[input_dataset, reference_dataset],
    job_facets=job_facets,
    run_facets=create_processing_facet(mode="batch")
)
```

#### COMPLETE Event:

```python
# Add output statistics
output_dataset_with_stats = add_output_statistics_to_dataset(
    output_dataset.copy(),
    row_count=len(df),
    size_bytes=os.path.getsize(output_file)
)

# Emit COMPLETE event
ol_client.emit_complete_event(
    job_name=job_name,
    run_id=run_id,
    inputs=[input_dataset],
    outputs=[output_dataset_with_stats],
    job_facets=job_facets,
    run_facets=create_processing_facet(mode="batch", record_count=len(df))
)
```

#### FAIL Event:

```python
try:
    # Your processing code
    pass
except Exception as e:
    # Emit FAIL event
    ol_client.emit_fail_event(
        job_name=job_name,
        run_id=run_id,
        error_message=str(e),
        inputs=[input_dataset],
        job_facets=job_facets
    )
    raise
```

## Complete Example: Email Validation

Here's a complete example for the email validation DAG:

```python
import os
import pandas as pd
from uuid import uuid4
from common.libs import s3_utils
from common.libs.openlineage_utils import (
    OpenLineageClient, 
    create_processing_facet, 
    add_output_statistics_to_dataset
)

def validate_emails_with_lineage(input_bucket, input_key, out_bucket, out_key):
    # Initialize OpenLineage
    ol_client = OpenLineageClient()
    run_id = str(uuid4())
    job_name = "email_validation_batch"
    
    # Define schema
    email_schema = [
        {"name": "email", "type": "STRING", "description": "Email address"},
        {"name": "name", "type": "STRING", "description": "Customer name"},
        {"name": "is_valid", "type": "BOOLEAN", "description": "Email validation result"}
    ]
    
    # Create datasets
    input_dataset = ol_client.create_s3_dataset(
        bucket=input_bucket,
        key=input_key,
        schema_fields=email_schema[:2],
        description="Raw customer emails for validation"
    )
    
    output_dataset = ol_client.create_s3_dataset(
        bucket=out_bucket,
        key=out_key,
        schema_fields=email_schema,
        description="Validated customer emails"
    )
    
    # Job facets
    job_facets = {
        "jobType": {
            "_producer": ol_client.producer,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/JobTypeJobFacet.json",
            "processingType": "BATCH",
            "integration": "S3",
            "jobType": "VALIDATION"
        }
    }
    
    # Emit START
    ol_client.emit_start_event(
        job_name=job_name,
        run_id=run_id,
        inputs=[input_dataset],
        job_facets=job_facets,
        run_facets=create_processing_facet(mode="batch")
    )
    
    try:
        # Download and process
        local_input = "/tmp/input.csv"
        local_output = "/tmp/output.csv"
        
        s3_utils.download_file(input_bucket, input_key, local_input)
        df = pd.read_csv(local_input)
        
        # Validate emails (your logic here)
        df['is_valid'] = df['email'].str.contains('@')
        
        df.to_csv(local_output, index=False)
        s3_utils.upload_file(out_bucket, out_key, local_output)
        
        # Add statistics and emit COMPLETE
        output_dataset_with_stats = add_output_statistics_to_dataset(
            output_dataset.copy(),
            len(df),
            os.path.getsize(local_output)
        )
        
        ol_client.emit_complete_event(
            job_name=job_name,
            run_id=run_id,
            inputs=[input_dataset],
            outputs=[output_dataset_with_stats],
            job_facets=job_facets,
            run_facets=create_processing_facet(mode="batch", record_count=len(df))
        )
        
    except Exception as e:
        # Emit FAIL
        ol_client.emit_fail_event(
            job_name=job_name,
            run_id=run_id,
            error_message=str(e),
            inputs=[input_dataset],
            job_facets=job_facets
        )
        raise
```

## DAG-Level Integration

### Add Lineage Entities to DAG

```python
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.lineage.entities import File

# Helper functions
def create_s3_lineage_entity(bucket: str, key: str) -> File:
    return File(url=f"s3://{bucket}/{key}")

def create_kafka_lineage_entity(topic: str) -> File:
    kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
    return File(url=f"kafka://{kafka_broker}/{topic}")

# In your DAG
with DAG(
    dag_id="my_enrichment_dag",
    tags=["enrichment", "openlineage"],
    description="My enrichment pipeline with OpenLineage tracking"
) as dag:
    
    task = DockerOperator(
        task_id="enrich_data",
        image="my_image",
        environment={
            "OPENLINEAGE_URL": "http://host.docker.internal:5000",
            "OPENLINEAGE_NAMESPACE": "airflow",
            # ... other env vars
        },
        # Define lineage
        inlets=[
            create_s3_lineage_entity("raw", "input.csv"),
            create_s3_lineage_entity("reference", "lookup.csv")
        ],
        outlets=[
            create_s3_lineage_entity("enriched", "output.csv")
        ],
        doc_md="""
        ### Data Enrichment Task
        
        **Inputs:**
        - S3: raw/input.csv
        - Reference: reference/lookup.csv
        
        **Output:**
        - S3: enriched/output.csv
        
        **Lineage:** Tracked via OpenLineage to Marquez
        """
    )
```

## Common Schema Definitions

### Customer Data:

```python
customer_schema = [
    {"name": "customer_id", "type": "STRING", "description": "Unique customer identifier"},
    {"name": "email", "type": "STRING", "description": "Email address"},
    {"name": "name", "type": "STRING", "description": "Full name"},
    {"name": "created_at", "type": "TIMESTAMP", "description": "Record creation timestamp"}
]
```

### Geolocation Data:

```python
geo_schema = [
    {"name": "latitude", "type": "DOUBLE", "description": "Latitude coordinate"},
    {"name": "longitude", "type": "DOUBLE", "description": "Longitude coordinate"},
    {"name": "city", "type": "STRING", "description": "City name"},
    {"name": "country", "type": "STRING", "description": "Country name"}
]
```

### Gender Enrichment:

```python
gender_schema = [
    {"name": "name", "type": "STRING", "description": "Person's name"},
    {"name": "gender", "type": "STRING", "description": "Inferred gender"},
    {"name": "confidence", "type": "DOUBLE", "description": "Confidence score"}
]
```

## Best Practices

### 1. Use Consistent Job Names

```python
# Good: Descriptive and consistent
job_name = "email_validation_batch"
job_name = "reverse_geocode_stream"

# Bad: Generic or inconsistent
job_name = "job1"
job_name = "process_data"
```

### 2. Always Generate Unique Run IDs

```python
from uuid import uuid4

# Good: Unique per run
run_id = str(uuid4())

# Bad: Reusing IDs
run_id = "run-1"
```

### 3. Include Comprehensive Schema Information

```python
# Good: Detailed schema
schema_fields = [
    {"name": "email", "type": "STRING", "description": "Customer email address"},
    {"name": "is_valid", "type": "BOOLEAN", "description": "Email validation result"}
]

# Bad: Minimal schema
schema_fields = [
    {"name": "email", "type": "STRING"},
    {"name": "is_valid", "type": "BOOLEAN"}
]
```

### 4. Handle Errors Gracefully

```python
try:
    # Processing code
    pass
except Exception as e:
    # Always emit FAIL event
    ol_client.emit_fail_event(...)
    # Then re-raise
    raise
```

### 5. Add Output Statistics

```python
# Always include row counts and file sizes when available
output_dataset_with_stats = add_output_statistics_to_dataset(
    dataset.copy(),
    row_count=len(df),
    size_bytes=os.path.getsize(file_path)
)
```

## Environment Setup

Ensure these environment variables are set in your Docker containers:

```yaml
environment:
  OPENLINEAGE_URL: http://host.docker.internal:5000
  OPENLINEAGE_NAMESPACE: airflow
  S3_ENDPOINT: http://minio:9000
  KAFKA_BROKER: kafka:9092
```

## Testing Lineage

### 1. Check Event Emission in Logs

Look for these messages in task logs:

```
✓ OpenLineage START event sent successfully for job my_job
✓ OpenLineage COMPLETE event sent successfully for job my_job
```

### 2. Query Marquez API

```bash
# List all jobs
curl http://localhost:5000/api/v1/namespaces/airflow/jobs

# Get specific job
curl http://localhost:5000/api/v1/namespaces/airflow/jobs/email_validation_batch

# Get job runs
curl http://localhost:5000/api/v1/namespaces/airflow/jobs/email_validation_batch/runs
```

### 3. View in Marquez UI

1. Open http://localhost:3000
2. Navigate to "Jobs" tab
3. Search for your job name
4. Click to view lineage graph

## Troubleshooting

### Events Not Appearing

```python
# Add debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Check if Marquez is accessible
import requests
response = requests.get("http://host.docker.internal:5000/api/v1/namespaces")
print(response.status_code, response.json())
```

### Network Issues

```bash
# From inside Docker container
curl http://host.docker.internal:5000/api/v1/namespaces

# If fails, check docker-compose network configuration
```

## Migration Checklist

To add OpenLineage to an existing DAG:

- [ ] Import OpenLineage utilities
- [ ] Initialize OpenLineageClient in run script
- [ ] Define dataset schemas
- [ ] Create input/output datasets
- [ ] Emit START event at beginning
- [ ] Emit COMPLETE event on success
- [ ] Emit FAIL event on error
- [ ] Add environment variables to DockerOperator
- [ ] Add inlets/outlets to DAG tasks
- [ ] Add "openlineage" tag to DAG
- [ ] Test and verify in Marquez UI

## Support Resources

- **OpenLineage Utils**: `common/libs/openlineage_utils.py`
- **Example Implementation**: `reverse_geocode/dags/reverse_geocode_run.py`
- **Documentation**: `reverse_geocode/OPENLINEAGE_README.md`
- **Marquez Docs**: https://marquezproject.github.io/marquez/
- **OpenLineage Spec**: https://openlineage.io/spec/

