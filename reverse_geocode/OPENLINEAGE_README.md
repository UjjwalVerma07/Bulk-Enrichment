# OpenLineage Integration for Reverse Geocode DAG

This document explains the OpenLineage integration implemented for the reverse geocoding enrichment pipeline, which tracks data lineage using Marquez as the backend.

## Overview

OpenLineage is an open standard for data lineage that enables tracking of data flows across different systems. This implementation provides comprehensive lineage tracking for both batch (S3) and streaming (Kafka) modes of the reverse geocoding pipeline.

## Architecture

```
┌─────────────────┐
│  Airflow DAG    │
│  (Scheduler)    │
└────────┬────────┘
         │ Triggers
         ▼
┌─────────────────┐      ┌──────────────────┐
│ Docker Operator │─────▶│ Python Script    │
│  (Task Runner)  │      │ reverse_geocode_ │
└─────────────────┘      │     run.py       │
                         └────────┬─────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │                           │
                    ▼                           ▼
         ┌──────────────────┐      ┌──────────────────┐
         │ OpenLineage      │      │ Data Processing  │
         │ Events (HTTP)    │      │ (Pandas)         │
         └────────┬─────────┘      └──────────────────┘
                  │
                  ▼
         ┌──────────────────┐
         │ Marquez Backend  │
         │ (Port 5000)      │
         └────────┬─────────┘
                  │
                  ▼
         ┌──────────────────┐
         │ Marquez UI       │
         │ (Port 3000)      │
         └──────────────────┘
```

## Components

### 1. OpenLineage Utilities (`common/libs/openlineage_utils.py`)

A comprehensive utility module that provides:

- **OpenLineageClient**: Main client for emitting lineage events
- **Dataset Creation**: Helper methods for S3 and Kafka datasets
- **Event Emission**: START, COMPLETE, and FAIL events
- **Facets**: Schema, datasource, documentation, and statistics facets

#### Key Features:

```python
# Initialize client
ol_client = OpenLineageClient()

# Create S3 dataset with schema
dataset = ol_client.create_s3_dataset(
    bucket="raw",
    key="data.csv",
    schema_fields=[
        {"name": "latitude", "type": "DOUBLE"},
        {"name": "longitude", "type": "DOUBLE"}
    ],
    description="Input data for geocoding"
)

# Emit events
ol_client.emit_start_event(job_name="my_job", run_id=uuid4(), inputs=[dataset])
ol_client.emit_complete_event(job_name="my_job", run_id=uuid4(), outputs=[dataset])
```

### 2. Enhanced DAG (`reverse_geocode_dag.py`)

The DAG includes:

- **Lineage Entities**: File entities for inlets/outlets
- **Environment Variables**: OpenLineage URL and namespace passed to containers
- **Documentation**: Task-level documentation with lineage information
- **Tags**: `openlineage` tag for easy filtering

#### Lineage Declaration:

```python
# Batch task lineage
inlets=[
    create_s3_lineage_entity(DEFAULT_INPUT_BUCKET, DEFAULT_INPUT_KEY),
    create_s3_lineage_entity(BUCKET, GEO_MASTER_CSV)
],
outlets=[
    create_s3_lineage_entity(DEFAULT_OUT_BUCKET, DEFAULT_OUT_KEY)
]

# Streaming task lineage
inlets=[
    create_kafka_lineage_entity(DEFAULT_INPUT_TOPIC),
    create_s3_lineage_entity(BUCKET, GEO_MASTER_CSV)
],
outlets=[
    create_kafka_lineage_entity(DEFAULT_OUTPUT_TOPIC)
]
```

### 3. Enhanced Run Script (`reverse_geocode_run.py`)

The execution script emits detailed lineage events:

#### Batch Mode Lineage:

```
START Event:
  - Job: reverse_geocode_batch
  - Inputs: S3 input file, geo_master reference
  - Job Facets: processingType=BATCH, integration=S3

COMPLETE Event:
  - Outputs: S3 output file with statistics (row count, file size)
  - Run Facets: record count, processing mode

FAIL Event (on error):
  - Error message with stack trace
```

#### Streaming Mode Lineage:

```
START Event:
  - Job: reverse_geocode_stream
  - Inputs: Kafka input topic, geo_master reference
  - Job Facets: processingType=STREAMING, integration=KAFKA

COMPLETE Event:
  - Outputs: Kafka output topic with record count
  - Special handling for batch fallback scenario

FAIL Event (on error):
  - Error message with context
```

## Lineage Events

### Event Types

1. **START**: Emitted when job begins
   - Declares input datasets
   - Includes job metadata (type, integration)
   - Generates unique run_id

2. **COMPLETE**: Emitted on successful completion
   - Declares output datasets
   - Includes output statistics (row count, size)
   - Records processing metrics

3. **FAIL**: Emitted on error
   - Captures error message
   - Maintains lineage even on failure
   - Helps with debugging

### Facets Tracked

#### Job Facets:
- **jobType**: Processing type (BATCH/STREAMING), integration (S3/KAFKA)

#### Run Facets:
- **processing**: Mode, record count
- **errorMessage**: Error details (on failure)

#### Dataset Facets:
- **schema**: Column names, types, descriptions
- **dataSource**: Source type and URI
- **documentation**: Dataset descriptions

#### Output Facets:
- **outputStatistics**: Row count, file size

## Configuration

### Environment Variables

The following environment variables are required:

```bash
# Marquez connection
OPENLINEAGE_URL=http://host.docker.internal:5000
OPENLINEAGE_NAMESPACE=airflow

# Data sources
S3_ENDPOINT=http://minio:9000
KAFKA_BROKER=kafka:9092
```

### Airflow Configuration

In `config/airflow.cfg`:

```ini
[openlineage]
transport = {"type": "http", "url": "http://host.docker.internal:5000", "endpoint": "/api/v1/lineage"}
namespace = airflow
extractors = {}
```

### Docker Compose

Ensure the environment variables are passed to containers:

```yaml
environment:
  OPENLINEAGE_URL: http://host.docker.internal:5000
  OPENLINEAGE_NAMESPACE: airflow
```

## Viewing Lineage in Marquez

### Accessing Marquez UI

1. **Start Marquez**: Navigate to `marquez/` directory and run:
   ```bash
   ./docker/up.sh --db-port 2345
   ```

2. **Access UI**: Open browser to `http://localhost:3000`

3. **Access API**: Available at `http://localhost:5000`

### Exploring Lineage

#### Jobs View:
- Navigate to "Jobs" tab
- Search for `reverse_geocode_batch` or `reverse_geocode_stream`
- View job runs, duration, success/failure status

#### Datasets View:
- Navigate to "Datasets" tab
- Search for datasets:
  - `s3://raw/customer_raw.csv`
  - `s3://enriched/reverse_geocode_enriched.csv`
  - `kafka://kafka:9092/reverse-geocode-input`
- View dataset lineage graph
- See upstream and downstream dependencies

#### Lineage Graph:
- Click on any dataset to see visual lineage
- Trace data flow from source to destination
- Identify dependencies across jobs

### Example Queries

Using Marquez API:

```bash
# Get job details
curl http://localhost:5000/api/v1/namespaces/airflow/jobs/reverse_geocode_batch

# Get dataset lineage
curl http://localhost:5000/api/v1/namespaces/s3:%2F%2Fraw/datasets/customer_raw.csv/lineage

# Get job runs
curl http://localhost:5000/api/v1/namespaces/airflow/jobs/reverse_geocode_batch/runs
```

## Benefits

### 1. **Data Lineage Visibility**
- Track data flow from raw input to enriched output
- Understand dependencies between datasets
- Identify impact of changes

### 2. **Debugging & Troubleshooting**
- View historical job runs
- Identify failure patterns
- Trace data quality issues to source

### 3. **Compliance & Auditing**
- Complete audit trail of data transformations
- Schema evolution tracking
- Data provenance for regulatory requirements

### 4. **Operational Insights**
- Monitor job performance over time
- Identify bottlenecks
- Track data volume trends

### 5. **Cross-Platform Lineage**
- Track data across S3, Kafka, and other systems
- Unified view of data pipeline
- Integration with other tools using OpenLineage

## Testing

### Test Batch Mode

```bash
# Trigger DAG with batch mode
curl -X POST http://localhost:8080/api/v1/dags/reverse_geocode_dag/dagRuns \
  -H "Content-Type: application/json" \
  -d '{
    "conf": {
      "mode": "batch",
      "input_bucket": "raw",
      "input_key": "customer_raw.csv",
      "out_bucket": "enriched",
      "out_key": "reverse_geocode_enriched.csv"
    }
  }'
```

### Test Streaming Mode

```bash
# Trigger DAG with streaming mode
curl -X POST http://localhost:8080/api/v1/dags/reverse_geocode_dag/dagRuns \
  -H "Content-Type: application/json" \
  -d '{
    "conf": {
      "mode": "stream",
      "input_topic": "reverse-geocode-input",
      "output_topic": "reverse-geocode-output"
    }
  }'
```

### Verify Lineage Events

Check task logs for OpenLineage event confirmations:

```
✓ OpenLineage START event sent successfully for job reverse_geocode_batch
✓ OpenLineage COMPLETE event sent successfully for job reverse_geocode_batch
```

## Troubleshooting

### Events Not Appearing in Marquez

1. **Check Marquez is running**:
   ```bash
   curl http://localhost:5000/api/v1/namespaces
   ```

2. **Verify environment variables** in Docker container:
   ```bash
   docker exec <container_id> env | grep OPENLINEAGE
   ```

3. **Check task logs** for HTTP errors

4. **Verify network connectivity** from container to host:
   ```bash
   docker exec <container_id> curl http://host.docker.internal:5000
   ```

### Missing Dataset Information

- Ensure schema fields are correctly defined
- Check dataset naming conventions
- Verify S3/Kafka URIs are correct

### Performance Impact

- OpenLineage events are sent asynchronously
- Failures to send events don't block job execution
- Minimal overhead (< 1% of job runtime)

## Future Enhancements

1. **Column-Level Lineage**: Track transformations at column level
2. **Data Quality Metrics**: Add data quality facets
3. **Custom Facets**: Business-specific metadata
4. **Integration with dbt**: Connect with dbt lineage
5. **Automated Alerts**: Lineage-based alerting for data issues

## References

- [OpenLineage Specification](https://openlineage.io/spec/)
- [Marquez Documentation](https://marquezproject.github.io/marquez/)
- [Airflow OpenLineage Provider](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/)
- [OpenLineage Python Client](https://openlineage.io/docs/client/python)

## Support

For issues or questions:
1. Check Marquez logs: `docker logs <marquez_container>`
2. Check Airflow logs: Task logs in Airflow UI
3. Review OpenLineage events: Marquez API or UI
4. Consult OpenLineage community: https://openlineage.io/community

