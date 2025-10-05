# OpenLineage Integration - Changes Summary

This document summarizes all changes made to integrate OpenLineage data lineage tracking with Marquez backend into the reverse geocoding pipeline.

## 📋 Overview

OpenLineage integration has been added to track complete data lineage for both batch (S3) and streaming (Kafka) modes. This provides visibility into data flows, job dependencies, schema evolution, and operational metrics.

## 🆕 New Files Created

### 1. `common/libs/openlineage_utils.py`
**Purpose**: Core OpenLineage utility module for the entire project

**Key Components**:
- `OpenLineageClient`: Main client class for emitting lineage events
- `create_s3_dataset()`: Helper for S3 dataset definitions
- `create_kafka_dataset()`: Helper for Kafka topic definitions
- `emit_start_event()`: Emit job start events
- `emit_complete_event()`: Emit job completion events
- `emit_fail_event()`: Emit job failure events
- `create_processing_facet()`: Custom processing metadata
- `add_output_statistics_to_dataset()`: Add row counts and file sizes

**Features**:
- Automatic schema tracking
- Datasource facets for S3 and Kafka
- Error handling and retry logic
- Documentation facets
- Output statistics tracking

### 2. `reverse_geocode/OPENLINEAGE_README.md`
**Purpose**: Comprehensive documentation for OpenLineage integration

**Contents**:
- Architecture diagrams
- Component descriptions
- Event types and facets
- Configuration instructions
- Marquez UI usage guide
- Troubleshooting tips
- API examples
- Testing procedures

### 3. `OPENLINEAGE_INTEGRATION_GUIDE.md`
**Purpose**: Quick reference guide for adding OpenLineage to other DAGs

**Contents**:
- Quick start examples
- Code templates for batch and streaming
- Common schema definitions
- Best practices
- Migration checklist
- Troubleshooting guide

### 4. `OPENLINEAGE_CHANGES_SUMMARY.md`
**Purpose**: This file - summary of all changes made

## 🔧 Modified Files

### 1. `reverse_geocode/dags/reverse_geocode_run.py`

**Changes Made**:

#### Imports Added:
```python
from uuid import uuid4
from common.libs.openlineage_utils import (
    OpenLineageClient, 
    create_processing_facet, 
    add_output_statistics_to_dataset
)
```

#### Batch Function (`run_reverse_geocode`):
- ✅ Initialize OpenLineageClient
- ✅ Generate unique run_id
- ✅ Define geo_schema with field descriptions
- ✅ Create input/output/reference dataset definitions
- ✅ Emit START event before processing
- ✅ Track output statistics (row count, file size)
- ✅ Emit COMPLETE event on success
- ✅ Emit FAIL event on error

#### Streaming Function (`run_reverse_geocode_stream`):
- ✅ Initialize OpenLineageClient
- ✅ Generate unique run_id
- ✅ Define Kafka topic datasets
- ✅ Emit START event before consuming
- ✅ Track total records processed
- ✅ Handle batch fallback scenario with special lineage
- ✅ Emit COMPLETE event with record counts
- ✅ Emit FAIL event on error

**Lineage Tracked**:
- Input datasets: S3 files or Kafka topics
- Reference data: geo_master.csv
- Output datasets: Enriched S3 files or Kafka topics
- Processing mode: batch vs stream
- Record counts and file sizes
- Error messages on failure

### 2. `reverse_geocode/dags/reverse_geocode_dag.py`

**Changes Made**:

#### Imports Added:
```python
from airflow.lineage.entities import File
```

#### Helper Functions Added:
```python
def create_s3_lineage_entity(bucket: str, key: str) -> File
def create_kafka_lineage_entity(topic: str) -> File
```

#### DAG Configuration:
- ✅ Added "openlineage" tag
- ✅ Added comprehensive DAG description
- ✅ Added environment variables for OpenLineage:
  - `OPENLINEAGE_URL`
  - `OPENLINEAGE_NAMESPACE`

#### Batch Task (`reverse_geocode_task`):
- ✅ Added `inlets` with input datasets
- ✅ Added `outlets` with output datasets
- ✅ Added detailed `doc_md` documentation
- ✅ Environment variables for OpenLineage

#### Streaming Task (`realtime_task`):
- ✅ Added `inlets` with Kafka input topic
- ✅ Added `outlets` with Kafka output topic
- ✅ Added detailed `doc_md` documentation
- ✅ Environment variables for OpenLineage

**Lineage Entities**:
- Batch inlets: S3 input file, geo_master reference
- Batch outlets: S3 output file
- Stream inlets: Kafka input topic, geo_master reference
- Stream outlets: Kafka output topic

### 3. `reverse_geocode/requirements.txt`

**Changes Made**:
```diff
  boto3
  kafka-python
  pandas
  pyyaml
+ requests
```

**Reason**: Required for HTTP requests to Marquez API

### 4. `README.md`

**Changes Made**:

#### Features Section:
- ✅ Added OpenLineage feature highlight

#### New Section Added:
- ✅ "OpenLineage & Data Lineage" section
- ✅ Feature list
- ✅ Quick start instructions
- ✅ Documentation links
- ✅ Example lineage flow diagram

#### Notes Section:
- ✅ Added note about OpenLineage events
- ✅ Added log confirmation message example

## 📊 Lineage Events Flow

### Batch Mode:

```
1. START Event
   ├─ Job: reverse_geocode_batch
   ├─ Inputs: 
   │  ├─ s3://raw/customer_raw.csv
   │  └─ s3://raw/reference/geo_master.csv
   └─ Facets: jobType=BATCH, integration=S3

2. Processing
   └─ Pandas merge on lat/lon

3. COMPLETE Event
   ├─ Outputs:
   │  └─ s3://enriched/reverse_geocode_enriched.csv
   │     └─ Statistics: row_count, file_size
   └─ Facets: record_count, mode=batch
```

### Streaming Mode:

```
1. START Event
   ├─ Job: reverse_geocode_stream
   ├─ Inputs:
   │  ├─ kafka://kafka:9092/reverse-geocode-input
   │  └─ s3://raw/reference/geo_master.csv
   └─ Facets: jobType=STREAMING, integration=KAFKA

2. Processing
   └─ Consume Kafka records in micro-batches

3. COMPLETE Event
   ├─ Outputs:
   │  └─ kafka://kafka:9092/reverse-geocode-output
   │     └─ Statistics: record_count
   └─ Facets: record_count, mode=stream
```

## 🎯 Benefits Achieved

### 1. **Data Lineage Visibility**
- ✅ Complete tracking of data flow from source to destination
- ✅ Visual lineage graphs in Marquez UI
- ✅ Cross-platform lineage (S3 ↔ Kafka)

### 2. **Schema Tracking**
- ✅ Field-level schema documentation
- ✅ Schema evolution over time
- ✅ Type information and descriptions

### 3. **Operational Insights**
- ✅ Job run history and duration
- ✅ Success/failure tracking
- ✅ Record counts and data volumes
- ✅ Performance metrics

### 4. **Debugging & Troubleshooting**
- ✅ Error messages captured in lineage
- ✅ Failed run tracking
- ✅ Input/output validation

### 5. **Compliance & Auditing**
- ✅ Complete audit trail
- ✅ Data provenance tracking
- ✅ Regulatory compliance support

## 🔌 Integration Points

### Marquez Backend:
- **URL**: http://host.docker.internal:5000
- **API Endpoint**: /api/v1/lineage
- **UI**: http://localhost:3000
- **Namespace**: airflow

### Airflow Configuration:
- **Config File**: `config/airflow.cfg`
- **Transport**: HTTP
- **Provider**: openlineage-airflow (already installed)

### Docker Environment:
- **Network**: final-bulk-enrichment-v2_airflow_network
- **Environment Variables**: Passed to all containers
- **Dependencies**: requests library added

## 📝 Usage Examples

### View Lineage in Marquez UI:

1. Start Marquez:
   ```bash
   cd marquez
   ./docker/up.sh --db-port 2345
   ```

2. Run a DAG in Airflow

3. Open Marquez UI: http://localhost:3000

4. Navigate to:
   - **Jobs** → Search for `reverse_geocode_batch`
   - **Datasets** → Search for `raw/customer_raw.csv`
   - Click on any item to see lineage graph

### Query via API:

```bash
# Get job details
curl http://localhost:5000/api/v1/namespaces/airflow/jobs/reverse_geocode_batch

# Get job runs
curl http://localhost:5000/api/v1/namespaces/airflow/jobs/reverse_geocode_batch/runs

# Get dataset lineage
curl http://localhost:5000/api/v1/lineage?nodeId=dataset:s3://raw:customer_raw.csv
```

### Check Logs:

Look for these messages in Airflow task logs:
```
✓ OpenLineage START event sent successfully for job reverse_geocode_batch
✓ OpenLineage COMPLETE event sent successfully for job reverse_geocode_batch
```

## 🧪 Testing

### Test Batch Mode:

```python
# Trigger DAG via Airflow UI or API
# Check Marquez UI for lineage
# Verify datasets appear correctly
```

### Test Streaming Mode:

```python
# Trigger DAG in stream mode
# Send test messages to Kafka
# Check Marquez for streaming lineage
# Verify Kafka topics tracked
```

### Verify Events:

```bash
# Check Marquez API
curl http://localhost:5000/api/v1/namespaces/airflow/jobs

# Should return reverse_geocode_batch and reverse_geocode_stream
```

## 🚀 Next Steps

### For Other DAGs:

1. **Email Validation DAG**:
   - Add OpenLineage to `email_validation/dags/enrich_email_run.py`
   - Follow pattern from reverse_geocode_run.py
   - Track C++ validator as processing step

2. **Gender Enrichment DAG**:
   - Add OpenLineage to `gender_enrichment/dags/enrich_gender_run.py`
   - Track gender_master.csv reference data
   - Add confidence score to output statistics

3. **Orchestrator DAG**:
   - Add pipeline-level lineage
   - Track cross-DAG dependencies
   - Emit orchestration events

### Enhancements:

- [ ] Column-level lineage
- [ ] Data quality metrics facets
- [ ] Custom business facets
- [ ] Integration with dbt
- [ ] Automated lineage-based alerts

## 📚 Documentation

### Primary Documentation:
- **Integration Guide**: `OPENLINEAGE_INTEGRATION_GUIDE.md`
- **Detailed README**: `reverse_geocode/OPENLINEAGE_README.md`
- **Main README**: `README.md` (updated with OpenLineage section)

### External Resources:
- [OpenLineage Specification](https://openlineage.io/spec/)
- [Marquez Documentation](https://marquezproject.github.io/marquez/)
- [Airflow OpenLineage Provider](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/)

## ✅ Verification Checklist

- [x] OpenLineage utilities module created
- [x] Reverse geocode run script updated (batch mode)
- [x] Reverse geocode run script updated (streaming mode)
- [x] Reverse geocode DAG updated with lineage entities
- [x] Environment variables added to Docker tasks
- [x] Requirements.txt updated with requests
- [x] Comprehensive documentation created
- [x] Integration guide created
- [x] Main README updated
- [x] Example lineage flows documented
- [x] Testing procedures documented
- [x] Troubleshooting guide included

## 🎉 Summary

The OpenLineage integration is now complete for the reverse geocoding pipeline! This provides:

- **Full data lineage tracking** from source to destination
- **Visual lineage graphs** in Marquez UI
- **Schema and metadata tracking** for all datasets
- **Operational insights** with job metrics
- **Error tracking** for debugging
- **Audit trails** for compliance

The implementation serves as a **reference pattern** for adding OpenLineage to other DAGs in the project.

---

**Date**: October 5, 2025  
**Integration**: OpenLineage with Marquez  
**Status**: ✅ Complete and Ready for Use

