# OpenLineage Architecture - Reverse Geocode Pipeline

This document provides visual representations of the OpenLineage integration architecture.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Airflow Scheduler                               │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │                    reverse_geocode_dag                          │    │
│  │                                                                 │    │
│  │  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    │    │
│  │  │    start     │───▶│ branch_mode  │───▶│     end      │    │    │
│  │  └──────────────┘    └──────┬───────┘    └──────────────┘    │    │
│  │                              │                                 │    │
│  │                    ┌─────────┴─────────┐                      │    │
│  │                    ▼                   ▼                       │    │
│  │         ┌──────────────────┐  ┌──────────────────┐           │    │
│  │         │ reverse_geocode  │  │    realtime      │           │    │
│  │         │     _task        │  │ _reverse_geocode │           │    │
│  │         │   (batch)        │  │   (streaming)    │           │    │
│  │         └────────┬─────────┘  └────────┬─────────┘           │    │
│  │                  │                     │                      │    │
│  │         inlets:  │            inlets:  │                      │    │
│  │         - S3 input              - Kafka input                 │    │
│  │         - geo_master            - geo_master                  │    │
│  │                  │                     │                      │    │
│  │         outlets: │            outlets: │                      │    │
│  │         - S3 output             - Kafka output                │    │
│  └─────────────────┼─────────────────────┼──────────────────────┘    │
└─────────────────────┼─────────────────────┼───────────────────────────┘
                      │                     │
                      ▼                     ▼
           ┌──────────────────────────────────────┐
           │     DockerOperator Execution         │
           │                                      │
           │  ┌────────────────────────────────┐ │
           │  │  reverse_geocode_run.py        │ │
           │  │                                │ │
           │  │  ┌──────────────────────────┐ │ │
           │  │  │ OpenLineageClient        │ │ │
           │  │  │                          │ │ │
           │  │  │ • emit_start_event()     │ │ │
           │  │  │ • emit_complete_event()  │ │ │
           │  │  │ • emit_fail_event()      │ │ │
           │  │  └────────────┬─────────────┘ │ │
           │  └───────────────┼───────────────┘ │
           └──────────────────┼─────────────────┘
                              │
                              │ HTTP POST
                              │ /api/v1/lineage
                              ▼
           ┌──────────────────────────────────────┐
           │      Marquez Backend                 │
           │      (Port 5000)                     │
           │                                      │
           │  ┌────────────────────────────────┐ │
           │  │  OpenLineage API               │ │
           │  │  • Receive events              │ │
           │  │  • Parse lineage data          │ │
           │  │  • Store in PostgreSQL         │ │
           │  └────────────────────────────────┘ │
           │                                      │
           │  ┌────────────────────────────────┐ │
           │  │  PostgreSQL Database           │ │
           │  │  • Jobs                        │ │
           │  │  • Datasets                    │ │
           │  │  • Runs                        │ │
           │  │  • Facets                      │ │
           │  └────────────────────────────────┘ │
           └──────────────────┬───────────────────┘
                              │
                              │ GraphQL/REST API
                              ▼
           ┌──────────────────────────────────────┐
           │      Marquez Web UI                  │
           │      (Port 3000)                     │
           │                                      │
           │  • Jobs View                         │
           │  • Datasets View                     │
           │  • Lineage Graphs                    │
           │  • Run History                       │
           │  • Search & Filter                   │
           └──────────────────────────────────────┘
```

## Batch Mode Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        BATCH MODE LINEAGE                            │
└─────────────────────────────────────────────────────────────────────┘

Step 1: START Event
═══════════════════════════════════════════════════════════════════════
┌──────────────────────────────────────────────────────────────────┐
│  OpenLineage START Event                                         │
│  ────────────────────────────────────────────────────────────── │
│  Job: reverse_geocode_batch                                      │
│  Run ID: 550e8400-e29b-41d4-a716-446655440000                   │
│  Event Type: START                                               │
│  Timestamp: 2025-10-05T10:30:00.000Z                            │
│                                                                  │
│  Inputs:                                                         │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ Dataset 1: s3://raw/customer_raw.csv                       │ │
│  │ ├─ Namespace: s3://raw                                     │ │
│  │ ├─ Schema:                                                 │ │
│  │ │  ├─ latitude: DOUBLE                                     │ │
│  │ │  └─ longitude: DOUBLE                                    │ │
│  │ └─ Description: Raw input data for reverse geocoding      │ │
│  │                                                            │ │
│  │ Dataset 2: s3://raw/reference/geo_master.csv              │ │
│  │ ├─ Namespace: s3://raw                                     │ │
│  │ ├─ Schema:                                                 │ │
│  │ │  ├─ latitude: DOUBLE                                     │ │
│  │ │  ├─ longitude: DOUBLE                                    │ │
│  │ │  └─ city: STRING                                         │ │
│  │ └─ Description: Reference dataset for geocoding lookup    │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  Job Facets:                                                     │
│  ├─ processingType: BATCH                                       │
│  ├─ integration: S3                                             │
│  └─ jobType: ENRICHMENT                                         │
│                                                                  │
│  Run Facets:                                                     │
│  └─ mode: batch                                                 │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
Step 2: Data Processing
═══════════════════════════════════════════════════════════════════════
┌──────────────────────────────────────────────────────────────────┐
│  Processing Steps:                                               │
│  1. Download customer_raw.csv from S3                           │
│  2. Download geo_master.csv from S3                             │
│  3. Load both into Pandas DataFrames                            │
│  4. Perform left join on (latitude, longitude)                  │
│  5. Fill missing cities with "Unknown"                          │
│  6. Save to local file                                          │
│  7. Upload to S3 enriched bucket                                │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
Step 3: COMPLETE Event
═══════════════════════════════════════════════════════════════════════
┌──────────────────────────────────────────────────────────────────┐
│  OpenLineage COMPLETE Event                                      │
│  ────────────────────────────────────────────────────────────── │
│  Job: reverse_geocode_batch                                      │
│  Run ID: 550e8400-e29b-41d4-a716-446655440000                   │
│  Event Type: COMPLETE                                            │
│  Timestamp: 2025-10-05T10:31:30.000Z                            │
│                                                                  │
│  Outputs:                                                        │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ Dataset: s3://enriched/reverse_geocode_enriched.csv        │ │
│  │ ├─ Namespace: s3://enriched                                │ │
│  │ ├─ Schema:                                                 │ │
│  │ │  ├─ latitude: DOUBLE                                     │ │
│  │ │  ├─ longitude: DOUBLE                                    │ │
│  │ │  └─ city: STRING                                         │ │
│  │ ├─ Description: Enriched output with city information     │ │
│  │ └─ Output Statistics:                                      │ │
│  │    ├─ rowCount: 1,234                                      │ │
│  │    └─ size: 45,678 bytes                                   │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  Run Facets:                                                     │
│  ├─ mode: batch                                                 │
│  └─ recordCount: 1,234                                          │
└──────────────────────────────────────────────────────────────────┘
```

## Streaming Mode Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                      STREAMING MODE LINEAGE                          │
└─────────────────────────────────────────────────────────────────────┘

Step 1: START Event
═══════════════════════════════════════════════════════════════════════
┌──────────────────────────────────────────────────────────────────┐
│  OpenLineage START Event                                         │
│  ────────────────────────────────────────────────────────────── │
│  Job: reverse_geocode_stream                                     │
│  Run ID: 660e8400-e29b-41d4-a716-446655440001                   │
│  Event Type: START                                               │
│  Timestamp: 2025-10-05T10:30:00.000Z                            │
│                                                                  │
│  Inputs:                                                         │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ Dataset 1: kafka://kafka:9092/reverse-geocode-input        │ │
│  │ ├─ Namespace: kafka://kafka:9092                           │ │
│  │ ├─ Schema:                                                 │ │
│  │ │  ├─ latitude: DOUBLE                                     │ │
│  │ │  └─ longitude: DOUBLE                                    │ │
│  │ └─ Description: Input stream for reverse geocoding        │ │
│  │                                                            │ │
│  │ Dataset 2: s3://raw/reference/geo_master.csv              │ │
│  │ ├─ Namespace: s3://raw                                     │ │
│  │ ├─ Schema: [latitude, longitude, city]                    │ │
│  │ └─ Description: Reference dataset for lookup              │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  Job Facets:                                                     │
│  ├─ processingType: STREAMING                                   │
│  ├─ integration: KAFKA                                          │
│  └─ jobType: ENRICHMENT                                         │
│                                                                  │
│  Run Facets:                                                     │
│  └─ mode: stream                                                │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
Step 2: Stream Processing
═══════════════════════════════════════════════════════════════════════
┌──────────────────────────────────────────────────────────────────┐
│  Streaming Loop:                                                 │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Consume micro-batch from Kafka                          │   │
│  │ ├─ Records: [{"lat": 28.6, "lon": 77.2}, ...]          │   │
│  │ └─ Batch size: 1-5 records                              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                     │                                            │
│                     ▼                                            │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Join with geo_master                                     │   │
│  │ ├─ Merge on (latitude, longitude)                       │   │
│  │ └─ Fill missing with "Unknown"                          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                     │                                            │
│                     ▼                                            │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Publish to output topic                                  │   │
│  │ └─ Records: [{"lat": 28.6, "lon": 77.2, "city": "..."}, │   │
│  └─────────────────────────────────────────────────────────┘   │
│                     │                                            │
│                     └──────────┐                                 │
│                                │                                 │
│  Total Records Processed: 543  │                                 │
└────────────────────────────────┼─────────────────────────────────┘
                                 │
                                 ▼
Step 3: COMPLETE Event
═══════════════════════════════════════════════════════════════════════
┌──────────────────────────────────────────────────────────────────┐
│  OpenLineage COMPLETE Event                                      │
│  ────────────────────────────────────────────────────────────── │
│  Job: reverse_geocode_stream                                     │
│  Run ID: 660e8400-e29b-41d4-a716-446655440001                   │
│  Event Type: COMPLETE                                            │
│  Timestamp: 2025-10-05T10:35:00.000Z                            │
│                                                                  │
│  Outputs:                                                        │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ Dataset: kafka://kafka:9092/reverse-geocode-output        │ │
│  │ ├─ Namespace: kafka://kafka:9092                           │ │
│  │ ├─ Schema:                                                 │ │
│  │ │  ├─ latitude: DOUBLE                                     │ │
│  │ │  ├─ longitude: DOUBLE                                    │ │
│  │ │  └─ city: STRING                                         │ │
│  │ ├─ Description: Enriched output stream with city info     │ │
│  │ └─ Output Statistics:                                      │ │
│  │    └─ rowCount: 543                                        │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  Run Facets:                                                     │
│  ├─ mode: stream                                                │
│  └─ recordCount: 543                                            │
└──────────────────────────────────────────────────────────────────┘
```

## Error Handling Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        ERROR HANDLING                                │
└─────────────────────────────────────────────────────────────────────┘

Normal Flow:
START ──▶ PROCESSING ──▶ COMPLETE
  │                          │
  │                          └─▶ Success lineage recorded
  │
  └─▶ Inputs declared

Error Flow:
START ──▶ PROCESSING ──✗──▶ FAIL
  │                          │
  │                          ├─▶ Error message captured
  │                          ├─▶ Stack trace recorded
  │                          └─▶ Partial lineage preserved
  │
  └─▶ Inputs declared

┌──────────────────────────────────────────────────────────────────┐
│  OpenLineage FAIL Event                                          │
│  ────────────────────────────────────────────────────────────── │
│  Job: reverse_geocode_batch                                      │
│  Run ID: 770e8400-e29b-41d4-a716-446655440002                   │
│  Event Type: FAIL                                                │
│  Timestamp: 2025-10-05T10:31:00.000Z                            │
│                                                                  │
│  Run Facets:                                                     │
│  └─ errorMessage:                                               │
│     ├─ message: "S3 bucket 'raw' not found"                    │
│     ├─ programmingLanguage: python                             │
│     └─ stackTrace: [...]                                       │
│                                                                  │
│  Inputs: (same as START event)                                  │
│  Outputs: (none - job failed)                                   │
└──────────────────────────────────────────────────────────────────┘
```

## Lineage Graph Visualization

```
┌─────────────────────────────────────────────────────────────────────┐
│                    LINEAGE GRAPH IN MARQUEZ                          │
└─────────────────────────────────────────────────────────────────────┘

Dataset View:
═══════════════════════════════════════════════════════════════════════

                    ┌──────────────────────┐
                    │  s3://raw/           │
                    │  customer_raw.csv    │
                    │  ──────────────────  │
                    │  Schema:             │
                    │  • latitude: DOUBLE  │
                    │  • longitude: DOUBLE │
                    └──────────┬───────────┘
                               │
                               │ consumed by
                               ▼
                    ┌──────────────────────┐
                    │  reverse_geocode_    │
                    │  batch               │
                    │  ──────────────────  │
                    │  Type: BATCH         │
                    │  Status: SUCCESS     │
                    │  Duration: 90s       │
                    └──────────┬───────────┘
                               │
                               │ produced
                               ▼
                    ┌──────────────────────┐
                    │  s3://enriched/      │
                    │  reverse_geocode_    │
                    │  enriched.csv        │
                    │  ──────────────────  │
                    │  Schema:             │
                    │  • latitude: DOUBLE  │
                    │  • longitude: DOUBLE │
                    │  • city: STRING      │
                    │  ──────────────────  │
                    │  Rows: 1,234         │
                    │  Size: 45 KB         │
                    └──────────────────────┘

Reference Data:
═══════════════════════════════════════════════════════════════════════

                    ┌──────────────────────┐
                    │  s3://raw/           │
                    │  reference/          │
                    │  geo_master.csv      │
                    │  ──────────────────  │
                    │  Schema:             │
                    │  • latitude: DOUBLE  │
                    │  • longitude: DOUBLE │
                    │  • city: STRING      │
                    └──────────┬───────────┘
                               │
                               │ used by
                               ▼
                    ┌──────────────────────┐
                    │  reverse_geocode_    │
                    │  batch               │
                    └──────────────────────┘

Multi-DAG Pipeline View:
═══════════════════════════════════════════════════════════════════════

┌────────────┐     ┌────────────┐     ┌────────────┐     ┌────────────┐
│ S3: raw/   │────▶│  email_    │────▶│ S3: stage/ │────▶│  reverse_  │
│ customer_  │     │  validation│     │ validated  │     │  geocode   │
│ raw.csv    │     └────────────┘     └────────────┘     └──────┬─────┘
└────────────┘                                                   │
                                                                 ▼
                                                        ┌────────────────┐
                                                        │ S3: enriched/  │
                                                        │ geo_enriched   │
                                                        └────────┬───────┘
                                                                 │
                                                                 ▼
                                                        ┌────────────────┐
                                                        │  gender_       │
                                                        │  enrichment    │
                                                        └────────┬───────┘
                                                                 │
                                                                 ▼
                                                        ┌────────────────┐
                                                        │ S3: enriched/  │
                                                        │ final_enriched │
                                                        └────────────────┘
```

## Event Sequence Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                    EVENT SEQUENCE TIMELINE                           │
└─────────────────────────────────────────────────────────────────────┘

Time    Airflow         Docker Container        OpenLineage Client      Marquez
────    ───────         ────────────────        ──────────────────      ───────

10:30   Trigger DAG
  │         │
  │         ├──────▶ Start Container
  │         │              │
  │         │              ├──────▶ Initialize Client
  │         │              │              │
  │         │              │              ├──────▶ POST /api/v1/lineage
  │         │              │              │        (START event)
  │         │              │              │              │
  │         │              │              │              └─▶ Store in DB
  │         │              │              │
  │         │              ├──────▶ Download S3 files
  │         │              │
10:31   │   │              ├──────▶ Process data
  │         │              │
  │         │              ├──────▶ Upload results
  │         │              │              │
  │         │              │              ├──────▶ POST /api/v1/lineage
  │         │              │              │        (COMPLETE event)
  │         │              │              │              │
  │         │              │              │              └─▶ Store in DB
  │         │              │              │
  │         │              └──────▶ Exit
  │         │
  │         └──────▶ Task Complete
  │
10:32   DAG Complete

                                                    ┌─────────────────┐
                                                    │ Marquez UI      │
                                                    │ Updates in      │
                                                    │ Real-time       │
                                                    └─────────────────┘
```

## Data Model

```
┌─────────────────────────────────────────────────────────────────────┐
│                    MARQUEZ DATA MODEL                                │
└─────────────────────────────────────────────────────────────────────┘

┌──────────────┐
│  Namespaces  │
├──────────────┤
│ • airflow    │
│ • s3://raw   │
│ • s3://enr.. │
│ • kafka://.. │
└──────┬───────┘
       │
       ├─────────────┐
       │             │
       ▼             ▼
┌──────────┐   ┌──────────┐
│   Jobs   │   │ Datasets │
├──────────┤   ├──────────┤
│ • name   │   │ • name   │
│ • type   │   │ • schema │
│ • latest │   │ • facets │
└────┬─────┘   └────┬─────┘
     │              │
     │              │
     ▼              ▼
┌──────────┐   ┌──────────┐
│   Runs   │   │ Versions │
├──────────┤   ├──────────┤
│ • run_id │   │ • version│
│ • status │   │ • created│
│ • start  │   │ • schema │
│ • end    │   └──────────┘
│ • facets │
└────┬─────┘
     │
     ▼
┌──────────┐
│  Facets  │
├──────────┤
│ • job    │
│ • run    │
│ • dataset│
│ • input  │
│ • output │
└──────────┘
```

This architecture enables comprehensive lineage tracking, providing visibility into data flows, dependencies, and transformations across the entire pipeline.

