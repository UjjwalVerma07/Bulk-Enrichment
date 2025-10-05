"""
OpenLineage utilities for tracking data lineage with Marquez backend.

This module provides helper functions to emit OpenLineage events for:
- Dataset inputs/outputs (S3, Kafka topics)
- Job runs with custom facets
- Schema information
- Data quality metrics
"""

import os
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from uuid import uuid4
import requests


class OpenLineageClient:
    """Client for sending OpenLineage events to Marquez backend."""
    
    def __init__(self, url: str = None, namespace: str = None):
        """
        Initialize OpenLineage client.
        
        Args:
            url: Marquez API URL (defaults to OPENLINEAGE_URL env var)
            namespace: Namespace for lineage events (defaults to OPENLINEAGE_NAMESPACE env var)
        """
        self.url = url or os.getenv("OPENLINEAGE_URL", "http://host.docker.internal:5000")
        self.namespace = namespace or os.getenv("OPENLINEAGE_NAMESPACE", "airflow")
        self.endpoint = f"{self.url}/api/v1/lineage"
        self.producer = "https://github.com/OpenLineage/OpenLineage/tree/python/client"
        
    def create_dataset(
        self,
        name: str,
        namespace: str = None,
        schema_fields: List[Dict[str, str]] = None,
        datasource_type: str = None,
        datasource_uri: str = None,
        description: str = None
    ) -> Dict[str, Any]:
        """
        Create an OpenLineage dataset definition.
        
        Args:
            name: Dataset name (e.g., 's3://bucket/key' or 'kafka://topic')
            namespace: Dataset namespace (defaults to client namespace)
            schema_fields: List of field definitions [{"name": "col", "type": "STRING"}]
            datasource_type: Type of datasource (e.g., 's3', 'kafka')
            datasource_uri: URI of the datasource
            description: Dataset description
            
        Returns:
            OpenLineage dataset object
        """
        dataset = {
            "namespace": namespace or self.namespace,
            "name": name,
            "facets": {}
        }
        
        # Add schema facet if fields provided
        if schema_fields:
            dataset["facets"]["schema"] = {
                "_producer": self.producer,
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
                "fields": schema_fields
            }
        
        # Add datasource facet
        if datasource_type and datasource_uri:
            dataset["facets"]["dataSource"] = {
                "_producer": self.producer,
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json",
                "name": datasource_type,
                "uri": datasource_uri
            }
        
        # Add documentation facet
        if description:
            dataset["facets"]["documentation"] = {
                "_producer": self.producer,
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DocumentationDatasetFacet.json",
                "description": description
            }
        
        return dataset
    
    def create_s3_dataset(
        self,
        bucket: str,
        key: str,
        schema_fields: List[Dict[str, str]] = None,
        description: str = None
    ) -> Dict[str, Any]:
        """
        Create an S3 dataset definition.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            schema_fields: Schema fields
            description: Dataset description
            
        Returns:
            OpenLineage dataset object
        """
        s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
        return self.create_dataset(
            name=f"{bucket}/{key}",
            namespace=f"s3://{bucket}",
            schema_fields=schema_fields,
            datasource_type="s3",
            datasource_uri=f"{s3_endpoint}/{bucket}/{key}",
            description=description
        )
    
    def create_kafka_dataset(
        self,
        topic: str,
        schema_fields: List[Dict[str, str]] = None,
        description: str = None
    ) -> Dict[str, Any]:
        """
        Create a Kafka topic dataset definition.
        
        Args:
            topic: Kafka topic name
            schema_fields: Schema fields
            description: Dataset description
            
        Returns:
            OpenLineage dataset object
        """
        kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
        return self.create_dataset(
            name=topic,
            namespace=f"kafka://{kafka_broker}",
            schema_fields=schema_fields,
            datasource_type="kafka",
            datasource_uri=f"kafka://{kafka_broker}/{topic}",
            description=description
        )
    
    def emit_event(
        self,
        job_name: str,
        job_namespace: str,
        run_id: str,
        event_type: str,
        inputs: List[Dict[str, Any]] = None,
        outputs: List[Dict[str, Any]] = None,
        job_facets: Dict[str, Any] = None,
        run_facets: Dict[str, Any] = None,
        event_time: str = None
    ) -> bool:
        """
        Emit an OpenLineage event to Marquez.
        
        Args:
            job_name: Name of the job
            job_namespace: Namespace of the job
            run_id: Unique run identifier
            event_type: Type of event (START, RUNNING, COMPLETE, FAIL, ABORT)
            inputs: List of input datasets
            outputs: List of output datasets
            job_facets: Additional job-level facets
            run_facets: Additional run-level facets
            event_time: ISO 8601 timestamp (defaults to now)
            
        Returns:
            True if event was sent successfully, False otherwise
        """
        event = {
            "eventType": event_type,
            "eventTime": event_time or datetime.now(timezone.utc).isoformat(),
            "producer": self.producer,
            "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
            "job": {
                "namespace": job_namespace,
                "name": job_name,
                "facets": job_facets or {}
            },
            "run": {
                "runId": run_id,
                "facets": run_facets or {}
            },
            "inputs": inputs or [],
            "outputs": outputs or []
        }
        
        try:
            response = requests.post(
                self.endpoint,
                json=event,
                headers={"Content-Type": "application/json"},
                timeout=5
            )
            response.raise_for_status()
            print(f"✓ OpenLineage {event_type} event sent successfully for job {job_name}")
            return True
        except requests.exceptions.RequestException as e:
            print(f"⚠ Failed to send OpenLineage event: {str(e)}")
            return False
    
    def emit_start_event(
        self,
        job_name: str,
        run_id: str,
        inputs: List[Dict[str, Any]] = None,
        job_facets: Dict[str, Any] = None,
        run_facets: Dict[str, Any] = None
    ) -> bool:
        """Emit a START event."""
        return self.emit_event(
            job_name=job_name,
            job_namespace=self.namespace,
            run_id=run_id,
            event_type="START",
            inputs=inputs,
            job_facets=job_facets,
            run_facets=run_facets
        )
    
    def emit_complete_event(
        self,
        job_name: str,
        run_id: str,
        inputs: List[Dict[str, Any]] = None,
        outputs: List[Dict[str, Any]] = None,
        job_facets: Dict[str, Any] = None,
        run_facets: Dict[str, Any] = None
    ) -> bool:
        """Emit a COMPLETE event."""
        return self.emit_event(
            job_name=job_name,
            job_namespace=self.namespace,
            run_id=run_id,
            event_type="COMPLETE",
            inputs=inputs,
            outputs=outputs,
            job_facets=job_facets,
            run_facets=run_facets
        )
    
    def emit_fail_event(
        self,
        job_name: str,
        run_id: str,
        error_message: str,
        inputs: List[Dict[str, Any]] = None,
        outputs: List[Dict[str, Any]] = None,
        job_facets: Dict[str, Any] = None,
        run_facets: Dict[str, Any] = None
    ) -> bool:
        """Emit a FAIL event with error information."""
        # Add error message to run facets
        if run_facets is None:
            run_facets = {}
        
        run_facets["errorMessage"] = {
            "_producer": self.producer,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json",
            "message": error_message,
            "programmingLanguage": "python"
        }
        
        return self.emit_event(
            job_name=job_name,
            job_namespace=self.namespace,
            run_id=run_id,
            event_type="FAIL",
            inputs=inputs,
            outputs=outputs,
            job_facets=job_facets,
            run_facets=run_facets
        )


def create_processing_facet(name:str,mode: str, record_count: int = None) -> Dict[str, Any]:
    """
    Create a custom processing facet with job metadata.
    
    Args:
        mode: Processing mode (batch or stream)
        record_count: Number of records processed
        
    Returns:
        Processing facet dictionary
    """
    facet = {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/python/client",
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ProcessingEngineRunFacet.json",
        "version": "1.0",
        "name": name,
        "mode": mode
    }
    
    if record_count is not None:
        facet["recordCount"] = record_count
    
    return facet


def create_output_statistics_facet(row_count: int, size_bytes: int = None) -> Dict[str, Any]:
    """
    Create output statistics facet for dataset.
    
    Args:
        row_count: Number of rows in output
        size_bytes: Size of output in bytes
        
    Returns:
        Output statistics facet
    """
    facet = {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/python/client",
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/OutputStatisticsOutputDatasetFacet.json",
        "rowCount": row_count
    }
    
    if size_bytes is not None:
        facet["size"] = size_bytes
    
    return facet


def add_output_statistics_to_dataset(dataset: Dict[str, Any], row_count: int, size_bytes: int = None) -> Dict[str, Any]:
    """
    Add output statistics to a dataset definition.
    
    Args:
        dataset: Dataset dictionary
        row_count: Number of rows
        size_bytes: Size in bytes
        
    Returns:
        Updated dataset with output statistics
    """
    if "outputFacets" not in dataset:
        dataset["outputFacets"] = {}
    
    dataset["outputFacets"]["outputStatistics"] = create_output_statistics_facet(row_count, size_bytes)
    return dataset

