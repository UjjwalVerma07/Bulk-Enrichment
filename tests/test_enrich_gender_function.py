import pytest
from unittest.mock import patch, MagicMock
from gender_enrichment.dags.enrich_gender_run import enrich_gender, real_enrich_gender, send_status, CAP_SIZE
import pandas as pd

@patch("gender_enrichment.dags.enrich_gender_run.s3_utils.download_file")
@patch("gender_enrichment.dags.enrich_gender_run.s3_utils.upload_file")
@patch("gender_enrichment.dags.enrich_gender_run.kafka_utils.send_event")
@patch("gender_enrichment.dags.enrich_gender_run.progress.upload_progress_file")
@patch("pandas.read_csv")
@patch("pandas.DataFrame.to_csv")
def test_enrich_gender(mock_to_csv, mock_read_csv, mock_upload_progress, mock_send_event, mock_upload_file, mock_download_file):
   
    mock_read_csv.side_effect = [
        pd.DataFrame([{"name": "Alice"}]),  
        pd.DataFrame([{"name": "Alice", "gender": "F"}])  
    ]


    enrich_gender(input_bucket="raw", input_key="input.csv", out_bucket="enriched", out_key="output.csv")


    mock_download_file.assert_any_call("raw", "input.csv", "/samples/input/gender_enrichment.csv")
    mock_download_file.assert_any_call("raw", "/reference/gender_master.csv", "/gender_enrichment/data/gender_master.csv")
    mock_upload_file.assert_called_once_with("enriched", "output.csv", "/samples/output/final_enriched.csv")
    assert mock_send_event.call_count >= 1
    assert mock_upload_progress.call_count >= 1
    mock_to_csv.assert_called_once()



@patch("gender_enrichment.dags.enrich_gender_run.s3_utils.download_file")
@patch("gender_enrichment.dags.enrich_gender_run.s3_utils.upload_file")
@patch("gender_enrichment.dags.enrich_gender_run.kafka_utils.send_event")
@patch("gender_enrichment.dags.enrich_gender_run.progress.upload_progress_file")
@patch("gender_enrichment.dags.enrich_gender_run.kafka_utils.consume_records")
@patch("pandas.read_csv")
@patch("pandas.DataFrame.to_csv")

def test_enrich_gender_stream(mock_to_csv,
mock_read_csv,
mock_consume_records,
mock_upload_progress,
mock_send_event,
mock_upload_file,
mock_download_file):
    mock_read_csv.return_value = pd.DataFrame([{"name": "Alice", "gender": "M"}])

    records = [{"name": f"Name{i}"} for i in range(CAP_SIZE + 1)]
    mock_consume_records.return_value = [records]
 
    real_enrich_gender(topic="gender-input", out_topic="gender-output")


    mock_download_file.assert_called_once()

    mock_upload_file.assert_called_once()

    assert mock_send_event.call_count == 2 
 
    assert mock_upload_progress.call_count >= 1

    mock_to_csv.assert_called_once()
