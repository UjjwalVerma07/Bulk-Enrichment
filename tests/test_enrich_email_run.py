import pytest
from unittest.mock import patch, MagicMock
from email_validation.dags.enrich_email_run import validate_emails, run_email_validation_stream, send_status, CAP_SIZE
import pandas as pd

@patch("email_validation.dags.enrich_email_run.subprocess.run")
@patch("email_validation.dags.enrich_email_run.s3_utils.download_file")
@patch("email_validation.dags.enrich_email_run.s3_utils.upload_file")
@patch("email_validation.dags.enrich_email_run.kafka_utils.send_event")
@patch("email_validation.dags.enrich_email_run.progress.upload_progress_file")
@patch("pandas.read_csv")
@patch("pandas.DataFrame.to_csv")
def test_validate_emails(mock_to_csv, mock_read_csv, mock_upload_progress, mock_send_event, mock_upload_file, mock_download_file, mock_subprocess):

    mock_read_csv.return_value = pd.DataFrame([{"name":"Alice","email": "alice@example.com"}])

    validate_emails(input_bucket="raw", input_key="input.csv", out_bucket="enriched", out_key="output.csv")


    mock_download_file.assert_called_once_with("raw", "input.csv", "/samples/input/customer_raw.csv")
    mock_upload_file.assert_called_once_with("enriched", "output.csv", "/samples/output/email_validated.csv")
    mock_subprocess.assert_called_once()  # Verify C++ validator script was called
    assert mock_send_event.call_count >= 1
    assert mock_upload_progress.call_count >= 1
    mock_read_csv.assert_called_once()

@patch("email_validation.dags.enrich_email_run.subprocess.run")
@patch("email_validation.dags.enrich_email_run.s3_utils.download_file")
@patch("email_validation.dags.enrich_email_run.s3_utils.upload_file")
@patch("email_validation.dags.enrich_email_run.kafka_utils.send_event")
@patch("email_validation.dags.enrich_email_run.progress.upload_progress_file")
@patch("email_validation.dags.enrich_email_run.kafka_utils.consume_records")
@patch("pandas.read_csv")
@patch("pandas.DataFrame.to_csv")

def test_email_validation_stream(mock_to_csv,
mock_read_csv,
mock_consume_records,
mock_upload_progress,
mock_send_event,
mock_upload_file,
mock_download_file,
mock_subprocess):
    mock_read_csv.return_value = pd.DataFrame([{"name": "Alice", "email": "alice@example.com", "is_valid": True}])

    records = [{"name": f"Name{i}", "email": f"name{i}@example.com"} for i in range(CAP_SIZE + 1)]
    mock_consume_records.return_value = [records]
 
    run_email_validation_stream(topic="email-validation-input", out_topic="email-validation-output")

    mock_download_file.assert_not_called()

    mock_upload_file.assert_called_once()

    mock_subprocess.assert_called_once()


    assert mock_send_event.call_count == 2 

    assert mock_upload_progress.call_count >= 1

    mock_to_csv.assert_called_once()
