import pytest
from unittest.mock import patch, MagicMock
from reverse_geocode.dags.reverse_geocode_run import run_reverse_geocode, run_reverse_geocode_stream, send_status, CAP_SIZE
import pandas as pd

@patch("reverse_geocode.dags.reverse_geocode_run.s3_utils.download_file")
@patch("reverse_geocode.dags.reverse_geocode_run.s3_utils.upload_file")
@patch("reverse_geocode.dags.reverse_geocode_run.kafka_utils.send_event")
@patch("reverse_geocode.dags.reverse_geocode_run.progress.upload_progress_file")
@patch("pandas.read_csv")
@patch("pandas.DataFrame.to_csv")
def test_reverse_geocode_run(mock_to_csv,mock_read_csv,mock_upload_progress,mock_send_event,mock_upload_file,mock_download_file):

    mock_read_csv.side_effect = [
        pd.DataFrame([{"name": "Alice","latitude":12.9715,"longitude":77.5945}]), 
        pd.DataFrame([{"latitude":12.9715,"longitude":77.5945,"city":"Bangalore"}]) 
    ]

    run_reverse_geocode(input_bucket="raw", input_key="input.csv", out_bucket="enriched", out_key="output.csv")

    mock_download_file.assert_any_call("raw", "input.csv", "/samples/input/reverse_geocode.csv")
    mock_download_file.assert_any_call("raw", "/reference/geo_master.csv", "/reverse_geocode/data/geo_master.csv")
    mock_upload_file.assert_called_once_with("enriched", "output.csv", "/samples/output/reverse_geocode_enriched.csv")
    assert mock_send_event.call_count >= 1
    assert mock_upload_progress.call_count >= 1
    mock_to_csv.assert_called_once()

    


@patch("reverse_geocode.dags.reverse_geocode_run.s3_utils.download_file")
@patch("reverse_geocode.dags.reverse_geocode_run.s3_utils.upload_file")
@patch("reverse_geocode.dags.reverse_geocode_run.kafka_utils.send_event")
@patch("reverse_geocode.dags.reverse_geocode_run.progress.upload_progress_file")
@patch("reverse_geocode.dags.reverse_geocode_run.kafka_utils.consume_records")
@patch("pandas.read_csv")
@patch("pandas.DataFrame.to_csv")

def test_reverse_geocode_stream(mock_to_csv,
mock_read_csv,
mock_consume_records,
mock_upload_progress,
mock_send_event,
mock_upload_file,
mock_download_file):
    mock_read_csv.return_value = pd.DataFrame([{"name": "Alice", "latitude":12.9715,"longitude":77.5945, "city": "Bangalore"}])

    records = [{"name": f"Name{i}", "latitude": 12.9715, "longitude": 77.5945} for i in range(CAP_SIZE + 1)]
    mock_consume_records.return_value = [records]
 
    run_reverse_geocode_stream(topic="reverse-geocode-input", out_topic="reverse-geocode-output")


    mock_download_file.assert_called_once()

    mock_upload_file.assert_called_once()

    assert mock_send_event.call_count == 2 
 
    assert mock_upload_progress.call_count >= 1

    mock_to_csv.assert_called_once()