import argparse
import json
import os
from urllib.parse import urlparse

from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import PhoneNumberUuidTable, IOUtils
from google.cloud import storage
from rapid_pro_tools.rapid_pro_client import RapidProClient

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetches all the raw data for this project from Rapid Pro. "
                                                 "This script must be run from its parent directory.")

    parser.add_argument("user", help="Identifier of the user launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file"),
    parser.add_argument("phone_number_uuid_table_path", metavar="phone-number-uuid-table-path",
                        help="Path to a ")
    parser.add_argument("raw_data_dir", metavar="raw-data-dir",
                        help="Path to a directory to save the raw data to")

    args = parser.parse_args()

    user = args.user
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    phone_number_uuid_table_path = args.phone_number_uuid_table_path
    raw_data_dir = args.raw_data_dir

    SHOWS = [
        #"imaqal_s01e01_activation",
        #"imaqal_s01e02_activation",
        #"imaqal_s01e03_activation",
        #"imaqal_s01e04_activation",
        #"imaqal_s01e05_activation",
        #"imaqal_s01e06_activation",
        #"imaqal_s01e07_activation",
        "mag_s01e03_activation",
        "mag_s01e04_activation",
        "mag_s01e05_activation"

    ]

    SURVEYS = [
        "imaqal_demog",
        "imaqal_s01_follow_up_w2",
    ]

    # Read the settings from the configuration file
    with open(pipeline_configuration_file_path) as f:
        pipeline_config = json.load(f)

        rapid_pro_domain = pipeline_config["RapidProDomain"]
        rapid_pro_token_file_url = pipeline_config["RapidProTokenFileURL"]
        rapid_pro_test_contact_uuids = pipeline_config["RapidProTestContactUUIDs"]

    # Fetch the Rapid Pro Token from the Google Cloud Storage URL
    parsed_rapid_pro_token_file_url = urlparse(rapid_pro_token_file_url)
    assert parsed_rapid_pro_token_file_url.scheme == "gs", "RapidProTokenFileURL needs to be a gs URL " \
                                                           "(i.e. of the form gs://bucket-name/file)"
    bucket_name = parsed_rapid_pro_token_file_url.netloc
    blob_name = parsed_rapid_pro_token_file_url.path.lstrip("/")

    print(f"Downloading Rapid Pro token from file '{blob_name}' in bucket '{bucket_name}'...")
    storage_client = storage.Client.from_service_account_json(google_cloud_credentials_file_path)
    credentials_bucket = storage_client.bucket(bucket_name)
    credentials_blob = credentials_bucket.blob(blob_name)
    rapid_pro_token = credentials_blob.download_as_string().strip().decode("utf-8")
    print("Downloaded Rapid Pro token.")

    with open(phone_number_uuid_table_path) as f:
        phone_number_uuid_table = PhoneNumberUuidTable.load(f)

    rapid_pro = RapidProClient(rapid_pro_domain, rapid_pro_token)
    raw_contacts = rapid_pro.get_raw_contacts()

    # Download all the runs for each of the radio shows
    for show in SHOWS:
        output_file_path = f"{raw_data_dir}/{show}.json"
        print(f"Exporting show '{show}' to '{output_file_path}'...")

        flow_id = rapid_pro.get_flow_id(show)
        raw_runs = rapid_pro.get_raw_runs_for_flow_id(flow_id)
        raw_contacts = rapid_pro.update_raw_contacts_with_latest_modified(raw_contacts)
        traced_runs = rapid_pro.convert_runs_to_traced_data(
            user, raw_runs, raw_contacts, phone_number_uuid_table, rapid_pro_test_contact_uuids)

        with open(phone_number_uuid_table_path, "w") as f:
            phone_number_uuid_table.dump(f)

        IOUtils.ensure_dirs_exist_for_file(output_file_path)
        with open(output_file_path, "w") as f:
            TracedDataJsonIO.export_traced_data_iterable_to_json(traced_runs, f, pretty_print=True)

    # Download all the runs for each of the surveys
    for survey in SURVEYS:
        output_file_path = f"{raw_data_dir}/{survey}.json"
        print(f"Exporting survey '{survey}' to '{output_file_path}'...")

        flow_id = rapid_pro.get_flow_id(survey)
        raw_runs = rapid_pro.get_raw_runs_for_flow_id(flow_id)
        raw_contacts = rapid_pro.update_raw_contacts_with_latest_modified(raw_contacts)
        traced_runs = rapid_pro.convert_runs_to_traced_data(
            user, raw_runs, raw_contacts, phone_number_uuid_table, rapid_pro_test_contact_uuids)
        traced_runs = rapid_pro.coalesce_traced_runs_by_key(user, traced_runs, "avf_phone_id")

        with open(phone_number_uuid_table_path, "w") as f:
            phone_number_uuid_table.dump(f)

        IOUtils.ensure_dirs_exist_for_file(output_file_path)
        with open(output_file_path, "w") as f:
            TracedDataJsonIO.export_traced_data_iterable_to_json(traced_runs, f, pretty_print=True)
