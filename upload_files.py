import argparse
import json
import os

from core_data_modules.logging import Logger
from storage.google_cloud import google_cloud_utils
from storage.google_drive import drive_client_wrapper

from src.lib import PipelineConfiguration

Logger.set_project_name("IMAQAL")
log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Uploads output files")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file-path",
                        help="Path to the pipeline configuration json file")
    parser.add_argument("run_id", metavar="run-id",
                        help="Identifier of this pipeline run")
    parser.add_argument("production_csv_input_path", metavar="production-csv-input-path",
                        help="Path to a CSV file with raw message and demographic response, for use in "
                             "radio show production"),
    parser.add_argument("messages_csv_input_path", metavar="messages-csv-input-path",
                        help="Path to analysis dataset CSV where messages are the unit for analysis (i.e. one message "
                             "per row)"),
    parser.add_argument("individuals_csv_input_path", metavar="individuals-csv-input-path",
                        help="Path to analysis dataset CSV where respondents are the unit for analysis (i.e. one "
                             "respondent per row, with all their messages joined into a single cell)"),
    parser.add_argument("memory_profile_file_path", metavar="memory-profile-file-path",
                        help="Path to the memory profile log file to upload")
    parser.add_argument("data_archive_file_path", metavar="data-archive-file-path",
                        help="Path to the data archive file to upload")
    parser.add_argument("pipeline_run_mode", help="whether to generate analysis files or not", choices=["all-stages", "auto-code-only"])

    args = parser.parse_args()

    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    run_id = args.run_id
    production_csv_input_path = args.production_csv_input_path
    messages_csv_input_path = args.messages_csv_input_path
    individuals_csv_input_path = args.individuals_csv_input_path
    memory_profile_file_path = args.memory_profile_file_path
    data_archive_file_path = args.data_archive_file_path
    pipeline_run_mode = args.pipeline_run_mode

    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)

    log.info(f"Downloading Google Drive service account credentials...")
    credentials_info = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, pipeline_configuration.drive_upload.drive_credentials_file_url))
    drive_client_wrapper.init_client_from_info(credentials_info)

    log.info("Uploading production file to Google Drive...")
    production_csv_drive_dir = os.path.dirname(pipeline_configuration.drive_upload.production_upload_path)
    production_csv_drive_file_name = os.path.basename(pipeline_configuration.drive_upload.production_upload_path)
    drive_client_wrapper.update_or_create(production_csv_input_path, production_csv_drive_dir,
                                          target_file_name=production_csv_drive_file_name,
                                          target_folder_is_shared_with_me=True)

    if pipeline_run_mode == "all-stages":
        log.info("Uploading Analysis CSVs to Google Drive...")

        messages_csv_drive_dir = os.path.dirname(pipeline_configuration.drive_upload.messages_upload_path)
        messages_csv_drive_file_name = os.path.basename(pipeline_configuration.drive_upload.messages_upload_path)
        drive_client_wrapper.update_or_create(messages_csv_input_path, messages_csv_drive_dir,
                                              target_file_name=messages_csv_drive_file_name,
                                              target_folder_is_shared_with_me=True)

        individuals_csv_drive_dir = os.path.dirname(pipeline_configuration.drive_upload.individuals_upload_path)
        individuals_csv_drive_file_name = os.path.basename(pipeline_configuration.drive_upload.individuals_upload_path)
        drive_client_wrapper.update_or_create(individuals_csv_input_path, individuals_csv_drive_dir,
                                              target_file_name=individuals_csv_drive_file_name,
                                              target_folder_is_shared_with_me=True)

    memory_profile_upload_location = f"{pipeline_configuration.memory_profile_upload_url_prefix}{run_id}.profile"
    log.info(f"Uploading the memory profile from {memory_profile_file_path} to "
             f"{memory_profile_upload_location}...")
    with open(memory_profile_file_path, "rb") as f:
        google_cloud_utils.upload_file_to_blob(
            google_cloud_credentials_file_path, memory_profile_upload_location, f
        )

    data_archive_upload_location = f"{pipeline_configuration.data_archive_upload_url_prefix}{run_id}.tar.gzip"
    log.info(f"Uploading the data archive from {data_archive_file_path} to "
             f"{data_archive_upload_location}...")
    with open(data_archive_file_path, "rb") as f:
        google_cloud_utils.upload_file_to_blob(
            google_cloud_credentials_file_path, data_archive_upload_location, f
        )
