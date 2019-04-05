import argparse
import json
import os
from urllib.parse import urlparse

from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import PhoneNumberUuidTable, IOUtils
from google.cloud import storage
from storage.google_drive import drive_client_wrapper

from src import  CombineRawDatasets, TranslateRapidProKeys
from src.lib import PipelineConfiguration

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Runs the post-fetch phase of the ReDSS pipeline",
                                     # Support \n and long lines
                                     formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument("--drive-upload", nargs=4,
                        metavar=("drive-credentials-path", "csv-by-message-drive-path",
                                 "csv-by-individual-drive-path", "production-csv-drive-path"),
                        help="Upload message csv, individual csv, and production csv to Drive. Parameters:\n"
                             "  drive-credentials-path: Path to a G Suite service account JSON file\n"
                             "  csv-by-message-drive-path: 'Path' to a file in the service account's Drive to "
                             "upload the messages CSV to\n"
                             "  csv-by-individual-drive-path: 'Path' to a file in the service account's Drive to "
                             "upload the individuals CSV to\n"
                             "  production-csv-drive-path: 'Path' to a file in the service account's Drive to "
                             "upload the production CSV to"),

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file"),
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")

    parser.add_argument("phone_number_uuid_table_path", metavar="phone-number-uuid-table-path",
                        help="JSON file containing the phone number <-> UUID lookup table for the messages/surveys "
                             "datasets")
    parser.add_argument("s01e01_input_path", metavar="s01e01-input-path",
                        help="Path to the episode 1 raw messages JSON file, containing a list of serialized TracedData "
                             "objects")
    parser.add_argument("s01e02_input_path", metavar="s01e02-input-path",
                        help="Path to the episode 2 raw messages JSON file, containing a list of serialized TracedData "
                             "objects")
    parser.add_argument("s01e03_input_path", metavar="s01e03-input-path",
                        help="Path to the episode 3 raw messages JSON file, containing a list of serialized TracedData "
                             "objects")
    parser.add_argument("s01e04_input_path", metavar="s01e04-input-path",
                        help="Path to the episode 4 raw messages JSON file, containing a list of serialized TracedData "
                             "objects")
    parser.add_argument("s01e05_input_path", metavar="s01e05-input-path",
                        help="Path to the episode 5 raw messages JSON file, containing a list of serialized TracedData "
                             "objects")
    parser.add_argument("s01e06_input_path", metavar="s01e06-input-path",
                        help="Path to the episode 6 raw messages JSON file, containing a list of serialized TracedData "
                             "objects")
    parser.add_argument("s01e07_input_path", metavar="s01e07-input-path",
                        help="Path to the episode 6 raw messages JSON file, containing a list of serialized TracedData "
                             "objects")
    parser.add_argument("s01_demog_input_path", metavar="s01-demog-input-path",
                        help="Path to the raw demographics JSON file for season 1, containing a list of serialized "
                             "TracedData objects")
    parser.add_argument("s01_follow_up_w2_input_path", metavar="s01_follow_up_w2_input_path",
                        help="Path to the raw week 2 follow up survey JSON file for season 1, containing a list of serialized "
                             "TracedData objects")
    parser.add_argument("s01_follow_up_w6_input_path", metavar="s01_follow_up_w6_input_path",
                        help="Path to the raw week 6 follow up survey JSON file for season 1, containing a list of serialized "
                             "TracedData objects")
    parser.add_argument("prev_coded_dir_path", metavar="prev-coded-dir-path",
                        help="Directory containing Coda files generated by a previous run of this pipeline. "
                             "New data will be appended to these files.")
    parser.add_argument("json_output_path", metavar="json-output-path",
                        help="Path to a JSON file to write TracedData for final analysis file to")
    parser.add_argument("icr_output_dir", metavar="icr-output-dir",
                        help="Directory to write CSV files to, each containing 200 messages and message ids for use " 
                             "in inter-code reliability evaluation"),
    parser.add_argument("coded_dir_path", metavar="coded-dir-path",
                        help="Directory to write coded Coda files to")
    parser.add_argument("csv_by_message_output_path", metavar="csv-by-message-output-path",
                        help="Analysis dataset where messages are the unit for analysis (i.e. one message per row)")
    parser.add_argument("csv_by_individual_output_path", metavar="csv-by-individual-output-path",
                        help="Analysis dataset where respondents are the unit for analysis (i.e. one respondent "
                             "per row, with all their messages joined into a single cell)")
    parser.add_argument("production_csv_output_path", metavar="production-csv-output-path",
                        help="Path to a CSV file to write raw message and demographic responses to, for use in "
                             "radio show production"),

    args = parser.parse_args()

    csv_by_message_drive_path = None
    csv_by_individual_drive_path = None
    production_csv_drive_path = None

    drive_upload = args.drive_upload is not None
    if drive_upload:
        csv_by_message_drive_path = args.drive_upload[1]
        csv_by_individual_drive_path = args.drive_upload[2]
        production_csv_drive_path = args.drive_upload[3]

    user = args.user
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path

    phone_number_uuid_table_path = args.phone_number_uuid_table_path
    s01e01_input_path = args.s01e01_input_path
    s01e02_input_path = args.s01e02_input_path
    s01e03_input_path = args.s01e03_input_path
    s01e04_input_path = args.s01e04_input_path
    s01e05_input_path = args.s01e05_input_path
    s01e06_input_path = args.s01e06_input_path
    s01e07_input_path = args.s01e07_input_path
    s01_demog_input_path = args.s01_demog_input_path
    s01_follow_up_w2_input_path = args.s01_follow_up_w2_input_path
    s01_follow_up_w6_input_path = args.s01_follow_up_w6_input_path
    prev_coded_dir_path = args.prev_coded_dir_path

    json_output_path = args.json_output_path
    icr_output_dir = args.icr_output_dir
    coded_dir_path = args.coded_dir_path
    csv_by_message_output_path = args.csv_by_message_output_path
    csv_by_individual_output_path = args.csv_by_individual_output_path
    production_csv_output_path = args.production_csv_output_path

    message_paths = [s01e01_input_path, s01e02_input_path, s01e03_input_path,
                     s01e04_input_path, s01e05_input_path, s01e06_input_path, s01e07_input_path]
    
    follow_up_survey_paths = [s01_follow_up_w2_input_path, s01_follow_up_w6_input_path]

    # Load the pipeline configuration file
    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)

    if pipeline_configuration.drive_upload is not None:
        # Fetch the Rapid Pro Token from the Google Cloud Storage URL
        parsed_rapid_pro_token_file_url = urlparse(pipeline_configuration.drive_upload.drive_credentials_file_url)
        bucket_name = parsed_rapid_pro_token_file_url.netloc
        blob_name = parsed_rapid_pro_token_file_url.path.lstrip("/")

        log.info(f"Downloading Drive service account credentials from file '{blob_name}' in bucket '{bucket_name}'...")
        storage_client = storage.Client.from_service_account_json(google_cloud_credentials_file_path)
        credentials_bucket = storage_client.bucket(bucket_name)
        credentials_blob = credentials_bucket.blob(blob_name)
        credentials_info = json.loads(credentials_blob.download_as_string())
        log.info("Downloaded Drive service account credentials")

        drive_client_wrapper.init_client_from_info(credentials_info)

    # Load phone number <-> UUID table
    log.info("Loading Phone Number <-> UUID Table...")
    with open(phone_number_uuid_table_path, "r") as f:
        phone_number_uuid_table = PhoneNumberUuidTable.load(f)

    # Load messages
    messages_datasets = []
    for i, path in enumerate(message_paths):
        log.info("Loading Episode {}/{}...".format(i + 1, len(message_paths)))
        with open(path, "r") as f:
            messages_datasets.append(TracedDataJsonIO.import_json_to_traced_data_iterable(f))
        log.debug(f"Loaded {len(messages_datasets[-1])} messages")

    # Load demogs
    log.info("Loading Demographics...")
    with open(s01_demog_input_path, "r") as f:
        s01_demographics = TracedDataJsonIO.import_json_to_traced_data_iterable(f)
        log.debug(f"Loaded {len(s01_demographics)} contacts")
    
    # Load Follow up Surveys
    survey_datasets = []
    for i, path in enumerate(follow_up_survey_paths):
        log.info("Loading Follow up {}/{}...".format(i + 1, len(follow_up_survey_paths)))
        with open(path, "r") as f:
            survey_datasets.append(TracedDataJsonIO.import_json_to_traced_data_iterable(f))
        log.debug(f"Loaded {len(survey_datasets[-1])} messages")
        

    # Add survey data to the messages
    log.info("Combining Datasets...")
    data = CombineRawDatasets.combine_raw_datasets(user, messages_datasets, [s01_demographics], survey_datasets)

    log.info("Translating Rapid Pro Keys...")
    data = TranslateRapidProKeys.translate_rapid_pro_keys(user, data, pipeline_configuration, prev_coded_dir_path)

    log.info("Writing TracedData to file...")
    IOUtils.ensure_dirs_exist_for_file(json_output_path)
    with open(json_output_path, "w") as f:
        TracedDataJsonIO.export_traced_data_iterable_to_json(data, f, pretty_print=True)
    
    # Upload to Google Drive, if requested.
    # Note: This should happen as late as possible in order to reduce the risk of the remainder of the pipeline failing
    # after a Drive upload has occurred. Failures could result in inconsistent outputs or outputs with no
    # traced data log.
    if pipeline_configuration.drive_upload is not None:
        log.info("Uploading CSVs to Google Drive...")
        
        #TODO 
        '''
        production_csv_drive_dir = os.path.dirname(pipeline_configuration.drive_upload.production_upload_path)
        production_csv_drive_file_name = os.path.basename(pipeline_configuration.drive_upload.production_upload_path)
        drive_client_wrapper.update_or_create(production_csv_output_path, production_csv_drive_dir,
                                              target_file_name=production_csv_drive_file_name,
                                              target_folder_is_shared_with_me=True)
        
        messages_csv_drive_dir = os.path.dirname(pipeline_configuration.drive_upload.messages_upload_path)
        messages_csv_drive_file_name = os.path.basename(pipeline_configuration.drive_upload.messages_upload_path)
        drive_client_wrapper.update_or_create(csv_by_message_output_path, messages_csv_drive_dir,
                                              target_file_name=messages_csv_drive_file_name,
                                              target_folder_is_shared_with_me=True)

        individuals_csv_drive_dir = os.path.dirname(pipeline_configuration.drive_upload.individuals_upload_path)
        individuals_csv_drive_file_name = os.path.basename(pipeline_configuration.drive_upload.individuals_upload_path)
        drive_client_wrapper.update_or_create(csv_by_individual_output_path, individuals_csv_drive_dir,
                                              target_file_name=individuals_csv_drive_file_name,
                                              target_folder_is_shared_with_me=True)
        '''
        traced_data_drive_dir = os.path.dirname(pipeline_configuration.drive_upload.traced_data_upload_path)
        traced_data_drive_file_name = os.path.basename(pipeline_configuration.drive_upload.traced_data_upload_path)
        drive_client_wrapper.update_or_create(json_output_path, traced_data_drive_dir,
                                              target_file_name=traced_data_drive_file_name,
                                              target_folder_is_shared_with_me=True)
    else:
        log.info("Skipping uploading to Google Drive (because the pipeline configuration json does not contain the key "
                 "'DriveUploadPaths')")

    log.info("Python script complete")