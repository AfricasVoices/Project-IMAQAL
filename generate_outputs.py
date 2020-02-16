import argparse
import json
import random

from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import IOUtils
from id_infrastructure.firestore_uuid_table import FirestoreUuidTable
from storage.google_cloud import google_cloud_utils
from storage.google_drive import drive_client_wrapper

from src import LoadData, TranslateRapidProKeys, \
    AutoCode, ProductionFile, ApplyManualCodes, AnalysisFile, WSCorrection

from src.lib import PipelineConfiguration

Logger.set_project_name("IMAQAL")
log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Runs the post-fetch phase of the ReDSS pipeline",
                                     # Support \n and long lines
                                     formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file"),
    parser.add_argument("pipeline_run_mode", help="whether to generate analysis files or not", choices=["all-stages", "auto-code-only"])

    parser.add_argument("raw_data_dir", metavar="raw-data-dir",
                        help="Path to a directory containing the raw data files exported by fetch_raw_data.py")
    parser.add_argument("prev_coded_dir_path", metavar="prev-coded-dir-path",
                        help="Directory containing Coda files generated by a previous run of this pipeline. "
                             "New data will be appended to these files.")
    parser.add_argument("icr_output_dir", metavar="icr-output-dir",
                        help="Directory to write CSV files to, each containing 200 messages and message ids for use "
                             "in inter-code reliability evaluation"),
    parser.add_argument("coded_dir_path", metavar="coded-dir-path",
                        help="Directory to write coded Coda files to")

    parser.add_argument("production_csv_output_path", metavar="production-csv-output-path",
                        help="Path to a CSV file to write raw message and demographic responses to, for use in "
                             "radio show production"),
    parser.add_argument("auto_coding_json_output_path", metavar="auto-coding-json-output-path",
                        help="Path to a JSON file to write the TracedData associated with auto-coding stage of the pipeline")
    parser.add_argument("csv_by_message_output_path", metavar="csv-by-message-output-path",
                        help="Analysis dataset where messages are the unit for analysis (i.e. one message per row)")
    parser.add_argument("csv_by_individual_output_path", metavar="csv-by-individual-output-path",
                        help="Analysis dataset where respondents are the unit for analysis (i.e. one respondent "
                             "per row, with all their messages joined into a single cell)")
    parser.add_argument("messages_json_output_path", metavar="messages-json-output-path",
                        help="Path to a JSONL file to write the TracedData associated with the messages analysis file")
    parser.add_argument("individuals_json_output_path", metavar="individuals-json-output-path",
                        help="Path to a JSON file to write the TracedData associated with the individuals analysis file")
    parser.add_argument("messages_history_json_output_path", metavar="messages-history-json-output-path",
                        help="Path to a JSON file to flush the messages TracedData history to before writing that "
                             "TracedData to <messages-json-output-path>")
    parser.add_argument("individuals_history_json_output_path", metavar="individuals-history-json-output-path",
                        help="Path to a JSON file to flush the individuals TracedData history to before writing that "
                             "TracedData to <individuals-json-output-path>")

    args = parser.parse_args()

    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    pipeline_run_mode = args.pipeline_run_mode

    raw_data_dir = args.raw_data_dir
    prev_coded_dir_path = args.prev_coded_dir_path

    icr_output_dir = args.icr_output_dir
    coded_dir_path = args.coded_dir_path
    production_csv_output_path = args.production_csv_output_path
    auto_coding_json_output_path = args.auto_coding_json_output_path
    csv_by_message_output_path = args.csv_by_message_output_path
    csv_by_individual_output_path = args.csv_by_individual_output_path
    messages_history_json_output_path = args.messages_history_json_output_path
    individuals_history_json_output_path = args.individuals_history_json_output_path
    messages_json_output_path = args.messages_json_output_path
    individuals_json_output_path = args.individuals_json_output_path

    # Load the pipeline configuration file
    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)

    log.info("Downloading Firestore Uuid Table credentials...")
    firestore_uuid_table_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        pipeline_configuration.phone_number_uuid_table.firebase_credentials_file_url
    ))
    phone_number_uuid_table = FirestoreUuidTable(
        pipeline_configuration.phone_number_uuid_table.table_name,
        firestore_uuid_table_credentials,
        "avf-phone-uuid-"
    )

    if pipeline_configuration.drive_upload is not None:
        log.info(f"Downloading Google Drive service account credentials...")
        credentials_info = json.loads(google_cloud_utils.download_blob_to_string(
            google_cloud_credentials_file_path, pipeline_configuration.drive_upload.drive_credentials_file_url))
        drive_client_wrapper.init_client_from_info(credentials_info)

    log.info("Loading the raw data...")
    data = LoadData.load_raw_data(user, raw_data_dir, pipeline_configuration)

    # Infer which RQA coding plans to use from the pipeline name.
    if pipeline_configuration.pipeline_name == "q4_pipeline":
        log.info("Running Q4 pipeline")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.Q4_RQA_CODING_PLANS
    elif pipeline_configuration.pipeline_name == "q5_pipeline":
        log.info("Running Q5 pipeline")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.Q5_RQA_CODING_PLANS
    elif pipeline_configuration.pipeline_name == "q6_pipeline":
        log.info("Running Q6 pipeline")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.Q6_RQA_CODING_PLANS
    elif pipeline_configuration.pipeline_name == "q7_pipeline":
        log.info("Running Q7 pipeline")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.Q7_RQA_CODING_PLANS
    else:
        assert pipeline_configuration.pipeline_name == "full_pipeline", "PipelineName must be either 'a quartely pipeline name' or 'full pipeline'"
        log.info("Running full Pipeline")
        PipelineConfiguration.RQA_CODING_PLANS = PipelineConfiguration.FULL_PIPELINE_RQA_CODING_PLANS

    log.info("Translating Rapid Pro Keys...")
    data = TranslateRapidProKeys.translate_rapid_pro_keys(user, data, pipeline_configuration, prev_coded_dir_path)

    if pipeline_configuration.move_ws_messages:
        log.info("Moving WS messages...")
        data = WSCorrection.move_wrong_scheme_messages(user, data, prev_coded_dir_path)
    else:
        log.info("Not moving WS messages (because the 'MoveWSMessages' key in the pipeline configuration "
                 "json was set to 'false')")

    log.info("Auto Coding...")
    data = AutoCode.auto_code(user, data, phone_number_uuid_table, icr_output_dir, coded_dir_path)

    log.info("Exporting production CSV...")
    data = ProductionFile.generate(data, production_csv_output_path)

    if pipeline_run_mode == "all-stages":
        log.info("Running post labelling pipeline stages...")
        log.info("Applying Manual Codes from Coda...")
        data = ApplyManualCodes.apply_manual_codes(user, data, prev_coded_dir_path)

        log.info("Generating Analysis CSVs...")
        messages_data, individuals_data = AnalysisFile.generate(user, data, csv_by_message_output_path,
                                                                csv_by_individual_output_path)

        #log.info("Flushing messages TracedData history...")
        #IOUtils.ensure_dirs_exist_for_file(messages_history_json_output_path)
        #TracedDataJsonIO.flush_history_from_traced_data_iterable(user, data, messages_history_json_output_path)

        log.info("Writing latest messages TracedData to file...")
        IOUtils.ensure_dirs_exist_for_file(messages_json_output_path)
        with open(messages_json_output_path, "w") as f:
            TracedDataJsonIO.export_traced_data_iterable_to_jsonl(messages_data, f)

        #log.info("Flushing individuals TracedData history...")
        #IOUtils.ensure_dirs_exist_for_file(individuals_history_json_output_path)
        #TracedDataJsonIO.flush_history_from_traced_data_iterable(user, data, individuals_history_json_output_path)

        log.info("Writing individuals TracedData to file...")
        IOUtils.ensure_dirs_exist_for_file(individuals_json_output_path)
        with open(individuals_json_output_path, "w") as f:
            TracedDataJsonIO.export_traced_data_iterable_to_jsonl(individuals_data, f)
    else:
        assert pipeline_run_mode == "auto-code-only", "generate analysis files must be either auto-code-only or all-stages"
        log.info("Writing Auto-Coding TracedData to file...")
        IOUtils.ensure_dirs_exist_for_file(auto_coding_json_output_path)
        with open(auto_coding_json_output_path, "w") as f:
            TracedDataJsonIO.export_traced_data_iterable_to_jsonl(data, f)

    log.info("Python script complete")
