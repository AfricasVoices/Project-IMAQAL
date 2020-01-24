import argparse
import csv
import json

from core_data_modules.logging import Logger
from id_infrastructure.firestore_uuid_table import FirestoreUuidTable
from storage.google_cloud import google_cloud_utils

from src.lib import PipelineConfiguration

Logger.set_project_name("IMAQAL")
log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates a list of phone numbers of consented participants"
                                                 "who participated in the last five episodes, to be used in the weekly "
                                                 "sms advert")

    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to Imaqal full pipeline configuration json file")
    parser.add_argument("demog_map_json_input_dir", metavar="demog-map-json-input-dir",
                        help="Path to a directory to read per episode demog map .json files. "
                             "The files should be in the format <rqa_raw_field>_demog_map.json")
    parser.add_argument("advert_rqa_raw_field", metavar="current-rqa-raw-field",
                        help="The raw_field of the radio show we are advertising for i.e radio show currently airing."
                             "The string should be in the format <rqa_raw_field>")
    parser.add_argument("csv_output_file_path", metavar="csv-output-file-path",
                        help="Path to a CSV file to write the  advert phone numbers to. "
                             "Exported file is in a format suitable for direct upload to Rapid Pro")

    args = parser.parse_args()

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    demog_map_json_input_dir = args.demog_map_json_input_dir
    advert_rqa_raw_field = args.advert_rqa_raw_field
    csv_output_file_path = args.csv_output_file_path

    # Read the settings from the configuration file
    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)

    log.info("Downloading Firestore UUID Table credentials...")
    firestore_uuid_table_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        pipeline_configuration.phone_number_uuid_table.firebase_credentials_file_url
    ))

    phone_number_uuid_table = FirestoreUuidTable(
        pipeline_configuration.phone_number_uuid_table.table_name,
        firestore_uuid_table_credentials,
        "avf-phone-uuid-"
    )
    log.info("Initialised the Firestore UUID table")

    # create a list of previous episodes
    previous_episodes = []
    for plan in PipelineConfiguration.FULL_PIPELINE_RQA_CODING_PLANS:
        if plan.raw_field == advert_rqa_raw_field:
            break
        previous_episodes.append(plan.raw_field)

    # Fetch for avf-phone-uuids of the last five episode participants
    advert_uuids = set()
    for rqa_raw_field in previous_episodes[-5:]:
        with open(f'{demog_map_json_input_dir}/{rqa_raw_field}_demog_map.json') as f:
            data = json.load(f)
            for uid in data:
                advert_uuids.add(uid)

    # Convert the uuids to phone numbers
    log.info(f'Converting {len(advert_uuids)} uuids to phone numbers...')
    uuids_to_phone_numbers = phone_number_uuid_table.uuid_to_data_batch(list(advert_uuids))
    recent_episodes_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in advert_uuids]

    # Filter non Hormuud(+25261) and Golis(+25290) phone numbers
    log.info(f'Filtering out non Hormuud(+25261) and Golis(+25290) phone numbers...')
    advert_phone_numbers = set()
    other_mno_count = 0
    for phone_number in recent_episodes_phone_numbers:
        if phone_number.startswith('+25261') or phone_number.startswith('+25290'):
            advert_phone_numbers.add(phone_number)
        else:
            other_mno_count +=1
    log.info(f'Filtered {other_mno_count} phone numbers, returning {len(advert_phone_numbers)} phone numbers...')

    # Export contacts CSV
    log.warning(f"Exporting {len(advert_phone_numbers)} phone numbers to {csv_output_file_path}...")
    with open(f'{csv_output_file_path}', "w") as f:
        writer = csv.DictWriter(f, fieldnames=["URN:Tel", "Name"], lineterminator="\n")
        writer.writeheader()

        for number in advert_phone_numbers:
            writer.writerow({
                "URN:Tel": number
            })
        log.info(f"Wrote {len(advert_phone_numbers)} contacts to {csv_output_file_path}")
